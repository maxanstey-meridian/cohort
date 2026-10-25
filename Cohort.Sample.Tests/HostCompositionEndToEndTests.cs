using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class HostCompositionEndToEndTests(PostgresFixture fixture)
{
    [Fact]
    public async Task StartAsync_Rejects_Legacy_Cohort_Schema_When_Host_Migrations_Are_Not_Applied()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await LegacyCohortSchema.BootstrapPreRowDispatchAsync(database.ConnectionString);
        using var host = BuildHost<ValidRetentionDbContext>(
            options => options.UseNpgsql(database.ConnectionString),
            new SingleCategoryRepository(
                "valid",
                new RetentionRule(
                    new RetentionPeriod(Days: 30),
                    Strategy.Purge,
                    AuditRowDetail: AuditRowDetail.PerRow
                )
            )
        );

        var act = async () => await host.StartAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .ContainSingle(error =>
                error.Contains("\"public\".\"sweep_run\"", StringComparison.Ordinal)
                && error.Contains("Status", StringComparison.Ordinal)
                && error.Contains("\"public\".\"sweep_run_row_detail\"", StringComparison.Ordinal)
                && error.Contains("RetentionEntityId", StringComparison.Ordinal)
                && error.Contains("sweep_row_handler_status", StringComparison.Ordinal)
                && error.Contains("pending EF Core migrations", StringComparison.Ordinal)
            );
    }

    [Fact]
    public async Task Sample_Migrates_A_Fresh_Database_Before_Host_Schema_Validation()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            [$"{SampleOptions.SectionName}:{nameof(SampleOptions.ConnectionString)}"] =
                database.ConnectionString,
        });
        builder.Services.AddSampleRetentionServices();
        using var host = builder.Build();

        await using (var scope = host.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<SampleDbContext>();
            await db.Database.MigrateAsync();
        }

        await host.StartAsync();
        await host.StopAsync();
    }

    [Fact]
    public void AddCohort_Execution_Settings_Keep_The_Last_Valid_Snapshot_After_Invalid_Reload()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    [$"{CohortOptions.SectionName}:RowHandlerDispatch:BatchSize"] = "20",
                }
            )
            .Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(configuration);
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<ValidRetentionDbContext>();
        using var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredService<CohortOptionsSnapshot>();

        configuration[$"{CohortOptions.SectionName}:RowHandlerDispatch:BatchSize"] = "10001";
        var reload = () => configuration.Reload();

        reload.Should().Throw<OptionsValidationException>();
        settings.Current.RowHandlerDispatch.BatchSize.Should().Be(20);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Caller_Erasure_Observers_Run_Once_After_Commit_However_The_Host_Registers_Its_Context(bool pooled)
    {
        // The host registers its context after AddCohort, with its own transaction
        // interceptor; Cohort's commit interceptor is added alongside it, once.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var hostInterceptor = new CommitCountingInterceptor();
        var observer = new EventNameObserver();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddSingleton<IRetentionRuleProvider>(
            new SingleCategoryRepository("caller-erasure", new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge))
        );
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddSingleton<IRetentionAuditObserver>(observer);
        services.AddCohort<CallerErasureDbContext>();
        if (pooled)
        {
            services.AddDbContextPool<CallerErasureDbContext>(options =>
                options.UseNpgsql(database.ConnectionString).AddInterceptors(hostInterceptor)
            );
        }
        else
        {
            services.AddDbContext<CallerErasureDbContext>(options =>
                options.UseNpgsql(database.ConnectionString).AddInterceptors(hostInterceptor)
            );
        }

        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var subject = Guid.NewGuid();
        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<CallerErasureDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Records.Add(new CallerErasureRecord { Id = Guid.NewGuid(), TenantId = tenant.Id, SubjectId = subject, CreatedAt = DateTimeOffset.UtcNow.AddDays(-1) });
            await db.SaveChangesAsync();
        }

        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<CallerErasureDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                RetentionErasureRequest.Tenanted(tenant, new ErasureScope("user", subject), transaction: ErasureTransaction.Caller)
            );
            observer.Names.Should().BeEmpty();
            await transaction.CommitAsync();
        }

        hostInterceptor.Commits.Should().BeGreaterThan(0);
        observer.Names.Should().ContainSingle(name => name == "Started");
        observer.Names.Should().ContainSingle(name => name == "Completed");
        observer.Names.Last().Should().Be("Completed");
    }

    [Fact]
    public async Task The_Hosts_Execution_Scope_Wraps_Cohorts_Work_So_Its_Async_Flow_Reaches_It()
    {
        // A host sets its tenant (or a Sluice flow) in an AsyncLocal around the work; host code
        // Cohort calls during that work, here the rule provider, must see it.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var rules = new FlowRecordingRuleProvider(
            new SingleCategoryRepository("caller-erasure", new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge))
        );
        await using var provider = BuildFlowProvider(database.ConnectionString, rules, new FlowingExecutionScope());
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var subject = Guid.NewGuid();
        await SeedCallerErasureRecordAsync(provider, tenant.Id, subject);

        await provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(RetentionSweepRequest.Tenanted(tenant));
        var seenBySweep = rules.Seen.Count;
        await using (var scope = provider.CreateAsyncScope())
        {
            await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                RetentionErasureRequest.Tenanted(tenant, new ErasureScope("user", subject))
            );
        }

        seenBySweep.Should().BePositive();
        rules.Seen.Count.Should().BeGreaterThan(seenBySweep);
        // Readiness's nested System(Readiness) work runs before the rule is resolved; the hook
        // never restores, so the tenant seen afterwards is Cohort restoring the outer flow.
        rules.Seen.Should().OnlyContain(context => context == new RetentionExecutionContext.Tenant(tenant.Id));
    }

    [Theory]
    [InlineData(HookFault.Skips, "*returned without running*")]
    [InlineData(HookFault.RunsTwice, "*more than once*")]
    [InlineData(HookFault.DoesNotAwait, "*returned before*finished*")]
    [InlineData(HookFault.SwallowsFailure, "*rule store down*")]
    public async Task Cohort_Refuses_An_Execution_Scope_That_Does_Not_Await_Its_Work_Exactly_Once(
        HookFault fault,
        string message
    )
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var rules = new FlowRecordingRuleProvider(
            new SingleCategoryRepository("caller-erasure", new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge)),
            failWith: fault == HookFault.SwallowsFailure ? "rule store down" : null
        );
        await using var provider = BuildFlowProvider(database.ConnectionString, rules, new FaultyExecutionScope(fault));
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        await SeedCallerErasureRecordAsync(provider, tenant.Id, Guid.NewGuid());

        var sweep = () => provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(RetentionSweepRequest.Tenanted(tenant));

        await sweep.Should().ThrowAsync<Exception>().WithMessage(message);
        await using var scope = provider.CreateAsyncScope();
        var runs = await scope.ServiceProvider.GetRequiredService<CallerErasureDbContext>().Database
            .SqlQueryRaw<int>("SELECT count(*)::int AS \"Value\" FROM \"sweep_run\"")
            .SingleAsync();
        runs.Should().Be(fault is HookFault.Skips or HookFault.SwallowsFailure ? 0 : 1);
    }

    [Fact]
    public async Task Caller_Erasure_Refuses_A_Context_Built_Outside_The_Container()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        using var host = BuildHost<CallerErasureDbContext>(
            options => options.UseNpgsql(database.ConnectionString),
            new SingleCategoryRepository("caller-erasure", new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge)),
            configureServices: services => services.AddScoped(_ => new CallerErasureDbContext(
                new DbContextOptionsBuilder<CallerErasureDbContext>().UseNpgsql(database.ConnectionString).Options
            ))
        );
        await using var scope = host.Services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<CallerErasureDbContext>();
        await db.Database.EnsureCreatedAsync();
        await using var transaction = await db.Database.BeginTransactionAsync();

        var erase = () => scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
            RetentionErasureRequest.Tenanted(
                new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                new ErasureScope("user", Guid.NewGuid()),
                transaction: ErasureTransaction.Caller
            )
        );

        (await erase.Should().ThrowAsync<InvalidOperationException>())
            .Which.Message.Should().StartWith("ErasureTransaction.Caller needs Cohort's commit interceptor");
    }

    private static IHost BuildHost<TContext>(
        Action<DbContextOptionsBuilder> configureDb,
        ITestRetentionRuleProvider categoryRepository,
        IReadOnlyDictionary<string, string?>? settings = null,
        Action<IServiceCollection>? configureServices = null
    )
        where TContext : DbContext
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(settings ?? new Dictionary<string, string?>());
        builder.Services.AddDbContext<TContext>(configureDb);
        builder.Services.AddSingleton<IRetentionRuleProvider>(categoryRepository);
        configureServices?.Invoke(builder.Services);
        builder.Services.AddSingleton(RetentionExecutionScope.Unisolated);
        builder.Services.AddCohort<TContext>();
        return builder.Build();
    }

    private static ServiceProvider BuildFlowProvider(
        string connectionString,
        IRetentionRuleProvider rules,
        IRetentionExecutionScope executionScope
    )
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddSingleton(rules);
        services.AddSingleton(executionScope);
        services.AddCohort<CallerErasureDbContext>();
        services.AddDbContext<CallerErasureDbContext>(options => options.UseNpgsql(connectionString));
        return services.BuildServiceProvider(validateScopes: true);
    }

    private static async Task SeedCallerErasureRecordAsync(IServiceProvider provider, Guid tenantId, Guid subject)
    {
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<CallerErasureDbContext>();
        await db.Database.EnsureCreatedAsync();
        db.Records.Add(new CallerErasureRecord { Id = Guid.NewGuid(), TenantId = tenantId, SubjectId = subject, CreatedAt = DateTimeOffset.UtcNow.AddDays(-1) });
        await db.SaveChangesAsync();
    }

    /// <summary>
    /// A host that keeps whose work this is in an AsyncLocal set before the work, and leaves
    /// restoring it to Cohort, as the docs promise it may.
    /// </summary>
    private sealed class FlowingExecutionScope : IRetentionExecutionScope
    {
        public static readonly AsyncLocal<RetentionExecutionContext?> Current = new();

        public Task RunAsync(RetentionExecutionContext context, Func<Task> work, CancellationToken ct)
        {
            Current.Value = context;
            return work();
        }
    }

    public enum HookFault
    {
        Skips,
        RunsTwice,
        DoesNotAwait,
        SwallowsFailure,
    }

    /// <summary>A hook that gets tenant work wrong in one way and runs system work properly.</summary>
    private sealed class FaultyExecutionScope(HookFault fault) : IRetentionExecutionScope
    {
        public async Task RunAsync(RetentionExecutionContext context, Func<Task> work, CancellationToken ct)
        {
            if (context is not RetentionExecutionContext.Tenant)
            {
                await work();
                return;
            }

            switch (fault)
            {
                case HookFault.Skips:
                    return;
                case HookFault.RunsTwice:
                    await work();
                    try
                    {
                        await work();
                    }
                    catch (InvalidOperationException)
                    {
                        // Swallows Cohort's refusal of the second run.
                    }

                    return;
                case HookFault.DoesNotAwait:
                    _ = work();
                    return;
                case HookFault.SwallowsFailure:
                    try
                    {
                        await work();
                    }
                    catch (Exception)
                    {
                        // Logs and carries on, as a careless hook might.
                    }

                    return;
            }
        }
    }

    private sealed class FlowRecordingRuleProvider(SingleCategoryRepository inner, string? failWith = null)
        : ITestRetentionRuleProvider
    {
        private readonly System.Collections.Concurrent.ConcurrentQueue<RetentionExecutionContext?> seen = new();

        public IReadOnlyList<RetentionExecutionContext?> Seen => [.. seen];

        public Task<ITestRetentionRule?> GetAsync(string requestedCategory, CancellationToken ct)
        {
            seen.Enqueue(FlowingExecutionScope.Current.Value);
            return failWith is null ? inner.GetAsync(requestedCategory, ct) : throw new InvalidOperationException(failWith);
        }
    }

    private sealed class SingleCategoryRepository(string category, RetentionRule rule)
        : ITestRetentionRuleProvider
    {
        public Task<ITestRetentionRule?> GetAsync(
            string requestedCategory,
            CancellationToken ct
        ) =>
            Task.FromResult<ITestRetentionRule?>(
                requestedCategory == category ? new StaticTestRetentionRule(rule) : null
            );
    }

    private sealed class CommitCountingInterceptor : Microsoft.EntityFrameworkCore.Diagnostics.DbTransactionInterceptor
    {
        private int commits;

        public int Commits => Volatile.Read(ref commits);

        public override Task TransactionCommittedAsync(
            System.Data.Common.DbTransaction transaction,
            Microsoft.EntityFrameworkCore.Diagnostics.TransactionEndEventData eventData,
            CancellationToken cancellationToken = default
        )
        {
            Interlocked.Increment(ref commits);
            return Task.CompletedTask;
        }
    }

    private sealed class EventNameObserver : IRetentionAuditObserver
    {
        private readonly System.Collections.Concurrent.ConcurrentQueue<string> names = new();

        public IReadOnlyList<string> Names => [.. names];

        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            names.Enqueue(evt.GetType().Name);
            return Task.CompletedTask;
        }
    }

    public sealed class CallerErasureDbContext(DbContextOptions<CallerErasureDbContext> options)
        : DbContext(options)
    {
        public DbSet<CallerErasureRecord> Records => Set<CallerErasureRecord>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CallerErasureRecord>().ToTable("caller_erasure_records").HasKey(record => record.Id);
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("caller-erasure", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-0000000000c3")]
    public sealed class CallerErasureRecord
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }

        [ErasureSubject("user")]
        public Guid? SubjectId { get; set; }

        public DateTimeOffset CreatedAt { get; set; }
    }

    private sealed class ValidRetentionDbContext(DbContextOptions<ValidRetentionDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ValidRetentionRecord>().HasKey(record => record.Id);
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("valid", nameof(ValidRetentionRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000000a")]
    private sealed class ValidRetentionRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }
}
