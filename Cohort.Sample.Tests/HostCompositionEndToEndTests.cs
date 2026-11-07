using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
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
                && error.Contains("\"sweep_run_row_detail\".\"Failure\"", StringComparison.Ordinal)
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
                    [$"{CohortOptions.SectionName}:OccurrenceMaxAttempts"] = "20",
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

        configuration[$"{CohortOptions.SectionName}:OccurrenceMaxAttempts"] = "101";
        var reload = () => configuration.Reload();

        reload.Should().Throw<OptionsValidationException>();
        settings.Current.OccurrenceMaxAttempts.Should().Be(20);
    }

    [Theory]
    [InlineData(false, "connection-string")]
    [InlineData(true, "connection-string")]
    [InlineData(true, "data-source")]
    [InlineData(true, "connection")]
    public async Task Caller_Erasure_Observers_Run_Once_After_Commit_However_The_Host_Registers_Its_Context(
        bool pooled,
        string configuredWith
    )
    {
        // The host registers its context after AddCohort, with its own transaction
        // interceptor; Cohort's commit interceptor is added alongside it, once. A dependents
        // step's context (pooled or not) carries the interceptor too, joins the caller's
        // connection and transaction, never commits, and goes back with the connection it was
        // configured with: its own, or the one connection a host supplied to every pooled instance.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var hostInterceptor = new CommitCountingInterceptor();
        var observer = new EventNameObserver();
        var probe = new StepConnectionProbe();
        await using var npgsqlDataSource = Npgsql.NpgsqlDataSource.Create(database.ConnectionString);
        await using var hostConnection = new Npgsql.NpgsqlConnection(database.ConnectionString);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddSingleton<IRetentionRuleProvider>(
            new SingleCategoryRepository("caller-erasure", new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge))
        );
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddSingleton<IRetentionAuditObserver>(observer);
        services.AddSingleton(probe);
        services.AddCohort<CallerErasureDbContext>();
        services.AddRetentionDependents<CallerErasureRecord, ConnectionProbingStep>(retiresReferencingRows: false);
        if (pooled)
        {
            services.AddDbContextPool<CallerErasureDbContext>(options =>
                (configuredWith switch
                {
                    "data-source" => options.UseNpgsql(npgsqlDataSource),
                    "connection" => options.UseNpgsql(hostConnection),
                    _ => options.UseNpgsql(database.ConnectionString),
                })
                    .AddInterceptors(hostInterceptor)
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

        System.Data.Common.DbConnection callerConnection;
        System.Data.Common.DbTransaction callerTransaction;
        CallerErasureDbContext callerContext;
        await using (var scope = provider.CreateAsyncScope())
        {
            var db = callerContext = scope.ServiceProvider.GetRequiredService<CallerErasureDbContext>();
            callerConnection = db.Database.GetDbConnection();
            await using var transaction = await db.Database.BeginTransactionAsync();
            callerTransaction = transaction.GetDbTransaction();
            var result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                RetentionErasureRequest.Tenanted(tenant, new ErasureScope("user", subject), transaction: ErasureTransaction.Caller)
            );
            result.Counts.Should().ContainSingle().Which.Affected.Should().Be(1);
            observer.Names.Should().BeEmpty();
            await transaction.CommitAsync();
        }

        hostInterceptor.Commits.Should().BeGreaterThan(0);
        observer.Names.Should().ContainSingle(name => name == "Started");
        observer.Names.Should().ContainSingle(name => name == "Completed");
        observer.Names.Last().Should().Be("Completed");
        probe.Connections.Should().ContainSingle().Which.Should().BeSameAs(callerConnection);
        probe.Transactions.Should().ContainSingle().Which.Should().BeSameAs(callerTransaction);
        probe.Contexts.Should().ContainSingle().Which.Should().NotBeSameAs(callerContext);

        // Two leases at once, so a pool hands back both the caller's context and the step's.
        // The caller's keeps its own connection; the step's must not keep the caller's, unless
        // the host gave every instance that one connection.
        await using var first = provider.CreateAsyncScope();
        await using var second = provider.CreateAsyncScope();
        CallerErasureDbContext[] later =
        [
            first.ServiceProvider.GetRequiredService<CallerErasureDbContext>(),
            second.ServiceProvider.GetRequiredService<CallerErasureDbContext>(),
        ];
        var stepContext = probe.Contexts.Single();
        later.Contains(stepContext).Should().Be(pooled);
        foreach (var context in later)
        {
            if (configuredWith == "connection")
            {
                context.Database.GetDbConnection().Should().BeSameAs(hostConnection);
            }
            else if (!ReferenceEquals(context, callerContext))
            {
                context.Database.GetDbConnection().Should().NotBeSameAs(callerConnection);
            }

            (await context.Records.CountAsync()).Should().Be(0);
        }
    }

    [Fact]
    public async Task A_caller_erasure_with_a_dependents_step_reads_tenantless_holds_on_the_one_connection_the_host_supplies()
    {
        // Same host as the theory's (pooled: true, "connection") row: one connection, supplied to
        // every pooled context, so a System(Readiness) scope's context sits on the connection
        // that carries the caller's transaction. Three things differ. The erased entity has a
        // dependents step. The model has a tenantless retained entity, so before the step runs
        // the hold guard reads tenantless holds through its own System(Readiness) scope. Nothing
        // validated the model before: the model pass first runs inside the caller's transaction.
        // The recording scope hook shows the path was taken: the erasure opens exactly two
        // System(Readiness) scopes, the model pass's stored-identities read and the guard's
        // tenantless-holds read, and both run on the connection that carries the caller's
        // transaction, so neither may begin a transaction of its own.
        // Make-fail: RetentionHoldGuard.ReadsTenantlessHolds returns false (the guard's scope
        // never opens, so one Readiness scope is logged, not two).
        // Make-fail: RetentionHoldGuard.ReadSystemTenantlessHoldsAsync begins a transaction on its
        // scope's context (`systemDb.Database.BeginTransactionAsync`) before the read: the
        // erasure fails (its entity failure is an InvalidOperationException, the connection
        // already carrying the caller's transaction), so the result assertion goes red.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var hostInterceptor = new CommitCountingInterceptor();
        var probe = new DependentsStepProbe();
        var executionScope = new RecordingExecutionScope();
        await using var hostConnection = new Npgsql.NpgsqlConnection(database.ConnectionString);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddSingleton<IRetentionRuleProvider>(
            new SingleCategoryRepository("caller-erasure", new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge))
        );
        services.AddSingleton<IRetentionExecutionScope>(executionScope);
        services.AddSingleton(probe);
        services.AddCohort<SharedConnectionDbContext>();
        services.AddRetentionDependents<SharedConnectionRecord, RecordingDependentsStep>();
        services.AddDbContextPool<SharedConnectionDbContext>(options =>
            options.UseNpgsql(hostConnection).AddInterceptors(hostInterceptor)
        );

        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var subject = Guid.NewGuid();
        var recordId = Guid.NewGuid();
        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<SharedConnectionDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Records.Add(new SharedConnectionRecord { Id = recordId, TenantId = tenant.Id, SubjectId = subject, CreatedAt = DateTimeOffset.UtcNow.AddDays(-1) });
            await db.SaveChangesAsync();
        }

        var commitsBefore = hostInterceptor.Commits;
        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<SharedConnectionDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            var result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                RetentionErasureRequest.Tenanted(tenant, new ErasureScope("user", subject), transaction: ErasureTransaction.Caller)
            );
            result.EntityFailures.Should().BeEmpty();
            result.Counts.Should().ContainSingle(count => count.EntityType == typeof(SharedConnectionRecord)).Which.Affected.Should().Be(1);
            await transaction.CommitAsync();
        }

        hostInterceptor.Commits.Should().Be(commitsBefore + 1);
        probe.Rows.Should().Equal(recordId);
        executionScope.Ran.OfType<RetentionExecutionContext.System>().Should().Equal(
            new RetentionExecutionContext.System(RetentionSystemWork.Readiness),
            new RetentionExecutionContext.System(RetentionSystemWork.Readiness)
        );
        await using var check = provider.CreateAsyncScope();
        (await check.ServiceProvider.GetRequiredService<SharedConnectionDbContext>().Records.CountAsync()).Should().Be(0);
    }

    [Fact]
    public async Task A_hold_check_that_read_the_callers_uncommitted_writes_on_the_one_connection_is_not_kept()
    {
        // A host that gives every context one connection object.
        // Make-fail: RetentionRuntimeReadinessValidator.RunModelPassAsync sets
        // `StoredEntityIdentitiesPassed = true` whatever `ownSession` is (the second call
        // passes).
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var hostConnection = new Npgsql.NpgsqlConnection(database.ConnectionString);

        await AssertUncommittedHoldCheckIsNotKeptAsync(
            database.ConnectionString,
            services => services.AddDbContextPool<SharedConnectionDbContext>(options => options.UseNpgsql(hostConnection))
        );
    }

    [Fact]
    public async Task A_hold_check_that_read_the_callers_uncommitted_writes_through_a_wrapper_over_the_one_session_is_not_kept()
    {
        // A host whose contexts share one session through different connection objects: each DI
        // scope gets its own DbConnection wrapper over the one shared NpgsqlConnection (a
        // per-scope profiler or unit-of-work wrapper). The System(Readiness) scope's wrapper is
        // not the caller's object, but it runs in the caller's backend, so it still sees the
        // caller's uncommitted writes.
        // Make-fail: the stored-identities step returns
        // `!ReferenceEquals(systemDatabase.GetDbConnection(), db.Database.GetDbConnection())`
        // in place of `scopePid != callerPid` (the second call passes).
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var sharedConnection = new Npgsql.NpgsqlConnection(database.ConnectionString);
        var wrappers = new List<PassThroughConnection>();

        await AssertUncommittedHoldCheckIsNotKeptAsync(
            database.ConnectionString,
            services => services.AddDbContext<SharedConnectionDbContext>(options =>
            {
                var wrapper = new PassThroughConnection(sharedConnection, ownsInner: false);
                wrappers.Add(wrapper);
                options.UseNpgsql(wrapper);
            })
        );

        wrappers.Should().HaveCountGreaterThan(2, "each scope, the System(Readiness) one included, has a wrapper of its own");
    }

    // The stored-identities read of the model pass runs in the caller's session, inside the
    // caller's transaction, so it sees the caller's uncommitted writes. Here the caller ends the
    // only hold on an identity no retained entity registers, so the check passes inside the
    // transaction, and the transaction rolls back. The pass must not be remembered: the same call
    // outside a transaction finds the hold active again and is refused.
    private static async Task AssertUncommittedHoldCheckIsNotKeptAsync(
        string connectionString,
        Action<ServiceCollection> addContext
    )
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddSingleton<IRetentionRuleProvider>(
            new SingleCategoryRepository("caller-erasure", new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge))
        );
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<SharedConnectionDbContext>();
        addContext(services);

        await using var provider = services.BuildServiceProvider(validateScopes: true);
        // Set up on a context of its own: EF's database creator needs an NpgsqlConnection, which
        // a wrapper is not.
        await using (var setup = new SharedConnectionDbContext(
            new DbContextOptionsBuilder<SharedConnectionDbContext>().UseNpgsql(connectionString).Options
        ))
        {
            await setup.Database.EnsureCreatedAsync();
            var holdId = Guid.NewGuid();
            var orphanedEntity = Guid.NewGuid();
            await setup.Database.ExecuteSqlAsync(
                $"""
                INSERT INTO "retention_holds"
                    ("HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt")
                VALUES ({holdId}, {orphanedEntity}, 'row', NULL, 'litigation', {DateTimeOffset.UtcNow.AddDays(-1)}, NULL, NULL)
                """
            );
        }

        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<SharedConnectionDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            await db.Database.ExecuteSqlRawAsync(
                """UPDATE "retention_holds" SET "RemovedAt" = pg_catalog.statement_timestamp()"""
            );

            var inside = () => scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>().ListActiveAsync(CancellationToken.None);

            await inside.Should().NotThrowAsync("the only hold is ended inside the transaction");
            await transaction.RollbackAsync();
        }

        await using var outside = provider.CreateAsyncScope();
        var after = () => outside.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>().ListActiveAsync(CancellationToken.None);

        (await after.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                error.Contains("is not registered by any retained entity", StringComparison.Ordinal)
            );
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
    public async Task A_Step_Batch_Runs_When_The_Hook_Opened_Its_Scopes_Context()
    {
        // A hook that applies isolation by opening its scope's context and leaving it open.
        // The step scope's context still joins the run's connection and transaction, which the
        // same hook already isolated for the same tenant.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var probe = new StepConnectionProbe();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddSingleton<IRetentionRuleProvider>(
            new SingleCategoryRepository("caller-erasure", new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge))
        );
        services.AddScoped<IRetentionExecutionScope, ConnectionOpeningExecutionScope>();
        services.AddSingleton(probe);
        services.AddCohort<CallerErasureDbContext>();
        services.AddRetentionDependents<CallerErasureRecord, ConnectionProbingStep>(retiresReferencingRows: false);
        services.AddDbContext<CallerErasureDbContext>(options => options.UseNpgsql(database.ConnectionString));
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var subject = Guid.NewGuid();
        await SeedCallerErasureRecordAsync(provider, tenant.Id, subject);

        await using var scope = provider.CreateAsyncScope();
        var result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
            RetentionErasureRequest.Tenanted(tenant, new ErasureScope("user", subject))
        );

        result.EntityFailures.Should().BeEmpty();
        result.Complete.Should().BeTrue();
        probe.Contexts.Should().ContainSingle();
        probe.Transactions.Should().ContainSingle().Which.Should().NotBeNull();
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

    private sealed class StepConnectionProbe
    {
        public List<System.Data.Common.DbConnection> Connections { get; } = [];

        public List<System.Data.Common.DbTransaction?> Transactions { get; } = [];

        public List<object> Contexts { get; } = [];
    }

    /// <summary>Opens its scope's context's connection before the work, and leaves it open.</summary>
    private sealed class ConnectionOpeningExecutionScope(CallerErasureDbContext db) : IRetentionExecutionScope
    {
        public async Task RunAsync(RetentionExecutionContext context, Func<Task> work, CancellationToken ct)
        {
            await db.Database.OpenConnectionAsync(ct);
            await work();
        }
    }

    /// <summary>
    /// Reads its row through its own context and records which connection and transaction that
    /// context is on.
    /// </summary>
    private sealed class ConnectionProbingStep(CallerErasureDbContext db, StepConnectionProbe probe)
        : IRetentionDependents<CallerErasureRecord>
    {
        public async Task RetireAsync(CallerErasureRecord row, RetentionDependentsContext ctx, CancellationToken ct)
        {
            (await db.Records.AsNoTracking().AnyAsync(record => record.Id == row.Id, ct)).Should().BeTrue();
            probe.Connections.Add(db.Database.GetDbConnection());
            probe.Transactions.Add(db.Database.CurrentTransaction?.GetDbTransaction());
            probe.Contexts.Add(db);
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

    private sealed class DependentsStepProbe
    {
        public List<Guid> Rows { get; } = [];
    }

    private sealed class RecordingDependentsStep(DependentsStepProbe probe) : IRetentionDependents<SharedConnectionRecord>
    {
        public Task RetireAsync(SharedConnectionRecord row, RetentionDependentsContext ctx, CancellationToken ct)
        {
            probe.Rows.Add(row.Id);
            return Task.CompletedTask;
        }
    }

    private sealed class SharedConnectionDbContext(DbContextOptions<SharedConnectionDbContext> options)
        : DbContext(options)
    {
        public DbSet<SharedConnectionRecord> Records => Set<SharedConnectionRecord>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SharedConnectionRecord>().ToTable("shared_connection_records").HasKey(record => record.Id);
            modelBuilder.Entity<SharedConnectionTenantless>().ToTable("shared_connection_tenantless").HasKey(row => row.Id);
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("caller-erasure", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-0000000000c4")]
    private sealed class SharedConnectionRecord
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }

        [ErasureSubject("user")]
        public Guid? SubjectId { get; set; }

        public DateTimeOffset CreatedAt { get; set; }
    }

    // Its presence makes the hold guard read tenantless holds.
    [RetentionTenantless]
    [Retain("caller-erasure", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-0000000000c5")]
    private sealed class SharedConnectionTenantless
    {
        public Guid Id { get; set; }
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
