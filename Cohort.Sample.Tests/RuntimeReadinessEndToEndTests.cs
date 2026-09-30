using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;
using Cohort.Sample.Entities;
using Microsoft.Extensions.Configuration;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class RuntimeReadinessEndToEndTests(PostgresFixture fixture)
{
    [Fact]
    public async Task Direct_public_operation_rejects_a_non_npgsql_provider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddDbContext<UnsupportedProviderDbContext>();
        services.AddSingleton<IRetentionRuleProvider>(new SampleRetentionRuleProvider());
        services.AddCohort<UnsupportedProviderDbContext>();
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var preview = provider.GetRequiredService<IRetentionPreview>();

        var act = () => preview.PreviewAsync(
            new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
            DateTimeOffset.UtcNow
        );

        await act.Should()
            .ThrowAsync<RetentionConfigurationException>()
            .WithMessage("*requires the Npgsql Entity Framework Core provider*<unknown>*");
    }

    [Theory]
    [InlineData(PublicDatabaseOperation.Sweep)]
    [InlineData(PublicDatabaseOperation.Erasure)]
    [InlineData(PublicDatabaseOperation.CreateHold)]
    [InlineData(PublicDatabaseOperation.FlushDispatcher)]
    public async Task Direct_public_database_operations_reject_an_unmigrated_schema(
        PublicDatabaseOperation operation
    )
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        using var host = new CohortTestHost(database.ConnectionString);

        var act = () => InvokeAsync(host, operation, CancellationToken.None);

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            error.Contains("Apply the host application's pending EF Core migrations", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Direct_sweep_retries_after_schema_repair_and_caches_successful_readiness()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(database.ConnectionString)
            .Options;
        await using (var db = new SampleDbContext(options))
        {
            await db.Database.MigrateAsync();
            await db.Database.ExecuteSqlRawAsync(
                "ALTER TABLE public.sweep_run_row_detail RENAME TO malformed_sweep_run_row_detail"
            );
        }

        var ruleProvider = new CountingRuleProvider();
        using var host = new CohortTestHost(database.ConnectionString, ruleProvider);
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);

        var firstCall = () => host.RunWithServicesAsync(services =>
            services.GetRequiredService<IRetentionSweep>().SweepAsync(tenant, now)
        );

        await firstCall.Should().ThrowAsync<RetentionConfigurationException>();
        var callsAfterFailedReadiness = ruleProvider.CapabilityCallCount;
        callsAfterFailedReadiness.Should().BePositive();
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT COUNT(*) FROM public.sweep_run";
            ((long)(await command.ExecuteScalarAsync())!).Should().Be(0);
        }
        await using (var repair = connection.CreateCommand())
        {
            repair.CommandText =
                "ALTER TABLE public.malformed_sweep_run_row_detail RENAME TO sweep_run_row_detail";
            await repair.ExecuteNonQueryAsync();
        }

        var result = await host.RunWithServicesAsync(services =>
            services.GetRequiredService<IRetentionSweep>().SweepAsync(tenant, now)
        );
        result.EntityFailures.Should().BeEmpty();
        ruleProvider.CapabilityCallCount.Should().Be(callsAfterFailedReadiness);

        await using (var breakSchemaAgain = connection.CreateCommand())
        {
            breakSchemaAgain.CommandText =
                "ALTER TABLE public.sweep_run_row_detail RENAME TO malformed_sweep_run_row_detail";
            await breakSchemaAgain.ExecuteNonQueryAsync();
        }
        await host.RunWithServicesAsync(services =>
            services.GetRequiredService<IRetentionSweep>().SweepAsync(tenant, now)
        );

        ruleProvider.CapabilityCallCount.Should().Be(callsAfterFailedReadiness);
        await using var count = connection.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM public.sweep_run";
        ((long)(await count.ExecuteScalarAsync())!).Should().Be(2);
    }

    [Theory]
    [InlineData("CREATE DOMAIN session_instant AS timestamptz", "session_instant")]
    [InlineData("CREATE DOMAIN session_ids AS timestamptz[]", "session_ids")]
    [InlineData("", "tstzrange")]
    [InlineData("", "double precision[]")]
    public async Task Readiness_rejects_a_record_id_column_whose_text_form_depends_on_session_settings(
        string typeDefinition,
        string columnType
    )
    {
        // The EF model says text; only the catalog knows the column is a domain, array or range
        // over a type whose text form follows TimeZone, DateStyle or extra_float_digits.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var options = new DbContextOptionsBuilder<SessionKeyDbContext>()
            .UseNpgsql(database.ConnectionString)
            .Options;
        await using (var db = new SessionKeyDbContext(options))
        {
            await db.Database.ExecuteSqlRawAsync(db.Database.GenerateCreateScript());
            if (typeDefinition != "")
            {
                await db.Database.ExecuteSqlRawAsync(typeDefinition);
            }
            var alter = $"ALTER TABLE public.session_key_records ALTER COLUMN \"Id\" TYPE {columnType} USING NULL";
            await db.Database.ExecuteSqlRawAsync(alter);
        }

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddDbContext<SessionKeyDbContext>(builder => builder.UseNpgsql(database.ConnectionString));
        services.AddSingleton<IRetentionRuleProvider>(new SessionKeyRuleProvider());
        services.AddCohort<SessionKeyDbContext>();
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        var act = () => provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(
            RetentionSweepRequest.Tenantless(DateTimeOffset.UtcNow)
        );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            error.Contains("record-id column", StringComparison.Ordinal)
            && error.Contains("depends on session settings", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Cancelled_first_readiness_call_is_retryable()
    {
        using var host = new CohortTestHost(fixture.ConnectionString);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        var cancelledCall = () => InvokeAsync(
            host,
            PublicDatabaseOperation.Preview,
            cancellation.Token
        );

        await cancelledCall.Should().ThrowAsync<OperationCanceledException>();
        await InvokeAsync(host, PublicDatabaseOperation.Preview, CancellationToken.None);
    }

    [Fact]
    public async Task Readiness_success_does_not_leak_between_routed_databases_in_one_service_provider()
    {
        await using var unmigrated = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var route = new DatabaseRoute(fixture.ConnectionString);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddSingleton(route);
        services.AddDbContext<SampleDbContext>((provider, options) =>
            options.UseNpgsql(provider.GetRequiredService<DatabaseRoute>().ConnectionString)
        );
        services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
        services.AddSingleton<GuidTombstoneFactory>();
        services.AddSingleton<OriginalValueTombstoneFactory>();
        services.AddSingleton<IAnonymiseValueFactory>(provider =>
            provider.GetRequiredService<GuidTombstoneFactory>()
        );
        services.AddSingleton<IAnonymiseValueFactory>(provider =>
            provider.GetRequiredService<OriginalValueTombstoneFactory>()
        );
        services.AddCohort<SampleDbContext>();
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        await InvokePreviewAsync(provider);
        route.ConnectionString = unmigrated.ConnectionString;

        var act = () => InvokePreviewAsync(provider);

        await act.Should().ThrowAsync<RetentionConfigurationException>();
    }

    private static Task InvokeAsync(
        CohortTestHost host,
        PublicDatabaseOperation operation,
        CancellationToken ct
    )
    {
        var tenantId = Guid.NewGuid();
        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        return host.RunWithServicesAsync(async services =>
        {
            switch (operation)
            {
                case PublicDatabaseOperation.Sweep:
                    await services.GetRequiredService<IRetentionSweep>().SweepAsync(tenant, now, ct);
                    break;
                case PublicDatabaseOperation.Preview:
                    await services.GetRequiredService<IRetentionPreview>().PreviewAsync(tenant, now, ct);
                    break;
                case PublicDatabaseOperation.Erasure:
                    await services.GetRequiredService<IRetentionErasureService>().EraseAsync(
                        tenant,
                        new ErasureScope(Guid.NewGuid()),
                        now,
                        ct
                    );
                    break;
                case PublicDatabaseOperation.CreateHold:
                    await services.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                        new RetentionHoldRequest(
                            Guid.NewGuid(),
                            RetentionEntityIdentity.For<Note>(),
                            Guid.NewGuid().ToString("D"),
                            tenantId,
                            "litigation",
                            now
                        ),
                        ct
                    );
                    break;
                case PublicDatabaseOperation.FlushDispatcher:
                    await services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync(ct);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(operation));
            }
        });
    }

    private static async Task InvokePreviewAsync(IServiceProvider provider)
    {
        await using var scope = provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IRetentionPreview>().PreviewAsync(
            new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
            DateTimeOffset.UtcNow
        );
    }

    public enum PublicDatabaseOperation
    {
        Sweep,
        Preview,
        Erasure,
        CreateHold,
        FlushDispatcher,
    }

    private sealed class CountingRuleProvider : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();
        private int capabilityCallCount;

        public int CapabilityCallCount => Volatile.Read(ref capabilityCallCount);

        public RetentionCategoryCapabilities? GetCapabilities(string category)
        {
            Interlocked.Increment(ref capabilityCallCount);
            return inner.GetCapabilities(category);
        }

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        ) => inner.ResolveAsync(context, ct);
    }

    private sealed class DatabaseRoute(string connectionString)
    {
        public string ConnectionString { get; set; } = connectionString;
    }

    private sealed class SessionKeyDbContext(DbContextOptions<SessionKeyDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SessionKeyRecord>().ToTable("session_key_records");
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("session-key", nameof(CreatedAt))]
    [RetentionEntityId("0c8e5a4f-6b1d-4e27-9a3c-5f2d7e8b1a96")]
    [RetentionTenantless]
    private sealed class SessionKeyRecord
    {
        public string Id { get; set; } = "";
        public DateTimeOffset CreatedAt { get; set; }
    }

    private sealed class SessionKeyRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == "session-key" ? new RetentionCategoryCapabilities([Strategy.Purge]) : null;

        public Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct) =>
            Task.FromResult<RetentionRule?>(new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge));
    }

    private sealed class UnsupportedProviderDbContext(
        DbContextOptions<UnsupportedProviderDbContext> options
    ) : DbContext(options);
}
