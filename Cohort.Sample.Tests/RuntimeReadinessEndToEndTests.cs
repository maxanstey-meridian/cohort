using System.Collections.Concurrent;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

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
        services.AddSingleton(RetentionExecutionScope.Unisolated);
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
            services.GetRequiredService<IRetentionSweep>().ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, now))
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
            services.GetRequiredService<IRetentionSweep>().ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, now))
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
            services.GetRequiredService<IRetentionSweep>().ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, now))
        );

        ruleProvider.CapabilityCallCount.Should().Be(callsAfterFailedReadiness);
        await using var count = connection.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM public.sweep_run";
        ((long)(await count.ExecuteScalarAsync())!).Should().Be(2);
    }

    [Fact]
    public async Task Direct_sweep_refuses_a_0_9_schema_until_the_resolved_period_migration_is_applied()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(database.ConnectionString)
            .Options;
        await using (var db = new SampleDbContext(options))
        {
            await db.Database.GetService<IMigrator>()
                .MigrateAsync("20261009090417_AddErasureSubjectKinds");
        }

        using var host = new CohortTestHost(database.ConnectionString);
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);

        var refused = () => host.RunSweepAsync(tenant, now);

        var exception = await refused.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().Contain(error =>
            error.Contains("ResolvedMonths", StringComparison.Ordinal)
            && error.Contains("ResolvedDays", StringComparison.Ordinal)
        );

        await using (var db = new SampleDbContext(options))
        {
            await db.Database.MigrateAsync();
        }

        var result = await host.RunSweepAsync(tenant, now);
        result.EntityFailures.Should().BeEmpty();
    }

    [Fact]
    public async Task Direct_sweep_refuses_a_required_column_cohort_does_not_write_and_accepts_ones_the_database_fills()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(database.ConnectionString)
            .Options;
        await using (var db = new SampleDbContext(options))
        {
            await db.Database.MigrateAsync();
            // A hand-edited 0.10.0 upgrade that lost its drop step, beside host columns that
            // Cohort's inserts can leave out.
            await db.Database.ExecuteSqlRawAsync(
                """
                ALTER TABLE public.sweep_run_entity_summary
                    ADD COLUMN "ResolvedPeriod" interval NOT NULL,
                    ADD COLUMN "HostNote" text NULL,
                    ADD COLUMN "HostFlag" integer NOT NULL DEFAULT 0,
                    ADD COLUMN "HostSequence" bigint GENERATED BY DEFAULT AS IDENTITY
                """
            );
        }

        using var host = new CohortTestHost(database.ConnectionString);
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);

        var refused = () => host.RunSweepAsync(tenant, now);

        var exception = await refused.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            error.Contains("required columns Cohort does not write", StringComparison.Ordinal)
            && error.Contains("\"ResolvedPeriod\"", StringComparison.Ordinal)
            && !error.Contains("HostNote", StringComparison.Ordinal)
            && !error.Contains("HostFlag", StringComparison.Ordinal)
            && !error.Contains("HostSequence", StringComparison.Ordinal)
        );

        await using (var db = new SampleDbContext(options))
        {
            await db.Database.ExecuteSqlRawAsync(
                "ALTER TABLE public.sweep_run_entity_summary DROP COLUMN \"ResolvedPeriod\""
            );
        }

        var result = await host.RunSweepAsync(tenant, now);
        result.EntityFailures.Should().BeEmpty();
    }

    [Fact]
    public async Task Direct_sweep_refuses_an_active_hold_whose_entity_identity_is_no_longer_registered()
    {
        // A hold stored under an identity the code no longer declares would never match a row
        // again, so the sweep would purge the held note without a word.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(database.ConnectionString)
            .Options;
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var holdId = Guid.NewGuid();
        var oldIdentity = Guid.NewGuid();
        await using (var db = new SampleDbContext(options))
        {
            await db.Database.MigrateAsync();
            db.Notes.Add(new Note
            {
                Id = noteId,
                TenantId = tenantId,
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-400),
                Body = "held",
            });
            await db.SaveChangesAsync();
        }
        await InsertHoldAsync(options, holdId, oldIdentity, noteId.ToString(), tenantId, expiresAt: null, removedAt: null);

        using var host = new CohortTestHost(database.ConnectionString);
        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());

        var refused = () => host.RunSweepAsync(tenant);

        var exception = await refused.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            error.Contains($"'{oldIdentity}'", StringComparison.Ordinal)
            && error.Contains("1 active hold(s)", StringComparison.Ordinal)
            && error.Contains($"[RetentionEntityId(\"{oldIdentity}\")]", StringComparison.Ordinal)
        );
        (await NoteExistsAsync(options, noteId)).Should().BeTrue();

        // Re-pointing the hold at the entity's current identity is the remedy; the refusal was
        // not cached, so the next sweep checks again and honours the hold.
        await using (var db = new SampleDbContext(options))
        {
            await db.Database.ExecuteSqlAsync(
                $"""UPDATE "retention_holds" SET "RetentionEntityId" = {RetentionEntityIdentity.For<Note>()} WHERE "HoldId" = {holdId}"""
            );
        }

        var result = await host.RunSweepAsync(tenant);

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
            .Which.HeldCount.Should().Be(1);
        (await NoteExistsAsync(options, noteId)).Should().BeTrue();
    }

    [Fact]
    public async Task Readiness_ignores_expired_and_removed_holds_under_an_unregistered_entity_identity()
    {
        // Only a hold that still protects something is lost when its identity goes; history is not.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(database.ConnectionString)
            .Options;
        var tenantId = Guid.NewGuid();
        var oldIdentity = Guid.NewGuid();
        await using (var db = new SampleDbContext(options))
        {
            await db.Database.MigrateAsync();
        }
        var past = DateTimeOffset.UtcNow.AddDays(-1);
        await InsertHoldAsync(options, Guid.NewGuid(), oldIdentity, Guid.NewGuid().ToString(), tenantId, expiresAt: past, removedAt: null);
        await InsertHoldAsync(options, Guid.NewGuid(), oldIdentity, Guid.NewGuid().ToString(), tenantId, expiresAt: null, removedAt: past);

        using var host = new CohortTestHost(database.ConnectionString);

        var result = await host.RunSweepAsync(new TenantContext(tenantId, "uk", new Dictionary<string, string>()));

        result.EntityFailures.Should().BeEmpty();
    }

    [Fact]
    public async Task Readiness_refuses_a_host_whose_system_scope_reaches_a_different_database()
    {
        // The cross-tenant hold check runs in its own System(Readiness) scope; if the host routed
        // that scope elsewhere, it would certify the caller's database from another one.
        await using var tenantDatabase = await MigratedTemporaryDatabaseAsync();
        await using var provider = BuildSystemRoutedProvider(
            tenantDatabase.ConnectionString,
            fixture.ConnectionString
        );

        var act = () => InvokePreviewAsync(provider);

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                error.Contains("different model or database", StringComparison.Ordinal)
            );
    }

    [Fact]
    public async Task Readiness_accepts_a_system_scope_that_reaches_the_same_database_through_a_differently_spelled_connection_string()
    {
        // localhost and 127.0.0.1, or a pooler and the direct port, are one database to the
        // server; only the server can say so.
        await using var database = await MigratedTemporaryDatabaseAsync();
        var spelledDifferently = new NpgsqlConnectionStringBuilder(database.ConnectionString)
        {
            ApplicationName = "cohort-readiness",
        };
        spelledDifferently.Host = AlternateSpelling(spelledDifferently.Host!);
        await using var provider = BuildSystemRoutedProvider(
            database.ConnectionString,
            spelledDifferently.ConnectionString
        );

        await InvokePreviewAsync(provider);
    }

    // A host name becomes its IPv4 address and a loopback address becomes "localhost"; any
    // other address keeps its spelling, and the ApplicationName still makes the strings differ.
    private static string AlternateSpelling(string host)
    {
        if (System.Net.IPAddress.TryParse(host, out var address))
        {
            return System.Net.IPAddress.IsLoopback(address) ? "localhost" : host;
        }

        return System.Net.Dns.GetHostAddresses(host)
            .First(candidate => candidate.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
            .ToString();
    }

    [Fact]
    public async Task Readiness_runs_the_hold_check_once_for_calls_inside_a_caller_transaction()
    {
        // The hold check reads on its own pooled connection, so the caller's transaction cannot
        // change its answer; a second call in that transaction must not take another connection.
        await using var database = await MigratedTemporaryDatabaseAsync();
        var executionScope = new RecordingExecutionScope();
        using var host = new CohortTestHost(
            database.ConnectionString,
            configureServices: services =>
            {
                services.RemoveAll<IRetentionExecutionScope>();
                services.AddSingleton<IRetentionExecutionScope>(executionScope);
            }
        );
        var readiness = new RetentionExecutionContext.System(RetentionSystemWork.Readiness);

        await host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            var holds = services.GetRequiredService<IRetentionHoldsRepository>();
            await using var transaction = await db.Database.BeginTransactionAsync();

            await holds.ListActiveAsync(CancellationToken.None);
            executionScope.Entered.Should().ContainSingle(context => context == readiness);

            await holds.ListActiveAsync(CancellationToken.None);
            executionScope.Entered.Should().ContainSingle(context => context == readiness);
        });
    }

    [Fact]
    public async Task Readiness_refuses_an_orphaned_active_hold_when_no_entity_is_retained()
    {
        // Removing the last [Retain] orphans its holds just as renaming an identity does.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var options = new DbContextOptionsBuilder<NoRetainedEntityDbContext>()
            .UseNpgsql(database.ConnectionString)
            .Options;
        var oldIdentity = Guid.NewGuid();
        await using (var db = new NoRetainedEntityDbContext(options))
        {
            await db.Database.ExecuteSqlRawAsync(db.Database.GenerateCreateScript());
            await InsertHoldAsync(db, Guid.NewGuid(), oldIdentity, Guid.NewGuid().ToString(), Guid.NewGuid(), expiresAt: null, removedAt: null);
        }

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddDbContext<NoRetainedEntityDbContext>(builder => builder.UseNpgsql(database.ConnectionString));
        services.AddSingleton<IRetentionRuleProvider>(new SessionKeyRuleProvider());
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<NoRetainedEntityDbContext>();
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        var act = () => provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(
            RetentionSweepRequest.Tenantless()
        );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            error.Contains($"'{oldIdentity}'", StringComparison.Ordinal)
            && error.Contains("1 active hold(s)", StringComparison.Ordinal)
        );
    }

    private async Task<TemporaryDatabase> MigratedTemporaryDatabaseAsync()
    {
        var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var db = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>().UseNpgsql(database.ConnectionString).Options
        );
        await db.Database.MigrateAsync();
        return database;
    }

    // Every scope reaches callerConnectionString until it is entered as System work, which the
    // host routes to systemConnectionString.
    private static ServiceProvider BuildSystemRoutedProvider(
        string callerConnectionString,
        string systemConnectionString
    )
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddScoped(_ => new DatabaseRoute(callerConnectionString));
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
        services.AddScoped<IRetentionExecutionScope>(provider =>
            new SystemWorkRoutingScope(provider.GetRequiredService<DatabaseRoute>(), systemConnectionString)
        );
        services.AddCohort<SampleDbContext>();
        return services.BuildServiceProvider(validateScopes: true);
    }

    private sealed class RecordingExecutionScope : IRetentionExecutionScope
    {
        private readonly ConcurrentQueue<RetentionExecutionContext> entered = new();

        public IReadOnlyCollection<RetentionExecutionContext> Entered => entered;

        public ValueTask EnterAsync(RetentionExecutionContext context, CancellationToken ct)
        {
            entered.Enqueue(context);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class SystemWorkRoutingScope(DatabaseRoute route, string systemConnectionString)
        : IRetentionExecutionScope
    {
        public ValueTask EnterAsync(RetentionExecutionContext context, CancellationToken ct)
        {
            if (context is RetentionExecutionContext.System)
            {
                route.ConnectionString = systemConnectionString;
            }

            return ValueTask.CompletedTask;
        }
    }

    private static async Task InsertHoldAsync(
        DbContextOptions<SampleDbContext> options,
        Guid holdId,
        Guid retentionEntityId,
        string recordId,
        Guid tenantId,
        DateTimeOffset? expiresAt,
        DateTimeOffset? removedAt
    )
    {
        await using var db = new SampleDbContext(options);
        await InsertHoldAsync(db, holdId, retentionEntityId, recordId, tenantId, expiresAt, removedAt);
    }

    private static async Task InsertHoldAsync(
        DbContext db,
        Guid holdId,
        Guid retentionEntityId,
        string recordId,
        Guid tenantId,
        DateTimeOffset? expiresAt,
        DateTimeOffset? removedAt
    )
    {
        var createdAt = DateTimeOffset.UtcNow.AddDays(-10);
        await db.Database.ExecuteSqlAsync(
            $"""
            INSERT INTO "retention_holds"
                ("HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt")
            VALUES ({holdId}, {retentionEntityId}, {recordId}, {tenantId}, 'litigation', {createdAt}, {expiresAt}, {removedAt})
            """
        );
    }

    private static async Task<bool> NoteExistsAsync(DbContextOptions<SampleDbContext> options, Guid noteId)
    {
        await using var db = new SampleDbContext(options);
        return await db.Notes.AnyAsync(note => note.Id == noteId);
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
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<SessionKeyDbContext>();
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        var act = () => provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(
            RetentionSweepRequest.Tenantless()
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
        services.AddSingleton(RetentionExecutionScope.Unisolated);
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
                    await services.GetRequiredService<IRetentionSweep>().ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, now), ct);
                    break;
                case PublicDatabaseOperation.Preview:
                    await services.GetRequiredService<IRetentionPreview>().PreviewAsync(tenant, now, ct);
                    break;
                case PublicDatabaseOperation.Erasure:
                    await services.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                        RetentionErasureRequest.Tenanted(tenant, new ErasureScope("user", Guid.NewGuid()), now),
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

    private sealed class NoRetainedEntityDbContext(DbContextOptions<NoRetainedEntityDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
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
            Task.FromResult<RetentionRule?>(new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge));
    }

    private sealed class UnsupportedProviderDbContext(
        DbContextOptions<UnsupportedProviderDbContext> options
    ) : DbContext(options);
}
