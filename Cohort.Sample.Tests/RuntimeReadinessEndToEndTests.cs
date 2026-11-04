using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class RuntimeReadinessEndToEndTests(PostgresFixture fixture)
{
    // One cache every context shares, as a host's UseMemoryCache gives them, but too small for
    // any model: each context builds a model of its own. (A new cache per context would be a
    // different internal service provider each time, which EF treats as different models.)
    private static readonly MemoryCache ModelCacheThatHoldsNothing = new(new MemoryCacheOptions { SizeLimit = 1 });

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

    // Under multiplexing a connection holds one server session only inside a transaction, so
    // Cohort's session advisory locks would be taken on one session and released on another.
    [Theory]
    [InlineData(PublicDatabaseOperation.Sweep)]
    [InlineData(PublicDatabaseOperation.Preview)]
    [InlineData(PublicDatabaseOperation.Erasure)]
    [InlineData(PublicDatabaseOperation.CreateHold)]
    [InlineData(PublicDatabaseOperation.FlushDispatcher)]
    public async Task Direct_public_database_operations_refuse_a_multiplexed_connection(
        PublicDatabaseOperation operation
    )
    {
        var multiplexed = new NpgsqlConnectionStringBuilder(fixture.ConnectionString) { Multiplexing = true };
        using var host = new CohortTestHost(multiplexed.ConnectionString);

        var act = () => InvokeAsync(host, operation, CancellationToken.None);

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                error.Contains("Multiplexing", StringComparison.Ordinal)
                && error.Contains("session advisory locks", StringComparison.Ordinal)
            );
    }

    // A wrapped DbConnection (a profiler, say) is not an NpgsqlConnection, but its string says
    // Multiplexing=true just the same.
    [Fact]
    public async Task A_wrapped_multiplexed_connection_is_refused()
    {
        var multiplexed = new NpgsqlConnectionStringBuilder(fixture.ConnectionString) { Multiplexing = true };
        var wrappers = new List<PassThroughConnection>();
        await using var provider = BuildAnonymiseTargetProvider(
            multiplexed.ConnectionString,
            options =>
            {
                var wrapper = new PassThroughConnection(new NpgsqlConnection(multiplexed.ConnectionString));
                wrappers.Add(wrapper);
                options.UseNpgsql(wrapper);
            }
        );

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var act = () => ListActiveHoldsAsync(scope.ServiceProvider);

            (await act.Should().ThrowAsync<RetentionConfigurationException>())
                .Which.Errors.Should().ContainSingle(error =>
                    error.Contains("Multiplexing", StringComparison.Ordinal)
                    && error.Contains("session advisory locks", StringComparison.Ordinal)
                );
            wrappers.Should().NotBeEmpty("the context runs on the wrapper, not on an NpgsqlConnection");
        }
        finally
        {
            foreach (var wrapper in wrappers)
            {
                await wrapper.DisposeAsync();
            }
        }
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
    public async Task Direct_sweep_refuses_a_0_10_schema_until_the_retained_deferred_and_blocked_counts_migration_is_applied()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(database.ConnectionString)
            .Options;
        await using (var db = new SampleDbContext(options))
        {
            await db.Database.GetService<IMigrator>()
                .MigrateAsync("20261022095727_StoreResolvedPeriodAsMonthsAndDays");
            // A summary a 0.10 host wrote before the upgrade.
            await db.Database.ExecuteSqlRawAsync(
                """
                INSERT INTO public.sweep_run ("SweepId", "DryRun", "StartedAt", "SettledAt", "Status", "TenantId", "TriggerKind")
                VALUES ('00000000-0000-0000-0000-0000000010a0', false, now(), now(), 1, gen_random_uuid(), 0);
                INSERT INTO public.sweep_run_entity_summary
                    ("SweepId", "RetentionEntityId", "Category", "TenantId", "Strategy", "Affected", "At",
                     "EntityType", "HeldCount", "NullAnchorCount", "ResolvedDays", "ResolvedMonths", "SkippedCount")
                VALUES ('00000000-0000-0000-0000-0000000010a0', gen_random_uuid(), 'written-before-upgrade',
                        gen_random_uuid(), 0, 4, now(), 'Note', 1, 0, 0, 12, 0);
                """
            );
        }

        using var host = new CohortTestHost(database.ConnectionString);
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);

        var refused = () => host.RunSweepAsync(tenant, now);

        var exception = await refused.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().Contain(error =>
            error.Contains("RetainedCount", StringComparison.Ordinal)
            && error.Contains("DeferredCount", StringComparison.Ordinal)
            && error.Contains("BlockedCount", StringComparison.Ordinal)
        );

        await using (var db = new SampleDbContext(options))
        {
            await db.Database.MigrateAsync();
        }

        var result = await host.RunSweepAsync(tenant, now);
        result.EntityFailures.Should().BeEmpty();

        // The migration backfills with 0 and then drops the defaults, so a 0.10 instance,
        // whose summary insert leaves the three counts out, fails instead of recording 0 for
        // counts it never measured; 0.10's readiness refuses such columns for the same reason.
        await using (var db = new SampleDbContext(options))
        {
            await db.Database.OpenConnectionAsync();
            var connection = db.Database.GetDbConnection();
            await using (var defaults = connection.CreateCommand())
            {
                defaults.CommandText =
                    """
                    SELECT count(*) FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'sweep_run_entity_summary'
                      AND column_name IN ('RetainedCount', 'DeferredCount', 'BlockedCount')
                      AND column_default IS NULL AND is_nullable = 'NO'
                    """;
                ((long)(await defaults.ExecuteScalarAsync())!).Should().Be(3);
            }

            // The pre-upgrade summary was backfilled with 0: unknown, since 0.10 never measured it.
            await using (var backfilled = connection.CreateCommand())
            {
                backfilled.CommandText =
                    """
                    SELECT "RetainedCount" + "DeferredCount" + "BlockedCount"
                    FROM public.sweep_run_entity_summary WHERE "Category" = 'written-before-upgrade'
                    """;
                ((long)(await backfilled.ExecuteScalarAsync())!).Should().Be(0);
            }

            await using var legacyInsert = connection.CreateCommand();
            legacyInsert.CommandText =
                """
                INSERT INTO public.sweep_run_entity_summary
                    ("SweepId", "RetentionEntityId", "Category", "TenantId", "Strategy", "Affected", "At",
                     "EntityType", "HeldCount", "NullAnchorCount", "ResolvedDays", "ResolvedMonths",
                     "RuleReason", "RuleSource", "SkippedCount")
                SELECT "SweepId", "RetentionEntityId", 'written-by-0.10', "TenantId", "Strategy", "Affected", "At",
                       "EntityType", "HeldCount", "NullAnchorCount", "ResolvedDays", "ResolvedMonths",
                       "RuleReason", "RuleSource", "SkippedCount"
                FROM public.sweep_run_entity_summary
                LIMIT 1
                """;
            var insert = () => legacyInsert.ExecuteNonQueryAsync();
            (await insert.Should().ThrowAsync<PostgresException>()).Which.SqlState.Should().Be("23502");
        }
    }

    [Fact]
    public async Task Direct_sweep_refuses_a_default_left_on_a_count_column()
    {
        // An upgrade migration that kept EF's backfill default: a 0.10 host would then record 0
        // for counts it never measured instead of failing.
        await using var database = await MigratedTemporaryDatabaseAsync();
        await using (var connection = new NpgsqlConnection(database.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(
                """ALTER TABLE public.sweep_run_entity_summary ALTER COLUMN "BlockedCount" SET DEFAULT 0""",
                connection
            );
            await command.ExecuteNonQueryAsync();
        }

        using var host = new CohortTestHost(database.ConnectionString);

        var act = () => host.RunSweepAsync(new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()));

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            error.Contains("DROP DEFAULT", StringComparison.Ordinal)
            && error.Contains("\"BlockedCount\"", StringComparison.Ordinal)
            && !error.Contains("RetainedCount", StringComparison.Ordinal)
        );
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
                error.Contains("different database", StringComparison.Ordinal)
            );
    }

    // The model pass checks the System(Readiness) scope's database once per entry. The guard that
    // reads tenantless holds before IRetentionDeletion runs checks its own scope's database each
    // time, so a scope a hook routes elsewhere after readiness passed is refused before it reads.
    // Make-fail: the database comparison skipped in the readiness scope's check (the deletion
    // completes and the callback removes the row).
    [Fact]
    public async Task A_readiness_scope_switched_to_another_database_is_refused_before_the_guard_reads_holds()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await using var otherDatabase = await MigratedTemporaryDatabaseAsync();
        string? systemRoute = null;
        await using var provider = BuildSystemRoutedProvider(database.ConnectionString, () => systemRoute);
        var first = await SeedTenantlessLogAsync(database.ConnectionString);
        var second = await SeedTenantlessLogAsync(database.ConnectionString);

        await using (var scope = provider.CreateAsyncScope())
        {
            (await DeleteTenantlessLogAsync(scope.ServiceProvider, first)).Should().Be(RetentionDeletionOutcome.Executed);
        }

        systemRoute = otherDatabase.ConnectionString;
        await using var switchedScope = provider.CreateAsyncScope();
        var act = () => DeleteTenantlessLogAsync(switchedScope.ServiceProvider, second);

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                error.Contains("different database", StringComparison.Ordinal)
            );
        (await TenantlessLogExistsAsync(database.ConnectionString, second)).Should().BeTrue();
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

    [Fact]
    public async Task Readiness_accepts_a_system_scope_whose_context_runs_a_different_model_on_the_same_database()
    {
        // The System(Readiness) scope's context is used only through its connection: the holds
        // relation and the registered ids come from the caller's model. So a host whose key
        // factory gives that scope a different model still reaches the same database, and
        // that is all the scope has to do.
        await using var database = await MigratedTemporaryDatabaseAsync();
        var spelledDifferently = new NpgsqlConnectionStringBuilder(database.ConnectionString)
        {
            ApplicationName = "cohort-readiness",
        };
        await using var provider = BuildSystemRoutedProvider(
            database.ConnectionString,
            spelledDifferently.ConnectionString,
            options => options.ReplaceService<IModelCacheKeyFactory, ConnectionStringModelCacheKeyFactory>()
        );

        await InvokePreviewAsync(provider);
    }

    [Fact]
    public async Task Readiness_accepts_a_system_scope_whose_context_built_its_own_copy_of_the_model()
    {
        // EF keeps built models in its memory cache and drops one that doesn't fit, so with the
        // cache full two contexts of one type and options hold equal models that are not the same
        // object. The System(Readiness) scope's context is used only through its connection, so
        // its copy of the model is as good as the caller's.
        await using var database = await MigratedTemporaryDatabaseAsync();
        await using var provider = BuildModelCopyProvider(database.ConnectionString);

        using (var scope = provider.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<SampleDbContext>();
            using var other = provider.CreateScope();
            ReferenceEquals(db.Model, other.ServiceProvider.GetRequiredService<SampleDbContext>().Model)
                .Should().BeFalse("the full cache gives every context a model of its own");
        }

        var result = await provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(
            RetentionSweepRequest.Tenantless()
        );

        result.EntityFailures.Should().BeEmpty();

        await using var holdsScope = provider.CreateAsyncScope();
        (await holdsScope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>().HasActiveHoldAsync(
            RetentionEntityIdentity.For<Note>(),
            Guid.NewGuid().ToString(),
            Guid.NewGuid(),
            CancellationToken.None
        )).Should().BeFalse();
    }

    // T1: the catalog checks a model copy shares are not run again for the next copy.
    [Fact]
    public async Task A_rebuilt_copy_of_a_validated_model_runs_no_catalog_check_again()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await using var provider = BuildModelCopyProvider(database.ConnectionString);
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var sweep = provider.GetRequiredService<IRetentionSweep>();

        using (var first = provider.CreateScope())
        using (var second = provider.CreateScope())
        {
            ReferenceEquals(
                first.ServiceProvider.GetRequiredService<SampleDbContext>().Model,
                second.ServiceProvider.GetRequiredService<SampleDbContext>().Model
            ).Should().BeFalse("the full cache gives every context a model of its own");
        }

        (await sweep.ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, now))).EntityFailures.Should().BeEmpty();

        // Readiness's schema check would refuse this; the sweep then only fails on the missing
        // table, per entity, as in Direct_sweep_retries_after_schema_repair_and_caches_successful_readiness.
        await ExecuteAsync(
            database,
            "ALTER TABLE public.sweep_run_row_detail RENAME TO malformed_sweep_run_row_detail"
        );

        var result = await sweep.ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, now));

        result.EntityFailures.Should().NotBeEmpty().And.OnlyContain(failure =>
            failure.Contains("code=sqlstate:42P01", StringComparison.Ordinal)
        );
    }

    // T2: nor does it open the System(Readiness) scope again, though the host is asked for its
    // capabilities again (Decision 6: capabilities stay per model instance).
    [Fact]
    public async Task A_rebuilt_copy_of_a_validated_model_opens_no_second_readiness_scope()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        var executionScope = new RecordingExecutionScope();
        var ruleProvider = new CountingRuleProvider();
        await using var provider = BuildModelCopyProvider(
            database.ConnectionString,
            ruleProvider,
            executionScope
        );
        var readiness = new RetentionExecutionContext.System(RetentionSystemWork.Readiness);

        await using var first = provider.CreateAsyncScope();
        var firstModel = first.ServiceProvider.GetRequiredService<SampleDbContext>().Model;
        await ListActiveHoldsAsync(first.ServiceProvider);
        executionScope.Ran.Should().ContainSingle(context => context == readiness);
        var capabilityCallsForFirstCopy = ruleProvider.CapabilityCallCount;
        capabilityCallsForFirstCopy.Should().BePositive();

        await using var second = provider.CreateAsyncScope();
        ReferenceEquals(firstModel, second.ServiceProvider.GetRequiredService<SampleDbContext>().Model)
            .Should().BeFalse("the full cache gives every context a model of its own");
        await ListActiveHoldsAsync(second.ServiceProvider);

        executionScope.Ran.Should().ContainSingle(context => context == readiness);
        ruleProvider.CapabilityCallCount.Should().Be(
            2 * capabilityCallsForFirstCopy,
            "each model instance asks the host for its categories' capabilities again"
        );
    }

    // T3: Cohort's per-model state dies with the model.
    [Fact]
    public async Task Cohort_keeps_no_model_alive_after_its_context()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await using var provider = BuildModelCopyProvider(database.ConnectionString);

        var model = await ReadinessModelInDisposedScopeAsync(provider);
        // The helper's continuation runs this one inline, on the helper's own stack, which still
        // refers to the context; yielding gets this method a fresh stack.
        await Task.Yield();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        model.IsAlive.Should().BeFalse("readiness's and validation's per-model state is held weakly");
    }

    // T5: a model copy that reads different things is checked on those things.
    [Fact]
    public async Task A_model_copy_that_maps_a_retained_entity_elsewhere_is_checked_again()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await using var provider = BuildModelCopyProvider(database.ConnectionString);

        await using (var passing = provider.CreateAsyncScope())
        {
            await ListActiveHoldsAsync(passing.ServiceProvider);
        }

        await using var elsewhere = provider.CreateAsyncScope();
        elsewhere.ServiceProvider.GetRequiredService<ModelVariant>()
            .CustomizeModelWith<RetainedEntityElsewhereModelCustomizer>();
        var act = () => ListActiveHoldsAsync(elsewhere.ServiceProvider);

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesWith(error, typeof(Note), "42P01", "\"public.notes_elsewhere\"")
            );
    }

    // T5a: each shared check's key carries what its check reads. A copy that no longer registers
    // an entity with an active hold would otherwise skip the orphaned-hold refusal and leave
    // that hold silently matching nothing.
    [Fact]
    public async Task A_model_copy_that_stops_registering_an_entity_with_an_active_hold_is_checked_again()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        var noteIdentity = RetentionEntityIdentity.For<Note>();
        await InsertHoldAsync(
            new DbContextOptionsBuilder<SampleDbContext>().UseNpgsql(database.ConnectionString).Options,
            Guid.NewGuid(),
            noteIdentity,
            Guid.NewGuid().ToString(),
            Guid.NewGuid(),
            expiresAt: null,
            removedAt: null
        );
        await using var provider = BuildModelCopyProvider(database.ConnectionString);

        await using (var passing = provider.CreateAsyncScope())
        {
            await ListActiveHoldsAsync(passing.ServiceProvider);
        }

        await using var withoutNote = provider.CreateAsyncScope();
        withoutNote.ServiceProvider.GetRequiredService<ModelVariant>()
            .CustomizeModelWith<RetainedEntityIgnoredModelCustomizer>();
        var act = () => ListActiveHoldsAsync(withoutNote.ServiceProvider);

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                error.Contains($"'{noteIdentity}'", StringComparison.Ordinal)
                && error.Contains("is not registered by any retained entity", StringComparison.Ordinal)
                && error.Contains("1 active hold(s)", StringComparison.Ordinal)
            );
    }

    // T5b: a copy that maps a hold relation's link member to another column is checked on it.
    [Fact]
    public async Task A_model_copy_that_maps_a_hold_relation_link_elsewhere_is_checked_again()
    {
        await using var database = await AnonymiseTargetDatabaseAsync<HoldLinkDbContext>();
        await using var provider = BuildModelCopyProvider<HoldLinkDbContext>(
            database.ConnectionString,
            new HoldLinkRuleProvider()
        );

        await using (var passing = provider.CreateAsyncScope())
        {
            await ListActiveHoldsAsync(passing.ServiceProvider);
        }

        await using var elsewhere = provider.CreateAsyncScope();
        elsewhere.ServiceProvider.GetRequiredService<ModelVariant>()
            .CustomizeModelWith<HoldLinkColumnElsewhereModelCustomizer>();
        var act = () => ListActiveHoldsAsync(elsewhere.ServiceProvider);

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesWith(error, typeof(HoldLinkChild), "42703", "\"CaseIdElsewhere\"")
            );
    }

    // T5b, skip side: a copy that maps the link to the column the first copy passed on is not
    // checked again, even though the database has since lost that column. Nothing else reads the
    // link column, so only the link check could refuse.
    [Fact]
    public async Task A_model_copy_that_maps_a_hold_relation_link_as_before_is_not_checked_again()
    {
        await using var database = await AnonymiseTargetDatabaseAsync<HoldLinkDbContext>();
        await using var provider = BuildModelCopyProvider<HoldLinkDbContext>(
            database.ConnectionString,
            new HoldLinkRuleProvider()
        );

        await using var first = provider.CreateAsyncScope();
        var firstModel = first.ServiceProvider.GetRequiredService<HoldLinkDbContext>().Model;
        await ListActiveHoldsAsync(first.ServiceProvider);

        await ExecuteAsync(
            database,
            "ALTER TABLE public.hold_link_children RENAME COLUMN \"CaseId\" TO \"CaseIdGone\""
        );

        await using var second = provider.CreateAsyncScope();
        ReferenceEquals(firstModel, second.ServiceProvider.GetRequiredService<HoldLinkDbContext>().Model)
            .Should().BeFalse("the full cache gives every context a model of its own");
        var act = () => ListActiveHoldsAsync(second.ServiceProvider);

        await act.Should().NotThrowAsync();
    }

    // T5c: a copy that writes its soft-delete flag to another column is checked on that column.
    [Fact]
    public async Task A_model_copy_that_writes_a_marker_to_a_referenced_column_is_checked_again()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await ExecuteAsync(
            database,
            """
            ALTER TABLE public.soft_delete_records
                ADD COLUMN "IsDeletedReferenced" boolean NOT NULL DEFAULT false,
                ADD CONSTRAINT soft_delete_referenced_live UNIQUE ("Id", "IsDeletedReferenced");
            CREATE TABLE public.soft_delete_referenced_children (
                "ParentId" uuid,
                "ParentIsDeleted" boolean,
                FOREIGN KEY ("ParentId", "ParentIsDeleted")
                    REFERENCES public.soft_delete_records ("Id", "IsDeletedReferenced")
            );
            """
        );
        await using var provider = BuildModelCopyProvider(database.ConnectionString);

        await using (var passing = provider.CreateAsyncScope())
        {
            await ListActiveHoldsAsync(passing.ServiceProvider);
        }

        await using var referenced = provider.CreateAsyncScope();
        referenced.ServiceProvider.GetRequiredService<ModelVariant>()
            .CustomizeModelWith<SoftDeleteFlagInReferencedColumnModelCustomizer>();
        var act = () => ListActiveHoldsAsync(referenced.ServiceProvider);

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                error.Contains(typeof(SoftDeleteRecord).FullName!, StringComparison.Ordinal)
                && error.Contains("member IsDeleted", StringComparison.Ordinal)
                && error.Contains("which is referenced by a foreign key", StringComparison.Ordinal)
            );
    }

    // T5d: and a copy that reads the same things as one that passed is not checked on them again,
    // whatever the database has become since: the retained-tables and retained-columns checks would
    // each refuse what the database looks like now.
    [Fact]
    public async Task A_model_copy_that_reads_what_a_passed_copy_read_skips_the_retained_table_and_marker_checks()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await using var provider = BuildModelCopyProvider(database.ConnectionString);

        await using var first = provider.CreateAsyncScope();
        var firstModel = first.ServiceProvider.GetRequiredService<SampleDbContext>().Model;
        await ListActiveHoldsAsync(first.ServiceProvider);

        await ExecuteAsync(
            database,
            """
            ALTER TABLE public.tenantless_logs RENAME TO tenantless_logs_gone;
            ALTER TABLE public.soft_delete_records
                ADD CONSTRAINT soft_delete_live UNIQUE ("Id", "IsDeleted");
            CREATE TABLE public.soft_delete_children (
                "ParentId" uuid,
                "ParentIsDeleted" boolean,
                FOREIGN KEY ("ParentId", "ParentIsDeleted")
                    REFERENCES public.soft_delete_records ("Id", "IsDeleted")
            );
            """
        );

        await using var second = provider.CreateAsyncScope();
        ReferenceEquals(firstModel, second.ServiceProvider.GetRequiredService<SampleDbContext>().Model)
            .Should().BeFalse("the full cache gives every context a model of its own");
        var act = () => ListActiveHoldsAsync(second.ServiceProvider);

        await act.Should().NotThrowAsync();
    }

    // A1: the database a shared pass was recorded against is the one the server reports, not the
    // connection string's spelling. A database dropped and rebuilt under the same name, with the
    // same connection string, is another database.
    [Fact]
    public async Task A_database_rebuilt_under_the_same_name_is_checked_again()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await using var provider = BuildModelCopyProvider(database.ConnectionString);

        await using (var passing = provider.CreateAsyncScope())
        {
            await ListActiveHoldsAsync(passing.ServiceProvider);
        }

        var holdsTableBefore = await HoldsTableOidAsync(database);
        var name = new NpgsqlConnectionStringBuilder(database.ConnectionString).Database!;
        var admin = new NpgsqlConnectionStringBuilder(database.ConnectionString) { Database = "postgres" }
            .ConnectionString;
        NpgsqlConnection.ClearAllPools();
        await ExecuteAsync(admin, $"DROP DATABASE \"{name}\" WITH (FORCE)");
        await ExecuteAsync(admin, $"CREATE DATABASE \"{name}\"");
        await using (var rebuilt = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>().UseNpgsql(database.ConnectionString).Options
        ))
        {
            await rebuilt.Database.MigrateAsync();
        }

        await ExecuteAsync(database, "DROP TABLE public.tenantless_logs");
        (await HoldsTableOidAsync(database)).Should().NotBe(
            holdsTableBefore,
            "a database built afresh gets tables of its own, which is what tells it from the old one"
        );

        await using var again = provider.CreateAsyncScope();
        var act = () => ListActiveHoldsAsync(again.ServiceProvider);

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesWith(error, typeof(TenantlessLog), "42P01", "\"public.tenantless_logs\"")
            );
    }

    // A1: one model instance reaches one database under two connection strings that differ in
    // what a pooler can route by (user name, options), and each is validated: the model's own
    // memory is keyed by the connection string without its password. The anonymise check is not
    // shared between model copies, so only that memory could skip it.
    [Fact]
    public async Task A_second_user_name_on_the_same_model_and_database_is_validated_again()
    {
        var role = $"cohort_readiness_{Guid.NewGuid():N}";
        const string password = "readiness-test-password";
        await CreateReadRoleAsync(role, password);
        try
        {
            await using var database = await AnonymiseTargetDatabaseAsync();
            await GrantCohortPrivilegesAsync(database, role);

            var thrown = await ListActiveHoldsAfterAnonymiseColumnShrinksAsync(
                database,
                database.ConnectionString,
                () => Task.FromResult(
                    new NpgsqlConnectionStringBuilder(database.ConnectionString)
                    {
                        Username = role,
                        Password = password,
                    }.ConnectionString
                )
            );

            AssertAnonymiseColumnRefused(thrown);
        }
        finally
        {
            await ExecuteAsync(fixture.ConnectionString, $"DROP ROLE IF EXISTS \"{role}\"");
        }
    }

    // Privileges depend on the role, so the statement check's shared pass is keyed by it. Role B
    // differs from role A only in its user name: if the key left the role out, B would find A's
    // pass.
    [Fact]
    public async Task A_second_role_on_the_same_database_is_checked_again()
    {
        var full = $"cohort_readiness_{Guid.NewGuid():N}";
        var readOnly = $"cohort_readiness_{Guid.NewGuid():N}";
        const string password = "readiness-test-password";
        await CreateReadRoleAsync(full, password);
        await CreateReadRoleAsync(readOnly, password);
        try
        {
            await using var database = await AnonymiseTargetDatabaseAsync();
            await GrantCohortPrivilegesAsync(database, full);
            await GrantReadOnlyAsync(database, readOnly);
            var connectionString = AsRole(database, full, password);
            await using var provider = BuildAnonymiseTargetProvider(
                connectionString,
                options => options.UseNpgsql(connectionString)
            );

            await using (var first = provider.CreateAsyncScope())
            {
                await ListActiveHoldsAsync(first.ServiceProvider);
            }

            connectionString = AsRole(database, readOnly, password);
            await using var second = provider.CreateAsyncScope();
            var act = () => ListActiveHoldsAsync(second.ServiceProvider);

            (await act.Should().ThrowAsync<RetentionConfigurationException>())
                .Which.Errors.Should().Contain(error =>
                    error.Contains("Retained entity ", StringComparison.Ordinal)
                    && error.Contains(" 42501: permission denied for table ", StringComparison.Ordinal)
                    && error.Contains($"The database role '{readOnly}' lacks this privilege", StringComparison.Ordinal)
                );
        }
        finally
        {
            await ExecuteAsync(fixture.ConnectionString, $"DROP ROLE IF EXISTS \"{full}\"");
            await ExecuteAsync(fixture.ConnectionString, $"DROP ROLE IF EXISTS \"{readOnly}\"");
        }
    }

    // Unqualified type names resolve through the search path, so the statement check's pass is
    // keyed by it too. The domain is in a schema only the first connection string searches.
    [Fact]
    public async Task A_second_search_path_is_checked_again()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var searching = new NpgsqlConnectionStringBuilder(database.ConnectionString)
        {
            SearchPath = "key_types,public",
        }.ConnectionString;
        var notSearching = new NpgsqlConnectionStringBuilder(database.ConnectionString)
        {
            SearchPath = "public",
        }.ConnectionString;
        // The create script names its table without a schema, so it lands in the first schema of
        // the path: public first, with the domain's schema after it.
        var creating = new NpgsqlConnectionStringBuilder(database.ConnectionString)
        {
            SearchPath = "public,key_types",
        }.ConnectionString;
        await using (var db = new SearchPathDbContext(
            new DbContextOptionsBuilder<SearchPathDbContext>().UseNpgsql(creating).Options
        ))
        {
            await db.Database.ExecuteSqlRawAsync(
                "CREATE SCHEMA key_types; CREATE DOMAIN key_types.search_path_key AS text"
            );
            await db.Database.ExecuteSqlRawAsync(db.Database.GenerateCreateScript());
        }

        var connectionString = searching;
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddDbContext<SearchPathDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(new SessionKeyRuleProvider());
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<SearchPathDbContext>();
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        await using (var first = provider.CreateAsyncScope())
        {
            await ListActiveHoldsAsync(first.ServiceProvider);
        }

        connectionString = notSearching;
        await using var second = provider.CreateAsyncScope();
        var act = () => ListActiveHoldsAsync(second.ServiceProvider);

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesWith(error, typeof(SearchPathKeyRecord), "42704", "type \"search_path_key\" does not exist")
            );
    }

    [Fact]
    public async Task A_second_options_string_on_the_same_model_and_database_is_validated_again()
    {
        await using var database = await AnonymiseTargetDatabaseAsync();

        var thrown = await ListActiveHoldsAfterAnonymiseColumnShrinksAsync(
            database,
            database.ConnectionString,
            () => Task.FromResult(
                new NpgsqlConnectionStringBuilder(database.ConnectionString)
                {
                    Options = "-c statement_timeout=0",
                }.ConnectionString
            )
        );

        AssertAnonymiseColumnRefused(thrown);
    }

    // The password stays out of the model's key: a rotating credential would otherwise make one
    // entry per token. The same role under a new password reaches the same database, so the pass
    // is shared and the anonymise check is not run again.
    [Fact]
    public async Task A_rotated_password_on_the_same_model_and_database_is_not_validated_again()
    {
        var role = $"cohort_readiness_{Guid.NewGuid():N}";
        const string firstPassword = "readiness-test-password";
        const string rotatedPassword = "readiness-test-rotated-password";
        await CreateReadRoleAsync(role, firstPassword);
        try
        {
            await using var database = await AnonymiseTargetDatabaseAsync();
            await GrantCohortPrivilegesAsync(database, role);
            var asRole = new NpgsqlConnectionStringBuilder(database.ConnectionString)
            {
                Username = role,
                Password = firstPassword,
            };

            var thrown = await ListActiveHoldsAfterAnonymiseColumnShrinksAsync(
                database,
                asRole.ConnectionString,
                async () =>
                {
                    await ExecuteAsync(
                        fixture.ConnectionString,
                        $"ALTER ROLE \"{role}\" PASSWORD '{rotatedPassword}'"
                    );
                    asRole.Password = rotatedPassword;
                    return asRole.ConnectionString;
                }
            );

            thrown.Should().BeNull("a rotated password reaches the same database, so the pass is kept");
        }
        finally
        {
            await ExecuteAsync(fixture.ConnectionString, $"DROP ROLE IF EXISTS \"{role}\"");
        }
    }

    // A role that lacks a privilege some statement needs is refused (42501), naming the role and
    // what it lacks: the row locks and writes need UPDATE, and every statement needs the schema.
    [Theory]
    [InlineData("REVOKE INSERT, UPDATE, DELETE ON public.soft_delete_records FROM \"{0}\"", "permission denied for table soft_delete_records")]
    // PUBLIC holds USAGE on public by default, so it is revoked from PUBLIC too (the temporary
    // database is dropped afterwards, so nothing needs restoring).
    [InlineData("REVOKE USAGE ON SCHEMA public FROM PUBLIC, \"{0}\"", "permission denied for schema public")]
    public async Task Readiness_refuses_a_role_without_the_privileges_cohort_needs(string revoke, string message)
    {
        var role = $"cohort_readiness_{Guid.NewGuid():N}";
        const string password = "readiness-test-password";
        await CreateReadRoleAsync(role, password);
        try
        {
            await using var database = await MigratedTemporaryDatabaseAsync();
            await GrantCohortPrivilegesAsync(database, role);
            await ExecuteAsync(database, string.Format(CultureInfo.InvariantCulture, revoke, role));
            using var host = new CohortTestHost(AsRole(database, role, password));

            var act = () => host.RunSweepAsync(new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()));

            (await act.Should().ThrowAsync<RetentionConfigurationException>())
                .Which.Errors.Should().Contain(error =>
                    error.Contains(" 42501: ", StringComparison.Ordinal)
                    && error.Contains(message, StringComparison.Ordinal)
                    && error.Contains($"The database role '{role}' lacks this privilege", StringComparison.Ordinal)
                );
        }
        finally
        {
            await ExecuteAsync(fixture.ConnectionString, $"DROP ROLE IF EXISTS \"{role}\"");
        }
    }

    // The role may use Cohort's schema but not the one the host's table lives in.
    [Fact]
    public async Task Readiness_refuses_a_role_without_usage_on_the_schema_of_a_retained_table()
    {
        var role = $"cohort_readiness_{Guid.NewGuid():N}";
        const string password = "readiness-test-password";
        await CreateReadRoleAsync(role, password);
        try
        {
            await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
            await using (var db = new HostSchemaDbContext(
                new DbContextOptionsBuilder<HostSchemaDbContext>().UseNpgsql(database.ConnectionString).Options
            ))
            {
                await db.Database.ExecuteSqlRawAsync(db.Database.GenerateCreateScript());
            }

            await GrantCohortPrivilegesAsync(database, role);
            await ExecuteAsync(
                database,
                $"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA host_data TO \"{role}\""
            );
            var connectionString = AsRole(database, role, password);
            await using var provider = BuildModelCopyProvider<HostSchemaDbContext>(
                connectionString,
                new SessionKeyRuleProvider()
            );
            await using var scope = provider.CreateAsyncScope();

            var act = () => ListActiveHoldsAsync(scope.ServiceProvider);

            (await act.Should().ThrowAsync<RetentionConfigurationException>())
                .Which.Errors.Should().ContainSingle(error =>
                    RefusesWith(error, typeof(HostSchemaRecord), "42501", "permission denied for schema host_data")
                    && error.Contains($"The database role '{role}' lacks this privilege", StringComparison.Ordinal)
                );
        }
        finally
        {
            await ExecuteAsync(fixture.ConnectionString, $"DROP ROLE IF EXISTS \"{role}\"");
        }
    }

    // A wrapped DbConnection (a profiler, say) is keyed like any other: by its parsed string
    // without the password, so a rotated password is not validated again.
    [Fact]
    public Task A_rotated_password_on_a_wrapped_connection_is_not_validated_again() =>
        RotatedPasswordOnAWrappedConnectionIsNotValidatedAgainAsync(connectionStringSuffix: "");

    // A wrapper that adds a keyword of its own to the string it reports is one Npgsql rejects, so
    // the entry key is built without parsing it as Npgsql settings. The password stays out of it
    // all the same.
    [Fact]
    public Task A_rotated_password_on_a_wrapped_connection_whose_string_Npgsql_rejects_is_not_validated_again() =>
        RotatedPasswordOnAWrappedConnectionIsNotValidatedAgainAsync(connectionStringSuffix: ";ProfilerTag=x");

    private async Task RotatedPasswordOnAWrappedConnectionIsNotValidatedAgainAsync(string connectionStringSuffix)
    {
        var role = $"cohort_readiness_{Guid.NewGuid():N}";
        const string firstPassword = "readiness-test-password";
        const string rotatedPassword = "readiness-test-rotated-password";
        await CreateReadRoleAsync(role, firstPassword);
        var wrappers = new List<PassThroughConnection>();
        try
        {
            await using var database = await AnonymiseTargetDatabaseAsync();
            await GrantCohortPrivilegesAsync(database, role);
            var asRole = new NpgsqlConnectionStringBuilder(database.ConnectionString)
            {
                Username = role,
                Password = firstPassword,
            };

            var thrown = await ListActiveHoldsAfterAnonymiseColumnShrinksAsync(
                database,
                asRole.ConnectionString,
                async () =>
                {
                    await ExecuteAsync(
                        fixture.ConnectionString,
                        $"ALTER ROLE \"{role}\" PASSWORD '{rotatedPassword}'"
                    );
                    asRole.Password = rotatedPassword;
                    return asRole.ConnectionString;
                },
                connectionString =>
                {
                    var wrapper = new PassThroughConnection(
                        new NpgsqlConnection(connectionString),
                        connectionStringSuffix
                    );
                    wrappers.Add(wrapper);
                    return wrapper;
                }
            );

            wrappers.Should().NotBeEmpty("the contexts run on the wrapper, not on an NpgsqlConnection");
            thrown.Should().BeNull("a rotated password reaches the same database, so the pass is kept");
        }
        finally
        {
            foreach (var wrapper in wrappers)
            {
                await wrapper.DisposeAsync();
            }

            await ExecuteAsync(fixture.ConnectionString, $"DROP ROLE IF EXISTS \"{role}\"");
        }
    }

    // Readiness passes for one model instance under firstConnectionString. The anonymise column
    // then shrinks in the database, secondConnectionString says how the next scope reaches the
    // same database, and what readiness throws in that scope comes back (null when nothing).
    // connect, when given, hands each context the DbConnection it wraps around the string.
    private async Task<Exception?> ListActiveHoldsAfterAnonymiseColumnShrinksAsync(
        TemporaryDatabase database,
        string firstConnectionString,
        Func<Task<string>> secondConnectionString,
        Func<string, DbConnection>? connect = null
    )
    {
        var connectionString = firstConnectionString;
        await using var provider = BuildAnonymiseTargetProvider(
            firstConnectionString,
            options =>
            {
                if (connect is null)
                {
                    options.UseNpgsql(connectionString);
                }
                else
                {
                    options.UseNpgsql(connect(connectionString));
                }
            }
        );

        await using var first = provider.CreateAsyncScope();
        var firstModel = first.ServiceProvider.GetRequiredService<AnonymiseTargetDbContext>().Model;
        await ListActiveHoldsAsync(first.ServiceProvider);

        await ExecuteAsync(
            database,
            "ALTER TABLE public.anonymise_literal_records ALTER COLUMN \"Label\" TYPE character varying(8)"
        );

        connectionString = await secondConnectionString();
        await using var second = provider.CreateAsyncScope();
        ReferenceEquals(firstModel, second.ServiceProvider.GetRequiredService<AnonymiseTargetDbContext>().Model)
            .Should().BeTrue("the default model cache gives both contexts one model");
        return await Record.ExceptionAsync(() => ListActiveHoldsAsync(second.ServiceProvider));
    }

    private static void AssertAnonymiseColumnRefused(Exception? thrown) =>
        thrown.Should().BeOfType<RetentionConfigurationException>()
            .Which.Errors.Should().ContainSingle(error =>
                error.Contains(typeof(AnonymiseLiteralRecord).FullName!, StringComparison.Ordinal)
                && error.Contains("member Label", StringComparison.Ordinal)
                && error.Contains("not valid for its column type character varying(8)", StringComparison.Ordinal)
            );

    private Task CreateReadRoleAsync(string role, string password) =>
        ExecuteAsync(fixture.ConnectionString, $"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'");

    private static Task GrantReadOnlyAsync(TemporaryDatabase database, string role)
    {
        var name = new NpgsqlConnectionStringBuilder(database.ConnectionString).Database!;
        return ExecuteAsync(
            database,
            $"""
            GRANT CONNECT ON DATABASE "{name}" TO "{role}";
            GRANT USAGE ON SCHEMA public TO "{role}";
            GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{role}";
            """
        );
    }

    private static string AsRole(TemporaryDatabase database, string role, string password) =>
        new NpgsqlConnectionStringBuilder(database.ConnectionString) { Username = role, Password = password }
            .ConnectionString;

    // Readiness refuses a role that lacks a privilege for any statement Cohort may send, so a
    // role that reaches it holds all of them: the reads, the writes, and the row locks (which
    // need UPDATE).
    private static async Task GrantCohortPrivilegesAsync(TemporaryDatabase database, string role)
    {
        var name = new NpgsqlConnectionStringBuilder(database.ConnectionString).Database!;
        await ExecuteAsync(
            database,
            $"""
            GRANT CONNECT ON DATABASE "{name}" TO "{role}";
            GRANT USAGE ON SCHEMA public TO "{role}";
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "{role}";
            """
        );
    }

    // ── each scope's role is checked for the statements its operation sends ────────────────────
    //
    // ScopeRoles gives every kind of scope Cohort creates a role of its own (the connecting
    // superuser switches into it when a connection opens), so these tests revoke a privilege
    // from one scope's role and see which operations notice.

    public enum CohortOperation
    {
        Sweep,
        SweepWithHandler,
        SweepWithoutPerRowAnonymise,
        CreateHold,
    }

    // The role is refused in the Cohort error for the privilege it lacks, naming the operation
    // and the role. The handler-status revoke is refused only where a run records handler
    // bookkeeping (a row handler): a host with only per-row Anonymise sends neither statement
    // for it, and neither does one with no per-row Anonymise at all.
    // Make-fails: ForSweep and ForHolds dropped from RetentionStatementReadiness.EnumerateAsync
    // (rows 1 to 4 pass instead of being refused); then HandlerBookkeeping derived from PathOf
    // (the per-row Anonymise row is refused).
    [Theory]
    [InlineData("INSERT ON public.sweep_run", CohortOperation.Sweep, "sweeps", "sweep_run", true)]
    [InlineData("UPDATE ON public.sweep_run_entity_summary", CohortOperation.Sweep, "sweeps", "sweep_run_entity_summary", true)]
    [InlineData("INSERT ON public.retention_holds", CohortOperation.CreateHold, "hold calls", "retention_holds", true)]
    [InlineData("INSERT ON public.sweep_row_handler_status", CohortOperation.SweepWithHandler, "sweeps", "sweep_row_handler_status", true)]
    [InlineData("INSERT ON public.sweep_row_handler_status", CohortOperation.Sweep, "sweeps", "sweep_row_handler_status", false)]
    [InlineData("INSERT ON public.sweep_row_handler_status", CohortOperation.SweepWithoutPerRowAnonymise, "sweeps", "sweep_row_handler_status", false)]
    public async Task Readiness_refuses_a_role_that_cannot_write_cohorts_tables(
        string revoke,
        CohortOperation operation,
        string scope,
        string table,
        bool refused
    )
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        // A hold is created in the caller's own scope; a sweep runs as the tenant.
        var role = operation == CohortOperation.CreateHold ? roles.Caller : roles.Tenant;
        await ExecuteAsync(database, $"REVOKE {revoke} FROM \"{role}\"");
        using var host = RoleHost(
            database,
            roles,
            operation == CohortOperation.SweepWithoutPerRowAnonymise ? new PurgeOnlyTombstoneRuleProvider() : null,
            configure: operation == CohortOperation.SweepWithHandler
                ? services => services.AddRowHandler<Note, NoOpNoteHandler>()
                : null
        );

        var act = () => operation == CohortOperation.CreateHold
            ? host.RunWithServicesAsync(services => services.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                new RetentionHoldRequest(Guid.NewGuid(), Note.RetentionIdentity, Guid.NewGuid().ToString(), Guid.NewGuid(), "litigation", DateTimeOffset.UtcNow),
                CancellationToken.None
            ))
            : host.RunSweepAsync(NewTenant());

        if (refused)
        {
            (await act.Should().ThrowAsync<RetentionConfigurationException>())
                .Which.Errors.Should().ContainSingle(error => RefusesCohortTables(error, scope, role, table));
        }
        else
        {
            // A sweep records a failure per entity rather than throwing.
            (await host.RunSweepAsync(NewTenant())).EntityFailures.Should().BeEmpty();
        }
    }

    // A role with only SELECT can preview (the preview's counts and the clock) and is refused at
    // a sweep (the audit rows and the host-table writes).
    // Make-fail: RetentionPreviewService validates RetentionStatementGroup.Sweep.
    [Fact]
    public async Task A_read_only_role_can_preview()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(database, $"REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public FROM \"{roles.Tenant}\"");
        using var host = RoleHost(database, roles);

        var preview = () => host.RunPreviewAsync(NewTenant());
        var sweep = () => host.RunSweepAsync(NewTenant());

        await preview.Should().NotThrowAsync();
        (await sweep.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().Contain(error =>
                error.StartsWith("Cohort's own tables and functions: ", StringComparison.Ordinal)
                && error.Contains(" in sweeps. ", StringComparison.Ordinal)
                && error.Contains($"The database role '{roles.Tenant}' lacks this privilege", StringComparison.Ordinal)
            );
    }

    // A caller role that manages holds and nothing else: SELECT, INSERT, and UPDATE on
    // retention_holds, SELECT and UPDATE (the row locks) on the host's tables, and nothing on the
    // run, summary, row-detail, and handler-status tables. Every holds operation passes: the
    // Holds group sends none of the sweep's recording.
    // Make-fail: the Holds group enumerates the Sweep group's Cohort statements
    // (CohortStatements.ForSweep in place of ForHolds); the first hold call is refused for a
    // write to sweep_run.
    [Fact]
    public async Task A_holds_only_role_can_manage_holds()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(
            database,
            $"""
            REVOKE ALL ON ALL TABLES IN SCHEMA public FROM "{roles.Caller}";
            GRANT SELECT, UPDATE ON ALL TABLES IN SCHEMA public TO "{roles.Caller}";
            REVOKE ALL ON public.sweep_run, public.sweep_run_entity_summary, public.sweep_run_row_detail,
                public.sweep_row_handler_status FROM "{roles.Caller}";
            GRANT INSERT ON public.retention_holds TO "{roles.Caller}"
            """
        );
        var tenant = NewTenant();
        var subject = Guid.NewGuid();
        var note = await SeedNoteAsync(database.ConnectionString, tenant.Id, subject);
        using var host = RoleHost(database, roles);
        var holdId = Guid.NewGuid();
        var recordId = note.ToString();

        await host.RunWithServicesAsync(services => services.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
            new RetentionHoldRequest(holdId, Note.RetentionIdentity, recordId, tenant.Id, "litigation", DateTimeOffset.UtcNow),
            CancellationToken.None
        ));
        (await host.RunWithServicesAsync(services => services.GetRequiredService<IRetentionHoldsRepository>().ListActiveAsync(CancellationToken.None)))
            .Should().ContainSingle(hold => hold.HoldId == holdId);
        (await host.RunWithServicesAsync(services => services.GetRequiredService<IRetentionHoldsRepository>()
            .HasActiveHoldAsync(Note.RetentionIdentity, recordId, tenant.Id, CancellationToken.None))).Should().BeTrue();
        (await host.RunWithServicesAsync(services => services.GetRequiredService<IRetentionHoldsRepository>()
            .ListCoveringAsync(Note.RetentionIdentity, recordId, tenant.Id, CancellationToken.None)))
            .Should().ContainSingle(hold => hold.HoldId == holdId);
        (await host.RunWithServicesAsync(services => services.GetRequiredService<IRetentionHoldsRepository>()
            .ListCoveringSubjectAsync("user", subject, tenant.Id, CancellationToken.None)))
            .Should().ContainSingle(hold => hold.HoldId == holdId);
        await host.RunWithServicesAsync(services => services.GetRequiredService<IRetentionHoldsRepository>()
            .RemoveAsync(holdId, DateTimeOffset.UtcNow, CancellationToken.None));
        (await host.RunWithServicesAsync(services => services.GetRequiredService<IRetentionHoldsRepository>().ListActiveAsync(CancellationToken.None)))
            .Should().BeEmpty();
        (await CountAsync(database, "sweep_run")).Should().Be(0, "a hold call records no run");
    }

    // A caller role that can only read retention_holds and the host's tables passes a deletion
    // of an unheld row: the Deletion group sends none of the sweep's recording, and the guard
    // reads holds only.
    // Make-fail: the Deletion group enumerates the Sweep group's Cohort statements
    // (CohortStatements.ForSweep in place of ForDeletion); the deletion is refused for a write
    // to sweep_run.
    [Fact]
    public async Task A_select_only_role_can_delete_an_unheld_row()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(
            database,
            $"""
            REVOKE ALL ON ALL TABLES IN SCHEMA public FROM "{roles.Caller}";
            GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{roles.Caller}";
            REVOKE ALL ON public.sweep_run, public.sweep_run_entity_summary, public.sweep_run_row_detail,
                public.sweep_row_handler_status FROM "{roles.Caller}"
            """
        );
        var tenant = NewTenant();
        var note = await SeedNoteAsync(database.ConnectionString, tenant.Id, Guid.NewGuid());
        using var host = RoleHost(database, roles);

        var outcome = await host.RunWithServicesAsync(services => services.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
            [new RetentionTarget(Note.RetentionIdentity, note.ToString(), tenant.Id)],
            _ => Task.CompletedTask
        ));

        outcome.Should().Be(RetentionDeletionOutcome.Executed);
    }

    // A System(Readiness) role that only reads passes startup, even with a serial row-detail Id
    // it has no sequence grant for: the port groups' privileges belong to roles startup can't
    // take on (a tenant's, say), so startup only reports a fault the database has whatever the
    // role. A retained column the database lost refuses startup. The same role, when a sweep
    // runs under it, is refused at its group pass for the privileges it lacks.
    // Make-fails: startup's port check refuses 42501 (tolerateInsufficientPrivilege false);
    // startup skips its port check; startup records its port check as a pass; startup runs
    // Decision 7's sequence rule.
    [Fact]
    public async Task Startup_refuses_a_schema_fault_but_not_a_port_privilege()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(database, $"REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public FROM \"{roles.Readiness}\"");
        await MakeRowDetailIdSerialAsync(database);
        var tenant = NewTenant();
        roles.AssignTenant(tenant.Id, roles.Readiness);
        using var host = RoleHost(database, roles);

        var start = () => host.RunWithServicesAsync(services => StartValidationAsync(services));
        await start.Should().NotThrowAsync();

        var sweep = () => host.RunSweepAsync(tenant);
        (await sweep.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().Contain(error => RefusesCohortTables(error, "sweeps", roles.Readiness, "sweep_run"));

        await ExecuteAsync(database, """ALTER TABLE public.soft_delete_records DROP COLUMN "DeletedAt" """);
        // One error per group that reads the column (sweeps, erasures, and so on), as each is
        // explained for its own scope.
        (await start.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().Contain(error =>
                RefusesWith(error, typeof(SoftDeleteRecord), "42703", "\"DeletedAt\"")
            );
    }

    // A serial Id default needs USAGE on its sequence, which EXPLAIN never checks (it never
    // calls nextval): a role without it fails the first row it inserts. An identity column needs
    // no grant.
    // Make-fail: the sequence rule removed from the Sweep and Erasure group passes.
    [Theory]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    [InlineData(false, false, false)]
    public async Task Readiness_refuses_a_serial_row_detail_id_without_sequence_usage(
        bool serial,
        bool grantUsage,
        bool refused
    )
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        if (serial)
        {
            await MakeRowDetailIdSerialAsync(database);
        }

        if (grantUsage)
        {
            await ExecuteAsync(database, $"GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO \"{roles.Tenant}\"");
        }

        using var host = RoleHost(database, roles);

        var act = () => host.RunSweepAsync(NewTenant());

        if (refused)
        {
            (await act.Should().ThrowAsync<RetentionConfigurationException>())
                .Which.Errors.Should().ContainSingle(error =>
                    error.StartsWith("Cohort's own tables and functions: ", StringComparison.Ordinal)
                    && error.Contains("\"public\".\"sweep_run_row_detail\".\"Id\"", StringComparison.Ordinal)
                    && error.Contains("sweep_run_row_detail_id_seq", StringComparison.Ordinal)
                    && error.Contains($"the database role '{roles.Tenant}' holds neither USAGE nor UPDATE on it", StringComparison.Ordinal)
                    && error.Contains("make the column an identity column", StringComparison.Ordinal)
                );
        }
        else
        {
            (await act()).EntityFailures.Should().BeEmpty();
        }
    }

    // Cohort's lock calls are functions with a privilege of their own, which PUBLIC holds
    // unless the database takes it away. Readiness binds the lock keys as real values: a NULL
    // argument would let PostgreSQL fold a strict function away unchecked.
    // Make-fail: the lock keys bound as typed NULLs in the readiness statements.
    [Fact]
    public async Task Readiness_refuses_a_role_without_execute_on_the_advisory_lock_functions()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(database, "REVOKE EXECUTE ON FUNCTION pg_catalog.pg_advisory_lock(bigint) FROM PUBLIC");
        using var host = RoleHost(database, roles);

        var act = () => host.RunSweepAsync(NewTenant());

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesCohortTables(error, "sweeps", roles.Tenant, "function pg_advisory_lock")
            );
    }

    // The stored entity identities are read in a System(Readiness) scope, which readiness
    // explains first, so a role that cannot read the holds is refused in the Cohort error
    // instead of failing the read.
    // Make-fail: the Readiness group not explained before the read (ForReadiness skipped).
    [Fact]
    public async Task Readiness_refuses_a_readiness_scope_that_cannot_read_holds()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(database, $"REVOKE SELECT ON public.retention_holds FROM \"{roles.Readiness}\"");
        using var host = RoleHost(database, roles);

        var act = () => host.RunSweepAsync(NewTenant());

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesCohortTables(error, "readiness", roles.Readiness, "retention_holds")
            );
    }

    // The model pass explains the Readiness group once per entry, under the role its scope ran
    // as. A scope hook that switches System(Readiness) to another role later is checked when it
    // first reaches the guard that reads tenantless holds: a refusal in the Cohort error, not
    // the raw 42501 of the read, and nothing deleted.
    // Make-fail: the guard's call to ValidateReadinessScopeAsync removed (the deletion throws
    // PostgresException).
    [Fact]
    public async Task A_readiness_role_switched_after_the_model_pass_is_checked_before_the_guard_reads_holds()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        using var host = RoleHost(database, roles);
        var first = await SeedTenantlessLogAsync(database.ConnectionString);
        var second = await SeedTenantlessLogAsync(database.ConnectionString);

        (await host.RunWithServicesAsync(services => DeleteTenantlessLogAsync(services, first)))
            .Should().Be(RetentionDeletionOutcome.Executed);
        var switched = await roles.UseReadinessRoleAsync(database);
        var act = () => host.RunWithServicesAsync(services => DeleteTenantlessLogAsync(services, second));

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesCohortTables(error, "readiness", switched, "retention_holds")
            );
        (await TenantlessLogExistsAsync(database.ConnectionString, second)).Should().BeTrue();
    }

    // Each role is checked the first time it reaches readiness, for what its operation sends.
    // Make-fail: ValidateAsync returns once the model pass stands (a validated entry), so the
    // second tenant's role is never checked.
    [Fact]
    public async Task Each_scopes_role_is_checked_for_what_it_sends()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        var first = NewTenant();
        var second = NewTenant();
        var secondRole = await roles.AddTenantRoleAsync(database, second.Id);
        await ExecuteAsync(database, $"REVOKE INSERT ON public.sweep_run FROM \"{secondRole}\"");
        // The dispatcher's role writes no host table: it sends only Cohort's own statements.
        await RevokeHostTableWritesAsync(database, roles.RowDispatch);
        using var host = RoleHost(database, roles);

        var firstSweep = () => host.RunSweepAsync(first);
        var secondSweep = () => host.RunSweepAsync(second);
        var flush = () => host.RunWithServicesAsync(services => services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync());

        (await firstSweep()).EntityFailures.Should().BeEmpty();
        (await secondSweep.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error => RefusesCohortTables(error, "sweeps", secondRole, "sweep_run"));
        await flush.Should().NotThrowAsync();
    }

    // A purge drops the blocking-key clause for a child table its role cannot SELECT, so the
    // statements a group sends, and the ones readiness explains, depend on the role. A role that
    // cannot read the child is checked for the statements it would send (which never name the
    // child), not for another role's, whether that role is a tenant's that swept first or the
    // readiness scope's startup check.
    // Make-fail: Enumerations keyed by the group alone (the second role is refused 42501 on the
    // child table).
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_role_that_cannot_read_a_blocking_child_is_checked_for_its_own_statements(
        bool startupFirst
    )
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await ExecuteAsync(
            database,
            """CREATE TABLE public.note_children ("NoteId" uuid NOT NULL REFERENCES public.notes ("Id"))"""
        );
        await roles.GrantAsync(database);
        var reader = NewTenant();
        await roles.AddTenantRoleAsync(database, reader.Id);
        await ExecuteAsync(database, $"REVOKE SELECT ON public.note_children FROM \"{roles.Tenant}\"");
        using var host = RoleHost(database, roles);

        if (startupFirst)
        {
            await host.RunWithServicesAsync(services => StartValidationAsync(services));
        }
        else
        {
            (await host.RunSweepAsync(reader)).EntityFailures.Should().BeEmpty();
        }

        (await host.RunSweepAsync(NewTenant())).EntityFailures.Should().BeEmpty();
    }

    // A dispatcher role that cannot claim work is refused in the Cohort error, from FlushAsync
    // and from a poll alike, but only after the payload scrub has run: personal data never
    // outlives its retention because the dispatcher's statements are refused. The tenant
    // role's sweeps are untouched.
    // A refused scrub leaves the poll's hourly gate unspent, and under the kill switch the poll
    // scrubs without checking Dispatch.
    // Make-fails: Dispatch not validated by FlushAsync and PollAsync; then Dispatch validated
    // before the scrub in both; then PollAsync's Scrub validation moved below
    // lastPayloadScrubAt = now; then its Dispatch validation moved above the kill-switch return.
    [Fact]
    public async Task The_dispatcher_is_refused_under_a_role_that_cannot_claim_work()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(database, $"REVOKE UPDATE ON public.sweep_row_handler_status FROM \"{roles.RowDispatch}\"");
        using var host = RoleHost(database, roles);
        var viaPoll = await SeedExpiredPayloadAsync(database);

        var poll = () => host.RunWithServicesAsync(services => services.GetRequiredService<RetentionRowDispatcher>().PollAsync(CancellationToken.None));
        var flush = () => host.RunWithServicesAsync(services => services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync());

        (await poll.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesCohortTables(error, "row-handler dispatch", roles.RowDispatch, "sweep_row_handler_status")
            );
        (await CapturedPayloadAsync(database, viaPoll)).Should().BeNull("the poll scrubbed before it refused");
        var viaFlush = await SeedExpiredPayloadAsync(database);
        (await flush.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesCohortTables(error, "row-handler dispatch", roles.RowDispatch, "sweep_row_handler_status")
            );
        (await CapturedPayloadAsync(database, viaFlush)).Should().BeNull("flush scrubbed before it refused");
        (await host.RunSweepAsync(NewTenant())).EntityFailures.Should().BeEmpty();

        // A poll whose scrub is refused leaves the hourly gate unspent: once the role can scrub,
        // the next poll does, though its dispatcher statements are still refused.
        await ExecuteAsync(database, $"REVOKE UPDATE ON public.sweep_run_row_detail FROM \"{roles.RowDispatch}\"");
        using var scrubHost = RoleHost(database, roles);
        var viaRefusedScrub = await SeedExpiredPayloadAsync(database);
        var scrubPoll = () => scrubHost.RunWithServicesAsync(services => services.GetRequiredService<RetentionRowDispatcher>().PollAsync(CancellationToken.None));

        (await scrubPoll.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesCohortTables(error, "the payload scrub", roles.RowDispatch, "sweep_run_row_detail")
            );
        (await CapturedPayloadAsync(database, viaRefusedScrub)).Should().NotBeNull("the refused scrub changed nothing");
        await ExecuteAsync(database, $"GRANT UPDATE ON public.sweep_run_row_detail TO \"{roles.RowDispatch}\"");
        (await scrubPoll.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesCohortTables(error, "row-handler dispatch", roles.RowDispatch, "sweep_row_handler_status")
            );
        (await CapturedPayloadAsync(database, viaRefusedScrub)).Should().BeNull("the refused poll did not spend the scrub's hourly gate");

        // Under the kill switch the dispatcher doesn't run, so a refusal of its statements stops
        // nothing: the poll scrubs and returns.
        using var killedHost = RoleHost(
            database,
            roles,
            configuration: new Dictionary<string, string?> { [$"{CohortOptions.SectionName}:KillSwitch"] = "true" }
        );
        var underKillSwitch = await SeedExpiredPayloadAsync(database);
        var killedPoll = () => killedHost.RunWithServicesAsync(services => services.GetRequiredService<RetentionRowDispatcher>().PollAsync(CancellationToken.None));

        await killedPoll.Should().NotThrowAsync();
        (await CapturedPayloadAsync(database, underKillSwitch)).Should().BeNull("the kill switch never keeps a payload past its retention");
    }

    // The worker's ledger read is the worker's group, checked when an iteration starts, before
    // the iteration reads the clock or runs any pass.
    // Make-fail: RetentionWorker.RunIterationAsync does not validate the Worker group.
    [Fact]
    public async Task The_worker_is_refused_under_a_role_that_cannot_read_the_ledger()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(database, $"REVOKE SELECT ON public.sweep_run FROM \"{roles.WorkerCoordination}\"");
        using var host = RoleHost(database, roles);

        var act = () => host.RunWithServicesAsync(services => services
            .GetServices<IHostedService>().OfType<RetentionWorker>().Single()
            .RunScheduledIterationAsync("0 0 0 * * *", dryRun: false, settled: null, CancellationToken.None));

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesCohortTables(error, "the scheduled worker", roles.WorkerCoordination, "sweep_run")
            );
        (await CountAsync(database, "sweep_run")).Should().Be(0, "no pass ran");
    }

    public enum PruningCase
    {
        HoldsOnly,
        RunsConfigured,
        HoldsOnlyThenRunsByReload,
    }

    // The pruner's role is checked for the retentions it prunes. Without DELETE on the run
    // tables, a pruner that only prunes inactive holds is fine, one that prunes settled runs is
    // refused before it deletes anything, and a reload that adds the run retention is caught at
    // the next prune.
    // Make-fails: Pruning always enumerated with PruningScope(true, true, ...); then the
    // Pruning enumeration kept without its scope.
    [Theory]
    [InlineData(PruningCase.HoldsOnly)]
    [InlineData(PruningCase.RunsConfigured)]
    [InlineData(PruningCase.HoldsOnlyThenRunsByReload)]
    public async Task Pruning_is_checked_only_for_the_retentions_it_prunes(PruningCase pruningCase)
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(
            database,
            $"""REVOKE DELETE ON public.sweep_run, public.sweep_run_entity_summary, public.sweep_run_row_detail FROM "{roles.HistoryPruning}" """
        );
        await SeedSettledRunAsync(database, DateTimeOffset.UtcNow.AddDays(-100));
        await SeedInactiveHoldAsync(database, DateTimeOffset.UtcNow.AddDays(-100));
        var settings = new Dictionary<string, string?>
        {
            [$"{CohortOptions.SectionName}:HistoryPruning:InactiveHoldRetention"] = "1.00:00:00",
        };
        if (pruningCase == PruningCase.RunsConfigured)
        {
            settings[$"{CohortOptions.SectionName}:HistoryPruning:SucceededRunRetention"] = "1.00:00:00";
        }

        using var host = RoleHost(database, roles, configuration: settings);
        var prune = () => host.RunWithServicesAsync(services => services.GetRequiredService<RetentionHistoryPruner>().PruneAsync());

        if (pruningCase == PruningCase.HoldsOnly)
        {
            (await prune()).Should().Be(new RetentionHistoryPruneResult(0, 1));
            (await CountAsync(database, "sweep_run")).Should().Be(1);
            return;
        }

        if (pruningCase == PruningCase.HoldsOnlyThenRunsByReload)
        {
            (await prune()).Should().Be(new RetentionHistoryPruneResult(0, 1));
            await host.RunWithServicesAsync(services =>
            {
                var configuration = (IConfigurationRoot)services.GetRequiredService<IConfiguration>();
                configuration[$"{CohortOptions.SectionName}:HistoryPruning:SucceededRunRetention"] = "1.00:00:00";
                configuration.Reload();
                return Task.CompletedTask;
            });
        }

        (await prune.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesCohortTables(error, "history pruning", roles.HistoryPruning, "sweep_run")
            );
        (await CountAsync(database, "sweep_run")).Should().Be(1, "nothing was deleted");
        if (pruningCase == PruningCase.RunsConfigured)
        {
            // The reload case already pruned its inactive hold.
            (await CountAsync(database, "retention_holds")).Should().Be(1, "refused before any delete, holds included");
        }
    }

    // Startup checks, in its own scope and under its own role, each background subsystem that
    // will run: a dispatcher role that cannot claim work stops the host, unless the kill switch
    // is on (the dispatcher doesn't run); a worker role that cannot read the ledger stops a host
    // with a schedule, unless the kill switch is on (the worker doesn't run); a pruner role
    // without DELETE does not while pruning is off.
    // Make-fails: the startup groups skipped (RetentionValidationHostedService); then Pruning
    // checked unconditionally; then the Worker check deleted; then Dispatch checked whatever the
    // kill switch says.
    [Fact]
    public async Task Startup_checks_each_group_that_will_run_in_its_own_scope()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(
            database,
            $"""REVOKE DELETE ON public.sweep_run, public.sweep_run_entity_summary, public.sweep_run_row_detail FROM "{roles.HistoryPruning}" """
        );
        using var pruningOff = RoleHost(database, roles);
        await pruningOff.RunWithServicesAsync(services => StartValidationAsync(services));

        await ExecuteAsync(database, $"REVOKE SELECT ON public.sweep_run FROM \"{roles.WorkerCoordination}\"");
        var scheduled = new Dictionary<string, string?> { [$"{CohortOptions.SectionName}:Schedule"] = "0 0 0 * * *" };
        var scheduledKilled = new Dictionary<string, string?>(scheduled) { [$"{CohortOptions.SectionName}:KillSwitch"] = "true" };
        using var worker = RoleHost(database, roles, configuration: scheduled);
        using var workerKilled = RoleHost(database, roles, configuration: scheduledKilled);

        var startWorker = () => worker.RunWithServicesAsync(services => StartValidationAsync(services));
        var startWorkerKilled = () => workerKilled.RunWithServicesAsync(services => StartValidationAsync(services));

        (await startWorker.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesCohortTables(error, "the scheduled worker", roles.WorkerCoordination, "sweep_run")
            );
        await startWorkerKilled.Should().NotThrowAsync("the worker does not run under the kill switch");

        await ExecuteAsync(database, $"REVOKE UPDATE ON public.sweep_row_handler_status FROM \"{roles.RowDispatch}\"");
        using var host = RoleHost(database, roles);
        using var dispatcherKilled = RoleHost(
            database,
            roles,
            configuration: new Dictionary<string, string?> { [$"{CohortOptions.SectionName}:KillSwitch"] = "true" }
        );

        var start = () => host.RunWithServicesAsync(services => StartValidationAsync(services));
        var startDispatcherKilled = () => dispatcherKilled.RunWithServicesAsync(services => StartValidationAsync(services));

        (await start.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesCohortTables(error, "row-handler dispatch", roles.RowDispatch, "sweep_row_handler_status")
            );
        await startDispatcherKilled.Should().NotThrowAsync("the dispatcher does not run under the kill switch");
    }

    private CohortTestHost RoleHost(
        TemporaryDatabase database,
        ScopeRoles roles,
        IRetentionRuleProvider? ruleProvider = null,
        IReadOnlyDictionary<string, string?>? configuration = null,
        Action<IServiceCollection>? configure = null
    ) =>
        new(
            database.ConnectionString,
            ruleProvider,
            configuration,
            services =>
            {
                roles.Install<SampleDbContext>(services);
                configure?.Invoke(services);
            }
        );

    private static TenantContext NewTenant() =>
        new(Guid.NewGuid(), "uk", new Dictionary<string, string>());

    private static async Task<Guid> SeedTenantlessLogAsync(string connectionString)
    {
        await using var db = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>().UseNpgsql(connectionString).Options
        );
        var log = new TenantlessLog { Id = Guid.NewGuid(), CreatedAt = DateTimeOffset.UtcNow, Payload = "readiness" };
        db.TenantlessLogs.Add(log);
        await db.SaveChangesAsync();
        return log.Id;
    }

    private static async Task<Guid> SeedNoteAsync(string connectionString, Guid tenantId, Guid subjectId)
    {
        await using var db = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>().UseNpgsql(connectionString).Options
        );
        var note = new Note
        {
            Id = Guid.NewGuid(),
            TenantId = tenantId,
            SubjectId = subjectId,
            CreatedAt = DateTimeOffset.UtcNow,
            Body = "readiness",
        };
        db.Notes.Add(note);
        await db.SaveChangesAsync();
        return note.Id;
    }

    private static async Task<bool> TenantlessLogExistsAsync(string connectionString, Guid id)
    {
        await using var db = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>().UseNpgsql(connectionString).Options
        );
        return await db.TenantlessLogs.AnyAsync(log => log.Id == id);
    }

    // Deletes one tenantless row through the deletion port, the callback removing it for real.
    private static Task<RetentionDeletionOutcome> DeleteTenantlessLogAsync(IServiceProvider services, Guid id)
    {
        var db = services.GetRequiredService<SampleDbContext>();
        return services.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
            [new RetentionTarget(RetentionEntityIdentity.For<TenantlessLog>(), id.ToString(), null)],
            async ct => await db.TenantlessLogs.Where(log => log.Id == id).ExecuteDeleteAsync(ct)
        );
    }

    // Cohort's error for statements against its own tables and functions: the operation, the
    // PostgreSQL message, and the role that lacks the privilege.
    private static bool RefusesCohortTables(string error, string scope, string role, string denied) =>
        error.StartsWith("Cohort's own tables and functions: PostgreSQL refused statements Cohort sends in ", StringComparison.Ordinal)
        && error.Contains($" in {scope}. ", StringComparison.Ordinal)
        && error.Contains($" 42501: permission denied for {(denied.StartsWith("function ", StringComparison.Ordinal) ? denied : $"table {denied}")}", StringComparison.Ordinal)
        && error.Contains($"The database role '{role}' lacks this privilege", StringComparison.Ordinal);

    // Runs the hosted startup check, which a host runs once before it serves.
    private static Task StartValidationAsync(IServiceProvider services) =>
        services.GetServices<IHostedService>().OfType<RetentionValidationHostedService>().Single()
            .StartAsync(CancellationToken.None);

    // A bigserial default on the row-detail Id, which the schema validator accepts and which
    // needs USAGE on the sequence.
    private static Task MakeRowDetailIdSerialAsync(TemporaryDatabase database) =>
        ExecuteAsync(
            database,
            """
            ALTER TABLE public.sweep_run_row_detail ALTER COLUMN "Id" DROP IDENTITY;
            CREATE SEQUENCE public.sweep_run_row_detail_id_seq OWNED BY public.sweep_run_row_detail."Id";
            ALTER TABLE public.sweep_run_row_detail
                ALTER COLUMN "Id" SET DEFAULT nextval('public.sweep_run_row_detail_id_seq')
            """
        );

    // Everything but Cohort's five tables: the host's own.
    private static Task RevokeHostTableWritesAsync(TemporaryDatabase database, string role) =>
        ExecuteAsync(
            database,
            $$"""
            DO $$
            DECLARE name text;
            BEGIN
                FOR name IN
                    SELECT tablename FROM pg_catalog.pg_tables
                    WHERE schemaname = 'public'
                      AND tablename NOT IN ('retention_holds', 'sweep_run', 'sweep_run_entity_summary',
                                            'sweep_run_row_detail', 'sweep_row_handler_status')
                LOOP
                    EXECUTE format('REVOKE INSERT, UPDATE, DELETE ON public.%I FROM %I', name, '{{role}}');
                END LOOP;
            END
            $$
            """
        );

    // A run's row detail whose captured payload is past PayloadRetention (30 days by default).
    private static async Task<long> SeedExpiredPayloadAsync(TemporaryDatabase database)
    {
        var sweepId = Guid.NewGuid();
        var at = DateTimeOffset.UtcNow.AddDays(-60);
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO "sweep_run"
                ("SweepId", "StartedAt", "Status", "SettledAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected")
            VALUES (@sweepId, @at, 1, @at, INTERVAL '0', 2, FALSE, @tenantId, 1);
            INSERT INTO "sweep_run_row_detail"
                ("SweepId", "At", "EntityType", "RetentionEntityId", "RecordId", "Category", "Strategy", "TenantId", "CapturedPayload")
            VALUES (@sweepId, @at, 'Cohort.Sample.Entities.Note', @entityId, @recordId, 'short-lived', 0, @tenantId, '{"body":"personal"}')
            RETURNING "Id";
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("at", at);
        command.Parameters.AddWithValue("tenantId", Guid.NewGuid());
        command.Parameters.AddWithValue("entityId", Note.RetentionIdentity);
        command.Parameters.AddWithValue("recordId", Guid.NewGuid().ToString());
        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }

    private static async Task<string?> CapturedPayloadAsync(TemporaryDatabase database, long rowDetailId)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """SELECT "CapturedPayload" FROM "sweep_run_row_detail" WHERE "Id" = @id""",
            connection
        );
        command.Parameters.AddWithValue("id", rowDetailId);
        return await command.ExecuteScalarAsync() as string;
    }

    private static async Task SeedSettledRunAsync(TemporaryDatabase database, DateTimeOffset settledAt)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO "sweep_run"
                ("SweepId", "StartedAt", "Status", "SettledAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected")
            VALUES (@sweepId, @settledAt - INTERVAL '1 minute', 1, @settledAt, INTERVAL '1 minute', 2, FALSE, @tenantId, 0)
            """,
            connection
        );
        command.Parameters.AddWithValue("sweepId", Guid.NewGuid());
        command.Parameters.AddWithValue("settledAt", settledAt);
        command.Parameters.AddWithValue("tenantId", Guid.NewGuid());
        await command.ExecuteNonQueryAsync();
    }

    private static async Task SeedInactiveHoldAsync(TemporaryDatabase database, DateTimeOffset removedAt)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO "retention_holds"
                ("HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt")
            VALUES (@holdId, @entityId, @recordId, @tenantId, 'pruning test', @removedAt - INTERVAL '1 day', NULL, @removedAt)
            """,
            connection
        );
        command.Parameters.AddWithValue("holdId", Guid.NewGuid());
        command.Parameters.AddWithValue("entityId", Note.RetentionIdentity);
        command.Parameters.AddWithValue("recordId", Guid.NewGuid().ToString());
        command.Parameters.AddWithValue("tenantId", Guid.NewGuid());
        command.Parameters.AddWithValue("removedAt", removedAt);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<long> CountAsync(TemporaryDatabase database, string table)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"""SELECT count(*) FROM public."{table}" """, connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    [RowHandlerId("3f6a9c12-5d84-4b07-a1e3-8c2d7b5f0e49")]
    private sealed class NoOpNoteHandler : IRetentionHandler<Note>;

    // The sample's rules, except that its per-row Anonymise category declares and resolves Purge.
    private sealed class PurgeOnlyTombstoneRuleProvider : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider sample = new();

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == "tombstone-anonymise"
                ? new RetentionCategoryCapabilities([Strategy.Purge])
                : sample.GetCapabilities(category);

        public Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct) =>
            context.Category == "tombstone-anonymise"
                ? Task.FromResult<RetentionRule?>(new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge))
                : sample.ResolveAsync(context, ct);
    }

    // T6: a check that passed inside a caller's transaction may have seen DDL that rolls back.
    [Fact]
    public async Task A_check_passed_inside_a_caller_transaction_is_not_kept()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await ExecuteAsync(
            database,
            "ALTER TABLE public.sweep_run_row_detail RENAME TO malformed_sweep_run_row_detail"
        );
        await using var provider = BuildModelCopyProvider(database.ConnectionString);

        await using (var first = provider.CreateAsyncScope())
        {
            var db = first.ServiceProvider.GetRequiredService<SampleDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            await db.Database.ExecuteSqlRawAsync(
                "ALTER TABLE public.malformed_sweep_run_row_detail RENAME TO sweep_run_row_detail"
            );

            await ListActiveHoldsAsync(first.ServiceProvider);

            await transaction.RollbackAsync();
        }

        await using var second = provider.CreateAsyncScope();
        var act = () => ListActiveHoldsAsync(second.ServiceProvider);

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                error.Contains("missing Cohort runtime capabilities", StringComparison.Ordinal)
                && error.Contains("sweep_run_row_detail", StringComparison.Ordinal)
            );
    }

    // Readiness explains its statements inside the caller's transaction, and PostgreSQL aborts a
    // transaction on the first error: each EXPLAIN is followed by a rollback to a savepoint, so a
    // refusal leaves the caller's transaction as usable as it found it. A wrapped DbConnection
    // can't batch, so its statements go one at a time with a savepoint loop of their own. The
    // erasure runs in the caller's own transaction and sends the statements that read the column
    // (a hold listing sends none of them).
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_statement_refused_inside_a_caller_transaction_leaves_it_usable(bool wrapped)
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await ExecuteAsync(database, """ALTER TABLE public.soft_delete_records DROP COLUMN "DeletedAt" """);
        var wrappers = new List<PassThroughConnection>();
        await using var provider = BuildModelCopyProvider(
            database.ConnectionString,
            connect: WrapWhen(wrapped, wrappers)
        );

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<SampleDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();

            var act = () => EraseInCallerTransactionAsync(scope.ServiceProvider);

            (await act.Should().ThrowAsync<RetentionConfigurationException>())
                .Which.Errors.Should().ContainSingle(error =>
                    RefusesWith(error, typeof(SoftDeleteRecord), "42703", "\"DeletedAt\"")
                );
            (await db.Database.SqlQueryRaw<int>("SELECT 1 AS \"Value\"").SingleAsync()).Should().Be(1);
            await transaction.RollbackAsync();
            (wrappers.Count > 0).Should().Be(wrapped, "the context runs on a wrapper exactly when wrapped");
        }
        finally
        {
            foreach (var wrapper in wrappers)
            {
                await wrapper.DisposeAsync();
            }
        }
    }

    // Every refused entity is reported, not the first: the error barriers keep the batch running
    // past a failed EXPLAIN, and the savepoint rollback does the same one at a time.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Readiness_reports_every_refused_entity_in_one_error_list(bool wrapped)
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await ExecuteAsync(
            database,
            """
            ALTER TABLE public.soft_delete_records DROP COLUMN "DeletedAt";
            ALTER TABLE public.tenantless_logs DROP COLUMN "CreatedAt" CASCADE;
            """
        );
        var wrappers = new List<PassThroughConnection>();
        await using var provider = BuildModelCopyProvider(
            database.ConnectionString,
            connect: WrapWhen(wrapped, wrappers)
        );

        try
        {
            var errors = await ReadinessErrorsAsync(provider);

            errors.Should().HaveCount(2);
            errors.Should().ContainSingle(error =>
                RefusesWith(error, typeof(SoftDeleteRecord), "42703", "\"DeletedAt\"")
            );
            errors.Should().ContainSingle(error =>
                RefusesWith(error, typeof(TenantlessLog), "42703", "\"CreatedAt\"")
            );
            (wrappers.Count > 0).Should().Be(wrapped, "the context runs on a wrapper exactly when wrapped");
        }
        finally
        {
            foreach (var wrapper in wrappers)
            {
                await wrapper.DisposeAsync();
            }
        }
    }

    // Not every refusal is a class 42 error. Plain EXPLAIN still starts the executor, which checks
    // each table an UPDATE or DELETE targets: a retained table in a publication that publishes
    // updates and deletes, with no replica identity, can take neither (SQLSTATE 55000). Every run
    // would hit that, so readiness refuses it, naming the entity, as it refuses a missing column.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Readiness_refuses_a_retained_table_whose_publication_cannot_take_its_updates(bool wrapped)
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await ExecuteAsync(
            database,
            """
            ALTER TABLE public.soft_delete_records REPLICA IDENTITY NOTHING;
            CREATE PUBLICATION cohort_readiness_pub FOR TABLE public.soft_delete_records WITH (publish = 'update, delete');
            """
        );
        var wrappers = new List<PassThroughConnection>();
        await using var provider = BuildModelCopyProvider(
            database.ConnectionString,
            connect: WrapWhen(wrapped, wrappers)
        );

        try
        {
            var errors = await ReadinessErrorsAsync(provider);

            errors.Should().ContainSingle(error =>
                RefusesWith(error, typeof(SoftDeleteRecord), "55000", "replica identity")
            );
            (wrappers.Count > 0).Should().Be(wrapped, "the context runs on a wrapper exactly when wrapped");
        }
        finally
        {
            foreach (var wrapper in wrappers)
            {
                await wrapper.DisposeAsync();
            }
        }
    }

    // Only a rule violation (SQLSTATE class 42 and the like) says the host's schema is wrong. A
    // lock timeout says nothing about it, so readiness lets it propagate as PostgreSQL raised it
    // (a PostgresException on a plain connection and on a wrapped one, even though the batch path
    // sees one failure per EXPLAIN that reads the table), and leaves a caller's transaction
    // usable, as a refusal does. A second connection holds an ACCESS EXCLUSIVE lock on a retained
    // table, so each EXPLAIN that reads it waits for 200 ms and times out.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Readiness_lets_an_operational_failure_propagate_instead_of_refusing(bool wrapped)
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        var shortLockTimeout = new NpgsqlConnectionStringBuilder(database.ConnectionString)
        {
            Options = "-c lock_timeout=200",
        }.ConnectionString;
        var wrappers = new List<PassThroughConnection>();
        await using var provider = BuildModelCopyProvider(shortLockTimeout, connect: WrapWhen(wrapped, wrappers));
        await using var blocker = new NpgsqlConnection(database.ConnectionString);
        await blocker.OpenAsync();
        await using var blocking = await blocker.BeginTransactionAsync();
        await using (var lockTable = new NpgsqlCommand(
            "LOCK TABLE public.soft_delete_records IN ACCESS EXCLUSIVE MODE",
            blocker,
            blocking
        ))
        {
            await lockTable.ExecuteNonQueryAsync();
        }

        try
        {
            var ownTransaction = () => provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(
                RetentionSweepRequest.Tenantless()
            );

            (await ownTransaction.Should().ThrowAsync<PostgresException>())
                .Which.SqlState.Should().Be(PostgresErrorCodes.LockNotAvailable);

            await using var scope = provider.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<SampleDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            var inCallerTransaction = () => ListActiveHoldsAsync(scope.ServiceProvider);

            (await inCallerTransaction.Should().ThrowAsync<PostgresException>())
                .Which.SqlState.Should().Be(PostgresErrorCodes.LockNotAvailable);
            (await db.Database.SqlQueryRaw<int>("SELECT 1 AS \"Value\"").SingleAsync()).Should().Be(1);
            await transaction.RollbackAsync();
            (wrappers.Count > 0).Should().Be(wrapped, "the context runs on a wrapper exactly when wrapped");
        }
        finally
        {
            await blocking.RollbackAsync();
            foreach (var wrapper in wrappers)
            {
                await wrapper.DisposeAsync();
            }
        }
    }

    // A wrapped DbConnection can't batch, so its statements are explained one at a time.
    [Fact]
    public async Task Readiness_checks_statements_over_a_wrapped_connection()
    {
        await using var database = await AnonymiseTargetDatabaseAsync(
            """ALTER TABLE public.anonymise_literal_records DROP COLUMN "Label" """
        );
        var wrappers = new List<PassThroughConnection>();
        await using var provider = BuildAnonymiseTargetProvider(
            database.ConnectionString,
            options =>
            {
                var wrapper = new PassThroughConnection(new NpgsqlConnection(database.ConnectionString));
                wrappers.Add(wrapper);
                options.UseNpgsql(wrapper);
            }
        );

        try
        {
            var errors = await ReadinessErrorsAsync(provider);

            errors.Should().ContainSingle(error =>
                RefusesWith(error, typeof(AnonymiseLiteralRecord), "42703", "\"Label\"")
            );
            wrappers.Should().NotBeEmpty("the context runs on the wrapper, not on an NpgsqlConnection");
        }
        finally
        {
            foreach (var wrapper in wrappers)
            {
                await wrapper.DisposeAsync();
            }
        }
    }

    // T7: the anonymise check reads session state no key captures, so every model copy runs it.
    [Fact]
    public async Task A_rebuilt_copy_runs_the_anonymise_check_again()
    {
        await using var database = await AnonymiseTargetDatabaseAsync();
        await using var provider = BuildAnonymiseTargetProvider(
            database.ConnectionString,
            options => options.UseMemoryCache(ModelCacheThatHoldsNothing)
        );

        await using var first = provider.CreateAsyncScope();
        var firstModel = first.ServiceProvider.GetRequiredService<AnonymiseTargetDbContext>().Model;
        await ListActiveHoldsAsync(first.ServiceProvider);

        await ExecuteAsync(
            database,
            "ALTER TABLE public.anonymise_literal_records ALTER COLUMN \"Label\" TYPE character varying(8)"
        );

        await using var second = provider.CreateAsyncScope();
        ReferenceEquals(firstModel, second.ServiceProvider.GetRequiredService<AnonymiseTargetDbContext>().Model)
            .Should().BeFalse("the full cache gives every context a model of its own");
        var act = () => ListActiveHoldsAsync(second.ServiceProvider);

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                error.Contains(typeof(AnonymiseLiteralRecord).FullName!, StringComparison.Ordinal)
                && error.Contains("member Label", StringComparison.Ordinal)
                && error.Contains("not valid for its column type character varying(8)", StringComparison.Ordinal)
            );
    }

    // What a scope changes about the options of the context it resolves, set before the scope
    // resolves it: here, the model customizer that makes the context run a variant model.
    private sealed class ModelVariant
    {
        public Action<DbContextOptionsBuilder>? Configure { get; private set; }

        public void CustomizeModelWith<TCustomizer>()
            where TCustomizer : class, IModelCustomizer =>
            Configure = options => options.ReplaceService<IModelCustomizer, TCustomizer>();
    }

    // Maps a retained entity to a table the database doesn't have.
    private sealed class RetainedEntityElsewhereModelCustomizer(ModelCustomizerDependencies dependencies)
        : ModelCustomizer(dependencies)
    {
        public override void Customize(ModelBuilder modelBuilder, DbContext context)
        {
            base.Customize(modelBuilder, context);
            modelBuilder.Entity<Note>().ToTable("notes_elsewhere");
        }
    }

    // Stops registering a retained entity.
    private sealed class RetainedEntityIgnoredModelCustomizer(ModelCustomizerDependencies dependencies)
        : ModelCustomizer(dependencies)
    {
        public override void Customize(ModelBuilder modelBuilder, DbContext context)
        {
            base.Customize(modelBuilder, context);
            modelBuilder.Ignore<Note>();
        }
    }

    // Maps a hold relation's link member to a column the database doesn't have.
    private sealed class HoldLinkColumnElsewhereModelCustomizer(ModelCustomizerDependencies dependencies)
        : ModelCustomizer(dependencies)
    {
        public override void Customize(ModelBuilder modelBuilder, DbContext context)
        {
            base.Customize(modelBuilder, context);
            modelBuilder.Entity<HoldLinkChild>().Property(child => child.CaseId).HasColumnName("CaseIdElsewhere");
        }
    }

    // Maps a soft-delete flag to a second column the test's DDL made foreign-key-referenced.
    private sealed class SoftDeleteFlagInReferencedColumnModelCustomizer(ModelCustomizerDependencies dependencies)
        : ModelCustomizer(dependencies)
    {
        public override void Customize(ModelBuilder modelBuilder, DbContext context)
        {
            base.Customize(modelBuilder, context);
            modelBuilder
                .Entity<SoftDeleteRecord>()
                .Property(record => record.IsDeleted)
                .HasColumnName("IsDeletedReferenced");
        }
    }

    // Every scope builds a model of its own, since the shared cache holds nothing. A scope's
    // ModelVariant, set before the scope resolves its context, makes that context run a variant
    // of the model. Readiness is triggered through ListActiveHoldsAsync, because IRetentionSweep
    // and IRetentionPreview open scopes of their own.
    private static ServiceProvider BuildModelCopyProvider(
        string connectionString,
        IRetentionRuleProvider? ruleProvider = null,
        IRetentionExecutionScope? executionScope = null,
        Func<string, DbConnection>? connect = null
    ) =>
        BuildModelCopyProvider<SampleDbContext>(
            connectionString,
            ruleProvider ?? new SampleRetentionRuleProvider(),
            services =>
            {
                services.AddSingleton<GuidTombstoneFactory>();
                services.AddSingleton<OriginalValueTombstoneFactory>();
                services.AddSingleton<IAnonymiseValueFactory>(provider =>
                    provider.GetRequiredService<GuidTombstoneFactory>()
                );
                services.AddSingleton<IAnonymiseValueFactory>(provider =>
                    provider.GetRequiredService<OriginalValueTombstoneFactory>()
                );
            },
            executionScope,
            connect
        );

    // The same over any context, such as one whose model carries a hold relation, which the
    // sample's doesn't. register adds what the context's retained entities need. connect, when
    // given, hands each context the DbConnection it wraps around the string.
    private static ServiceProvider BuildModelCopyProvider<TContext>(
        string connectionString,
        IRetentionRuleProvider ruleProvider,
        Action<IServiceCollection>? register = null,
        IRetentionExecutionScope? executionScope = null,
        Func<string, DbConnection>? connect = null
    )
        where TContext : DbContext
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddScoped<ModelVariant>();
        services.AddDbContext<TContext>(
            (provider, options) => UseModelCopy(options, provider, connectionString, connect)
        );
        services.AddSingleton(ruleProvider);
        register?.Invoke(services);
        services.AddSingleton<IRetentionExecutionScope>(executionScope ?? RetentionExecutionScope.Unisolated);
        services.AddCohort<TContext>();
        return services.BuildServiceProvider(validateScopes: true);
    }

    private static void UseModelCopy(
        DbContextOptionsBuilder options,
        IServiceProvider scopeServices,
        string connectionString,
        Func<string, DbConnection>? connect = null
    )
    {
        if (connect is null)
        {
            options.UseNpgsql(connectionString);
        }
        else
        {
            options.UseNpgsql(connect(connectionString));
        }

        options.UseMemoryCache(ModelCacheThatHoldsNothing);
        scopeServices.GetRequiredService<ModelVariant>().Configure?.Invoke(options);
    }

    // Hands each context a PassThroughConnection (not an NpgsqlConnection, so readiness explains
    // one statement at a time) when wrapped, and keeps it for the test to dispose.
    private static Func<string, DbConnection>? WrapWhen(bool wrapped, List<PassThroughConnection> wrappers) =>
        wrapped
            ? connectionString =>
            {
                var wrapper = new PassThroughConnection(new NpgsqlConnection(connectionString));
                wrappers.Add(wrapper);
                return wrapper;
            }
            : null;

    private static Task EraseInCallerTransactionAsync(IServiceProvider scopeServices) =>
        scopeServices.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
            RetentionErasureRequest.Tenanted(
                new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                new ErasureScope("user", Guid.NewGuid(), allowSoftDeleteAsErasure: true),
                transaction: ErasureTransaction.Caller
            )
        );

    private static Task ListActiveHoldsAsync(IServiceProvider scopeServices) =>
        scopeServices.GetRequiredService<IRetentionHoldsRepository>().ListActiveAsync(CancellationToken.None);

    // A separate method that returns only the weak reference, so nothing in the caller keeps the
    // scope, the context, or the model alive.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static async Task<WeakReference> ReadinessModelInDisposedScopeAsync(ServiceProvider provider)
    {
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<SampleDbContext>();
        await ListActiveHoldsAsync(scope.ServiceProvider);
        return new WeakReference(db.Model);
    }

    private static Task ExecuteAsync(TemporaryDatabase database, string sql) =>
        ExecuteAsync(database.ConnectionString, sql);

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<long> HoldsTableOidAsync(TemporaryDatabase database)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT to_regclass('public.retention_holds')::oid::bigint", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    // The same server under another name: a host name becomes one of its addresses, a loopback
    // address becomes "localhost", and any other address its reverse-DNS name.
    private static string AlternateSpelling(string host)
    {
        var alternate = System.Net.IPAddress.TryParse(host, out var address)
            ? System.Net.IPAddress.IsLoopback(address)
                ? "localhost"
                : System.Net.Dns.GetHostEntry(address).HostName
            : System.Net.Dns.GetHostAddresses(host)
                .OrderBy(candidate => candidate.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork)
                .FirstOrDefault()
                ?.ToString();
        if (alternate is null || string.Equals(alternate, host, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Test needs a second spelling of the database host '{host}' and found none."
            );
        }

        return alternate.Contains(':') ? $"[{alternate}]" : alternate;
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
            executionScope.Ran.Should().ContainSingle(context => context == readiness);

            await holds.ListActiveAsync(CancellationToken.None);
            executionScope.Ran.Should().ContainSingle(context => context == readiness);
        });
    }

    public enum NoMappingEntryPoint
    {
        HostStartup,
        Sweep,
        ListActiveHolds,
        CreateHold,
        HasActiveHold,
    }

    // Startup validation runs before readiness reads anything from the Cohort tables' mapping, so
    // a model with none gets the aggregated refusal that says what to call, not a bare exception.
    // Covers host startup, a sweep, and three holds-repository calls: listing, creating a hold,
    // and asking whether a row is held. The last two resolve a retained entity's target, which
    // reads the mapping, so they only pass when readiness runs before that resolve.
    [Theory]
    [InlineData(NoMappingEntryPoint.HostStartup)]
    [InlineData(NoMappingEntryPoint.Sweep)]
    [InlineData(NoMappingEntryPoint.ListActiveHolds)]
    [InlineData(NoMappingEntryPoint.CreateHold)]
    [InlineData(NoMappingEntryPoint.HasActiveHold)]
    public async Task Readiness_refuses_a_model_with_no_cohort_table_mappings_with_the_aggregated_error(
        NoMappingEntryPoint entryPoint
    )
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddDbContext<NoCohortTablesDbContext>(builder => builder.UseNpgsql(fixture.ConnectionString));
        services.AddSingleton<IRetentionRuleProvider>(new SessionKeyRuleProvider());
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<NoCohortTablesDbContext>();
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        await using var scope = provider.CreateAsyncScope();
        Func<Task> act = entryPoint switch
        {
            NoMappingEntryPoint.HostStartup => () => provider
                .GetServices<IHostedService>()
                .Single(service => service.GetType().Name == "RetentionValidationHostedService")
                .StartAsync(CancellationToken.None),
            NoMappingEntryPoint.Sweep => () => scope.ServiceProvider
                .GetRequiredService<IRetentionSweep>()
                .ExecuteAsync(RetentionSweepRequest.Tenantless()),
            NoMappingEntryPoint.ListActiveHolds => () => ListActiveHoldsAsync(scope.ServiceProvider),
            NoMappingEntryPoint.CreateHold => () => scope.ServiceProvider
                .GetRequiredService<IRetentionHoldsRepository>()
                .CreateAsync(
                    new RetentionHoldRequest(
                        Guid.NewGuid(),
                        RetentionEntityIdentity.For<SessionKeyRecord>(),
                        Guid.NewGuid().ToString("D"),
                        tenantId: null,
                        "litigation",
                        DateTimeOffset.UtcNow
                    ),
                    CancellationToken.None
                ),
            NoMappingEntryPoint.HasActiveHold => () => scope.ServiceProvider
                .GetRequiredService<IRetentionHoldsRepository>()
                .HasActiveHoldAsync(
                    RetentionEntityIdentity.For<SessionKeyRecord>(),
                    Guid.NewGuid().ToString("D"),
                    tenantId: null,
                    CancellationToken.None
                ),
            _ => throw new ArgumentOutOfRangeException(nameof(entryPoint)),
        };

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().Contain(error =>
                error.Contains("The EF model contains no Cohort table mappings", StringComparison.Ordinal)
            );
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

    [Theory]
    [InlineData("ALTER TABLE public.anonymise_literal_records ALTER COLUMN \"Label\" TYPE character varying(8)", "character varying(8)")]
    [InlineData("ALTER TABLE public.anonymise_literal_records ALTER COLUMN \"Label\" TYPE character(4)", "character(4)")]
    [InlineData("ALTER TABLE public.anonymise_literal_records ALTER COLUMN \"Label\" TYPE uuid USING NULL", "uuid")]
    public async Task Readiness_refuses_a_fixed_literal_its_column_cannot_take(string ddl, string columnType)
    {
        // The model says text; only the catalog knows the column became varchar(8) or a char(4)
        // declared by column type alone. "[redacted]" is too long for both, so every anonymise of
        // these rows would fail at runtime. EXPLAIN binds no literal, so the literal probe owns
        // this; a column that can't be assigned from text at all is the statement check's.
        await using var database = await AnonymiseTargetDatabaseAsync(ddl);
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseLiteralRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member Label", StringComparison.Ordinal)
            && error.Contains($"not valid for its column type {columnType}", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Readiness_refuses_an_empty_string_rejected_by_a_domain_check()
    {
        await using var database = await AnonymiseTargetDatabaseAsync(
            """
            CREATE DOMAIN nonblank AS text CHECK (VALUE <> '');
            ALTER TABLE public.anonymise_empty_string_records ALTER COLUMN "Label" TYPE nonblank;
            """
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseEmptyStringRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member Label", StringComparison.Ordinal)
            && error.Contains("not valid for its column type nonblank", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Readiness_refuses_a_fixed_literal_on_an_unfiltered_unique_index()
    {
        await using var database = await AnonymiseTargetDatabaseAsync(
            """CREATE UNIQUE INDEX anonymise_literal_label ON public.anonymise_literal_records ("Label")"""
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseLiteralRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member Label", StringComparison.Ordinal)
            && error.Contains("unique", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Readiness_accepts_a_fixed_literal_on_a_unique_index_filtered_on_anonymised_at()
    {
        // Only rows not yet anonymised must be unique, so every anonymised row may share the
        // literal.
        await using var database = await AnonymiseTargetDatabaseAsync(
            """CREATE UNIQUE INDEX anonymise_literal_label ON public.anonymise_literal_records ("Label") WHERE "AnonymisedAt" IS NULL"""
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var result = await provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(
            RetentionSweepRequest.Tenantless()
        );

        result.EntityFailures.Should().BeEmpty();
    }

    [Fact]
    public async Task Readiness_refuses_null_on_a_unique_index_that_treats_nulls_as_equal()
    {
        await using var database = await AnonymiseTargetDatabaseAsync(
            """CREATE UNIQUE INDEX anonymise_null_label ON public.anonymise_null_records ("Label") NULLS NOT DISTINCT"""
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseNullRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member Label", StringComparison.Ordinal)
            && error.Contains("unique", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Readiness_refuses_a_literal_on_a_foreign_key_column()
    {
        await using var database = await AnonymiseTargetDatabaseAsync(
            """
            CREATE TABLE public.anonymise_labels ("Code" text PRIMARY KEY);
            ALTER TABLE public.anonymise_literal_records
                ADD FOREIGN KEY ("Label") REFERENCES public.anonymise_labels ("Code");
            """
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseLiteralRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member Label", StringComparison.Ordinal)
            && error.Contains("foreign key", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Readiness_refuses_anonymising_a_referenced_key()
    {
        // Even Null: while a row references the key, NO ACTION refuses the update, and CASCADE or
        // SET NULL would silently rewrite another table.
        await using var database = await AnonymiseTargetDatabaseAsync(
            """
            ALTER TABLE public.anonymise_null_records ADD CONSTRAINT anonymise_null_label UNIQUE ("Label");
            CREATE TABLE public.anonymise_label_users (
                "Label" text REFERENCES public.anonymise_null_records ("Label")
            );
            """
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseNullRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member Label", StringComparison.Ordinal)
            && error.Contains("referenced by a foreign key", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Readiness_refuses_a_referenced_anonymised_at_marker()
    {
        // Every anonymise writes the marker, so a key over it changes on every row.
        await using var database = await AnonymiseTargetDatabaseAsync(
            """
            ALTER TABLE public.anonymise_null_records ADD CONSTRAINT anonymise_null_marker UNIQUE ("Id", "AnonymisedAt");
            CREATE TABLE public.anonymise_marker_users (
                "RecordId" uuid,
                "RecordAnonymisedAt" timestamp with time zone,
                FOREIGN KEY ("RecordId", "RecordAnonymisedAt")
                    REFERENCES public.anonymise_null_records ("Id", "AnonymisedAt")
            );
            """
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseNullRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member AnonymisedAt", StringComparison.Ordinal)
            && error.Contains("referenced by a foreign key", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Readiness_refuses_a_referenced_soft_delete_flag()
    {
        // "Children may only reference live parents": a soft-delete of a row with children would
        // be refused row by row.
        await using var database = await MigratedTemporaryDatabaseAsync();
        await using (var connection = new NpgsqlConnection(database.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(
                """
                ALTER TABLE public.soft_delete_records ADD CONSTRAINT soft_delete_live UNIQUE ("Id", "IsDeleted");
                CREATE TABLE public.soft_delete_children (
                    "ParentId" uuid,
                    "ParentIsDeleted" boolean,
                    FOREIGN KEY ("ParentId", "ParentIsDeleted")
                        REFERENCES public.soft_delete_records ("Id", "IsDeleted")
                );
                """,
                connection
            );
            await command.ExecuteNonQueryAsync();
        }

        using var host = new CohortTestHost(database.ConnectionString);

        var act = () => host.RunSweepAsync(new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()));

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            error.Contains(typeof(SoftDeleteRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member IsDeleted", StringComparison.Ordinal)
            && error.Contains("soft-deleting", StringComparison.Ordinal)
        );
    }

    [Theory]
    [InlineData("IsDeleted")]
    [InlineData("DeletedAt")]
    public async Task Readiness_refuses_a_missing_soft_delete_marker_column(string column)
    {
        // The migration that adds the marker wasn't applied; every soft-delete would fail at its UPDATE.
        await using var database = await MigratedTemporaryDatabaseAsync();
        await using (var connection = new NpgsqlConnection(database.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(
                $"""ALTER TABLE public.soft_delete_records DROP COLUMN "{column}" """,
                connection
            );
            await command.ExecuteNonQueryAsync();
        }

        using var host = new CohortTestHost(database.ConnectionString);

        var act = () => host.RunSweepAsync(new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()));

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            RefusesWith(error, typeof(SoftDeleteRecord), "42703", $"\"{column}\"")
        );
    }

    [Fact]
    public async Task Readiness_refuses_a_missing_anonymised_at_marker_column()
    {
        await using var database = await AnonymiseTargetDatabaseAsync(
            """ALTER TABLE public.anonymise_null_records DROP COLUMN "AnonymisedAt" """
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            RefusesWith(error, typeof(AnonymiseNullRecord), "42703", "\"AnonymisedAt\"")
        );
    }

    [Fact]
    public async Task Readiness_refuses_a_soft_delete_marker_column_the_database_generates()
    {
        // A generated column takes no UPDATE; every soft-delete would fail at "column can only be
        // updated to DEFAULT". A generated column is nullable unless declared otherwise, and the
        // model pass's nullable-flag rule refuses that before any group pass explains the UPDATE.
        await using var database = await MigratedTemporaryDatabaseAsync();
        await ExecuteAsync(
            database,
            """
            ALTER TABLE public.soft_delete_records DROP COLUMN "IsDeleted";
            ALTER TABLE public.soft_delete_records
                ADD COLUMN "IsDeleted" boolean GENERATED ALWAYS AS ("DeletedAt" IS NOT NULL) STORED
            """
        );
        using var host = new CohortTestHost(database.ConnectionString);

        var act = () => host.RunSweepAsync(new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()));

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            error.Contains(typeof(SoftDeleteRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member IsDeleted", StringComparison.Ordinal)
            && error.Contains("which is nullable in the database", StringComparison.Ordinal)
        );
    }

    // The same fault the flag rule skips: a generated column declared NOT NULL passes the rule,
    // so the soft-delete UPDATE's own group pass refuses it.
    [Fact]
    public async Task Readiness_refuses_a_not_null_soft_delete_marker_column_the_database_generates()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await ExecuteAsync(
            database,
            """
            ALTER TABLE public.soft_delete_records DROP COLUMN "IsDeleted";
            ALTER TABLE public.soft_delete_records
                ADD COLUMN "IsDeleted" boolean NOT NULL GENERATED ALWAYS AS ("DeletedAt" IS NOT NULL) STORED
            """
        );
        using var host = new CohortTestHost(database.ConnectionString);

        var act = () => host.RunSweepAsync(new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()));

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            RefusesWith(error, typeof(SoftDeleteRecord), "428C9", "column \"IsDeleted\" can only be updated to DEFAULT")
        );
    }

    // Cohort's SQL assigns the flag as boolean and equates the tenant with a uuid, so PostgreSQL
    // refuses a statement that compares a column that drifted to another type (42883).
    [Theory]
    [InlineData("IsDeleted", """ALTER COLUMN "IsDeleted" TYPE integer USING "IsDeleted"::integer""", "integer = boolean")]
    [InlineData("TenantId", """ALTER COLUMN "TenantId" TYPE text USING "TenantId"::text""", "text = uuid")]
    public async Task Readiness_refuses_a_retained_column_a_statement_cannot_compare(
        string column,
        string alteration,
        string comparison
    )
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await ExecuteAsync(database, $"ALTER TABLE public.soft_delete_records {alteration}");

        using var host = new CohortTestHost(database.ConnectionString);

        var act = () => host.RunSweepAsync(new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()));

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            RefusesWith(error, typeof(SoftDeleteRecord), "42883", $"\"{column}\"")
            && error.Contains($"operator does not exist: {comparison}", StringComparison.Ordinal)
        );
    }

    [Theory]
    [InlineData("DeletedAt", """ALTER COLUMN "DeletedAt" TYPE timestamp without time zone""")]
    [InlineData("CreatedAt", """ALTER COLUMN "CreatedAt" TYPE timestamp without time zone""")]
    public async Task Readiness_refuses_a_retained_column_of_the_wrong_type(string column, string alteration)
    {
        // Cohort's SQL writes and compares the timestamps as timestamptz; a timestamp without time
        // zone drifts with the session TimeZone, and PostgreSQL compares it without complaint, so
        // a catalog rule owns this. The column and its member share a name on this entity.
        typeof(SoftDeleteRecord).GetCustomAttribute<RetainAttribute>()!.AnchorMember.Should().Be("CreatedAt");
        await using var database = await MigratedTemporaryDatabaseAsync();
        await using (var connection = new NpgsqlConnection(database.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(
                $"ALTER TABLE public.soft_delete_records {alteration}",
                connection
            );
            await command.ExecuteNonQueryAsync();
        }

        using var host = new CohortTestHost(database.ConnectionString);

        var act = () => host.RunSweepAsync(new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()));

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            error.Contains(typeof(SoftDeleteRecord).FullName!, StringComparison.Ordinal)
            && error.Contains($"member {column}", StringComparison.Ordinal)
            && error.Contains($"\"{column}\"", StringComparison.Ordinal)
            && error.Contains("in the database, not", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Readiness_refuses_an_anonymised_at_marker_column_of_the_wrong_type()
    {
        await using var database = await AnonymiseTargetDatabaseAsync(
            """ALTER TABLE public.anonymise_null_records ALTER COLUMN "AnonymisedAt" TYPE text"""
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseNullRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member AnonymisedAt", StringComparison.Ordinal)
            && error.Contains("in the database, not", StringComparison.Ordinal)
        );
    }

    [Theory]
    [InlineData("CreatedAt")]
    [InlineData("TenantId")]
    public async Task Readiness_refuses_a_missing_anchor_or_tenant_column(string column)
    {
        // The migration that adds the column wasn't applied; every run reads it.
        await using var database = await MigratedTemporaryDatabaseAsync();
        await using (var connection = new NpgsqlConnection(database.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(
                $"""ALTER TABLE public.soft_delete_records DROP COLUMN "{column}" CASCADE""",
                connection
            );
            await command.ExecuteNonQueryAsync();
        }

        using var host = new CohortTestHost(database.ConnectionString);

        var act = () => host.RunSweepAsync(new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()));

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error =>
            RefusesWith(error, typeof(SoftDeleteRecord), "42703", $"\"{column}\"")
        );
    }

    // An erasure matches rows by the subject column and a hold listing joins through it, so a
    // subject column the database lost, or retyped away from the model's uuid, fails every
    // erasure of its kind. Only the erasure scopes and the subject-kind hold listing read it, so
    // an erasure's group pass refuses it, not a sweep's.
    [Theory]
    [InlineData("""ALTER TABLE public.soft_delete_records DROP COLUMN "SubjectId" """, "42703", "column ")]
    [InlineData(
        """ALTER TABLE public.soft_delete_records ALTER COLUMN "SubjectId" TYPE text USING "SubjectId"::text""",
        "42883",
        "operator does not exist: text = uuid"
    )]
    public async Task Readiness_refuses_an_erasure_subject_column_missing_or_of_the_wrong_type(
        string ddl,
        string sqlState,
        string message
    )
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await ExecuteAsync(database, ddl);
        using var host = new CohortTestHost(database.ConnectionString);

        // Only an erasure reads the subject column (a sweep sends no statement over it).
        var act = () => host.RunErasureAsync(
            new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
            new ErasureScope("user", Guid.NewGuid(), allowSoftDeleteAsErasure: true)
        );

        (await act.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                RefusesWith(error, typeof(SoftDeleteRecord), sqlState, "\"SubjectId\"")
                && error.Contains(message, StringComparison.Ordinal)
                && error.Contains("erasing subjects of kind 'user'", StringComparison.Ordinal)
            );
    }

    // A NULL tenant never matches a tenant's run, erasure, or hold, and `= FALSE` skips a NULL
    // flag, so such rows stay forever. PostgreSQL accepts the statements, so a catalog rule owns
    // this; a NOT NULL domain makes a nullable column NOT NULL.
    [Theory]
    [InlineData("""ALTER TABLE public.soft_delete_records ALTER COLUMN "TenantId" DROP NOT NULL""", true)]
    [InlineData("SELECT 1", false)]
    [InlineData(
        """
        CREATE DOMAIN public.required_tenant AS uuid NOT NULL;
        ALTER TABLE public.soft_delete_records ALTER COLUMN "TenantId" DROP NOT NULL;
        ALTER TABLE public.soft_delete_records ALTER COLUMN "TenantId" TYPE public.required_tenant;
        """,
        false
    )]
    public Task Readiness_checks_a_tenant_column_is_not_null(string ddl, bool refused) =>
        AssertSoftDeleteRecordColumnNullabilityAsync(ddl, refused, "TenantId");

    [Theory]
    [InlineData("""ALTER TABLE public.soft_delete_records ALTER COLUMN "IsDeleted" DROP NOT NULL""", true)]
    [InlineData("SELECT 1", false)]
    [InlineData(
        """
        CREATE DOMAIN public.required_flag AS boolean NOT NULL;
        ALTER TABLE public.soft_delete_records ALTER COLUMN "IsDeleted" DROP NOT NULL;
        ALTER TABLE public.soft_delete_records ALTER COLUMN "IsDeleted" TYPE public.required_flag;
        """,
        false
    )]
    public Task Readiness_checks_a_soft_delete_flag_is_not_null(string ddl, bool refused) =>
        AssertSoftDeleteRecordColumnNullabilityAsync(ddl, refused, "IsDeleted");

    private async Task AssertSoftDeleteRecordColumnNullabilityAsync(string ddl, bool refused, string member)
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await ExecuteAsync(database, ddl);
        using var host = new CohortTestHost(database.ConnectionString);

        var act = () => host.RunSweepAsync(new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()));

        if (refused)
        {
            (await act.Should().ThrowAsync<RetentionConfigurationException>())
                .Which.Errors.Should().ContainSingle(error =>
                    error.Contains(typeof(SoftDeleteRecord).FullName!, StringComparison.Ordinal)
                    && error.Contains($"member {member}", StringComparison.Ordinal)
                    && error.Contains("which is nullable in the database", StringComparison.Ordinal)
                );
        }
        else
        {
            (await act()).EntityFailures.Should().BeEmpty();
        }
    }

    // `AnonymisedAt IS NULL` finds the rows still to anonymise, so a NOT NULL marker, or a column
    // over a NOT NULL domain, never matches a row.
    [Theory]
    [InlineData("""ALTER TABLE public.anonymise_null_records ALTER COLUMN "AnonymisedAt" SET NOT NULL""", true)]
    [InlineData(
        """
        CREATE DOMAIN required_instant AS timestamptz NOT NULL;
        ALTER TABLE public.anonymise_null_records ALTER COLUMN "AnonymisedAt" TYPE required_instant;
        """,
        true
    )]
    [InlineData("SELECT 1", false)]
    public async Task Readiness_checks_an_anonymised_at_column_can_be_null(string ddl, bool refused)
    {
        await using var database = await AnonymiseTargetDatabaseAsync(ddl);
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        if (refused)
        {
            var errors = await ReadinessErrorsAsync(provider);

            errors.Should().ContainSingle(error =>
                error.Contains(typeof(AnonymiseNullRecord).FullName!, StringComparison.Ordinal)
                && error.Contains("member AnonymisedAt", StringComparison.Ordinal)
                && error.Contains("which is NOT NULL in the database", StringComparison.Ordinal)
            );
        }
        else
        {
            var result = await provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(
                RetentionSweepRequest.Tenantless()
            );

            result.EntityFailures.Should().BeEmpty();
        }
    }

    [Fact]
    public async Task Readiness_accepts_a_soft_delete_flag_on_a_domain_over_boolean()
    {
        // A domain over a domain over boolean assigns and compares as boolean.
        await using var database = await MigratedTemporaryDatabaseAsync();
        await using (var connection = new NpgsqlConnection(database.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(
                """
                CREATE DOMAIN public.flag AS boolean;
                CREATE DOMAIN public.flag2 AS public.flag;
                ALTER TABLE public.soft_delete_records ALTER COLUMN "IsDeleted" TYPE public.flag2;
                """,
                connection
            );
            await command.ExecuteNonQueryAsync();
        }

        using var host = new CohortTestHost(database.ConnectionString);

        var result = await host.RunSweepAsync(
            new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>())
        );

        result.EntityFailures.Should().BeEmpty();
    }

    [Fact]
    public async Task Readiness_accepts_a_missing_marker_column_no_declared_strategy_writes()
    {
        // The category declares only Purge, which never writes the soft-delete flag.
        await using var database = await MigratedTemporaryDatabaseAsync();
        await using (var connection = new NpgsqlConnection(database.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(
                """ALTER TABLE public.soft_delete_records DROP COLUMN "IsDeleted" """,
                connection
            );
            await command.ExecuteNonQueryAsync();
        }

        using var host = new CohortTestHost(database.ConnectionString, new PurgeOnlySoftDeleteRuleProvider());

        var result = await host.RunSweepAsync(
            new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>())
        );

        result.EntityFailures.Should().BeEmpty();
    }

    [Fact]
    public async Task Readiness_refuses_an_anonymised_column_the_database_generates()
    {
        await using var database = await AnonymiseTargetDatabaseAsync(
            """
            ALTER TABLE public.anonymise_null_records DROP COLUMN "Label";
            ALTER TABLE public.anonymise_null_records ADD COLUMN "Label" text GENERATED ALWAYS AS ('x') STORED
            """
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            RefusesWith(error, typeof(AnonymiseNullRecord), "428C9", "column \"Label\" can only be updated to DEFAULT")
        );
    }

    // An identity column is NOT NULL, so the model pass's Null-method rule refuses it before the
    // UPDATE (which PostgreSQL would also refuse, 428C9) is explained.
    [Fact]
    public async Task Readiness_refuses_a_null_anonymise_into_an_identity_column()
    {
        await using var database = await AnonymiseTargetDatabaseAsync(
            """
            ALTER TABLE public.anonymise_null_integer_records DROP COLUMN "Label";
            ALTER TABLE public.anonymise_null_integer_records ADD COLUMN "Label" integer GENERATED ALWAYS AS IDENTITY
            """
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseNullIntegerRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member Label", StringComparison.Ordinal)
            && error.Contains("uses Null but the column is NOT NULL", StringComparison.Ordinal)
        );
    }

    // A method other than Null passes the model pass, so the group pass explains the UPDATE and
    // PostgreSQL refuses an identity column GENERATED ALWAYS (428C9).
    // Make-fail: the column is a plain integer; nothing refuses it.
    [Fact]
    public async Task Readiness_refuses_a_fixed_literal_into_an_identity_column_generated_always()
    {
        await using var database = await AnonymiseTargetDatabaseAsync(
            """
            ALTER TABLE public.anonymise_numeric_code_records DROP COLUMN "Code";
            ALTER TABLE public.anonymise_numeric_code_records ADD COLUMN "Code" integer GENERATED ALWAYS AS IDENTITY
            """
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            RefusesWith(error, typeof(AnonymiseNumericCodeRecord), "428C9", "column \"Code\" can only be updated to DEFAULT")
        );
    }

    [Fact]
    public async Task Readiness_refuses_null_into_a_column_the_database_declares_not_null()
    {
        // The model says nullable; the database was changed underneath it.
        await using var database = await AnonymiseTargetDatabaseAsync(
            """ALTER TABLE public.anonymise_null_records ALTER COLUMN "Label" SET NOT NULL"""
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseNullRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member Label", StringComparison.Ordinal)
            && error.Contains("NOT NULL", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Readiness_accepts_a_fixed_literal_on_a_column_a_unique_index_only_includes()
    {
        // INCLUDE columns ride along in the index but take no part in its uniqueness.
        await using var database = await AnonymiseTargetDatabaseAsync(
            """CREATE UNIQUE INDEX anonymise_literal_created ON public.anonymise_literal_records ("CreatedAt") INCLUDE ("Label")"""
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var result = await provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(
            RetentionSweepRequest.Tenantless()
        );

        result.EntityFailures.Should().BeEmpty();
    }

    [Fact]
    public async Task Readiness_refuses_a_fixed_literal_on_a_unique_index_that_only_includes_the_record_id()
    {
        // The record id in INCLUDE doesn't make the key unique per row: the second anonymised row
        // still collides on the literal.
        await using var database = await AnonymiseTargetDatabaseAsync(
            """CREATE UNIQUE INDEX anonymise_literal_label ON public.anonymise_literal_records ("Label") INCLUDE ("Id")"""
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseLiteralRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member Label", StringComparison.Ordinal)
            && error.Contains("unique", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Readiness_refuses_a_static_factory_on_an_unfiltered_unique_index()
    {
        // A Static factory runs once per batch, so every row in it gets the same value.
        await using var database = await AnonymiseTargetDatabaseAsync(
            """CREATE UNIQUE INDEX anonymise_static_label ON public.anonymise_static_factory_records ("Label")"""
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseStaticFactoryRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("[AnonymiseWith] member Label", StringComparison.Ordinal)
            && error.Contains("unique", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Readiness_refuses_null_into_a_column_whose_nested_domain_is_not_null()
    {
        // The outer domain doesn't repeat NOT NULL, but PostgreSQL still enforces the inner one.
        await using var database = await AnonymiseTargetDatabaseAsync(
            """
            CREATE DOMAIN required_label AS text NOT NULL;
            CREATE DOMAIN label_text AS required_label;
            ALTER TABLE public.anonymise_null_records ALTER COLUMN "Label" TYPE label_text;
            """
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseNullRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member Label", StringComparison.Ordinal)
            && error.Contains("NOT NULL", StringComparison.Ordinal)
        );
    }

    // The UPDATE casts the value to the model's store type, then PostgreSQL assigns it to the
    // column, so the column needs an assignment path from the model's type. text to uuid has no
    // pg_cast row and uuid isn't a string type, so even a valid uuid literal, or a Null, fails
    // every run. integer to boolean has a pg_cast row, but it is explicit-only. An array assigns
    // only through its elements, and text to uuid fails there too. PostgreSQL refuses the
    // anonymise UPDATE itself (42804), which readiness explains. (A FixedLiteral that no uuid
    // accepts is the literal probe's refusal first: Readiness_refuses_a_fixed_literal_its_column_cannot_take.)
    [Theory]
    [InlineData("anonymise_uuid_literal_records", typeof(AnonymiseUuidLiteralRecord), "uuid", "text")]
    [InlineData("anonymise_null_records", typeof(AnonymiseNullRecord), "uuid", "text")]
    [InlineData("anonymise_null_integer_records", typeof(AnonymiseNullIntegerRecord), "boolean", "integer")]
    [InlineData("anonymise_null_text_array_records", typeof(AnonymiseNullTextArrayRecord), "uuid[]", "text[]")]
    public async Task Readiness_refuses_a_column_the_models_store_type_cannot_be_assigned_to(
        string table,
        Type entityType,
        string columnType,
        string modelType
    )
    {
        await using var database = await AnonymiseTargetDatabaseAsync(
            $"""ALTER TABLE public.{table} ALTER COLUMN "Label" TYPE {columnType} USING NULL"""
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            RefusesWith(
                error,
                entityType,
                "42804",
                $"column \"Label\" is of type {columnType} but expression is of type {modelType}"
            )
        );
    }

    // text assigns to varchar, and to a domain over varchar, through pg_cast's implicit row; the
    // array assigns through its elements; uuid reaches text, which has no pg_cast row from it,
    // through text I/O, as any type reaches a string type.
    [Theory]
    [InlineData(
        """
        CREATE DOMAIN short_label AS character varying(40);
        ALTER TABLE public.anonymise_uuid_literal_records ALTER COLUMN "Label" TYPE short_label;
        ALTER TABLE public.anonymise_null_records ALTER COLUMN "Label" TYPE character varying(40);
        """
    )]
    [InlineData(
        """ALTER TABLE public.anonymise_null_text_array_records ALTER COLUMN "Label" TYPE character varying(40)[]"""
    )]
    [InlineData("""ALTER TABLE public.anonymise_null_uuid_records ALTER COLUMN "Label" TYPE text""")]
    public async Task Readiness_accepts_a_column_the_models_store_type_can_be_assigned_to(string ddl)
    {
        await using var database = await AnonymiseTargetDatabaseAsync(ddl);
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var result = await provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(
            RetentionSweepRequest.Tenantless()
        );

        result.EntityFailures.Should().BeEmpty();
    }

    [Theory]
    [InlineData("""ALTER TABLE public.anonymise_literal_records DROP COLUMN "Label" """, "42703", "\"Label\"")]
    [InlineData("""ALTER TABLE public.anonymise_literal_records DROP COLUMN "Id" """, "42703", "\"Id\"")]
    [InlineData("DROP TABLE public.anonymise_literal_records", "42P01", "\"public.anonymise_literal_records\"")]
    public async Task Readiness_refuses_an_anonymise_target_missing_from_the_database(
        string ddl,
        string sqlState,
        string quoted
    )
    {
        await using var database = await AnonymiseTargetDatabaseAsync(ddl);
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            RefusesWith(error, typeof(AnonymiseLiteralRecord), sqlState, quoted)
        );
    }

    [Theory]
    [InlineData("VIEW", "a view, which")]
    [InlineData("MATERIALIZED VIEW", "a materialized view, whose")]
    public async Task Readiness_refuses_a_retained_entity_mapped_to_a_relation_that_is_not_a_table(
        string relation,
        string kind
    )
    {
        // Cohort changes rows in place by ctid: a view has none, a materialized view can't be changed.
        await using var database = await AnonymiseTargetDatabaseAsync(
            $"""
            ALTER TABLE public.anonymise_literal_records RENAME TO anonymise_literal_records_base;
            CREATE {relation} public.anonymise_literal_records AS
                SELECT * FROM public.anonymise_literal_records_base;
            """
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseLiteralRecord).FullName!, StringComparison.Ordinal)
            && error.Contains($"\"public\".\"anonymise_literal_records\" is {kind} ", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Readiness_probes_a_fixed_literal_after_its_value_converter()
    {
        // The converter upper-cases the literal, so the UPDATE sends "[REDACTED]", which the
        // domain refuses; "[redacted]" itself would pass.
        await using var database = await AnonymiseTargetDatabaseAsync(
            """
            CREATE DOMAIN not_shouting AS text CHECK (VALUE <> '[REDACTED]');
            ALTER TABLE public.anonymise_converted_records ALTER COLUMN "Label" TYPE not_shouting;
            """
        );
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseConvertedRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member Label", StringComparison.Ordinal)
            && error.Contains("not valid for its column type not_shouting", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Readiness_skips_the_probe_for_a_literal_whose_provider_value_is_not_text()
    {
        // The converter turns "7" into an integer, which has no text probe; the run writes it.
        await using var database = await AnonymiseTargetDatabaseAsync("SELECT 1");
        await using (var db = new AnonymiseTargetDbContext(
            new DbContextOptionsBuilder<AnonymiseTargetDbContext>().UseNpgsql(database.ConnectionString).Options
        ))
        {
            db.Add(new AnonymiseNumericCodeRecord
            {
                Id = Guid.NewGuid(),
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-60),
                Code = "42",
            });
            await db.SaveChangesAsync();
        }
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var result = await provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(
            RetentionSweepRequest.Tenantless()
        );

        result.EntityFailures.Should().BeEmpty();
        await using var check = new AnonymiseTargetDbContext(
            new DbContextOptionsBuilder<AnonymiseTargetDbContext>().UseNpgsql(database.ConnectionString).Options
        );
        (await check.Set<AnonymiseNumericCodeRecord>().SingleAsync()).Code.Should().Be("7");
    }

    [Fact]
    public async Task Readiness_refuses_a_literal_its_value_converter_throws_on()
    {
        await using var database = await AnonymiseTargetDatabaseAsync<ThrowingConverterDbContext>("SELECT 1");
        await using var provider = BuildAnonymiseTargetProvider<ThrowingConverterDbContext>(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseLiteralRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member Label", StringComparison.Ordinal)
            && error.Contains("value converter", StringComparison.Ordinal)
            && error.Contains(nameof(FormatException), StringComparison.Ordinal)
        );
    }

    // PostgreSQL's refusal as readiness reports it: one error per entity holding the SQLSTATE, the
    // message, and the statement text around the error's position. `quoted` is a fragment the
    // message or that text carries, such as a quoted column or a message's own wording; the
    // "reading" list of tables comes after both and never names a column.
    private static bool RefusesWith(string error, Type entity, string sqlState, string quoted) =>
        error.Contains($"Retained entity {entity.FullName}:", StringComparison.Ordinal)
        && error.Contains($" {sqlState}: ", StringComparison.Ordinal)
        && error.Contains(quoted, StringComparison.Ordinal);

    private Task<TemporaryDatabase> AnonymiseTargetDatabaseAsync(string? ddl = null) =>
        AnonymiseTargetDatabaseAsync<AnonymiseTargetDbContext>(ddl);

    private async Task<TemporaryDatabase> AnonymiseTargetDatabaseAsync<TContext>(string? ddl = null)
        where TContext : DbContext
    {
        var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var options = new DbContextOptionsBuilder<TContext>().UseNpgsql(database.ConnectionString).Options;
        await using var db = (TContext)Activator.CreateInstance(typeof(TContext), options)!;
        await db.Database.ExecuteSqlRawAsync(db.Database.GenerateCreateScript());
        if (ddl is not null)
        {
            await db.Database.ExecuteSqlRawAsync(ddl);
        }

        return database;
    }

    private static ServiceProvider BuildAnonymiseTargetProvider(
        string connectionString,
        Action<DbContextOptionsBuilder>? configureContext = null
    ) => BuildAnonymiseTargetProvider<AnonymiseTargetDbContext>(connectionString, configureContext);

    private static ServiceProvider BuildAnonymiseTargetProvider<TContext>(
        string connectionString,
        Action<DbContextOptionsBuilder>? configureContext = null
    )
        where TContext : DbContext
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddDbContext<TContext>(builder =>
        {
            builder.UseNpgsql(connectionString);
            configureContext?.Invoke(builder);
        });
        services.AddSingleton<IRetentionRuleProvider>(new AnonymiseTargetRuleProvider());
        services.AddSingleton<IAnonymiseValueFactory, StaticLabelFactory>();
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<TContext>();
        return services.BuildServiceProvider(validateScopes: true);
    }

    private static async Task<IReadOnlyList<string>> ReadinessErrorsAsync(ServiceProvider provider)
    {
        var act = () => provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(
            RetentionSweepRequest.Tenantless()
        );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        return exception.Which.Errors;
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

    // Every scope reaches callerConnectionString until its work runs as System work, which the
    // host routes to systemConnectionString.
    private static ServiceProvider BuildSystemRoutedProvider(
        string callerConnectionString,
        string systemConnectionString,
        Action<DbContextOptionsBuilder>? configureContext = null
    ) => BuildSystemRoutedProvider(callerConnectionString, () => systemConnectionString, configureContext);

    // A system route read each time a scope's work starts: null routes nowhere, so a test can
    // switch it after readiness has passed.
    private static ServiceProvider BuildSystemRoutedProvider(
        string callerConnectionString,
        Func<string?> systemConnectionString,
        Action<DbContextOptionsBuilder>? configureContext = null
    )
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddScoped(_ => new DatabaseRoute(callerConnectionString));
        services.AddDbContext<SampleDbContext>((provider, options) =>
        {
            options.UseNpgsql(provider.GetRequiredService<DatabaseRoute>().ConnectionString);
            configureContext?.Invoke(options);
        });
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

    private sealed class ConnectionStringModelCacheKeyFactory : IModelCacheKeyFactory
    {
        public object Create(DbContext context, bool designTime) =>
            (
                context.GetType(),
                context.GetService<IDbContextOptions>()
                    .FindExtension<CoreOptionsExtension>()
                    ?.ApplicationServiceProvider?.GetService<DatabaseRoute>()
                    ?.ConnectionString,
                designTime
            );
    }

    private sealed class RecordingExecutionScope : IRetentionExecutionScope
    {
        private readonly ConcurrentQueue<RetentionExecutionContext> ran = new();

        public IReadOnlyCollection<RetentionExecutionContext> Ran => ran;

        public Task RunAsync(RetentionExecutionContext context, Func<Task> work, CancellationToken ct)
        {
            ran.Enqueue(context);
            return work();
        }
    }

    private sealed class SystemWorkRoutingScope(DatabaseRoute route, Func<string?> systemConnectionString)
        : IRetentionExecutionScope
    {
        public Task RunAsync(RetentionExecutionContext context, Func<Task> work, CancellationToken ct)
        {
            if (context is RetentionExecutionContext.System && systemConnectionString() is { } routed)
            {
                route.ConnectionString = routed;
            }

            return work();
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
    [InlineData("CREATE DOMAIN session_instant AS timestamptz", "session_instant", "timestamptz")]
    [InlineData("CREATE DOMAIN session_ids AS timestamptz[]", "session_ids", "timestamptz")]
    [InlineData("", "tstzrange", "timestamptz")]
    [InlineData("", "double precision[]", "float8")]
    public async Task Readiness_rejects_a_record_id_column_whose_text_form_depends_on_session_settings(
        string typeDefinition,
        string columnType,
        string baseType
    )
    {
        // The EF model says text; only the catalog knows the column is a domain, array or range
        // over a type whose text form follows TimeZone, DateStyle or extra_float_digits. Here
        // PostgreSQL has no operator for the column against text either (42883), but the model
        // pass's column rule runs before any group pass explains a statement, so the rule's
        // refusal is the one reported, and it names the base type it walked to.
        await using var database = await SessionKeyDatabaseAsync<SessionKeyDbContext>(
            "session_key_records",
            typeDefinition,
            columnType
        );

        var errors = await SessionKeyErrorsAsync<SessionKeyDbContext>(database);

        errors.Should().ContainSingle(error =>
            error.Contains($"Record-id convention on {typeof(SessionKeyRecord).FullName}", StringComparison.Ordinal)
            && error.Contains("record-id column \"public\".\"session_key_records\".\"Id\"", StringComparison.Ordinal)
            && error.Contains($"is built on '{baseType}'", StringComparison.Ordinal)
            && error.Contains("depends on session settings", StringComparison.Ordinal)
        );
    }

    [Theory]
    [InlineData("", "double precision")]
    [InlineData("CREATE DOMAIN session_float AS double precision", "session_float")]
    public async Task Readiness_refuses_a_record_id_column_whose_text_form_depends_on_session_settings_when_it_compares(
        string typeDefinition,
        string columnType
    )
    {
        // A numeric record id over a double precision column, or a domain over one: float8 =
        // numeric resolves through an implicit cast (a domain compares as its base type), so the
        // statement check passes, and a hold join compares the column's text form, which follows
        // extra_float_digits. The catalog rule owns this, and walks the domain chain to see it.
        await using var database = await SessionKeyDatabaseAsync<NumericSessionKeyDbContext>(
            "numeric_session_key_records",
            typeDefinition,
            columnType
        );

        var errors = await SessionKeyErrorsAsync<NumericSessionKeyDbContext>(database);

        errors.Should().ContainSingle(error =>
            error.Contains("record-id column", StringComparison.Ordinal)
            && error.Contains("depends on session settings", StringComparison.Ordinal)
        );
    }

    // Creates the context's schema and retypes the record id column, which the model says is not
    // what the database has.
    private async Task<TemporaryDatabase> SessionKeyDatabaseAsync<TContext>(
        string table,
        string typeDefinition,
        string columnType
    )
        where TContext : DbContext
    {
        var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var options = new DbContextOptionsBuilder<TContext>().UseNpgsql(database.ConnectionString).Options;
        await using var db = (TContext)Activator.CreateInstance(typeof(TContext), options)!;
        await db.Database.ExecuteSqlRawAsync(db.Database.GenerateCreateScript());
        if (typeDefinition != "")
        {
            await db.Database.ExecuteSqlRawAsync(typeDefinition);
        }

        var alter = $"ALTER TABLE public.{table} ALTER COLUMN \"Id\" TYPE {columnType} USING NULL";
        await db.Database.ExecuteSqlRawAsync(alter);
        return database;
    }

    private static async Task<IReadOnlyList<string>> SessionKeyErrorsAsync<TContext>(TemporaryDatabase database)
        where TContext : DbContext
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddDbContext<TContext>(builder => builder.UseNpgsql(database.ConnectionString));
        services.AddSingleton<IRetentionRuleProvider>(new SessionKeyRuleProvider());
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<TContext>();
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        return await ReadinessErrorsAsync(provider);
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

    // A wrapper that forwards to an NpgsqlConnection and is not one itself, as a profiler's is.
    // connectionStringSuffix is appended to the string it reports, as a wrapper's own settings are.
    private sealed class PassThroughConnection(NpgsqlConnection inner, string connectionStringSuffix = "")
        : DbConnection
    {
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString
        {
            get => inner.ConnectionString + connectionStringSuffix;
            set => inner.ConnectionString = value;
        }

        public override string Database => inner.Database;

        public override string DataSource => inner.DataSource;

        public override string ServerVersion => inner.ServerVersion;

        public override ConnectionState State => inner.State;

        public override void ChangeDatabase(string databaseName) => inner.ChangeDatabase(databaseName);

        public override void Open() => inner.Open();

        public override Task OpenAsync(CancellationToken cancellationToken) => inner.OpenAsync(cancellationToken);

        public override void Close() => inner.Close();

        protected override DbCommand CreateDbCommand() => new PassThroughCommand(this, inner.CreateCommand());

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
            new PassThroughTransaction(this, inner.BeginTransaction(isolationLevel));

        public override ValueTask DisposeAsync() => inner.DisposeAsync();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                inner.Dispose();
            }

            base.Dispose(disposing);
        }
    }

    // The transaction and command of that wrapper, as a profiler's are: neither is an Npgsql type,
    // so a caller's transaction on the wrapper is not an NpgsqlTransaction and readiness explains
    // its statements one at a time, in that transaction.
    private sealed class PassThroughTransaction(PassThroughConnection connection, NpgsqlTransaction inner)
        : DbTransaction
    {
        internal NpgsqlTransaction Inner => inner;

        protected override DbConnection DbConnection => connection;

        public override IsolationLevel IsolationLevel => inner.IsolationLevel;

        public override void Commit() => inner.Commit();

        public override void Rollback() => inner.Rollback();

        public override Task CommitAsync(CancellationToken cancellationToken = default) =>
            inner.CommitAsync(cancellationToken);

        public override Task RollbackAsync(CancellationToken cancellationToken = default) =>
            inner.RollbackAsync(cancellationToken);

        public override ValueTask DisposeAsync() => inner.DisposeAsync();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                inner.Dispose();
            }

            base.Dispose(disposing);
        }
    }

    private sealed class PassThroughCommand(PassThroughConnection connection, NpgsqlCommand inner) : DbCommand
    {
        private PassThroughTransaction? transaction;

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText
        {
            get => inner.CommandText;
            set => inner.CommandText = value;
        }

        public override int CommandTimeout
        {
            get => inner.CommandTimeout;
            set => inner.CommandTimeout = value;
        }

        public override CommandType CommandType
        {
            get => inner.CommandType;
            set => inner.CommandType = value;
        }

        public override bool DesignTimeVisible
        {
            get => inner.DesignTimeVisible;
            set => inner.DesignTimeVisible = value;
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get => inner.UpdatedRowSource;
            set => inner.UpdatedRowSource = value;
        }

        protected override DbConnection? DbConnection
        {
            get => connection;
            set { }
        }

        protected override DbParameterCollection DbParameterCollection => inner.Parameters;

        protected override DbTransaction? DbTransaction
        {
            get => transaction;
            set
            {
                transaction = (PassThroughTransaction?)value;
                inner.Transaction = transaction?.Inner;
            }
        }

        public override void Cancel() => inner.Cancel();

        public override int ExecuteNonQuery() => inner.ExecuteNonQuery();

        public override object? ExecuteScalar() => inner.ExecuteScalar();

        public override void Prepare() => inner.Prepare();

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) =>
            inner.ExecuteNonQueryAsync(cancellationToken);

        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken) =>
            inner.ExecuteScalarAsync(cancellationToken);

        protected override DbParameter CreateDbParameter() => inner.CreateParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
            inner.ExecuteReader(behavior);

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
            CommandBehavior behavior,
            CancellationToken cancellationToken
        ) => await inner.ExecuteReaderAsync(behavior, cancellationToken);

        public override ValueTask DisposeAsync() => inner.DisposeAsync();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                inner.Dispose();
            }

            base.Dispose(disposing);
        }
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

    // A retained table in a schema of its own, Cohort's tables in public.
    private sealed class HostSchemaDbContext(DbContextOptions<HostSchemaDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<HostSchemaRecord>().ToTable("host_schema_records", "host_data");
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("session-key", nameof(CreatedAt))]
    [RetentionEntityId("9b3e7a15-4c82-4d60-b1f9-0a6d2e8c5f73")]
    [RetentionTenantless]
    private sealed class HostSchemaRecord
    {
        public Guid Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }

    // A record id whose store type is a domain, named without a schema, so PostgreSQL resolves
    // the cast Cohort's statements make through the search path.
    private sealed class SearchPathDbContext(DbContextOptions<SearchPathDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder
                .Entity<SearchPathKeyRecord>()
                .ToTable("search_path_key_records")
                .Property(record => record.Id)
                .HasColumnType("search_path_key");
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("session-key", nameof(CreatedAt))]
    [RetentionEntityId("2f6a9c13-8d47-4b05-a1e8-5c3b7d9f0e62")]
    [RetentionTenantless]
    private sealed class SearchPathKeyRecord
    {
        public string Id { get; set; } = "";
        public DateTimeOffset CreatedAt { get; set; }
    }

    private sealed class NumericSessionKeyDbContext(DbContextOptions<NumericSessionKeyDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<NumericSessionKeyRecord>().ToTable("numeric_session_key_records");
            modelBuilder.ConfigureCohortTables();
        }
    }

    // A retained entity and no ConfigureCohortTables().
    private sealed class NoCohortTablesDbContext(DbContextOptions<NoCohortTablesDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SessionKeyRecord>().ToTable("session_key_records");
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

    private sealed class HoldLinkDbContext(DbContextOptions<HoldLinkDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<HoldLinkCase>().ToTable("hold_link_cases");
            modelBuilder.Entity<HoldLinkChild>().ToTable("hold_link_children");
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("hold-link", nameof(CreatedAt))]
    [RetentionEntityId("a3f1c7d2-5b84-4e0a-9d16-2c8e7b4f1a05")]
    private sealed class HoldLinkCase
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }

    [Retain("hold-link", nameof(CreatedAt))]
    [RetentionEntityId("e7b2d9a4-1c63-4f58-8a0d-6b3f5e2c9d17")]
    [HeldWith(typeof(HoldLinkCase), nameof(CaseId))]
    private sealed class HoldLinkChild
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public Guid? CaseId { get; set; }
    }

    private sealed class HoldLinkRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == "hold-link" ? new RetentionCategoryCapabilities([Strategy.Purge]) : null;

        public Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct) =>
            Task.FromResult<RetentionRule?>(new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge));
    }

    [Retain("session-key", nameof(CreatedAt))]
    [RetentionEntityId("0c8e5a4f-6b1d-4e27-9a3c-5f2d7e8b1a96")]
    [RetentionTenantless]
    private sealed class SessionKeyRecord
    {
        public string Id { get; set; } = "";
        public DateTimeOffset CreatedAt { get; set; }
    }

    [Retain("session-key", nameof(CreatedAt))]
    [RetentionEntityId("7d1b4e92-3a60-4c85-b2f7-9e0a5c8d3f14")]
    [RetentionTenantless]
    private sealed class NumericSessionKeyRecord
    {
        public decimal Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }

    private sealed class SessionKeyRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == "session-key" ? new RetentionCategoryCapabilities([Strategy.Purge]) : null;

        public Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct) =>
            Task.FromResult<RetentionRule?>(new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge));
    }

    private sealed class AnonymiseTargetDbContext(DbContextOptions<AnonymiseTargetDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<AnonymiseLiteralRecord>().ToTable("anonymise_literal_records");
            modelBuilder.Entity<AnonymiseEmptyStringRecord>().ToTable("anonymise_empty_string_records");
            modelBuilder.Entity<AnonymiseNullRecord>().ToTable("anonymise_null_records");
            modelBuilder.Entity<AnonymiseUuidLiteralRecord>().ToTable("anonymise_uuid_literal_records");
            modelBuilder.Entity<AnonymiseStaticFactoryRecord>().ToTable("anonymise_static_factory_records");
            modelBuilder.Entity<AnonymiseNullIntegerRecord>().ToTable("anonymise_null_integer_records");
            modelBuilder.Entity<AnonymiseNullUuidRecord>().ToTable("anonymise_null_uuid_records");
            modelBuilder.Entity<AnonymiseNullTextArrayRecord>().ToTable("anonymise_null_text_array_records");
            modelBuilder
                .Entity<AnonymiseConvertedRecord>()
                .ToTable("anonymise_converted_records")
                .Property(record => record.Label)
                .HasConversion(label => label.ToUpperInvariant(), label => label);
            modelBuilder
                .Entity<AnonymiseNumericCodeRecord>()
                .ToTable("anonymise_numeric_code_records")
                .Property(record => record.Code)
                .HasConversion(code => int.Parse(code, CultureInfo.InvariantCulture), code => code.ToString(CultureInfo.InvariantCulture));
            modelBuilder.ConfigureCohortTables();
        }
    }

    // The literal entity again, behind a converter that can't parse its literal.
    private sealed class ThrowingConverterDbContext(DbContextOptions<ThrowingConverterDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder
                .Entity<AnonymiseLiteralRecord>()
                .ToTable("anonymise_literal_records")
                .Property(record => record.Label)
                .HasConversion(label => int.Parse(label, CultureInfo.InvariantCulture), label => label.ToString(CultureInfo.InvariantCulture));
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("anonymise-target", nameof(CreatedAt))]
    [RetentionEntityId("1d7e3a92-6c4b-4f15-a8e0-3b9d2c6f7a41")]
    [RetentionTenantless]
    private sealed class AnonymiseUuidLiteralRecord
    {
        public Guid Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [Anonymise(AnonymiseMethod.FixedLiteral, "00000000-0000-0000-0000-000000000000")]
        public string Label { get; set; } = "";

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    [Retain("anonymise-target", nameof(CreatedAt))]
    [RetentionEntityId("6a2f8c14-9e3d-4b70-b5a1-d8c4e2f9a063")]
    [RetentionTenantless]
    private sealed class AnonymiseStaticFactoryRecord
    {
        public Guid Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [AnonymiseWith(typeof(StaticLabelFactory))]
        public string Label { get; set; } = "";

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    [Retain("anonymise-target", nameof(CreatedAt))]
    [RetentionEntityId("e9b5d371-4a8c-4e26-9f0b-7c3a1d5e8b92")]
    [RetentionTenantless]
    private sealed class AnonymiseConvertedRecord
    {
        public Guid Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [Anonymise(AnonymiseMethod.FixedLiteral, "[redacted]")]
        public string Label { get; set; } = "";

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    [Retain("anonymise-target", nameof(CreatedAt))]
    [RetentionEntityId("3c8a6e25-b1f9-4d47-8e62-a5d0f7b3c914")]
    [RetentionTenantless]
    private sealed class AnonymiseNumericCodeRecord
    {
        public Guid Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [Anonymise(AnonymiseMethod.FixedLiteral, "7")]
        public string Code { get; set; } = "";

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    private sealed class StaticLabelFactory : IAnonymiseValueFactory
    {
        public object? Create(AnonymiseValueContext context) => "[gone]";
    }

    [Retain("anonymise-target", nameof(CreatedAt))]
    [RetentionEntityId("5b0f6c1e-2d7a-4e83-9c41-7a6e3f2d9b18")]
    [RetentionTenantless]
    private sealed class AnonymiseLiteralRecord
    {
        public Guid Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [Anonymise(AnonymiseMethod.FixedLiteral, "[redacted]")]
        public string Label { get; set; } = "";

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    [Retain("anonymise-target", nameof(CreatedAt))]
    [RetentionEntityId("8e2a4d7c-1f3b-4a65-b0d9-6c5e8f1a2b37")]
    [RetentionTenantless]
    private sealed class AnonymiseEmptyStringRecord
    {
        public Guid Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [Anonymise(AnonymiseMethod.EmptyString)]
        public string Label { get; set; } = "";

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    [Retain("anonymise-target", nameof(CreatedAt))]
    [RetentionEntityId("c4d9e1a2-7b6f-4c38-8a5d-2e1f9b7c3d60")]
    [RetentionTenantless]
    private sealed class AnonymiseNullRecord
    {
        public Guid Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [Anonymise(AnonymiseMethod.Null)]
        public string? Label { get; set; }

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    // Null-anonymised members whose model store types (integer, uuid, text[]) aren't text, so
    // the assignability check meets other pg_cast rows, text I/O and the array branch.
    [Retain("anonymise-target", nameof(CreatedAt))]
    [RetentionEntityId("a7c3e9f1-4b2d-4e86-9a15-0d6f8b2c4e73")]
    [RetentionTenantless]
    private sealed class AnonymiseNullIntegerRecord
    {
        public Guid Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [Anonymise(AnonymiseMethod.Null)]
        public int? Label { get; set; }

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    [Retain("anonymise-target", nameof(CreatedAt))]
    [RetentionEntityId("f2b8d4a6-3c1e-4f97-b6d0-9e5a7c3f1b28")]
    [RetentionTenantless]
    private sealed class AnonymiseNullUuidRecord
    {
        public Guid Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [Anonymise(AnonymiseMethod.Null)]
        public Guid? Label { get; set; }

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    [Retain("anonymise-target", nameof(CreatedAt))]
    [RetentionEntityId("4e9a1c7d-8b3f-4d62-a0e5-6f2c9b8d1a54")]
    [RetentionTenantless]
    private sealed class AnonymiseNullTextArrayRecord
    {
        public Guid Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [Anonymise(AnonymiseMethod.Null)]
        public string[]? Label { get; set; }

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    // The sample's rules, except that its soft-delete category declares and resolves Purge only.
    private sealed class PurgeOnlySoftDeleteRuleProvider : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider sample = new();

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == "soft-delete"
                ? new RetentionCategoryCapabilities([Strategy.Purge])
                : sample.GetCapabilities(category);

        public Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct) =>
            context.Category == "soft-delete"
                ? Task.FromResult<RetentionRule?>(new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge))
                : sample.ResolveAsync(context, ct);
    }

    private sealed class AnonymiseTargetRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == "anonymise-target" ? new RetentionCategoryCapabilities([Strategy.Anonymise]) : null;

        public Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct) =>
            Task.FromResult<RetentionRule?>(new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Anonymise));
    }

    private sealed class UnsupportedProviderDbContext(
        DbContextOptions<UnsupportedProviderDbContext> options
    ) : DbContext(options);
}
