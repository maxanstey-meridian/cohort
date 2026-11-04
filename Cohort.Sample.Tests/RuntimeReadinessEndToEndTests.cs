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

    // T5: a model copy is a new entry, so it is checked on what it reads, not passed on the first
    // copy's recorded pass.
    // Make-fail: key the readiness state by connection string alone, dropping the per-IModel
    // entry (the copy then finds the first copy's recorded pass and is not refused).
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

    // T5a: a model copy is a new entry, so its stored-identity check runs. A copy that no longer
    // registers an entity with an active hold is refused for the orphaned hold, not passed with
    // that hold silently matching nothing.
    // Make-fail: key the readiness state by connection string alone, dropping the per-IModel
    // entry (the copy then finds the first copy's recorded pass and is not refused).
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
                RefusesWith(error, typeof(HoldLinkChild), "42703", "column target.CaseIdElsewhere does not exist")
            );
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

    // A1: one model instance reaches one database under two connection strings that differ in
    // what a pooler can route by (user name, options), and each is validated: the model's entry
    // is keyed by the connection string without its password. The anonymise check runs on every
    // pass and is never recorded, so only a recorded pass on the entry could skip it.
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

    // Unqualified type names resolve through the search path. The domain is in a schema only the
    // first connection string searches, and the search path is part of the connection string, so
    // the second string is an entry of its own.
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
    // entry per token. The same role under a new password reaches the same database, so it is
    // the same entry, its recorded pass stands, and the anonymise check is not run again.
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

    // A login role that holds every privilege Cohort's statements need: the reads, the writes,
    // and the row locks (which need UPDATE).
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

    // ── each operation works under the role that sends it ──────────────────────────────────────
    //
    // ScopeRoles gives every kind of scope Cohort creates a role of its own (the connecting
    // superuser switches into it when a connection opens), so these tests revoke a privilege
    // from one scope's role and see what the operations under it do. Readiness checks no
    // privilege: a missing one fails at the statement, with PostgreSQL's own 42501.

    // A role with only SELECT can preview: the preview's counts and the clock are reads, and
    // nothing in it writes. Make-fail: RetentionPreviewService runs one write on a Cohort table
    // after its readiness call (the preview then fails with PostgreSQL's 42501).
    [Fact]
    public async Task A_read_only_role_can_preview()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(database, $"REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public FROM \"{roles.Tenant}\"");
        using var host = RoleHost(database, roles);

        var preview = () => host.RunPreviewAsync(NewTenant());

        await preview.Should().NotThrowAsync();
    }

    // A caller role that manages holds and nothing else: SELECT, INSERT, and UPDATE on
    // retention_holds, SELECT and UPDATE (the row locks) on the host's tables, and nothing on the
    // run, summary, row-detail, and handler-status tables. Every holds operation passes: a hold
    // call sends none of the sweep's recording.
    // Make-fail: EfRetentionHoldsRepository.CreateAsync runs one more statement, a write on
    // sweep_run (DELETE FROM public.sweep_run WHERE FALSE) after its readiness call; the first
    // hold call fails with PostgreSQL's 42501.
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
    // of an unheld row: a deletion sends none of the sweep's recording, and the guard reads
    // holds only.
    // Make-fail: EfRetentionDeletion.ExecuteAsync runs one more statement, a write on sweep_run
    // (DELETE FROM public.sweep_run WHERE FALSE) after its readiness call; the deletion fails
    // with PostgreSQL's 42501.
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

    // The model pass finds a schema fault in a statement over a retained table whatever the role's
    // privileges: PostgreSQL's parse analysis names the missing column before it checks
    // permissions, so a startup role that writes nothing still refuses the host. The fault is
    // there before the first start, since a started host records its pass.
    // Make-fail: RunModelPassAsync's Statements check skipped.
    [Fact]
    public async Task Startup_refuses_a_schema_fault_under_a_role_that_cannot_write()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(database, $"REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public FROM \"{roles.Readiness}\"");
        await ExecuteAsync(database, """ALTER TABLE public.soft_delete_records DROP COLUMN "DeletedAt" """);
        using var host = RoleHost(database, roles);

        var start = () => host.RunWithServicesAsync(services => StartValidationAsync(services));

        (await start.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().Contain(error =>
                RefusesWith(error, typeof(SoftDeleteRecord), "42703", "\"DeletedAt\"")
            );
    }

    // A System(Readiness) role that writes nothing passes startup: the model pass tolerates
    // PostgreSQL's 42501 for the statements it cannot run, which then fail at the statement
    // under the role that sends them. The same role, when a sweep runs under it, fails at the
    // sweep's first write, the sweep_run insert, with PostgreSQL's error and not a readiness
    // refusal.
    // Make-fail: RetentionStatementReadiness.ExplainAsync stops tolerating 42501 (startup
    // throws).
    [Fact]
    public async Task Startup_tolerates_a_role_that_cannot_write_and_the_sweep_fails_at_its_statement()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(database, $"REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public FROM \"{roles.Readiness}\"");
        var tenant = NewTenant();
        roles.AssignTenant(tenant.Id, roles.Readiness);
        using var host = RoleHost(database, roles);

        var start = () => host.RunWithServicesAsync(services => StartValidationAsync(services));
        await start.Should().NotThrowAsync();

        var sweep = () => host.RunSweepAsync(tenant);
        await ShouldBeDeniedAsync(sweep, "sweep_run");
    }

    // The stored entity identities are read in a System(Readiness) scope under that scope's
    // role. A role that cannot read the holds table fails the read with PostgreSQL's 42501, so
    // readiness fails instead of passing, and no run starts.
    // Make-fail: the stored-identities step skips its read.
    [Fact]
    public async Task Readiness_fails_with_42501_when_the_readiness_scope_cannot_read_holds()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(database, $"REVOKE SELECT ON public.retention_holds FROM \"{roles.Readiness}\"");
        using var host = RoleHost(database, roles);

        var act = () => host.RunSweepAsync(NewTenant());

        await ShouldBeDeniedAsync(act, "retention_holds");
        (await CountAsync(database, "sweep_run")).Should().Be(0, "readiness failed before a run started");
    }

    // The model pass reads the stored entity identities in the Readiness scope once per entry,
    // under the role the scope ran as. A scope hook that switches System(Readiness) to a role
    // that cannot read holds later reaches the guard that reads tenantless holds, and that read
    // fails with PostgreSQL's 42501: the deletion fails and the row is still there.
    // Make-fail: RetentionHoldGuard.ReadsTenantlessHolds returns false (the deletion succeeds).
    [Fact]
    public async Task A_readiness_role_switched_after_the_model_pass_cannot_read_holds_and_the_held_row_stays()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        using var host = RoleHost(database, roles);
        var first = await SeedTenantlessLogAsync(database.ConnectionString);
        var second = await SeedTenantlessLogAsync(database.ConnectionString);

        (await host.RunWithServicesAsync(services => DeleteTenantlessLogAsync(services, first)))
            .Should().Be(RetentionDeletionOutcome.Executed);
        await roles.UseReadinessRoleAsync(database);
        var act = () => host.RunWithServicesAsync(services => DeleteTenantlessLogAsync(services, second));

        await ShouldBeDeniedAsync(act, "retention_holds");
        (await TenantlessLogExistsAsync(database.ConnectionString, second)).Should().BeTrue();
    }

    // A dispatcher role that cannot claim work fails with PostgreSQL's 42501 at the claim, from
    // FlushAsync and from a poll alike, but only after the payload scrub has run: personal data
    // never outlives its retention because the dispatcher's claim fails. The tenant role's
    // sweeps are untouched.
    // A scrub that fails leaves the poll's hourly gate unspent, so the next poll retries it, and
    // under the kill switch the poll scrubs and returns before it claims anything.
    // Make-fail: PollAsync sets lastPayloadScrubAt before ScrubExpiredPayloadsAsync (the retry
    // finds the gate spent, so the payload stays).
    [Fact]
    public async Task The_dispatcher_fails_at_the_claim_under_a_role_that_cannot_claim_work()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(database, $"REVOKE UPDATE ON public.sweep_row_handler_status FROM \"{roles.RowDispatch}\"");
        using var host = RoleHost(database, roles);
        var viaPoll = await SeedExpiredPayloadAsync(database);

        var poll = () => host.RunWithServicesAsync(services => services.GetRequiredService<RetentionRowDispatcher>().PollAsync(CancellationToken.None));
        var flush = () => host.RunWithServicesAsync(services => services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync());

        await ShouldBeDeniedAsync(poll, "sweep_row_handler_status");
        (await CapturedPayloadAsync(database, viaPoll)).Should().BeNull("the poll scrubbed before it failed");
        var viaFlush = await SeedExpiredPayloadAsync(database);
        await ShouldBeDeniedAsync(flush, "sweep_row_handler_status");
        (await CapturedPayloadAsync(database, viaFlush)).Should().BeNull("flush scrubbed before it failed");
        (await host.RunSweepAsync(NewTenant())).EntityFailures.Should().BeEmpty();

        // A poll whose scrub fails leaves the hourly gate unspent: once the role can scrub, the
        // next poll does, though its claim still fails.
        await ExecuteAsync(database, $"REVOKE UPDATE ON public.sweep_run_row_detail FROM \"{roles.RowDispatch}\"");
        using var scrubHost = RoleHost(database, roles);
        var viaFailedScrub = await SeedExpiredPayloadAsync(database);
        var scrubPoll = () => scrubHost.RunWithServicesAsync(services => services.GetRequiredService<RetentionRowDispatcher>().PollAsync(CancellationToken.None));

        await ShouldBeDeniedAsync(scrubPoll, "sweep_run_row_detail");
        (await CapturedPayloadAsync(database, viaFailedScrub)).Should().NotBeNull("the failed scrub changed nothing");
        await ExecuteAsync(database, $"GRANT UPDATE ON public.sweep_run_row_detail TO \"{roles.RowDispatch}\"");
        await ShouldBeDeniedAsync(scrubPoll, "sweep_row_handler_status");
        (await CapturedPayloadAsync(database, viaFailedScrub)).Should().BeNull("the failed poll did not spend the scrub's hourly gate");

        // Under the kill switch the dispatcher doesn't run, so its failing claim stops nothing:
        // the poll scrubs and returns.
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

    // A worker role that cannot read the ledger fails the iteration with PostgreSQL's 42501. The
    // failure comes from the iteration's read-back of the ledger, after the passes: each pass
    // runs in its own tenant or tenantless scope, under that scope's role, not the coordination
    // role's, so passes run before the read fails.
    // Make-fail: the role is granted SELECT on sweep_run back (the iteration no longer fails).
    [Fact]
    public async Task The_worker_fails_under_a_role_that_cannot_read_the_ledger()
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedTemporaryDatabaseAsync();
        await roles.GrantAsync(database);
        await ExecuteAsync(database, $"REVOKE SELECT ON public.sweep_run FROM \"{roles.WorkerCoordination}\"");
        using var host = RoleHost(database, roles);

        var act = () => host.RunWithServicesAsync(services => services
            .GetServices<IHostedService>().OfType<RetentionWorker>().Single()
            .RunScheduledIterationAsync("0 0 0 * * *", dryRun: false, settled: null, CancellationToken.None));

        await ShouldBeDeniedAsync(act, "sweep_run");
    }

    // A pruner whose role has no DELETE on the run tables, with only InactiveHoldRetention set,
    // prunes the inactive holds and leaves the settled runs alone: it never sends a run-table
    // statement (docs/guides/history-pruning.md).
    // Make-fail: RetentionHistoryPruner.PruneAsync runs its run-table loop whatever scope.Runs
    // says, with a run cutoff of its own (`succeededCutoff ?? now`; with null cutoffs nothing is
    // eligible and no DELETE is sent); PruneAsync then throws PostgreSQL's 42501 on
    // sweep_run_row_detail. (Under the pruner's own loop that throw is logged and the pass
    // retries after the interval.)
    [Fact]
    public async Task A_pruner_without_delete_on_the_run_tables_prunes_holds_and_leaves_runs()
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

        using var host = RoleHost(database, roles, configuration: settings);

        var result = await host.RunWithServicesAsync(services => services.GetRequiredService<RetentionHistoryPruner>().PruneAsync());

        result.Should().Be(new RetentionHistoryPruneResult(0, 1));
        (await CountAsync(database, "retention_holds")).Should().Be(0, "the inactive hold is past its retention");
        (await CountAsync(database, "sweep_run")).Should().Be(1, "the settled run is left alone");
    }

    public enum StartupSubsystems
    {
        WorkerAndPruning,
        NoSchedule,
        NoPruning,
        KillSwitch,
    }

    // Startup runs the model pass in System(Readiness), then in each background subsystem's
    // scope that will run, so that scope's own context is checked too: RowDispatch always,
    // WorkerCoordination only when the worker runs (a schedule is set and the kill switch is
    // off), HistoryPruning only when pruning is set (a retention is set and the kill switch is
    // off). The scope log records the scopes the hosted service opens, in order.
    // Make-fails, each its own run, each in RetentionValidationHostedService.StartAsync: the
    // RowDispatch call dropped; the HistoryPruning call made unconditional; the
    // WorkerCoordination call dropped.
    [Theory]
    [InlineData(StartupSubsystems.WorkerAndPruning)]
    [InlineData(StartupSubsystems.NoSchedule)]
    [InlineData(StartupSubsystems.NoPruning)]
    [InlineData(StartupSubsystems.KillSwitch)]
    public async Task Startup_runs_the_model_pass_in_each_subsystem_scope_that_will_run(StartupSubsystems subsystems)
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        var schedule = $"{CohortOptions.SectionName}:Schedule";
        var pruning = $"{CohortOptions.SectionName}:HistoryPruning:InactiveHoldRetention";
        var killSwitch = $"{CohortOptions.SectionName}:KillSwitch";
        var (settings, expected) = subsystems switch
        {
            StartupSubsystems.WorkerAndPruning => (
                new Dictionary<string, string?> { [schedule] = "0 0 0 * * *", [pruning] = "1.00:00:00" },
                new[]
                {
                    RetentionSystemWork.Readiness,
                    RetentionSystemWork.RowDispatch,
                    RetentionSystemWork.WorkerCoordination,
                    RetentionSystemWork.HistoryPruning,
                }
            ),
            StartupSubsystems.NoSchedule => (
                new Dictionary<string, string?> { [pruning] = "1.00:00:00" },
                new[]
                {
                    RetentionSystemWork.Readiness,
                    RetentionSystemWork.RowDispatch,
                    RetentionSystemWork.HistoryPruning,
                }
            ),
            StartupSubsystems.NoPruning => (
                new Dictionary<string, string?> { [schedule] = "0 0 0 * * *" },
                new[]
                {
                    RetentionSystemWork.Readiness,
                    RetentionSystemWork.RowDispatch,
                    RetentionSystemWork.WorkerCoordination,
                }
            ),
            StartupSubsystems.KillSwitch => (
                new Dictionary<string, string?> { [schedule] = "0 0 0 * * *", [pruning] = "1.00:00:00", [killSwitch] = "true" },
                new[] { RetentionSystemWork.Readiness, RetentionSystemWork.RowDispatch }
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(subsystems)),
        };
        var executionScope = new RecordingExecutionScope();
        using var host = new CohortTestHost(
            database.ConnectionString,
            configurationOverrides: settings,
            configureServices: services =>
            {
                services.RemoveAll<IRetentionExecutionScope>();
                services.AddSingleton<IRetentionExecutionScope>(executionScope);
            }
        );

        await host.RunWithServicesAsync(services => StartValidationAsync(services));

        // The first Readiness is the hosted service's own. The model pass it runs reads the
        // stored entity identities in a Readiness scope of its own (holds span tenants), so the
        // log shows a second one straight after. Later passes find the model already validated
        // and open no scope of their own.
        var logged = new[] { RetentionSystemWork.Readiness }.Concat(expected);
        executionScope.Ran.Should().Equal(logged.Select(work => new RetentionExecutionContext.System(work)));
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

    // Runs the hosted startup check, which a host runs once before it serves.
    private static Task StartValidationAsync(IServiceProvider services) =>
        services.GetServices<IHostedService>().OfType<RetentionValidationHostedService>().Single()
            .StartAsync(CancellationToken.None);

    // The operation fails at the statement with PostgreSQL's own 42501 (a PostgresException, not
    // a readiness refusal), naming the table the role lacks a privilege on.
    private static async Task ShouldBeDeniedAsync(Func<Task> act, string table)
    {
        var thrown = (await act.Should().ThrowAsync<PostgresException>()).Which;
        thrown.SqlState.Should().Be(PostgresErrorCodes.InsufficientPrivilege);
        thrown.MessageText.Should().Be($"permission denied for table {table}");
    }

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

    // T6: a check that passed inside a caller's transaction may have seen DDL that rolls back,
    // so the pass is not recorded. Both calls run on one model instance (one entry), so only the
    // transaction rule can make the second call run the check again.
    // Make-fail: ValidateAsync sets ModelValidated whatever the transaction, and RunCheckAsync
    // records every check inside one.
    [Fact]
    public async Task A_check_passed_inside_a_caller_transaction_is_not_kept()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        await ExecuteAsync(
            database,
            "ALTER TABLE public.sweep_run_row_detail RENAME TO malformed_sweep_run_row_detail"
        );
        using var host = new CohortTestHost(database.ConnectionString);

        await host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            await db.Database.ExecuteSqlRawAsync(
                "ALTER TABLE public.malformed_sweep_run_row_detail RENAME TO sweep_run_row_detail"
            );

            await ListActiveHoldsAsync(services);

            await transaction.RollbackAsync();
        });

        var act = () => host.RunWithServicesAsync(services => ListActiveHoldsAsync(services));

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
                RefusesWith(error, typeof(TenantlessLog), "42703", "column target.CreatedAt does not exist")
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
    [InlineData("IsDeleted", "column target.IsDeleted does not exist")]
    [InlineData("DeletedAt", "column \"DeletedAt\" of relation \"soft_delete_records\" does not exist")]
    public async Task Readiness_refuses_a_missing_soft_delete_marker_column(string column, string message)
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
            RefusesWith(error, typeof(SoftDeleteRecord), "42703", message)
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
            RefusesWith(error, typeof(AnonymiseNullRecord), "42703", "column target.AnonymisedAt does not exist")
        );
    }

    [Fact]
    public async Task Readiness_refuses_a_soft_delete_marker_column_the_database_generates()
    {
        // A generated column takes no UPDATE; every soft-delete would fail at "column can only be
        // updated to DEFAULT". A generated column is nullable unless declared otherwise, and the
        // model pass's nullable-flag rule refuses that before its explain reaches the UPDATE.
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
    // so the model pass's explain of the soft-delete UPDATE refuses it.
    // Make-fail: RunModelPassAsync's Statements check skipped.
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
    [InlineData("""ALTER COLUMN "IsDeleted" TYPE integer USING "IsDeleted"::integer""", "integer = boolean")]
    [InlineData("""ALTER COLUMN "TenantId" TYPE text USING "TenantId"::text""", "text = uuid")]
    public async Task Readiness_refuses_a_retained_column_a_statement_cannot_compare(
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
            RefusesWith(error, typeof(SoftDeleteRecord), "42883", $"operator does not exist: {comparison}")
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
            RefusesWith(error, typeof(SoftDeleteRecord), "42703", $"column target.{column} does not exist")
        );
    }

    // An erasure matches rows by the subject column and a hold listing joins through it, so a
    // subject column the database lost, or retyped away from the model's uuid, fails every
    // erasure of its kind. Only the erasure scopes and the subject-kind hold listing read it, but
    // the model pass explains the union of every operation's statements, so it refuses the model
    // for a sweep too.
    // Make-fail: RetentionStatementReadiness.ScopesAsync leaves the erasure scopes out of the
    // union (the sweep half passes).
    [Theory]
    [InlineData("""ALTER TABLE public.soft_delete_records DROP COLUMN "SubjectId" """, "42703", "column target.SubjectId does not exist")]
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
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());

        // Only an erasure reads the subject column (a sweep sends no statement over it), yet the
        // sweep is refused too: the refusal is the model's.
        var sweep = () => host.RunSweepAsync(tenant);
        var erasure = () => host.RunErasureAsync(
            tenant,
            new ErasureScope("user", Guid.NewGuid(), allowSoftDeleteAsErasure: true)
        );

        foreach (var (operation, act) in new (string, Func<Task>)[] { ("a sweep", sweep), ("an erasure", erasure) })
        {
            (await act.Should().ThrowAsync<RetentionConfigurationException>(operation))
                .Which.Errors.Should().ContainSingle(
                    error =>
                        RefusesWith(error, typeof(SoftDeleteRecord), sqlState, message)
                        && error.Contains("erasing subjects of kind 'user'", StringComparison.Ordinal),
                    operation
                );
        }
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
    // UPDATE (which PostgreSQL would also refuse, 428C9) reaches the model pass's explain.
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

    // A method other than Null passes the model pass's rules, so its explain reaches the UPDATE
    // and PostgreSQL refuses an identity column GENERATED ALWAYS (428C9).
    // Make-fail: RunModelPassAsync's Statements check skipped.
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
    [InlineData("""ALTER TABLE public.anonymise_literal_records DROP COLUMN "Id" """, "42703", "column target.Id does not exist")]
    [InlineData("DROP TABLE public.anonymise_literal_records", "42P01", "\"public.anonymise_literal_records\"")]
    public async Task Readiness_refuses_an_anonymise_target_missing_from_the_database(
        string ddl,
        string sqlState,
        string message
    )
    {
        await using var database = await AnonymiseTargetDatabaseAsync(ddl);
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            RefusesWith(error, typeof(AnonymiseLiteralRecord), sqlState, message)
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

    // PostgreSQL's refusal as readiness reports it: one error per entity holding each distinct
    // SQLSTATE and message its statements drew, the operations that send them, and the tables
    // they read. `message` is a fragment of PostgreSQL's own wording.
    private static bool RefusesWith(string error, Type entity, string sqlState, string message) =>
        error.Contains($"Retained entity {entity.FullName}:", StringComparison.Ordinal)
        && error.Contains($" {sqlState}: ", StringComparison.Ordinal)
        && error.Contains(message, StringComparison.Ordinal);

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
        // pass's column rule runs before its explain of the statements, so the rule's refusal is
        // the one reported, and it names the base type it walked to.
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
