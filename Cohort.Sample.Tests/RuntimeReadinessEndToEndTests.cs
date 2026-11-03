using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Globalization;
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
                error.Contains(typeof(Note).FullName!, StringComparison.Ordinal)
                && error.Contains("\"public\".\"notes_elsewhere\"", StringComparison.Ordinal)
                && error.Contains("was not found in the database", StringComparison.Ordinal)
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
                error.Contains(typeof(HoldLinkChild).Name, StringComparison.Ordinal)
                && error.Contains(
                    "link column \"public\".\"hold_link_children\".\"CaseIdElsewhere\" was not found in the database",
                    StringComparison.Ordinal
                )
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
    // whatever the database has become since: the retained-tables and marker-column checks would
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
                error.Contains(typeof(TenantlessLog).FullName!, StringComparison.Ordinal)
                && error.Contains("\"public\".\"tenantless_logs\"", StringComparison.Ordinal)
                && error.Contains("was not found in the database", StringComparison.Ordinal)
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
            await GrantReadAsync(database, role);

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
            await GrantReadAsync(database, role);
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
            await GrantReadAsync(database, role);
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

    private static async Task GrantReadAsync(TemporaryDatabase database, string role)
    {
        var name = new NpgsqlConnectionStringBuilder(database.ConnectionString).Database!;
        await ExecuteAsync(
            database,
            $"""
            GRANT CONNECT ON DATABASE "{name}" TO "{role}";
            GRANT USAGE ON SCHEMA public TO "{role}";
            GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{role}";
            """
        );
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
        IRetentionExecutionScope? executionScope = null
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
            executionScope
        );

    // The same over any context, such as one whose model carries a hold relation, which the
    // sample's doesn't. register adds what the context's retained entities need.
    private static ServiceProvider BuildModelCopyProvider<TContext>(
        string connectionString,
        IRetentionRuleProvider ruleProvider,
        Action<IServiceCollection>? register = null,
        IRetentionExecutionScope? executionScope = null
    )
        where TContext : DbContext
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddScoped<ModelVariant>();
        services.AddDbContext<TContext>((provider, options) => UseModelCopy(options, provider, connectionString));
        services.AddSingleton(ruleProvider);
        register?.Invoke(services);
        services.AddSingleton<IRetentionExecutionScope>(executionScope ?? RetentionExecutionScope.Unisolated);
        services.AddCohort<TContext>();
        return services.BuildServiceProvider(validateScopes: true);
    }

    private static void UseModelCopy(
        DbContextOptionsBuilder options,
        IServiceProvider scopeServices,
        string connectionString
    )
    {
        options.UseNpgsql(connectionString).UseMemoryCache(ModelCacheThatHoldsNothing);
        scopeServices.GetRequiredService<ModelVariant>().Configure?.Invoke(options);
    }

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
        // The model says text; only the catalog knows the column became varchar(8), a char(4)
        // declared by column type alone, or a uuid. "[redacted]" is too long for the first two
        // and not a uuid, so every anonymise of these rows would fail at runtime.
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
            error.Contains(typeof(SoftDeleteRecord).FullName!, StringComparison.Ordinal)
            && error.Contains($"member {column}", StringComparison.Ordinal)
            && error.Contains("was not found", StringComparison.Ordinal)
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
            error.Contains(typeof(AnonymiseNullRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("member AnonymisedAt", StringComparison.Ordinal)
            && error.Contains("was not found", StringComparison.Ordinal)
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
    // only through its elements, and text to uuid fails there too.
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
            error.Contains(entityType.FullName!, StringComparison.Ordinal)
            && error.Contains("member Label", StringComparison.Ordinal)
            && error.Contains(
                $"model store type {modelType} can't be assigned to its column type {columnType} ",
                StringComparison.Ordinal
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
    [InlineData("""ALTER TABLE public.anonymise_literal_records DROP COLUMN "Label" """, "column", "\"Label\"")]
    [InlineData("""ALTER TABLE public.anonymise_literal_records DROP COLUMN "Id" """, "record-id column", "\"Id\"")]
    [InlineData("DROP TABLE public.anonymise_literal_records", "table", "\"public\".\"anonymise_literal_records\"")]
    public async Task Readiness_refuses_an_anonymise_target_missing_from_the_database(
        string ddl,
        string missing,
        string name
    )
    {
        await using var database = await AnonymiseTargetDatabaseAsync(ddl);
        await using var provider = BuildAnonymiseTargetProvider(database.ConnectionString);

        var errors = await ReadinessErrorsAsync(provider);

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(AnonymiseLiteralRecord).FullName!, StringComparison.Ordinal)
            && error.Contains($"{missing} ", StringComparison.Ordinal)
            && error.Contains(name, StringComparison.Ordinal)
            && error.Contains("not found in the database", StringComparison.Ordinal)
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

    private sealed class SystemWorkRoutingScope(DatabaseRoute route, string systemConnectionString)
        : IRetentionExecutionScope
    {
        public Task RunAsync(RetentionExecutionContext context, Func<Task> work, CancellationToken ct)
        {
            if (context is RetentionExecutionContext.System)
            {
                route.ConnectionString = systemConnectionString;
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

        protected override DbCommand CreateDbCommand() => inner.CreateCommand();

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
            inner.BeginTransaction(isolationLevel);

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
