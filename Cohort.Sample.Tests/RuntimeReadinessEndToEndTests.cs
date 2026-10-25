using System.Collections.Concurrent;
using System.Globalization;

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

    private Task<TemporaryDatabase> AnonymiseTargetDatabaseAsync(string ddl) =>
        AnonymiseTargetDatabaseAsync<AnonymiseTargetDbContext>(ddl);

    private async Task<TemporaryDatabase> AnonymiseTargetDatabaseAsync<TContext>(string ddl)
        where TContext : DbContext
    {
        var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var options = new DbContextOptionsBuilder<TContext>().UseNpgsql(database.ConnectionString).Options;
        await using var db = (TContext)Activator.CreateInstance(typeof(TContext), options)!;
        await db.Database.ExecuteSqlRawAsync(db.Database.GenerateCreateScript());
        await db.Database.ExecuteSqlRawAsync(ddl);
        return database;
    }

    private static ServiceProvider BuildAnonymiseTargetProvider(string connectionString) =>
        BuildAnonymiseTargetProvider<AnonymiseTargetDbContext>(connectionString);

    private static ServiceProvider BuildAnonymiseTargetProvider<TContext>(string connectionString)
        where TContext : DbContext
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddDbContext<TContext>(builder => builder.UseNpgsql(connectionString));
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
