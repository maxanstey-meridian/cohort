using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Channels;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Migrations;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Npgsql;

using Xunit.Sdk;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class RetentionWorkerEndToEndTests(PostgresFixture fixture) : IAsyncLifetime
{
    // The fixture database until a test moves to its own: tests that read the ledger across
    // tenants or check tenant coverage must see only the rows they seed.
    private string ConnectionString { get; set; } = fixture.ConnectionString;

    public async Task InitializeAsync()
    {
        await using var connection = new Npgsql.NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await fixture.Respawner.ResetAsync(connection);
    }

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }

    [Fact]
    public async Task Worker_Persists_Scheduled_DryRun_For_The_Tenanted_Entity_Scope_Without_Mutating_Rows()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: true,
            killSwitch: false
        );
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
            }
        );
        await SeedOldNoteAsync(tenant.Id, "scheduled-dry-run");

        await host.Host.StartAsync();
        await WaitUntilAsync(
            async () =>
            {
                var run = await LoadLatestRunAsync(tenant.Id);
                return run is { Status: SweepRunStatus.Succeeded }
                    && run.Value.EntityTypes.Contains(typeof(Note).FullName!);
            },
            TimeSpan.FromSeconds(8)
        );
        await host.Host.StopAsync();

        var run = await LoadLatestRunAsync(tenant.Id);
        run.Should().NotBeNull();
        run!.Value.Trigger.Should().Be(SweepTriggerKind.Scheduled);
        run.Value.DryRun.Should().BeTrue();
        run.Value.Status.Should().Be(SweepRunStatus.Succeeded);
        run.Value.EntityTypes.Should().Contain(typeof(Note).FullName!);
        run.Value.EntityTypes.Should().NotContain(typeof(TenantlessLog).FullName!);
        (await NoteExistsAsync("scheduled-dry-run")).Should().BeTrue();
    }

    [Fact]
    public async Task Worker_Isolates_A_Tenant_Failure_And_Still_Runs_Later_And_Tenantless_Passes()
    {
        var failingTenant = CreateTenant();
        var healthyTenant = CreateTenant();
        var settings = CreateSettings(
            ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        using var host = BuildHost(
            settings,
            failingTenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider>(
                    new TenantFailingRuleProvider(failingTenant.Id)
                );
                services.AddSingleton<IRetentionTenantSource>(
                    new StaticTenantSource(failingTenant, healthyTenant)
                );
            }
        );
        await SeedOldNoteAsync(failingTenant.Id, "isolated-failing-tenant");
        await SeedOldNoteAsync(healthyTenant.Id, "isolated-healthy-tenant");
        await SeedOldTenantlessLogAsync("isolated-tenantless");

        await host.Host.StartAsync();
        await WaitUntilAsync(
            async () =>
                !await NoteExistsAsync("isolated-healthy-tenant")
                && !await TenantlessLogExistsAsync("isolated-tenantless"),
            TimeSpan.FromSeconds(8)
        );
        await host.Host.StopAsync();

        (await NoteExistsAsync("isolated-failing-tenant")).Should().BeTrue();
        (await NoteExistsAsync("isolated-healthy-tenant")).Should().BeFalse();
        (await TenantlessLogExistsAsync("isolated-tenantless")).Should().BeFalse();
    }

    [Fact]
    public async Task Worker_Skips_Occurrences_While_Another_Instance_Holds_The_Sweep_Lock()
    {
        var sweepAdvisoryLockKey = RetentionRunAdvisoryLock.WorkerKeyFor(
            new RelationalObjectName("public", "sweep_run")
        );
        var skippedOccurrenceLog = new SkippedOccurrenceLogProvider();
        var tenant = CreateTenant();
        var settings = CreateSettings(
            ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
                services.AddSingleton<ILoggerProvider>(skippedOccurrenceLog);
            }
        );
        await SeedOldNoteAsync(tenant.Id, "lock-guarded-note");

        await using var lockConnection = new NpgsqlConnection(ConnectionString);
        await lockConnection.OpenAsync();
        await using (var acquire = lockConnection.CreateCommand())
        {
            acquire.CommandText = "SELECT pg_advisory_lock(@key)";
            acquire.Parameters.AddWithValue("key", sweepAdvisoryLockKey);
            await acquire.ExecuteScalarAsync();
        }

        await host.Host.StartAsync();
        await skippedOccurrenceLog.WaitForOccurrencesAsync(2).WaitAsync(TimeSpan.FromSeconds(8));

        skippedOccurrenceLog.OccurrenceCount.Should().BeGreaterThanOrEqualTo(2);
        (await NoteExistsAsync("lock-guarded-note")).Should().BeTrue();

        await using (var release = lockConnection.CreateCommand())
        {
            release.CommandText = "SELECT pg_advisory_unlock(@key)";
            release.Parameters.AddWithValue("key", sweepAdvisoryLockKey);
            await release.ExecuteScalarAsync();
        }

        await WaitUntilAsync(
            async () => !await NoteExistsAsync("lock-guarded-note"),
            TimeSpan.FromSeconds(8)
        );

        await host.Host.StopAsync();

        (await NoteExistsAsync("lock-guarded-note")).Should().BeFalse();
    }

    [Fact]
    public async Task Two_Replicas_Firing_For_The_Same_Occurrence_Record_One_Scheduled_Run()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            ConnectionString,
            schedule: "0 0 0 1 1 *",
            dryRun: false,
            killSwitch: false
        );
        using var first = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
            }
        );
        using var second = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
            }
        );
        await SeedOldNoteAsync(tenant.Id, "once-per-occurrence");
        var occurrence = DateTimeOffset.UtcNow.AddSeconds(-1);

        await GetWorker(first).RunIterationAsync(occurrence, dryRun: false, CancellationToken.None);
        (await GetWorker(second).RunIterationAsync(occurrence, dryRun: false, CancellationToken.None))
            .Should().Be(OccurrenceOutcome.Settled);

        (await CountScheduledRunsAsync(tenant.Id)).Should().Be(1);
        (await NoteExistsAsync("once-per-occurrence")).Should().BeFalse();

        var nextOccurrence = DateTimeOffset.UtcNow;
        await GetWorker(second).RunIterationAsync(nextOccurrence, dryRun: false, CancellationToken.None);

        (await CountScheduledRunsAsync(tenant.Id)).Should().Be(2);
    }

    [Fact]
    public async Task A_Replica_That_Crashed_Mid_Occurrence_Does_Not_Keep_The_Occurrence_From_Other_Replicas()
    {
        var tenant = CreateTenant();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false),
            tenant,
            services => services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>()
        );
        await SeedOldNoteAsync(tenant.Id, "crashed-replica");
        var occurrence = DateTimeOffset.UtcNow.AddSeconds(-1);

        // The replica that fired first died after committing Started: its worker lock went
        // with its session, and nothing will ever settle the run.
        await using (var connection = new NpgsqlConnection(ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = """
                INSERT INTO "sweep_run" ("SweepId", "StartedAt", "Status", "TriggerKind", "DryRun", "TenantId")
                VALUES (@sweepId, @startedAt, @started, @scheduled, FALSE, @tenantId)
                """;
            command.Parameters.AddWithValue("sweepId", Guid.NewGuid());
            command.Parameters.AddWithValue("startedAt", occurrence.AddMilliseconds(10));
            command.Parameters.AddWithValue("started", (int)SweepRunStatus.Started);
            command.Parameters.AddWithValue("scheduled", (int)SweepTriggerKind.Scheduled);
            command.Parameters.AddWithValue("tenantId", tenant.Id);
            await command.ExecuteNonQueryAsync();
        }

        (await GetWorker(host).RunIterationAsync(occurrence, dryRun: false, CancellationToken.None))
            .Should().Be(OccurrenceOutcome.Settled);

        (await NoteExistsAsync("crashed-replica")).Should().BeFalse();
        (await CountScheduledRunsAsync(tenant.Id)).Should().Be(2);
    }

    [Fact]
    public async Task Installs_In_Different_Schemas_Of_One_Database_Sweep_Independently()
    {
        // Advisory locks are per database, not per schema: the public install's worker holding
        // its lock must not stop a second install in another schema from sweeping.
        var tenant = CreateTenant();
        var blockingRules = new BlockingRuleProvider(new SampleRetentionRuleProvider());
        using var publicInstall = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false),
            tenant,
            services => services.AddSingleton<IRetentionRuleProvider>(blockingRules)
        );
        await SeedOldNoteAsync(tenant.Id, "public-install");

        await CreateSecondInstallAsync();
        try
        {
            using var secondInstall = BuildSecondInstallHost();
            await using (var scope = secondInstall.Host.Services.CreateAsyncScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<SecondInstallDbContext>();
                db.Add(new SecondInstallRecord { Id = Guid.NewGuid(), CreatedAt = DateTimeOffset.UtcNow.AddDays(-120) });
                await db.SaveChangesAsync();
            }

            var occurrence = DateTimeOffset.UtcNow.AddSeconds(-1);
            var publicIteration = GetWorker(publicInstall).RunIterationAsync(occurrence, dryRun: false, CancellationToken.None);
            await blockingRules.Entered.WaitAsync(TimeSpan.FromSeconds(10));
            try
            {
                await GetWorker(secondInstall).RunIterationAsync(occurrence, dryRun: false, CancellationToken.None);
            }
            finally
            {
                blockingRules.Release();
                await publicIteration.WaitAsync(TimeSpan.FromSeconds(10));
            }

            await using var verifyScope = secondInstall.Host.Services.CreateAsyncScope();
            var verify = verifyScope.ServiceProvider.GetRequiredService<SecondInstallDbContext>();
            (await verify.Set<SecondInstallRecord>().CountAsync()).Should().Be(0);
            (await NoteExistsAsync("public-install")).Should().BeFalse();
        }
        finally
        {
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            await using var drop = connection.CreateCommand();
            drop.CommandText = $"DROP SCHEMA IF EXISTS \"{SecondInstallDbContext.Schema}\" CASCADE";
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task A_Dry_Run_Replica_Does_Not_Take_The_Occurrence_From_A_Real_Sweep()
    {
        // Mid rolling deploy one replica may still be configured to dry run.
        var tenant = CreateTenant();
        var settings = CreateSettings(
            ConnectionString,
            schedule: "0 0 0 1 1 *",
            dryRun: false,
            killSwitch: false
        );
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
            }
        );
        await SeedOldNoteAsync(tenant.Id, "dry-run-replica");
        var occurrence = DateTimeOffset.UtcNow.AddSeconds(-1);

        await GetWorker(host).RunIterationAsync(occurrence, dryRun: true, CancellationToken.None);
        (await NoteExistsAsync("dry-run-replica")).Should().BeTrue();

        await GetWorker(host).RunIterationAsync(occurrence, dryRun: false, CancellationToken.None);

        (await NoteExistsAsync("dry-run-replica")).Should().BeFalse();
        (await CountScheduledRunsAsync(tenant.Id)).Should().Be(2);
    }

    [Fact]
    public async Task Worker_Survives_A_Failing_Iteration_And_Sweeps_On_A_Later_Tick()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        var categoryRepository = new FailingOnceCategoryRepository(
            new SampleRetentionRuleProvider()
        );
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider>(categoryRepository);
            }
        );
        await SeedOldNoteAsync(tenant.Id, "resilient-delete");

        await host.Host.StartAsync();
        await categoryRepository.FailedIteration.WaitAsync(TimeSpan.FromSeconds(8));
        await categoryRepository.LaterSuccessfulIteration.WaitAsync(TimeSpan.FromSeconds(8));
        await WaitUntilAsync(
            async () => !await NoteExistsAsync("resilient-delete"),
            TimeSpan.FromSeconds(8)
        );

        await host.Host.StopAsync();

        (await NoteExistsAsync("resilient-delete")).Should().BeFalse();
    }

    [Fact]
    public async Task A_Host_Started_After_A_Missed_Occurrence_Sweeps_It_On_Start()
    {
        await using var database = await UseTemporaryDatabaseAsync();
        var tenant = CreateTenant();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false),
            tenant,
            services => services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>()
        );
        await SeedOldNoteAsync(tenant.Id, "missed-occurrence");

        await host.Host.StartAsync();
        await WaitUntilAsync(
            async () => (await LoadScheduledRunsAsync(tenant.Id)).Any(run => run.Status == SweepRunStatus.Succeeded),
            TimeSpan.FromSeconds(8)
        );
        await host.Host.StopAsync();

        (await NoteExistsAsync("missed-occurrence")).Should().BeFalse();

        var thisYear = new DateTimeOffset(DateTimeOffset.UtcNow.Year, 1, 1, 0, 0, 0, TimeSpan.Zero);
        (await LoadScheduledRunsAsync(tenant.Id)).Should().ContainSingle()
            .Which.Should().Match<(SweepRunStatus Status, DateTimeOffset StartedAt, string? Error)>(
                run => run.Status == SweepRunStatus.Succeeded && run.StartedAt >= thisYear
            );
    }

    [Fact]
    public async Task A_Failed_Pass_Is_Retried_After_The_Retry_Delay_Not_At_The_Next_Occurrence()
    {
        await using var database = await UseTemporaryDatabaseAsync();
        var tenant = CreateTenant();
        using var host = BuildHost(
            CreateSettings(
                ConnectionString,
                schedule: "0 0 0 1 1 *",
                dryRun: false,
                killSwitch: false,
                retryDelay: TimeSpan.FromSeconds(1)
            ),
            tenant,
            services => services.AddSingleton<IRetentionRuleProvider>(
                new FailingOnceCategoryRepository(new SampleRetentionRuleProvider())
            )
        );
        await SeedOldNoteAsync(tenant.Id, "retried-pass");

        // The first rule resolution fails, so the first run settles PartiallyFailed.
        await host.Host.StartAsync();
        await WaitUntilAsync(
            async () => (await LoadScheduledRunsAsync(tenant.Id)).Any(run => run.Status == SweepRunStatus.Succeeded),
            TimeSpan.FromSeconds(8)
        );
        await host.Host.StopAsync();

        (await NoteExistsAsync("retried-pass")).Should().BeFalse();

        (await LoadScheduledRunsAsync(tenant.Id)).Select(run => run.Status).Should().Equal(
            SweepRunStatus.PartiallyFailed,
            SweepRunStatus.Succeeded
        );
    }

    [Fact]
    public async Task An_Occurrence_The_Database_Clock_Has_Not_Reached_Is_Not_Swept_Yet()
    {
        await using var database = await UseTemporaryDatabaseAsync();
        var tenant = CreateTenant();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false),
            tenant,
            services => services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>()
        );
        await SeedOldNoteAsync(tenant.Id, "early-replica");
        // A replica whose clock runs an hour ahead of the database's: its runs would start
        // before the occurrence on the database clock and never count towards it.
        var occurrence = DateTimeOffset.UtcNow.AddHours(1);

        var outcome = await GetWorker(host).RunIterationAsync(occurrence, dryRun: false, CancellationToken.None);

        outcome.Should().Be(OccurrenceOutcome.NotDue);
        (await NoteExistsAsync("early-replica")).Should().BeTrue();
        (await CountScheduledRunsAsync(tenant.Id)).Should().Be(0);
    }

    [Fact]
    public async Task A_Pass_Never_Started_Because_The_Owner_Died_Between_Passes_Still_Runs_And_Finished_Passes_Do_Not_Rerun()
    {
        await using var database = await UseTemporaryDatabaseAsync();
        var finished = CreateTenant();
        var neverStarted = CreateTenant();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false),
            finished,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
                services.AddSingleton<IRetentionTenantSource>(new StaticTenantSource(finished, neverStarted));
            }
        );
        // The finished tenant's note stands in for a row its pass legitimately left: its pass
        // already succeeded for this occurrence, so it must not run again.
        await SeedOldNoteAsync(finished.Id, "finished-pass");
        await SeedOldNoteAsync(neverStarted.Id, "never-started-pass");
        var occurrence = DateTimeOffset.UtcNow.AddSeconds(-1);
        await InsertScheduledRunAsync(finished.Id, occurrence.AddMilliseconds(10), SweepRunStatus.Succeeded);

        var outcome = await GetWorker(host).RunIterationAsync(occurrence, dryRun: false, CancellationToken.None);

        outcome.Should().Be(OccurrenceOutcome.Settled);
        (await NoteExistsAsync("never-started-pass")).Should().BeFalse();
        (await NoteExistsAsync("finished-pass")).Should().BeTrue();
        (await CountScheduledRunsAsync(finished.Id)).Should().Be(1);
        (await CountScheduledRunsAsync(neverStarted.Id)).Should().Be(1);
    }

    [Fact]
    public async Task A_Recovered_Abandoned_Run_Does_Not_Claim_Its_Occurrence()
    {
        await using var database = await UseTemporaryDatabaseAsync();
        var tenant = CreateTenant();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false),
            tenant,
            services => services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>()
        );
        await SeedOldNoteAsync(tenant.Id, "recovered-run");
        var occurrence = DateTimeOffset.UtcNow.AddSeconds(-1);
        // The row dispatcher settles a Started run whose owner died as Failed.
        await InsertScheduledRunAsync(tenant.Id, occurrence.AddMilliseconds(10), SweepRunStatus.Failed);

        var outcome = await GetWorker(host).RunIterationAsync(occurrence, dryRun: false, CancellationToken.None);

        outcome.Should().Be(OccurrenceOutcome.Settled);
        (await NoteExistsAsync("recovered-run")).Should().BeFalse();
        (await LoadScheduledRunsAsync(tenant.Id)).Select(run => run.Status).Should().Equal(
            SweepRunStatus.Failed,
            SweepRunStatus.Succeeded
        );
    }

    [Fact]
    public async Task An_Exhausted_Pass_Waits_For_The_Next_Occurrence()
    {
        await using var database = await UseTemporaryDatabaseAsync();
        var tenant = CreateTenant();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false, maxAttempts: 2),
            tenant,
            services => services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>()
        );
        await SeedOldNoteAsync(tenant.Id, "exhausted-pass");
        var occurrence = DateTimeOffset.UtcNow.AddSeconds(-1);
        await InsertScheduledRunAsync(tenant.Id, occurrence.AddMilliseconds(10), SweepRunStatus.Failed);
        await InsertScheduledRunAsync(tenant.Id, occurrence.AddMilliseconds(20), SweepRunStatus.PartiallyFailed);

        var outcome = await GetWorker(host).RunIterationAsync(occurrence, dryRun: false, CancellationToken.None);

        outcome.Should().Be(OccurrenceOutcome.Settled);
        (await NoteExistsAsync("exhausted-pass")).Should().BeTrue();
        (await CountScheduledRunsAsync(tenant.Id)).Should().Be(2);
    }

    [Fact]
    public async Task A_Cancelled_Run_Does_Not_Count_Toward_The_Attempt_Cap()
    {
        // Toggling the kill switch must not use up an occurrence's attempts.
        await using var database = await UseTemporaryDatabaseAsync();
        var tenant = CreateTenant();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false, maxAttempts: 2),
            tenant,
            services => services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>()
        );
        await SeedOldNoteAsync(tenant.Id, "cancelled-attempts");
        var occurrence = DateTimeOffset.UtcNow.AddSeconds(-1);
        await InsertScheduledRunAsync(tenant.Id, occurrence.AddMilliseconds(10), SweepRunStatus.Failed);
        await InsertScheduledRunAsync(tenant.Id, occurrence.AddMilliseconds(20), SweepRunStatus.Cancelled);
        await InsertScheduledRunAsync(tenant.Id, occurrence.AddMilliseconds(30), SweepRunStatus.Cancelled);

        var outcome = await GetWorker(host).RunIterationAsync(occurrence, dryRun: false, CancellationToken.None);

        outcome.Should().Be(OccurrenceOutcome.Settled);
        (await NoteExistsAsync("cancelled-attempts")).Should().BeFalse();
    }

    [Fact]
    public async Task A_Pass_That_Fails_Below_The_Cap_Leaves_The_Occurrence_Pending()
    {
        await using var database = await UseTemporaryDatabaseAsync();
        var tenant = CreateTenant();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false, maxAttempts: 2),
            tenant,
            services => services.AddSingleton<IRetentionRuleProvider>(new TenantFailingRuleProvider(tenant.Id))
        );
        await SeedOldNoteAsync(tenant.Id, "pending-pass");
        var occurrence = DateTimeOffset.UtcNow.AddSeconds(-1);
        var worker = GetWorker(host);

        (await worker.RunIterationAsync(occurrence, dryRun: false, CancellationToken.None))
            .Should().Be(OccurrenceOutcome.Pending);
        // The second failure exhausts the pass: the occurrence is done with until the next one.
        (await worker.RunIterationAsync(occurrence, dryRun: false, CancellationToken.None))
            .Should().Be(OccurrenceOutcome.Settled);
        (await worker.RunIterationAsync(occurrence, dryRun: false, CancellationToken.None))
            .Should().Be(OccurrenceOutcome.Settled);

        (await LoadScheduledRunsAsync(tenant.Id)).Select(run => run.Status).Should().Equal(
            SweepRunStatus.PartiallyFailed,
            SweepRunStatus.PartiallyFailed
        );
    }

    [Fact]
    public async Task A_Tenant_With_Rows_But_Missing_From_The_Source_Is_Recorded_As_A_Failed_Scheduled_Run()
    {
        await using var database = await UseTemporaryDatabaseAsync();
        var listed = CreateTenant();
        var missing = CreateTenant();
        var observer = new RecordingObserver();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false),
            listed,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
                services.AddSingleton<IRetentionTenantSource>(new StaticTenantSource(listed));
                services.AddSingleton<IRetentionAuditObserver>(observer);
            }
        );
        await SeedOldNoteAsync(listed.Id, "listed-tenant");
        await SeedOldNoteAsync(missing.Id, "missing-tenant");

        var outcome = await GetWorker(host).RunIterationAsync(
            DateTimeOffset.UtcNow.AddSeconds(-1),
            dryRun: false,
            CancellationToken.None
        );

        // Sweeping without the tenant's jurisdiction and tags could apply a shorter rule, so
        // its rows stay, and a durable failure says why; the refusal is retried like any failure.
        outcome.Should().Be(OccurrenceOutcome.Pending);
        (await NoteExistsAsync("listed-tenant")).Should().BeFalse();
        (await NoteExistsAsync("missing-tenant")).Should().BeTrue();
        var refused = (await LoadScheduledRunsAsync(missing.Id)).Should().ContainSingle().Subject;
        refused.Status.Should().Be(SweepRunStatus.Failed);
        refused.Error.Should().Contain("code=cohort:tenant-not-in-source");
        // Audit observers hear of it like any failed run, so alerting needs nothing new.
        var started = observer.Events.OfType<SweepEvent.Started>().Should()
            .ContainSingle(evt => evt.TenantId == missing.Id).Subject;
        started.Trigger.Should().Be(SweepTriggerKind.Scheduled);
        observer.Events.OfType<SweepEvent.Failed>().Should()
            .ContainSingle(evt => evt.SweepId == started.SweepId)
            .Which.Error.Should().Contain("code=cohort:tenant-not-in-source");
    }

    [Fact]
    public async Task An_Empty_Source_With_Tenanted_Data_Records_A_Failed_Run_Per_Tenant()
    {
        await using var database = await UseTemporaryDatabaseAsync();
        var first = CreateTenant();
        var second = CreateTenant();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false),
            first,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
                services.AddSingleton<IRetentionTenantSource>(new StaticTenantSource());
            }
        );
        await SeedOldNoteAsync(first.Id, "empty-source-first");
        await SeedOldNoteAsync(second.Id, "empty-source-second");
        await SeedOldTenantlessLogAsync("empty-source-tenantless");

        await GetWorker(host).RunIterationAsync(DateTimeOffset.UtcNow.AddSeconds(-1), dryRun: false, CancellationToken.None);

        foreach (var tenant in new[] { first, second })
        {
            (await LoadScheduledRunsAsync(tenant.Id)).Should().ContainSingle()
                .Which.Should().Match<(SweepRunStatus Status, DateTimeOffset StartedAt, string? Error)>(
                    run => run.Status == SweepRunStatus.Failed && run.Error!.Contains("code=cohort:tenant-not-in-source")
                );
        }
        (await NoteExistsAsync("empty-source-first")).Should().BeTrue();
        (await NoteExistsAsync("empty-source-second")).Should().BeTrue();
        (await TenantlessLogExistsAsync("empty-source-tenantless")).Should().BeFalse();
    }

    [Fact]
    public async Task An_Empty_Source_With_No_Tenanted_Data_Runs_Only_The_Tenantless_Pass()
    {
        await using var database = await UseTemporaryDatabaseAsync();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false),
            CreateTenant(),
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
                services.AddSingleton<IRetentionTenantSource>(new StaticTenantSource());
            }
        );
        await SeedOldTenantlessLogAsync("tenantless-only");

        var outcome = await GetWorker(host).RunIterationAsync(
            DateTimeOffset.UtcNow.AddSeconds(-1),
            dryRun: false,
            CancellationToken.None
        );

        outcome.Should().Be(OccurrenceOutcome.Settled);
        (await TenantlessLogExistsAsync("tenantless-only")).Should().BeFalse();
        (await LoadAllRunsAsync()).Should().Equal((Guid.Empty, SweepRunStatus.Succeeded));
    }

    [Fact]
    public async Task Conflicting_Contexts_Refuse_That_Tenant_And_Still_Sweep_The_Others()
    {
        await using var database = await UseTemporaryDatabaseAsync();
        var conflicted = CreateTenant();
        var healthy = CreateTenant();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false),
            healthy,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
                services.AddSingleton<IRetentionTenantSource>(new StaticTenantSource(
                    conflicted,
                    healthy,
                    new TenantContext(conflicted.Id, "fr", new Dictionary<string, string>())
                ));
            }
        );
        await SeedOldNoteAsync(conflicted.Id, "conflicted-tenant");
        await SeedOldNoteAsync(healthy.Id, "healthy-tenant");

        await GetWorker(host).RunIterationAsync(DateTimeOffset.UtcNow.AddSeconds(-1), dryRun: false, CancellationToken.None);

        // Jurisdiction and tags feed rule resolution: an ambiguous input is never guessed.
        (await NoteExistsAsync("conflicted-tenant")).Should().BeTrue();
        (await NoteExistsAsync("healthy-tenant")).Should().BeFalse();
        (await LoadScheduledRunsAsync(conflicted.Id)).Should().ContainSingle()
            .Which.Should().Match<(SweepRunStatus Status, DateTimeOffset StartedAt, string? Error)>(
                run => run.Status == SweepRunStatus.Failed && run.Error!.Contains("code=cohort:conflicting-tenant-contexts")
            );
        (await LoadScheduledRunsAsync(healthy.Id)).Should().ContainSingle()
            .Which.Status.Should().Be(SweepRunStatus.Succeeded);
    }

    [Fact]
    public async Task A_Refused_Pass_Is_Retried_And_Swept_Once_The_Source_Lists_The_Tenant()
    {
        await using var database = await UseTemporaryDatabaseAsync();
        var tenant = CreateTenant();
        var source = new MutableTenantSource();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false),
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
                services.AddSingleton<IRetentionTenantSource>(source);
            }
        );
        await SeedOldNoteAsync(tenant.Id, "fixed-source");
        var occurrence = DateTimeOffset.UtcNow.AddSeconds(-1);
        var worker = GetWorker(host);

        (await worker.RunIterationAsync(occurrence, dryRun: false, CancellationToken.None))
            .Should().Be(OccurrenceOutcome.Pending);
        source.Tenants = [tenant];
        (await worker.RunIterationAsync(occurrence, dryRun: false, CancellationToken.None))
            .Should().Be(OccurrenceOutcome.Settled);

        (await NoteExistsAsync("fixed-source")).Should().BeFalse();
        (await LoadScheduledRunsAsync(tenant.Id)).Select(run => run.Status).Should().Equal(
            SweepRunStatus.Failed,
            SweepRunStatus.Succeeded
        );
    }

    [Fact]
    public async Task Rows_With_An_Empty_Tenant_Id_In_A_Tenanted_Table_Log_An_Error_And_Refuse_Nothing()
    {
        // Guid.Empty is the tenantless pass's key, so these rows can't have a refused run of
        // their own; only host writes that bypass TenantContext can create them.
        await using var database = await UseTemporaryDatabaseAsync();
        var tenant = CreateTenant();
        var log = new WorkerLogProvider();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false),
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
                services.AddSingleton<ILoggerProvider>(log);
            }
        );
        await SeedOldNoteAsync(tenant.Id, "real-tenant");
        await SeedOldNoteAsync(Guid.Empty, "empty-tenant");
        await SeedOldTenantlessLogAsync("empty-tenant-tenantless");

        var outcome = await GetWorker(host).RunIterationAsync(
            DateTimeOffset.UtcNow.AddSeconds(-1),
            dryRun: false,
            CancellationToken.None
        );

        outcome.Should().Be(OccurrenceOutcome.Settled);
        (await NoteExistsAsync("real-tenant")).Should().BeFalse();
        (await NoteExistsAsync("empty-tenant")).Should().BeTrue();
        (await TenantlessLogExistsAsync("empty-tenant-tenantless")).Should().BeFalse();
        (await LoadAllRunsAsync()).Should().BeEquivalentTo([
            (tenant.Id, SweepRunStatus.Succeeded),
            (Guid.Empty, SweepRunStatus.Succeeded),
        ]);
        log.Entries.Should().Contain(entry =>
            entry.Level == LogLevel.Error
            && entry.Message.Contains("empty tenant ID")
            && entry.Message.Contains(typeof(Note).FullName!)
        );
    }

    [Fact]
    public async Task Kill_Switch_Stops_The_Pass_In_Progress()
    {
        await using var database = await UseTemporaryDatabaseAsync();
        var tenant = CreateTenant();
        var observer = new FirstNoteProgressObserver();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false, batchSize: 1),
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
                services.AddSingleton<IRetentionAuditObserver>(observer);
            }
        );
        // Observers run after each batch commits and before the next begins, so the switch
        // flips between the first and second batch of the tenant's notes.
        observer.OnFirstNoteProgress = () => host.Reload(killSwitch: true, dryRun: false);
        await SeedOldNoteAsync(tenant.Id, "kill-switch-1");
        await SeedOldNoteAsync(tenant.Id, "kill-switch-2");
        await SeedOldNoteAsync(tenant.Id, "kill-switch-3");
        await SeedOldTenantlessLogAsync("kill-switch-tenantless");

        var outcome = await GetWorker(host).RunIterationAsync(
            DateTimeOffset.UtcNow.AddSeconds(-1),
            dryRun: false,
            CancellationToken.None
        );

        outcome.Should().Be(OccurrenceOutcome.Pending);
        var remaining = new List<bool>();
        foreach (var body in new[] { "kill-switch-1", "kill-switch-2", "kill-switch-3" })
        {
            remaining.Add(await NoteExistsAsync(body));
        }
        remaining.Count(exists => exists).Should().Be(2);
        (await LoadScheduledRunsAsync(tenant.Id)).Should().ContainSingle()
            .Which.Status.Should().Be(SweepRunStatus.Cancelled);
        // The rest of the iteration is skipped: no tenantless pass started.
        (await LoadScheduledRunsAsync(Guid.Empty)).Should().BeEmpty();
        (await TenantlessLogExistsAsync("kill-switch-tenantless")).Should().BeTrue();
    }

    [Fact]
    public async Task Turning_The_Kill_Switch_Off_Catches_Up_The_Missed_Occurrence_Straight_Away()
    {
        await using var database = await UseTemporaryDatabaseAsync();
        var tenant = CreateTenant();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: true),
            tenant,
            services => services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>()
        );
        await SeedOldNoteAsync(tenant.Id, "kill-switch-off");

        await host.Host.StartAsync();
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        (await NoteExistsAsync("kill-switch-off")).Should().BeTrue();
        host.Reload(killSwitch: false, dryRun: false);
        // Wait for the run to settle, not just for the note to go: stopping the host between
        // the two would cancel the run's remaining entities.
        await WaitUntilAsync(
            async () => (await LoadScheduledRunsAsync(tenant.Id)).Any(run => run.Status == SweepRunStatus.Succeeded),
            TimeSpan.FromSeconds(8)
        );
        await host.Host.StopAsync();

        (await NoteExistsAsync("kill-switch-off")).Should().BeFalse();
        (await LoadScheduledRunsAsync(tenant.Id)).Should().ContainSingle()
            .Which.Status.Should().Be(SweepRunStatus.Succeeded);
    }

    [Fact]
    public async Task A_Pass_The_Kill_Switch_Cancelled_Reruns_As_Soon_As_It_Is_Turned_Off()
    {
        // The default 15-minute retry delay must not stand between the switch going off and
        // the catch-up, and the cancelled run must not use up an attempt.
        await using var database = await UseTemporaryDatabaseAsync();
        var tenant = CreateTenant();
        var observer = new FirstNoteProgressObserver();
        using var host = BuildHost(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false, batchSize: 1, maxAttempts: 1),
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
                services.AddSingleton<IRetentionAuditObserver>(observer);
            }
        );
        observer.OnFirstNoteProgress = () => host.Reload(killSwitch: true, dryRun: false);
        await SeedOldNoteAsync(tenant.Id, "rerun-after-kill-1");
        await SeedOldNoteAsync(tenant.Id, "rerun-after-kill-2");

        await host.Host.StartAsync();
        await WaitUntilAsync(
            async () => (await LoadScheduledRunsAsync(tenant.Id)).Any(run => run.Status == SweepRunStatus.Cancelled),
            TimeSpan.FromSeconds(8)
        );
        host.Reload(killSwitch: false, dryRun: false);
        await WaitUntilAsync(
            async () => !await NoteExistsAsync("rerun-after-kill-1") && !await NoteExistsAsync("rerun-after-kill-2"),
            TimeSpan.FromSeconds(8)
        );
        await host.Host.StopAsync();

        (await LoadScheduledRunsAsync(tenant.Id)).Select(run => run.Status).Should().Equal(
            SweepRunStatus.Cancelled,
            SweepRunStatus.Succeeded
        );
    }

    private WorkerTestHost BuildHost(
        IReadOnlyDictionary<string, string?> settings,
        TenantContext tenant,
        Action<IServiceCollection> configureServices
    )
    {
        var connectionString = settings[$"{CohortOptions.SectionName}:ConnectionString"]!;
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(settings);

        builder.Services.AddDbContext<SampleDbContext>(options => options.UseNpgsql(connectionString));
        builder.Services.AddSingleton(tenant);
        builder.Services.AddSingleton<GuidTombstoneFactory>();
        builder.Services.AddSingleton<OriginalValueTombstoneFactory>();
        builder.Services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<GuidTombstoneFactory>()
        );
        builder.Services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<OriginalValueTombstoneFactory>()
        );
        builder.Services.AddSingleton(RetentionExecutionScope.Unisolated);
        builder.Services.AddCohort<SampleDbContext>();
        configureServices(builder.Services);

        return new WorkerTestHost(builder.Build(), builder.Configuration);
    }

    private async Task CreateSecondInstallAsync()
    {
        var options = new DbContextOptionsBuilder<SecondInstallDbContext>()
            .UseNpgsql(ConnectionString)
            .Options;
        await using var db = new SecondInstallDbContext(options);
        await db.Database.ExecuteSqlRawAsync(db.Database.GenerateCreateScript());
    }

    private WorkerTestHost BuildSecondInstallHost()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(
            CreateSettings(ConnectionString, schedule: "0 0 0 1 1 *", dryRun: false, killSwitch: false)
        );
        builder.Services.AddDbContext<SecondInstallDbContext>(options => options.UseNpgsql(ConnectionString));
        builder.Services.AddSingleton<IRetentionRuleProvider, SecondInstallRuleProvider>();
        builder.Services.AddSingleton(RetentionExecutionScope.Unisolated);
        builder.Services.AddCohort<SecondInstallDbContext>();
        return new WorkerTestHost(builder.Build(), builder.Configuration);
    }

    private static RetentionWorker GetWorker(WorkerTestHost host) =>
        host.Host.Services.GetServices<IHostedService>().OfType<RetentionWorker>().Single();

    private async Task<long> CountScheduledRunsAsync(Guid tenantId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*) FROM "sweep_run"
            WHERE "TenantId" = @tenantId AND "TriggerKind" = @scheduled
            """;
        command.Parameters.AddWithValue("tenantId", tenantId);
        command.Parameters.AddWithValue("scheduled", (int)SweepTriggerKind.Scheduled);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static IReadOnlyDictionary<string, string?> CreateSettings(
        string connectionString,
        string? schedule,
        bool dryRun,
        bool killSwitch,
        TimeSpan? retryDelay = null,
        int? maxAttempts = null,
        int? batchSize = null
    )
    {
        var settings = new Dictionary<string, string?>
        {
            [$"{CohortOptions.SectionName}:ConnectionString"] = connectionString,
            [$"{CohortOptions.SectionName}:Schedule"] = schedule,
            [$"{CohortOptions.SectionName}:DryRun"] = dryRun.ToString(),
            [$"{CohortOptions.SectionName}:KillSwitch"] = killSwitch.ToString(),
        };
        if (retryDelay is { } delay)
        {
            settings[$"{CohortOptions.SectionName}:OccurrenceRetryDelay"] = delay.ToString("c", CultureInfo.InvariantCulture);
        }
        if (maxAttempts is { } attempts)
        {
            settings[$"{CohortOptions.SectionName}:OccurrenceMaxAttempts"] = attempts.ToString(CultureInfo.InvariantCulture);
        }
        if (batchSize is { } size)
        {
            settings[$"{CohortOptions.SectionName}:SweepBatchSize"] = size.ToString(CultureInfo.InvariantCulture);
        }

        return settings;
    }

    /// <summary>Moves this test to a fresh, migrated database of its own.</summary>
    private async Task<TemporaryDatabase> UseTemporaryDatabaseAsync()
    {
        var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var db = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>().UseNpgsql(database.ConnectionString).Options
        );
        await db.Database.MigrateAsync();
        ConnectionString = database.ConnectionString;
        return database;
    }

    private async Task InsertScheduledRunAsync(Guid tenantId, DateTimeOffset startedAt, SweepRunStatus status, bool dryRun = false)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO "sweep_run" ("SweepId", "StartedAt", "Status", "TriggerKind", "DryRun", "TenantId", "SettledAt")
            VALUES (@sweepId, @startedAt, @status, @scheduled, @dryRun, @tenantId, CASE WHEN @status = 0 THEN NULL ELSE @startedAt END)
            """;
        command.Parameters.AddWithValue("sweepId", Guid.NewGuid());
        command.Parameters.AddWithValue("startedAt", startedAt);
        command.Parameters.AddWithValue("status", (int)status);
        command.Parameters.AddWithValue("scheduled", (int)SweepTriggerKind.Scheduled);
        command.Parameters.AddWithValue("dryRun", dryRun);
        command.Parameters.AddWithValue("tenantId", tenantId);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<IReadOnlyList<(Guid TenantId, SweepRunStatus Status)>> LoadAllRunsAsync()
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """SELECT "TenantId", "Status" FROM "sweep_run" ORDER BY "StartedAt" """;
        var runs = new List<(Guid, SweepRunStatus)>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            runs.Add((reader.GetGuid(0), (SweepRunStatus)reader.GetInt32(1)));
        }

        return runs;
    }

    private async Task<IReadOnlyList<(SweepRunStatus Status, DateTimeOffset StartedAt, string? Error)>> LoadScheduledRunsAsync(Guid tenantId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT "Status", "StartedAt", "Error" FROM "sweep_run"
            WHERE "TenantId" = @tenantId AND "TriggerKind" = @scheduled
            ORDER BY "StartedAt"
            """;
        command.Parameters.AddWithValue("tenantId", tenantId);
        command.Parameters.AddWithValue("scheduled", (int)SweepTriggerKind.Scheduled);
        var runs = new List<(SweepRunStatus, DateTimeOffset, string?)>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            runs.Add((
                (SweepRunStatus)reader.GetInt32(0),
                reader.GetFieldValue<DateTimeOffset>(1),
                reader.IsDBNull(2) ? null : reader.GetString(2)
            ));
        }

        return runs;
    }

    private TenantContext CreateTenant()
    {
        return new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
    }

    private async Task SeedOldNoteAsync(Guid tenantId, string body)
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(ConnectionString)
            .Options;

        await using var db = new SampleDbContext(options);
        db.Notes.Add(
            new Note
            {
                Id = Guid.NewGuid(),
                TenantId = tenantId,
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-120),
                Body = body,
            }
        );
        await db.SaveChangesAsync();
    }

    private async Task<bool> NoteExistsAsync(string body)
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(ConnectionString)
            .Options;

        await using var db = new SampleDbContext(options);
        return await db.Notes.AnyAsync(note => note.Body == body);
    }

    private async Task SeedOldTenantlessLogAsync(string payload)
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(ConnectionString)
            .Options;

        await using var db = new SampleDbContext(options);
        db.TenantlessLogs.Add(
            new TenantlessLog
            {
                Id = Guid.NewGuid(),
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-120),
                Payload = payload,
            }
        );
        await db.SaveChangesAsync();
    }

    private async Task<bool> TenantlessLogExistsAsync(string payload)
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(ConnectionString)
            .Options;

        await using var db = new SampleDbContext(options);
        return await db.TenantlessLogs.AnyAsync(log => log.Payload == payload);
    }

    private async Task<(
        SweepTriggerKind Trigger,
        bool DryRun,
        SweepRunStatus Status,
        IReadOnlyList<string> EntityTypes
    )?> LoadLatestRunAsync(Guid tenantId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT run."TriggerKind", run."DryRun", run."Status", summary."EntityType"
            FROM "sweep_run" run
            LEFT JOIN "sweep_run_entity_summary" summary ON summary."SweepId" = run."SweepId"
            WHERE run."SweepId" = (
                SELECT "SweepId"
                FROM "sweep_run"
                WHERE "TenantId" = @tenantId
                ORDER BY "StartedAt" DESC
                LIMIT 1
            )
            ORDER BY summary."EntityType"
            """;
        command.Parameters.AddWithValue("tenantId", tenantId);

        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return null;
        }

        var trigger = (SweepTriggerKind)reader.GetInt32(0);
        var dryRun = reader.GetBoolean(1);
        var status = (SweepRunStatus)reader.GetInt32(2);
        var entityTypes = new List<string>();
        do
        {
            if (!reader.IsDBNull(3))
            {
                entityTypes.Add(reader.GetString(3));
            }
        } while (await reader.ReadAsync());

        return (trigger, dryRun, status, entityTypes);
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> predicate, TimeSpan timeout)
    {
        var stopwatch = Stopwatch.StartNew();

        while (stopwatch.Elapsed < timeout)
        {
            if (await predicate())
            {
                return;
            }

            await Task.Delay(100);
        }

        throw new XunitException("Condition was not met within the allotted timeout.");
    }

    private sealed class BlockingRuleProvider(IRetentionRuleProvider inner) : IRetentionRuleProvider
    {
        private readonly TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Entered => entered.Task;

        public void Release() => released.TrySetResult();

        public RetentionCategoryCapabilities? GetCapabilities(string category) => inner.GetCapabilities(category);

        public async Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct)
        {
            entered.TrySetResult();
            await released.Task.WaitAsync(ct);
            return await inner.ResolveAsync(context, ct);
        }
    }

    private sealed class SecondInstallDbContext(DbContextOptions<SecondInstallDbContext> options)
        : DbContext(options)
    {
        public const string Schema = "second_install";

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SecondInstallRecord>().ToTable("records", Schema);
            modelBuilder.ConfigureCohortTables(Schema);
        }
    }

    [Retain("second-install", nameof(CreatedAt))]
    [RetentionEntityId("5d7f4b8e-2c61-4a0f-9e3b-7a1c8d2e6f40")]
    [RetentionTenantless]
    private sealed class SecondInstallRecord
    {
        public Guid Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }

    private sealed class SecondInstallRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == "second-install" ? new RetentionCategoryCapabilities([Strategy.Purge]) : null;

        public Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct) =>
            Task.FromResult<RetentionRule?>(new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge));
    }

    private sealed class StaticTenantSource(params TenantContext[] tenants) : IRetentionTenantSource
    {
        public Task<IReadOnlyList<TenantContext>> GetTenantsAsync(CancellationToken ct)
        {
            return Task.FromResult<IReadOnlyList<TenantContext>>(tenants);
        }
    }

    private sealed class MutableTenantSource : IRetentionTenantSource
    {
        public IReadOnlyList<TenantContext> Tenants { get; set; } = [];

        public Task<IReadOnlyList<TenantContext>> GetTenantsAsync(CancellationToken ct) => Task.FromResult(Tenants);
    }

    private sealed class RecordingObserver : IRetentionAuditObserver
    {
        private readonly ConcurrentQueue<SweepEvent> events = new();

        public IReadOnlyList<SweepEvent> Events => events.ToArray();

        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            events.Enqueue(evt);
            return Task.CompletedTask;
        }
    }

    private sealed class FirstNoteProgressObserver : IRetentionAuditObserver
    {
        private int fired;

        public Action? OnFirstNoteProgress { get; set; }

        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            if (
                evt is SweepEvent.EntityProgress { Affected: > 0 } progress
                && progress.EntityType == typeof(Note)
                && Interlocked.Exchange(ref fired, 1) == 0
            )
            {
                OnFirstNoteProgress?.Invoke();
            }

            return Task.CompletedTask;
        }
    }

    private sealed class WorkerLogProvider : ILoggerProvider
    {
        private readonly ConcurrentQueue<(LogLevel Level, string Message)> entries = new();

        public IReadOnlyList<(LogLevel Level, string Message)> Entries => entries.ToArray();

        public ILogger CreateLogger(string categoryName) =>
            categoryName == typeof(RetentionWorker).FullName ? new Logger(entries) : Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

        public void Dispose() { }

        private sealed class Logger(ConcurrentQueue<(LogLevel Level, string Message)> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter
            ) => entries.Enqueue((logLevel, formatter(state, exception)));
        }
    }

    private sealed class FailingOnceCategoryRepository(IRetentionRuleProvider inner)
        : IRetentionRuleProvider
    {
        private readonly TaskCompletionSource failedIteration = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private readonly TaskCompletionSource laterSuccessfulIteration = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private int failureInjected;

        public Task FailedIteration => failedIteration.Task;

        public Task LaterSuccessfulIteration => laterSuccessfulIteration.Task;

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            inner.GetCapabilities(category);

        public async Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            if (Interlocked.CompareExchange(ref failureInjected, 1, 0) == 0)
            {
                failedIteration.TrySetResult();
                throw new InvalidOperationException(
                    "Simulated transient category resolver failure."
                );
            }

            var rule = await inner.ResolveAsync(context, ct);
            laterSuccessfulIteration.TrySetResult();
            return rule;
        }
    }

    private sealed class TenantFailingRuleProvider(Guid failingTenantId)
        : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            inner.GetCapabilities(category);

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            if (context.Tenant.Id == failingTenantId)
            {
                throw new InvalidOperationException("Simulated tenant-specific policy failure.");
            }

            return inner.ResolveAsync(context, ct);
        }
    }

    private sealed class WorkerTestHost(IHost host, IConfigurationRoot configuration) : IDisposable
    {
        private readonly TaskCompletionSource reloaded = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        public IHost Host => host;

        public Task Reloaded => reloaded.Task;

        public void Reload(bool killSwitch, bool dryRun, string? schedule = null)
        {
            configuration[$"{CohortOptions.SectionName}:KillSwitch"] = killSwitch.ToString();
            configuration[$"{CohortOptions.SectionName}:DryRun"] = dryRun.ToString();
            if (schedule is not null)
            {
                configuration[$"{CohortOptions.SectionName}:Schedule"] = schedule;
            }
            configuration.Reload();
            reloaded.TrySetResult();
        }

        public void Dispose()
        {
            host.Dispose();
        }
    }

    private sealed class SkippedOccurrenceLogProvider : ILoggerProvider
    {
        private const string WorkerCategory = "Cohort.Hosting.RetentionWorker";
        private const string SkipMessage =
            "Cohort worker skipped this occurrence: another instance holds the sweep advisory lock.";
        private readonly Channel<int> occurrences = Channel.CreateUnbounded<int>();
        private int occurrenceCount;

        public int OccurrenceCount => Volatile.Read(ref occurrenceCount);

        public ILogger CreateLogger(string categoryName)
        {
            return new SkippedOccurrenceLogger(this, categoryName);
        }

        public async Task WaitForOccurrencesAsync(int expectedCount)
        {
            while (OccurrenceCount < expectedCount)
            {
                await occurrences.Reader.ReadAsync();
            }
        }

        public void Dispose() { }

        private void Record(LogLevel level, string categoryName, string message)
        {
            if (
                level != LogLevel.Information
                || categoryName != WorkerCategory
                || message != SkipMessage
            )
            {
                return;
            }

            var count = Interlocked.Increment(ref occurrenceCount);
            occurrences.Writer.TryWrite(count);
        }

        private sealed class SkippedOccurrenceLogger(
            SkippedOccurrenceLogProvider provider,
            string categoryName
        ) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull
            {
                return null;
            }

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter
            )
            {
                provider.Record(logLevel, categoryName, formatter(state, exception));
            }
        }
    }
}
