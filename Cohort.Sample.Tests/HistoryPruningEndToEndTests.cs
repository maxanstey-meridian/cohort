using System.Diagnostics;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using Npgsql;

using Xunit.Sdk;

namespace Cohort.Sample.Tests;

// End-to-end tests for opt-in history pruning: seed Cohort's own tables in a real database,
// run the real pruner from a real AddCohort<TContext>() container, read the tables back.
public sealed class HistoryPruningEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    private static readonly DateTimeOffset Now = DateTimeOffset.UtcNow;

    [Fact]
    public async Task Prune_Deletes_Settled_Runs_Past_The_Retention_For_Their_Outcome()
    {
        var oldSucceeded = await SeedRunAsync(SweepRunStatus.Succeeded, Now.AddDays(-100));
        var recentSucceeded = await SeedRunAsync(SweepRunStatus.Succeeded, Now.AddDays(-30));
        var failedInsideItsRetention = await SeedRunAsync(SweepRunStatus.Failed, Now.AddDays(-100));
        var oldFailed = await SeedRunAsync(SweepRunStatus.Failed, Now.AddDays(-200));
        var oldPartiallyFailed = await SeedRunAsync(SweepRunStatus.PartiallyFailed, Now.AddDays(-200));
        var oldCancelled = await SeedRunAsync(SweepRunStatus.Cancelled, Now.AddDays(-200));
        var unsettled = await SeedRunAsync(SweepRunStatus.Started, settledAt: null, startedAt: Now.AddDays(-500));

        using var host = CreateHost(succeededRunRetention: "90.00:00:00", failedRunRetention: "180.00:00:00");
        var result = await PruneAsync(host);

        result.Should().Be(new RetentionHistoryPruneResult(Runs: 4, Holds: 0));
        (await LoadRunIdsAsync())
            .Should()
            .BeEquivalentTo([recentSucceeded, failedInsideItsRetention, unsettled]);
        (await CountDependentRowsAsync(oldSucceeded)).Should().Be(0);
        (await CountDependentRowsAsync(oldFailed)).Should().Be(0);
        (await CountDependentRowsAsync(oldPartiallyFailed)).Should().Be(0);
        (await CountDependentRowsAsync(oldCancelled)).Should().Be(0);
        (await CountDependentRowsAsync(recentSucceeded)).Should().Be(2);
    }

    [Fact]
    public async Task Prune_Keeps_A_Class_Of_Run_Whose_Retention_Is_Not_Configured()
    {
        var oldSucceeded = await SeedRunAsync(SweepRunStatus.Succeeded, Now.AddDays(-1000));
        var oldFailed = await SeedRunAsync(SweepRunStatus.Failed, Now.AddDays(-1000));

        using (var unconfigured = CreateHost())
        {
            (await PruneAsync(unconfigured)).Should().Be(new RetentionHistoryPruneResult(0, 0));
        }
        (await LoadRunIdsAsync()).Should().BeEquivalentTo([oldSucceeded, oldFailed]);

        using (var succeededOnly = CreateHost(succeededRunRetention: "90.00:00:00"))
        {
            await PruneAsync(succeededOnly);
        }
        (await LoadRunIdsAsync()).Should().BeEquivalentTo([oldFailed]);
    }

    [Fact]
    public async Task Prune_Keeps_The_Scheduled_Runs_Of_The_Current_Occurrence()
    {
        // The worker's ledger for the occurrence it is catching up: pruning it would rerun
        // passes that already succeeded and give exhausted ones fresh attempts.
        var occurrenceDay = Now.UtcDateTime.Date.AddDays(-10);
        var occurrence = new DateTimeOffset(occurrenceDay, TimeSpan.Zero);
        var schedule = $"0 0 {occurrenceDay.Day} {occurrenceDay.Month} *";
        var succeededInOccurrence = await SeedRunAsync(
            SweepRunStatus.Succeeded, occurrence.AddHours(2), startedAt: occurrence.AddHours(1), trigger: SweepTriggerKind.Scheduled
        );
        var failedInOccurrence = await SeedRunAsync(
            SweepRunStatus.Failed, occurrence.AddHours(4), startedAt: occurrence.AddHours(3), trigger: SweepTriggerKind.Scheduled
        );
        var scheduledBeforeIt = await SeedRunAsync(
            SweepRunStatus.Succeeded, occurrence.AddHours(-1), trigger: SweepTriggerKind.Scheduled
        );
        var manualInOccurrence = await SeedRunAsync(SweepRunStatus.Succeeded, occurrence.AddHours(2));

        using (var scheduled = CreateHost(succeededRunRetention: "1.00:00:00", failedRunRetention: "1.00:00:00", schedule: schedule))
        {
            (await PruneAsync(scheduled)).Runs.Should().Be(2);
        }
        (await LoadRunIdsAsync()).Should().BeEquivalentTo([succeededInOccurrence, failedInOccurrence]);
        (await CountDependentRowsAsync(scheduledBeforeIt)).Should().Be(0);
        (await CountDependentRowsAsync(manualInOccurrence)).Should().Be(0);

        // Without a schedule there is no occurrence to keep.
        using (var unscheduled = CreateHost(succeededRunRetention: "1.00:00:00", failedRunRetention: "1.00:00:00"))
        {
            (await PruneAsync(unscheduled)).Runs.Should().Be(2);
        }
        (await LoadRunIdsAsync()).Should().BeEmpty();
    }

    [Fact]
    public async Task Prune_Deletes_Holds_That_Stopped_Protecting_Before_The_Cutoff()
    {
        await SeedHoldAsync(expiresAt: null, removedAt: Now.AddDays(-100));
        await SeedHoldAsync(expiresAt: Now.AddDays(-100), removedAt: null);
        // Removed long ago; the later expiry no longer matters.
        await SeedHoldAsync(
            expiresAt: Now.AddDays(100),
            removedAt: Now.AddDays(-100)
        );
        var removedRecently = await SeedHoldAsync(expiresAt: null, removedAt: Now.AddDays(-10));
        var expiredRecently = await SeedHoldAsync(expiresAt: Now.AddDays(-10), removedAt: null);
        var indefinite = await SeedHoldAsync(expiresAt: null, removedAt: null);
        var expiresLater = await SeedHoldAsync(expiresAt: Now.AddDays(100), removedAt: null);
        var oldRun = await SeedRunAsync(SweepRunStatus.Succeeded, Now.AddDays(-1000));

        using var host = CreateHost(inactiveHoldRetention: "90.00:00:00");
        var result = await PruneAsync(host);

        result.Should().Be(new RetentionHistoryPruneResult(Runs: 0, Holds: 3));
        (await LoadHoldIdsAsync())
            .Should()
            .BeEquivalentTo([removedRecently, expiredRecently, indefinite, expiresLater]);
        (await LoadRunIdsAsync()).Should().BeEquivalentTo([oldRun]);
    }

    [Fact]
    public async Task Hold_Reporting_Gives_The_Same_Answer_Before_And_After_Pruning()
    {
        // Reporting answers for now on the database clock, and pruning only removes holds that
        // stopped protecting before its cutoff, so pruning can never change a reported answer.
        var tenantId = Guid.NewGuid();
        var expiredRecordId = Guid.NewGuid();
        var heldRecordId = Guid.NewGuid();
        await SeedHoldAsync(Now.AddDays(-100), removedAt: null, expiredRecordId, tenantId);
        var activeHold = await SeedHoldAsync(expiresAt: null, removedAt: null, heldRecordId, tenantId);
        using var host = CreateHost(inactiveHoldRetention: "1.00:00:00");

        var before = await ReportAsync(host);
        var result = await PruneAsync(host);
        var after = await ReportAsync(host);

        result.Should().Be(new RetentionHistoryPruneResult(Runs: 0, Holds: 1));
        (await LoadHoldIdsAsync()).Should().BeEquivalentTo([activeHold]);
        before.Should().BeEquivalentTo(after);
        after.Listed.Should().BeEquivalentTo([activeHold]);
        after.ExpiredHeld.Should().BeFalse();
        after.ActiveHeld.Should().BeTrue();

        Task<(List<Guid> Listed, bool ExpiredHeld, bool ActiveHeld)> ReportAsync(CohortTestHost reportHost) =>
            reportHost.RunWithServicesAsync(async services =>
            {
                var holds = services.GetRequiredService<IRetentionHoldsRepository>();
                var listed = await holds.ListActiveAsync(CancellationToken.None);
                return (
                    listed.Select(hold => hold.HoldId).ToList(),
                    await holds.HasActiveHoldAsync(Note.RetentionIdentity, expiredRecordId.ToString(), tenantId, CancellationToken.None),
                    await holds.HasActiveHoldAsync(Note.RetentionIdentity, heldRecordId.ToString(), tenantId, CancellationToken.None)
                );
            });
    }

    [Fact]
    public async Task Prune_Works_Through_A_Backlog_In_Batches_In_One_Pass()
    {
        const int backlog = RetentionHistoryPruner.BatchSize + 1;
        for (var i = 0; i < backlog; i++)
        {
            await SeedRunAsync(SweepRunStatus.Succeeded, Now.AddDays(-100 - i));
            await SeedHoldAsync(expiresAt: Now.AddDays(-100 - i), removedAt: null);
        }

        using var host = CreateHost(succeededRunRetention: "90.00:00:00", inactiveHoldRetention: "90.00:00:00");
        var result = await PruneAsync(host);

        result.Should().Be(new RetentionHistoryPruneResult(Runs: backlog, Holds: backlog));
        (await LoadRunIdsAsync()).Should().BeEmpty();
        (await LoadHoldIdsAsync()).Should().BeEmpty();
    }

    [Fact]
    public async Task Prune_Deletes_Nothing_While_The_Kill_Switch_Is_On()
    {
        var oldRun = await SeedRunAsync(SweepRunStatus.Succeeded, Now.AddDays(-100));
        var oldHold = await SeedHoldAsync(expiresAt: Now.AddDays(-100), removedAt: null);

        using var host = CreateHost(
            succeededRunRetention: "90.00:00:00",
            inactiveHoldRetention: "90.00:00:00",
            killSwitch: true
        );
        (await PruneAsync(host)).Should().Be(new RetentionHistoryPruneResult(0, 0));

        (await LoadRunIdsAsync()).Should().BeEquivalentTo([oldRun]);
        (await LoadHoldIdsAsync()).Should().BeEquivalentTo([oldHold]);
    }

    [Fact]
    public async Task Prune_Skips_A_Run_Another_Transaction_Has_Locked_Instead_Of_Waiting()
    {
        var locked = await SeedRunAsync(SweepRunStatus.Succeeded, Now.AddDays(-200));
        await SeedRunAsync(SweepRunStatus.Succeeded, Now.AddDays(-100));
        using var host = CreateHost(succeededRunRetention: "90.00:00:00");

        await using (var connection = new NpgsqlConnection(ConnectionString))
        {
            await connection.OpenAsync();
            await using var transaction = await connection.BeginTransactionAsync();
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """SELECT 1 FROM "sweep_run" WHERE "SweepId" = @id FOR UPDATE""";
                command.Parameters.AddWithValue("id", locked);
                await command.ExecuteScalarAsync();
            }

            var result = await PruneAsync(host).WaitAsync(TimeSpan.FromSeconds(10));

            result.Runs.Should().Be(1);
            (await LoadRunIdsAsync()).Should().BeEquivalentTo([locked]);
            await transaction.RollbackAsync();
        }

        (await PruneAsync(host)).Runs.Should().Be(1);
        (await LoadRunIdsAsync()).Should().BeEmpty();
    }

    [Fact]
    public async Task Hosted_Pruner_Prunes_When_The_Host_Starts()
    {
        var oldRun = await SeedRunAsync(SweepRunStatus.Succeeded, Now.AddDays(-100));
        var recentRun = await SeedRunAsync(SweepRunStatus.Succeeded, Now.AddDays(-10));

        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                ["Cohort:HistoryPruning:SucceededRunRetention"] = "90.00:00:00",
            }
        );
        builder.Services.AddDbContext<SampleDbContext>(options => options.UseNpgsql(ConnectionString));
        builder.Services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
        builder.Services.AddSingleton<GuidTombstoneFactory>();
        builder.Services.AddSingleton<OriginalValueTombstoneFactory>();
        builder.Services.AddSingleton<IAnonymiseValueFactory>(sp => sp.GetRequiredService<GuidTombstoneFactory>());
        builder.Services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<OriginalValueTombstoneFactory>()
        );
        builder.Services.AddSingleton(RetentionExecutionScope.Unisolated);
        builder.Services.AddCohort<SampleDbContext>();
        using var app = builder.Build();

        await app.StartAsync();
        try
        {
            var stopwatch = Stopwatch.StartNew();
            while ((await LoadRunIdsAsync()).Contains(oldRun))
            {
                if (stopwatch.Elapsed > TimeSpan.FromSeconds(10))
                {
                    throw new XunitException("The hosted pruner did not prune the old run.");
                }
                await Task.Delay(100);
            }
        }
        finally
        {
            await app.StopAsync();
        }

        (await LoadRunIdsAsync()).Should().BeEquivalentTo([recentRun]);
    }

    private CohortTestHost CreateHost(
        string? succeededRunRetention = null,
        string? failedRunRetention = null,
        string? inactiveHoldRetention = null,
        bool killSwitch = false,
        string? schedule = null
    )
    {
        var settings = new Dictionary<string, string?>
        {
            [$"{CohortOptions.SectionName}:KillSwitch"] = killSwitch.ToString(),
            [$"{CohortOptions.SectionName}:Schedule"] = schedule,
        };
        if (succeededRunRetention is not null)
        {
            settings["Cohort:HistoryPruning:SucceededRunRetention"] = succeededRunRetention;
        }
        if (failedRunRetention is not null)
        {
            settings["Cohort:HistoryPruning:FailedRunRetention"] = failedRunRetention;
        }
        if (inactiveHoldRetention is not null)
        {
            settings["Cohort:HistoryPruning:InactiveHoldRetention"] = inactiveHoldRetention;
        }

        return new CohortTestHost(ConnectionString, configurationOverrides: settings);
    }

    private static Task<RetentionHistoryPruneResult> PruneAsync(CohortTestHost host) =>
        host.RunWithServicesAsync(services =>
            services.GetRequiredService<RetentionHistoryPruner>().PruneAsync()
        );

    /// <summary>Seeds a run with one entity summary and one row detail.</summary>
    private async Task<Guid> SeedRunAsync(
        SweepRunStatus status,
        DateTimeOffset? settledAt,
        DateTimeOffset? startedAt = null,
        SweepTriggerKind trigger = SweepTriggerKind.Manual
    )
    {
        var sweepId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var started = startedAt ?? settledAt!.Value.AddMinutes(-1);
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO "sweep_run"
                ("SweepId", "StartedAt", "Status", "SettledAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected")
            VALUES (@sweepId, @startedAt, @status, @settledAt, @settledAt - @startedAt, @trigger, FALSE, @tenantId, 1);

            INSERT INTO "sweep_run_entity_summary"
                ("SweepId", "At", "EntityType", "RetentionEntityId", "Category", "TenantId", "Strategy",
                 "ResolvedMonths", "ResolvedDays", "Affected", "HeldCount", "SkippedCount", "NullAnchorCount",
                 "RetainedCount", "DeferredCount", "BlockedCount")
            VALUES (@sweepId, @startedAt, 'Cohort.Sample.Entities.Note', @entityId, 'short-lived', @tenantId, @strategy,
                    0, 30, 1, 0, 0, 0, 0, 0, 0);

            INSERT INTO "sweep_run_row_detail"
                ("SweepId", "At", "EntityType", "RetentionEntityId", "RecordId", "Category", "Strategy", "TenantId")
            VALUES (@sweepId, @startedAt, 'Cohort.Sample.Entities.Note', @entityId, @recordId, 'short-lived', @strategy, @tenantId);
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("startedAt", started);
        command.Parameters.AddWithValue("status", (int)status);
        command.Parameters.Add(new NpgsqlParameter("settledAt", NpgsqlTypes.NpgsqlDbType.TimestampTz)
        {
            Value = (object?)settledAt ?? DBNull.Value,
        });
        command.Parameters.AddWithValue("trigger", (int)trigger);
        command.Parameters.AddWithValue("tenantId", tenantId);
        command.Parameters.AddWithValue("entityId", Note.RetentionIdentity);
        command.Parameters.AddWithValue("recordId", Guid.NewGuid().ToString());
        command.Parameters.AddWithValue("strategy", (int)Strategy.Purge);
        await command.ExecuteNonQueryAsync();
        return sweepId;
    }

    private async Task<Guid> SeedHoldAsync(
        DateTimeOffset? expiresAt,
        DateTimeOffset? removedAt,
        Guid? recordId = null,
        Guid? tenantId = null
    )
    {
        var holdId = Guid.NewGuid();
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO "retention_holds"
                ("HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt")
            VALUES (@holdId, @entityId, @recordId, @tenantId, 'pruning test', @createdAt, @expiresAt, @removedAt)
            """;
        command.Parameters.AddWithValue("holdId", holdId);
        command.Parameters.AddWithValue("entityId", Note.RetentionIdentity);
        command.Parameters.AddWithValue("recordId", (recordId ?? Guid.NewGuid()).ToString());
        command.Parameters.AddWithValue("tenantId", tenantId ?? Guid.NewGuid());
        command.Parameters.AddWithValue("createdAt", Now.AddDays(-1000));
        command.Parameters.Add(new NpgsqlParameter("expiresAt", NpgsqlTypes.NpgsqlDbType.TimestampTz)
        {
            Value = (object?)expiresAt ?? DBNull.Value,
        });
        command.Parameters.Add(new NpgsqlParameter("removedAt", NpgsqlTypes.NpgsqlDbType.TimestampTz)
        {
            Value = (object?)removedAt ?? DBNull.Value,
        });
        await command.ExecuteNonQueryAsync();
        return holdId;
    }

    private async Task<List<Guid>> LoadRunIdsAsync() =>
        await QueryGuidsAsync("""SELECT "SweepId" FROM "sweep_run" """);

    private async Task<List<Guid>> LoadHoldIdsAsync() =>
        await QueryGuidsAsync("""SELECT "HoldId" FROM "retention_holds" """);

    private async Task<List<Guid>> QueryGuidsAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var ids = new List<Guid>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            ids.Add(reader.GetGuid(0));
        }
        return ids;
    }

    /// <summary>Entity summaries and row details still referencing the run.</summary>
    private async Task<long> CountDependentRowsAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                (SELECT count(*) FROM "sweep_run_entity_summary" WHERE "SweepId" = @sweepId)
                + (SELECT count(*) FROM "sweep_run_row_detail" WHERE "SweepId" = @sweepId)
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        return (long)(await command.ExecuteScalarAsync())!;
    }
}
