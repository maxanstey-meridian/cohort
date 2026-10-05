using System.Diagnostics;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Handlers;
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
    public async Task Prune_Keeps_A_Real_Run_Until_Its_Handler_Work_Has_Succeeded()
    {
        var tenantId = Guid.NewGuid();
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = Now.AddDays(-120),
                    Body = "handled",
                }
            );
            await db.SaveChangesAsync();
        }

        using var host = CreateHost(
            succeededRunRetention: "90.00:00:00",
            failedRunRetention: "90.00:00:00",
            configureServices: services => services.AddRowHandler<Note, PruningNoteHandler>()
        );
        var sweep = await host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            Now
        );
        await BackdateSettledAtAsync(sweep.SweepId, Now.AddDays(-100));

        (await PruneAsync(host)).Runs.Should().Be(0);
        (await LoadRunIdsAsync()).Should().Contain(sweep.SweepId);
        (await LoadHandlerStatesAsync(sweep.SweepId))
            .Should()
            .Equal(SweepRowHandlerDispatchState.Pending);

        await host.RunWithServicesAsync(services =>
            services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );
        (await LoadHandlerStatesAsync(sweep.SweepId))
            .Should()
            .Equal(SweepRowHandlerDispatchState.Succeeded);

        (await PruneAsync(host)).Runs.Should().Be(1);
        (await LoadRunIdsAsync()).Should().NotContain(sweep.SweepId);
        (await CountDependentRowsAsync(sweep.SweepId)).Should().Be(0);
    }

    [Theory]
    [InlineData(nameof(SweepRowHandlerDispatchState.Pending))]
    [InlineData(nameof(SweepRowHandlerDispatchState.InFlight))]
    public async Task Prune_Never_Deletes_A_Run_With_Unfinished_Handler_Work(string unfinishedState)
    {
        var unfinished = Enum.Parse<SweepRowHandlerDispatchState>(unfinishedState);
        var succeeded = await SeedRunAsync(SweepRunStatus.Succeeded, Now.AddDays(-400), unfinished);
        var failed = await SeedRunAsync(SweepRunStatus.Failed, Now.AddDays(-400), unfinished);

        using var host = CreateHost(succeededRunRetention: "1.00:00:00", failedRunRetention: "1.00:00:00");
        (await PruneAsync(host)).Runs.Should().Be(0);

        (await LoadRunIdsAsync()).Should().BeEquivalentTo([succeeded, failed]);
        (await CountDependentRowsAsync(succeeded)).Should().Be(3);
    }

    [Fact]
    public async Task Prune_Keeps_A_Succeeded_Run_With_DeadLettered_Handler_Work_For_The_Failed_Retention()
    {
        var deadLettered = await SeedRunAsync(
            SweepRunStatus.Succeeded,
            Now.AddDays(-100),
            SweepRowHandlerDispatchState.DeadLettered
        );
        var handled = await SeedRunAsync(
            SweepRunStatus.Succeeded,
            Now.AddDays(-100),
            SweepRowHandlerDispatchState.Succeeded
        );

        using (var host = CreateHost(succeededRunRetention: "90.00:00:00", failedRunRetention: "180.00:00:00"))
        {
            (await PruneAsync(host)).Runs.Should().Be(1);
        }
        (await LoadRunIdsAsync()).Should().BeEquivalentTo([deadLettered]);

        using (var host = CreateHost(succeededRunRetention: "90.00:00:00", failedRunRetention: "90.00:00:00"))
        {
            (await PruneAsync(host)).Runs.Should().Be(1);
        }
        (await LoadRunIdsAsync()).Should().BeEmpty();
        (await CountDependentRowsAsync(deadLettered)).Should().Be(0);
        (await CountDependentRowsAsync(handled)).Should().Be(0);
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
        Action<IServiceCollection>? configureServices = null
    )
    {
        var settings = new Dictionary<string, string?>
        {
            [$"{CohortOptions.SectionName}:KillSwitch"] = killSwitch.ToString(),
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

        return new CohortTestHost(
            ConnectionString,
            configurationOverrides: settings,
            configureServices: configureServices
        );
    }

    private static Task<RetentionHistoryPruneResult> PruneAsync(CohortTestHost host) =>
        host.RunWithServicesAsync(services =>
            services.GetRequiredService<RetentionHistoryPruner>().PruneAsync()
        );

    /// <summary>
    /// Seeds a run with one entity summary and one row detail, plus a handler status in
    /// <paramref name="handlerState"/> when given.
    /// </summary>
    private async Task<Guid> SeedRunAsync(
        SweepRunStatus status,
        DateTimeOffset? settledAt,
        SweepRowHandlerDispatchState? handlerState = null,
        DateTimeOffset? startedAt = null
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
                 "ResolvedPeriod", "Affected", "HeldCount", "SkippedCount", "NullAnchorCount")
            VALUES (@sweepId, @startedAt, 'Cohort.Sample.Entities.Note', @entityId, 'short-lived', @tenantId, @strategy,
                    INTERVAL '30 days', 1, 0, 0, 0);

            WITH detail AS (
                INSERT INTO "sweep_run_row_detail"
                    ("SweepId", "At", "EntityType", "RetentionEntityId", "RecordId", "Category", "Strategy", "TenantId")
                VALUES (@sweepId, @startedAt, 'Cohort.Sample.Entities.Note', @entityId, @recordId, 'short-lived', @strategy, @tenantId)
                RETURNING "Id"
            )
            INSERT INTO "sweep_row_handler_status"
                ("SweepRunRowDetailId", "HandlerType", "DispatchPhase", "State", "Attempt", "QueuedAt", "NextAttemptAt",
                 "ClaimedAt", "ClaimToken", "CompletedAt")
            SELECT detail."Id", 'pruning-test-handler', 0, @handlerState, 1, @startedAt, @startedAt,
                   CASE WHEN @handlerState = @inFlight THEN @startedAt END,
                   CASE WHEN @handlerState = @inFlight THEN gen_random_uuid() END,
                   CASE WHEN @handlerState IN (@succeeded, @deadLettered) THEN @startedAt END
            FROM detail
            WHERE @handlerState IS NOT NULL;
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("startedAt", started);
        command.Parameters.AddWithValue("status", (int)status);
        command.Parameters.Add(new NpgsqlParameter("settledAt", NpgsqlTypes.NpgsqlDbType.TimestampTz)
        {
            Value = (object?)settledAt ?? DBNull.Value,
        });
        command.Parameters.AddWithValue("trigger", (int)SweepTriggerKind.Manual);
        command.Parameters.AddWithValue("tenantId", tenantId);
        command.Parameters.AddWithValue("entityId", Note.RetentionIdentity);
        command.Parameters.AddWithValue("recordId", Guid.NewGuid().ToString());
        command.Parameters.AddWithValue("strategy", (int)Strategy.Purge);
        command.Parameters.Add(new NpgsqlParameter("handlerState", NpgsqlTypes.NpgsqlDbType.Integer)
        {
            Value = handlerState is null ? DBNull.Value : (int)handlerState.Value,
        });
        command.Parameters.AddWithValue("inFlight", (int)SweepRowHandlerDispatchState.InFlight);
        command.Parameters.AddWithValue("succeeded", (int)SweepRowHandlerDispatchState.Succeeded);
        command.Parameters.AddWithValue("deadLettered", (int)SweepRowHandlerDispatchState.DeadLettered);
        await command.ExecuteNonQueryAsync();
        return sweepId;
    }

    private async Task<Guid> SeedHoldAsync(DateTimeOffset? expiresAt, DateTimeOffset? removedAt)
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
        command.Parameters.AddWithValue("recordId", Guid.NewGuid().ToString());
        command.Parameters.AddWithValue("tenantId", Guid.NewGuid());
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

    private async Task BackdateSettledAtAsync(Guid sweepId, DateTimeOffset settledAt)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE "sweep_run"
            SET "StartedAt" = @settledAt - INTERVAL '1 minute', "SettledAt" = @settledAt, "Duration" = INTERVAL '1 minute'
            WHERE "SweepId" = @sweepId
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("settledAt", settledAt);
        (await command.ExecuteNonQueryAsync()).Should().Be(1);
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

    private async Task<List<SweepRowHandlerDispatchState>> LoadHandlerStatesAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT status."State"
            FROM "sweep_row_handler_status" AS status
            INNER JOIN "sweep_run_row_detail" AS detail ON detail."Id" = status."SweepRunRowDetailId"
            WHERE detail."SweepId" = @sweepId
            ORDER BY status."Id"
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        var states = new List<SweepRowHandlerDispatchState>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            states.Add((SweepRowHandlerDispatchState)reader.GetInt32(0));
        }
        return states;
    }

    /// <summary>Entity summaries, row details and handler statuses still referencing the run.</summary>
    private async Task<long> CountDependentRowsAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                (SELECT count(*) FROM "sweep_run_entity_summary" WHERE "SweepId" = @sweepId)
                + (SELECT count(*) FROM "sweep_run_row_detail" WHERE "SweepId" = @sweepId)
                + (SELECT count(*)
                   FROM "sweep_row_handler_status" AS status
                   INNER JOIN "sweep_run_row_detail" AS detail ON detail."Id" = status."SweepRunRowDetailId"
                   WHERE detail."SweepId" = @sweepId)
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        return (long)(await command.ExecuteScalarAsync())!;
    }
}

file sealed class PruningNoteHandler : IRetentionHandler<Note>;
