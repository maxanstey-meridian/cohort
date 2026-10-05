using Cohort.Application;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Handlers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cohort.Hosting;

// The only interpolations in this file's raw SQL are Cohort's own table names, quoted from
// the EF model, and the shared eligibility predicate; every value is a positional parameter.
#pragma warning disable EF1002

/// <summary>
/// Opt-in pruning of Cohort's own history: settled runs (with their entity summaries, row
/// details and handler statuses) past <see cref="HistoryPruningOptions.SucceededRunRetention"/>
/// or <see cref="HistoryPruningOptions.FailedRunRetention"/>, and holds that stopped protecting
/// longer ago than <see cref="HistoryPruningOptions.InactiveHoldRetention"/>. A run is never
/// pruned while any of its handler work is pending or in flight, and nothing is pruned while
/// <see cref="CohortOptions.KillSwitch"/> is on. Each batch commits on its own and claims with
/// <c>SKIP LOCKED</c>, so replicas prune disjoint batches.
/// </summary>
internal sealed class RetentionHistoryPruner(
    IServiceScopeFactory scopeFactory,
    CohortOptionsSnapshot options,
    ILogger<RetentionHistoryPruner> logger
) : BackgroundService
{
    private const int Started = (int)SweepRunStatus.Started;
    private const int Succeeded = (int)SweepRunStatus.Succeeded;
    private const int PartiallyFailed = (int)SweepRunStatus.PartiallyFailed;
    private const int Failed = (int)SweepRunStatus.Failed;
    private const int Cancelled = (int)SweepRunStatus.Cancelled;

    private const int HandlerPending = (int)SweepRowHandlerDispatchState.Pending;
    private const int HandlerInFlight = (int)SweepRowHandlerDispatchState.InFlight;
    private const int HandlerDeadLettered = (int)SweepRowHandlerDispatchState.DeadLettered;

    internal const int BatchSize = 100;
    private static readonly TimeSpan Interval = TimeSpan.FromHours(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (options.Current.HistoryPruning.Enabled)
            {
                try
                {
                    await PruneAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Cohort history pruning pass failed; it retries after the next interval.");
                }
            }

            try
            {
                await OperationalTime.DelayAsync(Interval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Runs one pruning pass: deletes every eligible run, then every eligible hold, in
    /// independently committed batches. Retentions left <c>null</c> prune nothing. The kill
    /// switch is checked before every batch, so engaging it stops the pass after the batch in
    /// progress.
    /// </summary>
    internal async Task<RetentionHistoryPruneResult> PruneAsync(CancellationToken ct = default)
    {
        var pruning = options.Current.HistoryPruning;
        if (!pruning.Enabled || options.Current.KillSwitch)
        {
            return new RetentionHistoryPruneResult(0, 0);
        }

        await using (var scope = scopeFactory.CreateAsyncScope())
        {
            await scope.ServiceProvider.GetRequiredService<RetentionRuntimeReadinessValidator>().ValidateAsync(ct);
        }

        var now = DateTimeOffset.UtcNow;
        var succeededCutoff = Cutoff(now, pruning.SucceededRunRetention);
        var failedCutoff = Cutoff(now, pruning.FailedRunRetention);
        var holdCutoff = Cutoff(now, pruning.InactiveHoldRetention);

        long runs = 0;
        if (succeededCutoff is not null || failedCutoff is not null)
        {
            int deleted;
            do
            {
                ct.ThrowIfCancellationRequested();
                deleted = await PruneRunBatchAsync(succeededCutoff, failedCutoff, ct);
                runs += deleted;
            }
            while (deleted == BatchSize && !options.Current.KillSwitch);
        }

        long holds = 0;
        if (holdCutoff is not null && !options.Current.KillSwitch)
        {
            int deleted;
            do
            {
                ct.ThrowIfCancellationRequested();
                deleted = await PruneHoldBatchAsync(holdCutoff.Value, ct);
                holds += deleted;
            }
            while (deleted == BatchSize && !options.Current.KillSwitch);
        }

        if (runs > 0 || holds > 0)
        {
            logger.LogInformation(
                "Cohort pruned {RunCount} settled retention runs and {HoldCount} inactive holds.",
                runs,
                holds
            );
        }

        return new RetentionHistoryPruneResult(runs, holds);
    }

    /// <summary>
    /// Claims a batch of eligible runs under <c>FOR UPDATE SKIP LOCKED</c>, then deletes their
    /// row details (handler statuses cascade), entity summaries and the runs themselves in one
    /// transaction.
    /// </summary>
    private async Task<int> PruneRunBatchAsync(
        DateTimeOffset? succeededCutoff,
        DateTimeOffset? failedCutoff,
        CancellationToken ct
    )
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext);
        var tables = new QuotedTables(CohortStoreTables.FromModel(db.Model));
        object[] parameters =
        [
            Started,
            Succeeded,
            PartiallyFailed,
            Failed,
            Cancelled,
            (object?)succeededCutoff ?? DBNull.Value,
            (object?)failedCutoff ?? DBNull.Value,
            HandlerPending,
            HandlerInFlight,
            HandlerDeadLettered,
            BatchSize,
        ];

        await using var transaction = await db.Database.BeginTransactionAsync(ct);
        var sweepIds = await db.Database
            .SqlQueryRaw<Guid>(
                $$"""
                SELECT run."SweepId" AS "Value"
                FROM {{tables.SweepRun}} AS run
                WHERE {{EligibleRunPredicate(tables)}}
                ORDER BY run."SettledAt", run."SweepId"
                LIMIT {10}
                FOR UPDATE OF run SKIP LOCKED
                """,
                parameters
            )
            .ToArrayAsync(ct);
        if (sweepIds.Length == 0)
        {
            return 0;
        }

        await db.Database.ExecuteSqlRawAsync(
            $$"""DELETE FROM {{tables.SweepRunRowDetail}} WHERE "SweepId" = ANY({0})""",
            [sweepIds],
            ct
        );
        await db.Database.ExecuteSqlRawAsync(
            $$"""DELETE FROM {{tables.SweepRunEntitySummary}} WHERE "SweepId" = ANY({0})""",
            [sweepIds],
            ct
        );
        await db.Database.ExecuteSqlRawAsync(
            $$"""DELETE FROM {{tables.SweepRun}} WHERE "SweepId" = ANY({0})""",
            [sweepIds],
            ct
        );

        await transaction.CommitAsync(ct);
        return sweepIds.Length;
    }

    /// <summary>
    /// Settled-run eligibility. Positional parameters: {0} Started, {1} Succeeded,
    /// {2} PartiallyFailed, {3} Failed, {4} Cancelled, {5} succeeded cutoff, {6} failed
    /// cutoff, {7} handler Pending, {8} handler InFlight, {9} handler DeadLettered. A null
    /// cutoff makes its comparison null, so that class of run is never eligible.
    /// </summary>
    private static string EligibleRunPredicate(QuotedTables tables)
    {
        var handlerWork = $"""
            FROM {tables.SweepRunRowDetail} AS detail
                    INNER JOIN {tables.SweepRowHandlerStatus} AS status
                        ON status."SweepRunRowDetailId" = detail."Id"
                    WHERE detail."SweepId" = run."SweepId"
            """;
        return $$"""
            run."Status" <> {0}
                  AND (
                      (
                          run."Status" = {1}
                          AND run."SettledAt" < CAST({5} AS timestamptz)
                          AND NOT EXISTS (SELECT 1 {{handlerWork}} AND status."State" = {9})
                      )
                      OR (
                          (
                              run."Status" IN ({2}, {3}, {4})
                              OR EXISTS (SELECT 1 {{handlerWork}} AND status."State" = {9})
                          )
                          AND run."SettledAt" < CAST({6} AS timestamptz)
                      )
                  )
                  AND NOT EXISTS (SELECT 1 {{handlerWork}} AND status."State" IN ({7}, {8}))
            """;
    }

    /// <summary>
    /// Deletes a batch of holds whose earlier of <c>RemovedAt</c> and <c>ExpiresAt</c> is
    /// before the cutoff. The eligibility test reads only the hold's own columns, so the
    /// lock re-evaluates it against the latest committed version of each claimed row.
    /// </summary>
    private async Task<int> PruneHoldBatchAsync(DateTimeOffset cutoff, CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext);
        var holds = PostgreSqlIdentifier.Format(CohortStoreTables.FromModel(db.Model).RetentionHolds);
        return await db.Database.ExecuteSqlRawAsync(
            $$"""
            WITH candidates AS (
                SELECT "HoldId"
                FROM {{holds}}
                WHERE LEAST("RemovedAt", "ExpiresAt") < {0}
                ORDER BY LEAST("RemovedAt", "ExpiresAt"), "HoldId"
                LIMIT {1}
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM {{holds}} AS hold
            USING candidates
            WHERE hold."HoldId" = candidates."HoldId"
              AND LEAST(hold."RemovedAt", hold."ExpiresAt") < {0}
            """,
            [cutoff, BatchSize],
            ct
        );
    }

    private static DateTimeOffset? Cutoff(DateTimeOffset now, TimeSpan? retention) =>
        retention is null ? null : OperationalTime.SubtractSaturating(now, retention.Value);

    private sealed class QuotedTables(CohortStoreTables tables)
    {
        public string SweepRun { get; } = PostgreSqlIdentifier.Format(tables.SweepRun);
        public string SweepRunEntitySummary { get; } = PostgreSqlIdentifier.Format(tables.SweepRunEntitySummary);
        public string SweepRunRowDetail { get; } = PostgreSqlIdentifier.Format(tables.SweepRunRowDetail);
        public string SweepRowHandlerStatus { get; } = PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus);
    }
}

internal sealed record RetentionHistoryPruneResult(long Runs, long Holds);
