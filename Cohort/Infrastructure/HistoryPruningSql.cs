using System.Data;

using Cohort.Application;
using Cohort.Infrastructure.Handlers;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Infrastructure;

// The only interpolations in this file's raw SQL are Cohort's own table names, quoted from
// the EF model, and the shared eligibility predicate; every value is a positional parameter.
#pragma warning disable EF1002

/// <summary>
/// The statements behind history pruning: one batch of settled runs, one batch of inactive
/// holds. Each batch is its own transaction and claims with <c>SKIP LOCKED</c>, so replicas
/// prune disjoint batches.
/// </summary>
internal static class HistoryPruningSql
{
    private const int Started = (int)SweepRunStatus.Started;
    private const int Succeeded = (int)SweepRunStatus.Succeeded;
    private const int PartiallyFailed = (int)SweepRunStatus.PartiallyFailed;
    private const int Failed = (int)SweepRunStatus.Failed;
    private const int Cancelled = (int)SweepRunStatus.Cancelled;

    private const int HandlerPending = (int)SweepRowHandlerDispatchState.Pending;
    private const int HandlerInFlight = (int)SweepRowHandlerDispatchState.InFlight;
    private const int HandlerDeadLettered = (int)SweepRowHandlerDispatchState.DeadLettered;

    /// <summary>
    /// Claims up to <paramref name="batchSize"/> eligible runs under <c>FOR UPDATE SKIP
    /// LOCKED</c>, then deletes their row details (handler statuses cascade), entity summaries
    /// and the runs themselves in one transaction. Returns how many runs it deleted.
    /// </summary>
    internal static async Task<int> PruneRunBatchAsync(
        DbContext db,
        DateTimeOffset? succeededCutoff,
        DateTimeOffset? failedCutoff,
        DateTimeOffset? currentOccurrence,
        int batchSize,
        CancellationToken ct
    )
    {
        var tables = CohortStoreTables.FromModel(db.Model).Quoted();
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
            batchSize,
            (int)SweepTriggerKind.Scheduled,
            (object?)currentOccurrence ?? DBNull.Value,
        ];

        await using var transaction = await db.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
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
    /// Deletes up to <paramref name="batchSize"/> holds whose earlier of <c>RemovedAt</c> and
    /// <c>ExpiresAt</c> is before the cutoff. The eligibility test reads only the hold's own
    /// columns, so the lock re-evaluates it against the latest committed version of each
    /// claimed row.
    /// </summary>
    internal static Task<int> PruneHoldBatchAsync(
        DbContext db,
        DateTimeOffset cutoff,
        int batchSize,
        CancellationToken ct
    )
    {
        var holds = CohortStoreTables.FromModel(db.Model).Quoted().RetentionHolds;
        return db.Database.ExecuteSqlRawAsync(
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
            [cutoff, batchSize],
            ct
        );
    }

    /// <summary>
    /// Settled-run eligibility. Positional parameters: {0} Started, {1} Succeeded,
    /// {2} PartiallyFailed, {3} Failed, {4} Cancelled, {5} succeeded cutoff, {6} failed
    /// cutoff, {7} handler Pending, {8} handler InFlight, {9} handler DeadLettered, {11}
    /// Scheduled, {12} the schedule's current occurrence. A null cutoff makes its comparison
    /// null, so that class of run is never eligible. A run whose row details still hold a
    /// snapshot is never eligible either: dead-lettered work keeps its snapshot until
    /// <c>RowHandlerDispatch:PayloadRetention</c> so it can be requeued. That can't race a
    /// requeue, because a requeue selects only rows that still hold a snapshot and this
    /// predicate only runs that hold none: the two sets are disjoint. A scheduled run started
    /// at or after the current occurrence is the worker's ledger for it, so pruning it would
    /// rerun passes that already succeeded and give exhausted ones fresh attempts.
    /// </summary>
    private static string EligibleRunPredicate(QuotedCohortTables tables)
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
                  AND NOT EXISTS (
                      SELECT 1 FROM {{tables.SweepRunRowDetail}} AS retained
                      WHERE retained."SweepId" = run."SweepId" AND retained."CapturedPayload" IS NOT NULL
                  )
                  AND (
                      CAST({12} AS timestamptz) IS NULL
                      OR run."TriggerKind" <> {11}
                      OR run."StartedAt" < CAST({12} AS timestamptz)
                  )
            """;
    }
}
