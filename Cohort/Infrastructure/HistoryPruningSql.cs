using System.Data;

using Cohort.Application;
using Cohort.Infrastructure.Handlers;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Infrastructure;

/// <summary>
/// What one history-pruning pass prunes: settled runs, inactive holds, and whether it reads the
/// database clock, which it does to find the schedule's current occurrence when a schedule is set.
/// </summary>
internal sealed record PruningScope(bool Runs, bool Holds, bool ReadsClock);

// The only interpolations in this file's raw SQL are Cohort's own table names, quoted from
// the EF model; every value is a named parameter.

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
        var tables = CohortStoreTables.FromModel(db.Model);
        var eligible = EligibleRunsStatement(tables, succeededCutoff, failedCutoff, currentOccurrence, batchSize);

        await using var transaction = await db.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        var sweepIds = await db.Database
            .SqlQueryRaw<Guid>(eligible.Sql, eligible.Parameters.ToDbParameters(db.Database.GetDbConnection()))
            .ToArrayAsync(ct);
        if (sweepIds.Length == 0)
        {
            return 0;
        }

        await ExecuteAsync(db, DeleteRowDetailsStatement(tables, sweepIds), ct);
        await ExecuteAsync(db, DeleteEntitySummariesStatement(tables, sweepIds), ct);
        await ExecuteAsync(db, DeleteRunsStatement(tables, sweepIds), ct);

        await transaction.CommitAsync(ct);
        return sweepIds.Length;
    }

    /// <summary>
    /// The oldest settled runs eligible for pruning, locked with <c>FOR UPDATE SKIP
    /// LOCKED</c>. A null cutoff or occurrence binds as SQL NULL.
    /// </summary>
    internal static SweepStatement EligibleRunsStatement(
        CohortStoreTables tables,
        DateTimeOffset? succeededCutoff,
        DateTimeOffset? failedCutoff,
        DateTimeOffset? currentOccurrence,
        int batchSize
    ) =>
        CohortStatements.Statement(
            "eligible-runs",
            $"""
            SELECT run."SweepId" AS "Value"
            FROM {PostgreSqlIdentifier.Format(tables.SweepRun)} AS run
            WHERE {EligibleRunPredicate(tables.Quoted())}
            ORDER BY run."SettledAt", run."SweepId"
            LIMIT @batchSize
            FOR UPDATE OF run SKIP LOCKED
            """,
            new SqlParams
            {
                ["started"] = Started,
                ["succeeded"] = Succeeded,
                ["partiallyFailed"] = PartiallyFailed,
                ["failed"] = Failed,
                ["cancelled"] = Cancelled,
                ["succeededCutoff"] = succeededCutoff,
                ["failedCutoff"] = failedCutoff,
                ["handlerPending"] = HandlerPending,
                ["handlerInFlight"] = HandlerInFlight,
                ["handlerDeadLettered"] = HandlerDeadLettered,
                ["batchSize"] = batchSize,
                ["scheduled"] = (int)SweepTriggerKind.Scheduled,
                ["currentOccurrence"] = currentOccurrence,
            },
            tables.SweepRun,
            tables.SweepRunRowDetail,
            tables.SweepRowHandlerStatus
        );

    /// <summary>Deletes the row details of the pruned runs (handler statuses cascade).</summary>
    internal static SweepStatement DeleteRowDetailsStatement(CohortStoreTables tables, Guid[] sweepIds) =>
        CohortStatements.Statement(
            "prune-row-details",
            $"""DELETE FROM {PostgreSqlIdentifier.Format(tables.SweepRunRowDetail)} WHERE "SweepId" = ANY(@sweepIds)""",
            new SqlParams { ["sweepIds"] = sweepIds },
            tables.SweepRunRowDetail
        );

    /// <summary>Deletes the entity summaries of the pruned runs.</summary>
    internal static SweepStatement DeleteEntitySummariesStatement(CohortStoreTables tables, Guid[] sweepIds) =>
        CohortStatements.Statement(
            "prune-entity-summaries",
            $"""DELETE FROM {PostgreSqlIdentifier.Format(tables.SweepRunEntitySummary)} WHERE "SweepId" = ANY(@sweepIds)""",
            new SqlParams { ["sweepIds"] = sweepIds },
            tables.SweepRunEntitySummary
        );

    /// <summary>Deletes the pruned runs.</summary>
    internal static SweepStatement DeleteRunsStatement(CohortStoreTables tables, Guid[] sweepIds) =>
        CohortStatements.Statement(
            "prune-runs",
            $"""DELETE FROM {PostgreSqlIdentifier.Format(tables.SweepRun)} WHERE "SweepId" = ANY(@sweepIds)""",
            new SqlParams { ["sweepIds"] = sweepIds },
            tables.SweepRun
        );

    /// <summary>
    /// Deletes up to <paramref name="batchSize"/> holds whose earlier of <c>RemovedAt</c>
    /// and <c>ExpiresAt</c> is before the cutoff. The eligibility test reads only the hold's own
    /// columns, so the lock re-evaluates it against the latest committed version of each
    /// claimed row.
    /// </summary>
    internal static SweepStatement PruneHoldsStatement(CohortStoreTables tables, DateTimeOffset cutoff, int batchSize)
    {
        var holds = PostgreSqlIdentifier.Format(tables.RetentionHolds);
        return CohortStatements.Statement(
            "prune-holds",
            $"""
            WITH candidates AS (
                SELECT "HoldId"
                FROM {holds}
                WHERE LEAST("RemovedAt", "ExpiresAt") < @cutoff
                ORDER BY LEAST("RemovedAt", "ExpiresAt"), "HoldId"
                LIMIT @batchSize
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM {holds} AS hold
            USING candidates
            WHERE hold."HoldId" = candidates."HoldId"
              AND LEAST(hold."RemovedAt", hold."ExpiresAt") < @cutoff
            """,
            new SqlParams { ["cutoff"] = cutoff, ["batchSize"] = batchSize },
            tables.RetentionHolds
        );
    }

    /// <summary>
    /// Deletes up to <paramref name="batchSize"/> holds whose earlier of <c>RemovedAt</c> and
    /// <c>ExpiresAt</c> is before the cutoff.
    /// </summary>
    internal static Task<int> PruneHoldBatchAsync(
        DbContext db,
        DateTimeOffset cutoff,
        int batchSize,
        CancellationToken ct
    ) => ExecuteAsync(db, PruneHoldsStatement(CohortStoreTables.FromModel(db.Model), cutoff, batchSize), ct);

    private static Task<int> ExecuteAsync(DbContext db, SweepStatement statement, CancellationToken ct) =>
        db.Database.ExecuteSqlRawAsync(
            statement.Sql,
            statement.Parameters.ToDbParameters(db.Database.GetDbConnection()),
            ct
        );

    /// <summary>
    /// Settled-run eligibility. Named parameters: @started, @succeeded, @partiallyFailed,
    /// @failed, @cancelled, @succeededCutoff, @failedCutoff, @handlerPending, @handlerInFlight,
    /// @handlerDeadLettered, @scheduled, and @currentOccurrence (the schedule's current
    /// occurrence). A null cutoff makes its comparison
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
        return $"""
            run."Status" <> @started
                  AND (
                      (
                          run."Status" = @succeeded
                          AND run."SettledAt" < CAST(@succeededCutoff AS timestamptz)
                          AND NOT EXISTS (SELECT 1 {handlerWork} AND status."State" = @handlerDeadLettered)
                      )
                      OR (
                          (
                              run."Status" IN (@partiallyFailed, @failed, @cancelled)
                              OR EXISTS (SELECT 1 {handlerWork} AND status."State" = @handlerDeadLettered)
                          )
                          AND run."SettledAt" < CAST(@failedCutoff AS timestamptz)
                      )
                  )
                  AND NOT EXISTS (SELECT 1 {handlerWork} AND status."State" IN (@handlerPending, @handlerInFlight))
                  AND NOT EXISTS (
                      SELECT 1 FROM {tables.SweepRunRowDetail} AS retained
                      WHERE retained."SweepId" = run."SweepId" AND retained."CapturedPayload" IS NOT NULL
                  )
                  AND (
                      CAST(@currentOccurrence AS timestamptz) IS NULL
                      OR run."TriggerKind" <> @scheduled
                      OR run."StartedAt" < CAST(@currentOccurrence AS timestamptz)
                  )
            """;
    }
}
