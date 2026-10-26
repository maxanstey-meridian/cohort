using System.Data;
using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Handlers;

// The only interpolations in this file's raw SQL are Cohort's own table names, quoted from
// the EF model; every value is a positional parameter.
#pragma warning disable EF1002

/// <summary>
/// The row-handler queue's statements: claiming due work, settling a delivery while its claim
/// is still owned, recovering expired claims, requeuing dead-lettered work and scrubbing
/// captured snapshots. Every statement runs through EF's raw-SQL API, so host interceptors see
/// each one. A claim token fences settlement, so a delivery can only settle the claim it took.
/// </summary>
internal static class RowHandlerQueueSql
{
    private const int Pending = (int)SweepRowHandlerDispatchState.Pending;
    private const int InFlight = (int)SweepRowHandlerDispatchState.InFlight;
    private const int Succeeded = (int)SweepRowHandlerDispatchState.Succeeded;
    private const int DeadLettered = (int)SweepRowHandlerDispatchState.DeadLettered;

    /// <summary>The <c>LastError</c> of a claim that expired on its last attempt.</summary>
    internal const string ExhaustedClaimReason = "Handler claim expired after reaching RowHandlerDispatch:MaxAttempts.";

    private const string DependentDeadLetterReason = "Skipped because an earlier handler for the same row dead-lettered.";

    // Values: {0} state, {1} completed-at, {2} last error.
    private const string DeadLetterAssignments =
        """
        "State" = {0}, "CompletedAt" = {1}, "ClaimedAt" = NULL, "ClaimToken" = NULL, "LastError" = {2}
        """;

    /// <summary>
    /// The queue's remaining work: claims in flight, pending rows, and dead-lettered rows that
    /// still hold a snapshot to requeue. Raw SQL, not LINQ: EF emits an unqualified COUNT that
    /// a search_path decoy can shadow.
    /// </summary>
    internal static async Task<RowDispatcherFlushResult> CountRemainingAsync(DbContext db, CancellationToken ct)
    {
        var tables = CohortStoreTables.FromModel(db.Model).Quoted();
        var remaining = await db.Database
            .SqlQueryRaw<RemainingWork>(
                $$"""
                SELECT
                    pg_catalog.count(*) FILTER (WHERE status."State" = {0}) AS "InFlight",
                    pg_catalog.count(*) FILTER (WHERE status."State" = {1}) AS "Pending",
                    pg_catalog.count(*) FILTER (
                        WHERE status."State" = {2} AND detail."CapturedPayload" IS NOT NULL
                    ) AS "DeadLetteredRequeueable"
                FROM {{tables.SweepRowHandlerStatus}} AS status
                INNER JOIN {{tables.SweepRunRowDetail}} AS detail
                    ON detail."Id" = status."SweepRunRowDetailId"
                """,
                InFlight,
                Pending,
                DeadLettered
            )
            .SingleAsync(ct);
        return new RowDispatcherFlushResult(
            remaining.InFlight,
            remaining.Pending,
            remaining.DeadLetteredRequeueable
        );
    }

    /// <summary>
    /// Requeues every dead-lettered handler of each row that still holds its snapshot,
    /// optionally only rows of <paramref name="sweepId"/> with a dead-lettered
    /// <paramref name="handlerId"/>. Locking the row details serialises this with the payload
    /// scrub, and READ COMMITTED rechecks the snapshot after the lock. Returns how many
    /// statuses it requeued.
    /// </summary>
    internal static Task<int> RequeueDeadLetteredAsync(
        DbContext db,
        Guid? sweepId,
        Guid? handlerId,
        DateTimeOffset now,
        CancellationToken ct
    )
    {
        var tables = CohortStoreTables.FromModel(db.Model).Quoted();
        return db.Database.ExecuteSqlRawAsync(
            $$"""
            WITH target AS (
                SELECT detail."Id"
                FROM {{tables.SweepRunRowDetail}} AS detail
                WHERE detail."CapturedPayload" IS NOT NULL
                  AND ({0} OR detail."SweepId" = {1})
                  AND EXISTS (
                      SELECT 1 FROM {{tables.SweepRowHandlerStatus}} AS dead
                      WHERE dead."SweepRunRowDetailId" = detail."Id"
                        AND dead."State" = {4}
                        AND ({2} OR dead."HandlerType" = {3})
                  )
                FOR UPDATE OF detail
            )
            UPDATE {{tables.SweepRowHandlerStatus}} AS status
            SET "State" = {5}, "NextAttemptAt" = {6}, "CompletedAt" = NULL
            FROM target
            WHERE status."SweepRunRowDetailId" = target."Id" AND status."State" = {4}
            """,
            [
                sweepId is null,
                sweepId ?? Guid.Empty,
                handlerId is null,
                handlerId?.ToString("D") ?? "",
                DeadLettered,
                Pending,
                now,
            ],
            ct
        );
    }

    /// <summary>Clears every captured snapshot taken before <paramref name="cutoff"/>.</summary>
    internal static Task<int> ScrubExpiredPayloadsAsync(DbContext db, DateTimeOffset cutoff, CancellationToken ct) =>
        db.Database.ExecuteSqlRawAsync(
            $$"""
            UPDATE {{CohortStoreTables.FromModel(db.Model).Quoted().SweepRunRowDetail}}
            SET "CapturedPayload" = NULL
            WHERE "CapturedPayload" IS NOT NULL AND "At" < {0}
            """,
            [cutoff],
            ct
        );

    /// <summary>
    /// Clears a row detail's captured snapshot once every handler for that row has succeeded.
    /// </summary>
    internal static Task<int> ClearDeliveredPayloadAsync(DbContext db, long rowDetailId)
    {
        var tables = CohortStoreTables.FromModel(db.Model).Quoted();
        return db.Database.ExecuteSqlRawAsync(
            $$"""
            UPDATE {{tables.SweepRunRowDetail}} AS detail
            SET "CapturedPayload" = NULL
            WHERE detail."Id" = {0}
              AND detail."CapturedPayload" IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {{tables.SweepRowHandlerStatus}} AS status
                  WHERE status."SweepRunRowDetailId" = detail."Id"
                    AND status."State" <> {1}
              )
            """,
            [rowDetailId, Succeeded]
        );
    }

    /// <summary>
    /// One page of claims held since <paramref name="leaseCutoff"/> or earlier, in
    /// (<c>ClaimedAt</c>, <c>Id</c>) order after <paramref name="after"/> (from the start when
    /// null).
    /// </summary>
    internal static Task<List<ExpiredClaim>> ReadExpiredClaimsAsync(
        DbContext db,
        DateTimeOffset leaseCutoff,
        ExpiredClaim? after,
        int batchSize,
        CancellationToken ct
    )
    {
        var tables = CohortStoreTables.FromModel(db.Model).Quoted();
        return db.Database
            .SqlQueryRaw<ExpiredClaim>(
                $$"""
                SELECT
                    status."Id" AS "StatusId",
                    status."SweepRunRowDetailId" AS "RowDetailId",
                    status."Attempt",
                    status."ClaimToken",
                    status."ClaimedAt",
                    detail."SweepId"
                FROM {{tables.SweepRowHandlerStatus}} AS status
                INNER JOIN {{tables.SweepRunRowDetail}} AS detail
                    ON detail."Id" = status."SweepRunRowDetailId"
                WHERE status."State" = {0} AND status."ClaimedAt" <= {1}
                  AND ({3} OR (status."ClaimedAt", status."Id") > ({4}, {5}))
                ORDER BY status."ClaimedAt", status."Id"
                LIMIT {2}
                """,
                InFlight,
                leaseCutoff,
                batchSize,
                after is null,
                after?.ClaimedAt ?? leaseCutoff,
                after?.StatusId ?? 0L
            )
            .ToListAsync(ct);
    }

    /// <summary>
    /// Reclaims one expired claim in one transaction. Locks the status row (skipping one a
    /// claim or another recovery holds), then tries the delivery lock on
    /// <paramref name="connection"/>, the context's open connection. Neither waits, so recovery
    /// can't deadlock with a claim, which takes the row lock first and then waits on the
    /// delivery lock. A claim with attempts left goes back to the queue; one on its last
    /// attempt is dead-lettered with the row's later handlers. The delivery lock is released
    /// before this returns.
    /// </summary>
    internal static async Task<ReclaimOutcome> TryReclaimAsync(
        DbContext db,
        DbConnection connection,
        ExpiredClaim claim,
        int maxAttempts,
        ILogger logger,
        CancellationToken ct
    )
    {
        var now = DateTimeOffset.UtcNow;
        var statusTable = CohortStoreTables.FromModel(db.Model).SweepRowHandlerStatus;
        var key = RetentionRunAdvisoryLock.RowHandlerKeyFor(statusTable, claim.StatusId);
        var locked = false;
        try
        {
            await using var transaction = await db.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
            var stillClaimed = await db.Database
                .SqlQueryRaw<long>(
                    $$"""
                    SELECT "Id" AS "Value" FROM {{PostgreSqlIdentifier.Format(statusTable)}}
                    WHERE "Id" = {0} AND "State" = {1} AND "ClaimToken" = {2}
                    FOR UPDATE SKIP LOCKED
                    """,
                    claim.StatusId,
                    InFlight,
                    claim.ClaimToken
                )
                .ToListAsync(ct);
            if (stillClaimed.Count == 0)
            {
                return ReclaimOutcome.NoLongerClaimed;
            }

            locked = await RetentionRunAdvisoryLock.TryAcquireAsync(connection, key, ct);
            if (!locked)
            {
                return ReclaimOutcome.OwnerStillRunning;
            }

            ReclaimOutcome outcome;
            if (claim.Attempt >= maxAttempts)
            {
                await UpdateOwnedAsync(
                    db,
                    claim.StatusId,
                    claim.ClaimToken,
                    DeadLetterAssignments,
                    [DeadLettered, now, ExhaustedClaimReason],
                    ct
                );
                await DeadLetterDependentsAsync(db, claim.RowDetailId, claim.StatusId, now, ct);
                outcome = ReclaimOutcome.DeadLettered;
            }
            else
            {
                await UpdateOwnedAsync(
                    db,
                    claim.StatusId,
                    claim.ClaimToken,
                    """
                    "State" = {0}, "NextAttemptAt" = {1}, "ClaimedAt" = NULL, "ClaimToken" = NULL
                    """,
                    [Pending, now],
                    ct
                );
                outcome = ReclaimOutcome.Requeued;
            }

            await transaction.CommitAsync(ct);
            return outcome;
        }
        finally
        {
            if (locked)
            {
                await OperationalConnectionCleanup.ReleaseLockAsync(
                    connection,
                    key,
                    "row handler delivery",
                    claim.StatusId,
                    logger
                );
            }
        }
    }

    /// <summary>
    /// Puts back in the queue every claim <paramref name="claimToken"/> still holds, for a
    /// batch whose deliveries were interrupted. The attempt they were interrupted in still
    /// counts.
    /// </summary>
    internal static Task<int> RequeueInterruptedAsync(DbContext db, Guid claimToken, CancellationToken ct) =>
        db.Database.ExecuteSqlRawAsync(
            $$"""
            UPDATE {{CohortStoreTables.FromModel(db.Model).Quoted().SweepRowHandlerStatus}}
            SET "State" = {0}, "ClaimedAt" = NULL, "ClaimToken" = NULL
            WHERE "ClaimToken" = {1} AND "State" = {2}
            """,
            [Pending, claimToken, InFlight],
            ct
        );

    /// <summary>
    /// Claims up to <paramref name="limit"/> due rows under <paramref name="claimToken"/> and
    /// takes each one's delivery lock on the context's connection (which the caller holds
    /// open) before the claim commits. Due rows are pending past their backoff; expired claims
    /// come back through recovery, never straight from here. Deferred-phase rows wait for their
    /// run to settle, and a row's handlers run one at a time in status order.
    /// </summary>
    internal static async Task<List<ClaimedHandlerRow>> ClaimAsync(
        DbContext leaseDb,
        Guid claimToken,
        int limit,
        CancellationToken ct
    )
    {
        var storeTables = CohortStoreTables.FromModel(leaseDb.Model);
        var tables = storeTables.Quoted();
        var now = DateTimeOffset.UtcNow;
        await using var transaction = await leaseDb.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);

        var claimed = await leaseDb.Database
            .SqlQueryRaw<ClaimedHandlerRow>(
                $$"""
                WITH due AS (
                    SELECT status."Id"
                    FROM {{tables.SweepRowHandlerStatus}} AS status
                    INNER JOIN {{tables.SweepRunRowDetail}} AS detail
                        ON detail."Id" = status."SweepRunRowDetailId"
                    INNER JOIN {{tables.SweepRun}} AS run
                        ON run."SweepId" = detail."SweepId"
                    WHERE status."State" = {0} AND status."NextAttemptAt" <= {2}
                      AND (
                          status."DispatchPhase" = {3}
                          OR (run."SettledAt" IS NOT NULL AND run."Status" <> {4})
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM {{tables.SweepRowHandlerStatus}} AS blocker
                          WHERE blocker."SweepRunRowDetailId" = status."SweepRunRowDetailId"
                            AND blocker."Id" < status."Id"
                            AND blocker."State" IN ({0}, {1})
                      )
                    ORDER BY status."NextAttemptAt", status."Id"
                    FOR UPDATE OF status SKIP LOCKED
                    LIMIT {5}
                ),
                claimed AS (
                    UPDATE {{tables.SweepRowHandlerStatus}} AS status
                    SET "State" = {1}, "ClaimedAt" = {2}, "ClaimToken" = {6}, "Attempt" = status."Attempt" + 1
                    FROM due
                    WHERE status."Id" = due."Id"
                    RETURNING status."Id", status."SweepRunRowDetailId", status."HandlerType", status."Attempt", status."ClaimedAt", status."ClaimToken"
                )
                SELECT
                    claimed."Id" AS "StatusId",
                    detail."Id" AS "RowDetailId",
                    claimed."HandlerType",
                    claimed."Attempt",
                    claimed."ClaimedAt",
                    claimed."ClaimToken",
                    detail."SweepId",
                    detail."At",
                    detail."RetentionEntityId",
                    detail."EntityType",
                    detail."RecordId",
                    detail."Category",
                    detail."Strategy",
                    detail."TenantId",
                    detail."CapturedPayload"
                FROM claimed
                INNER JOIN {{tables.SweepRunRowDetail}} AS detail
                    ON detail."Id" = claimed."SweepRunRowDetailId"
                ORDER BY claimed."Id"
                """,
                Pending,
                InFlight,
                now,
                (int)RowHandlerDispatchPhase.Immediate,
                (int)SweepRunStatus.Started,
                limit,
                claimToken
            )
            .ToListAsync(ct);

        if (claimed.Count > 0)
        {
            // Taken while the claimed rows are still row-locked by this transaction, so no
            // recovery can see them in flight without their delivery locks held.
            await RetentionRunAdvisoryLock.AcquireAllAsync(
                leaseDb.Database.GetDbConnection(),
                transaction.GetDbTransaction(),
                [.. claimed.Select(row => RetentionRunAdvisoryLock.RowHandlerKeyFor(storeTables.SweepRowHandlerStatus, row.StatusId))],
                ct
            );
        }

        await transaction.CommitAsync(ct);
        return claimed;
    }

    /// <summary>
    /// Records a successful delivery, if the claim is still the delivery's own. Returns false
    /// when it was taken over or dead-lettered.
    /// </summary>
    internal static async Task<bool> SettleSucceededAsync(DbContext db, ClaimedHandlerRow claimed, CancellationToken ct) =>
        await UpdateOwnedAsync(
            db,
            claimed.StatusId,
            claimed.ClaimToken,
            """
            "State" = {0}, "CompletedAt" = {1}, "ClaimedAt" = NULL, "ClaimToken" = NULL, "LastError" = NULL
            """,
            [Succeeded, DateTimeOffset.UtcNow],
            ct
        ) == 1;

    /// <summary>
    /// Puts a failed delivery back in the queue until <paramref name="nextAttemptAt"/>, if the
    /// claim is still the delivery's own. Returns false when it was taken over or dead-lettered.
    /// </summary>
    internal static async Task<bool> SettleForRetryAsync(
        DbContext db,
        ClaimedHandlerRow claimed,
        DateTimeOffset nextAttemptAt,
        string lastError,
        CancellationToken ct
    ) =>
        await UpdateOwnedAsync(
            db,
            claimed.StatusId,
            claimed.ClaimToken,
            """
            "State" = {0}, "NextAttemptAt" = {1}, "ClaimedAt" = NULL, "ClaimToken" = NULL, "LastError" = {2}
            """,
            [Pending, nextAttemptAt, lastError],
            ct
        ) == 1;

    /// <summary>
    /// Dead-letters this handler row and every later handler for the same row detail, in one
    /// transaction, if the claim is still the delivery's own: handlers for one row run in
    /// order, so later ones can never run. Returns false, changing nothing, when the claim was
    /// taken over or dead-lettered.
    /// </summary>
    internal static async Task<bool> DeadLetterAsync(
        DbContext db,
        ClaimedHandlerRow claimed,
        string lastError,
        CancellationToken ct
    )
    {
        var now = DateTimeOffset.UtcNow;
        await using var transaction = await db.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        var settled = await UpdateOwnedAsync(
            db,
            claimed.StatusId,
            claimed.ClaimToken,
            DeadLetterAssignments,
            [DeadLettered, now, lastError],
            ct
        );
        if (settled != 1)
        {
            return false;
        }

        await DeadLetterDependentsAsync(db, claimed.RowDetailId, claimed.StatusId, now, ct);
        await transaction.CommitAsync(ct);
        return true;
    }

    private static Task DeadLetterDependentsAsync(
        DbContext db,
        long rowDetailId,
        long statusId,
        DateTimeOffset now,
        CancellationToken ct
    ) =>
        db.Database.ExecuteSqlRawAsync(
            $$"""
            UPDATE {{CohortStoreTables.FromModel(db.Model).Quoted().SweepRowHandlerStatus}}
            SET {{DeadLetterAssignments}}
            WHERE "SweepRunRowDetailId" = {3} AND "Id" > {4} AND "State" IN ({5}, {6})
            """,
            [DeadLettered, now, DependentDeadLetterReason, rowDetailId, statusId, Pending, InFlight],
            ct
        );

    /// <summary>
    /// Updates this handler row only while this delivery still owns its claim: a reclaim
    /// replaces the token, so a stale delivery matches nothing. Raw SQL rather
    /// than ExecuteUpdateAsync: Cohort builds against EF Core 9, and EF Core 10 replaced the
    /// setter types, so a compiled ExecuteUpdateAsync call fails to load on EF Core 10 hosts.
    /// </summary>
    private static Task<int> UpdateOwnedAsync(
        DbContext db,
        long statusId,
        Guid claimToken,
        string assignments,
        object[] values,
        CancellationToken ct
    )
    {
        var n = values.Length;
        return db.Database.ExecuteSqlRawAsync(
            $$"""
            UPDATE {{CohortStoreTables.FromModel(db.Model).Quoted().SweepRowHandlerStatus}}
            SET {{assignments}}
            WHERE "Id" = {{{n}}} AND "State" = {{{n + 1}}} AND "ClaimToken" = {{{n + 2}}}
            """,
            [.. values, statusId, InFlight, claimToken],
            ct
        );
    }

    private sealed class RemainingWork
    {
        public long InFlight { get; init; }
        public long Pending { get; init; }
        public long DeadLetteredRequeueable { get; init; }
    }
}

/// <summary>What <see cref="RowHandlerQueueSql.TryReclaimAsync"/> did with an expired claim.</summary>
internal enum ReclaimOutcome
{
    /// <summary>
    /// The claim was settled, reclaimed, or is locked by another recovery.
    /// </summary>
    NoLongerClaimed,

    /// <summary>
    /// Its dispatcher still holds the delivery lock: a handler is still running it.
    /// </summary>
    OwnerStillRunning,

    /// <summary>Back in the queue; the next claim counts the attempt.</summary>
    Requeued,

    /// <summary>
    /// It used its last attempt, so it and the row's later handlers are dead-lettered.
    /// </summary>
    DeadLettered,
}

// Materialized by SqlQueryRaw; property names match the recovery query's column names.
internal sealed class ExpiredClaim
{
    public long StatusId { get; init; }
    public long RowDetailId { get; init; }
    public int Attempt { get; init; }
    public Guid ClaimToken { get; init; }
    public DateTimeOffset ClaimedAt { get; init; }
    public Guid SweepId { get; init; }
}

// Materialized by SqlQueryRaw; property names match the claim query's column names.
internal sealed class ClaimedHandlerRow
{
    public long StatusId { get; init; }
    public long RowDetailId { get; init; }
    public string HandlerType { get; init; } = "";
    public int Attempt { get; init; }
    public DateTimeOffset ClaimedAt { get; init; }
    public Guid ClaimToken { get; init; }
    public Guid SweepId { get; init; }
    public DateTimeOffset At { get; init; }
    public Guid RetentionEntityId { get; init; }
    public string EntityType { get; init; } = "";
    public string RecordId { get; init; } = "";
    public string Category { get; init; } = "";
    public Strategy Strategy { get; init; }
    public Guid TenantId { get; init; }
    public string? CapturedPayload { get; init; }
}
