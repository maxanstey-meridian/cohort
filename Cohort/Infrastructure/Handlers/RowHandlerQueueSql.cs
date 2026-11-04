using System.Data;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Handlers;

// The only interpolations in this file's raw SQL are Cohort's own table names, quoted from
// the EF model; every value is a named parameter.

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

    // Values: @state, @at (the completion time), @lastError.
    private const string DeadLetterAssignments =
        """
        "State" = @state, "CompletedAt" = @at, "ClaimedAt" = NULL, "ClaimToken" = NULL, "LastError" = @lastError
        """;

    /// <summary>
    /// Q1: the queue's remaining work: claims in flight, pending rows, and dead-lettered rows
    /// that still hold a snapshot to requeue. Raw SQL, not LINQ: EF emits an unqualified COUNT
    /// that a search_path decoy can shadow.
    /// </summary>
    internal static SweepStatement RemainingWorkStatement(CohortStoreTables tables) =>
        CohortStatements.Statement(
            "remaining-work",
            $"""
            SELECT
                pg_catalog.count(*) FILTER (WHERE status."State" = @inFlight) AS "InFlight",
                pg_catalog.count(*) FILTER (WHERE status."State" = @pending) AS "Pending",
                pg_catalog.count(*) FILTER (
                    WHERE status."State" = @deadLettered AND detail."CapturedPayload" IS NOT NULL
                ) AS "DeadLetteredRequeueable"
            FROM {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)} AS status
            INNER JOIN {PostgreSqlIdentifier.Format(tables.SweepRunRowDetail)} AS detail
                ON detail."Id" = status."SweepRunRowDetailId"
            """,
            new SqlParams { ["inFlight"] = InFlight, ["pending"] = Pending, ["deadLettered"] = DeadLettered },
            tables.SweepRowHandlerStatus,
            tables.SweepRunRowDetail
        );

    /// <summary>
    /// Counts the queue's remaining work. A composed <c>SingleAsync</c> would make EF wrap the
    /// statement, so it is read as a list and its one row taken.
    /// </summary>
    internal static async Task<RowDispatcherFlushResult> CountRemainingAsync(DbContext db, CancellationToken ct)
    {
        var statement = RemainingWorkStatement(CohortStoreTables.FromModel(db.Model));
        var remaining = (
            await db.Database
                .SqlQueryRaw<RemainingWork>(
                    statement.Sql,
                    statement.Parameters.ToDbParameters(db.Database.GetDbConnection())
                )
                .ToListAsync(ct)
        ).Single();
        return new RowDispatcherFlushResult(
            remaining.InFlight,
            remaining.Pending,
            remaining.DeadLetteredRequeueable
        );
    }

    /// <summary>
    /// Q2: requeues every dead-lettered handler of each row that still holds its snapshot,
    /// optionally only rows of <paramref name="sweepId"/> with a dead-lettered
    /// <paramref name="handlerId"/>. One text covers all four combinations: the two booleans
    /// switch each filter off.
    /// </summary>
    internal static SweepStatement RequeueDeadLetteredStatement(
        CohortStoreTables tables,
        Guid? sweepId,
        Guid? handlerId,
        DateTimeOffset now
    )
    {
        var detail = PostgreSqlIdentifier.Format(tables.SweepRunRowDetail);
        var status = PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus);
        return CohortStatements.Statement(
            "requeue-dead-lettered",
            $"""
            WITH target AS (
                SELECT detail."Id"
                FROM {detail} AS detail
                WHERE detail."CapturedPayload" IS NOT NULL
                  AND (@anySweep OR detail."SweepId" = @sweepId)
                  AND EXISTS (
                      SELECT 1 FROM {status} AS dead
                      WHERE dead."SweepRunRowDetailId" = detail."Id"
                        AND dead."State" = @deadLettered
                        AND (@anyHandler OR dead."HandlerType" = @handlerType)
                  )
                FOR UPDATE OF detail
            )
            UPDATE {status} AS status
            SET "State" = @pending, "NextAttemptAt" = @now, "CompletedAt" = NULL
            FROM target
            WHERE status."SweepRunRowDetailId" = target."Id" AND status."State" = @deadLettered
            """,
            new SqlParams
            {
                ["anySweep"] = sweepId is null,
                ["sweepId"] = sweepId ?? Guid.Empty,
                ["anyHandler"] = handlerId is null,
                ["handlerType"] = handlerId?.ToString("D") ?? "",
                ["deadLettered"] = DeadLettered,
                ["pending"] = Pending,
                ["now"] = now,
            },
            tables.SweepRunRowDetail,
            tables.SweepRowHandlerStatus
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
    ) =>
        ExecuteAsync(
            db,
            RequeueDeadLetteredStatement(CohortStoreTables.FromModel(db.Model), sweepId, handlerId, now),
            ct
        );

    /// <summary>Q3: clears every captured snapshot taken before <paramref name="cutoff"/>.</summary>
    internal static SweepStatement ScrubExpiredPayloadsStatement(CohortStoreTables tables, DateTimeOffset cutoff) =>
        CohortStatements.Statement(
            "payload-scrub",
            $"""
            UPDATE {PostgreSqlIdentifier.Format(tables.SweepRunRowDetail)}
            SET "CapturedPayload" = NULL
            WHERE "CapturedPayload" IS NOT NULL AND "At" < @cutoff
            """,
            new SqlParams { ["cutoff"] = cutoff },
            tables.SweepRunRowDetail
        );

    /// <summary>Clears every captured snapshot taken before <paramref name="cutoff"/>.</summary>
    internal static Task<int> ScrubExpiredPayloadsAsync(DbContext db, DateTimeOffset cutoff, CancellationToken ct) =>
        ExecuteAsync(db, ScrubExpiredPayloadsStatement(CohortStoreTables.FromModel(db.Model), cutoff), ct);

    /// <summary>
    /// Q4: clears a row detail's captured snapshot once every handler for that row has
    /// succeeded.
    /// </summary>
    internal static SweepStatement ClearDeliveredPayloadStatement(CohortStoreTables tables, long rowDetailId) =>
        CohortStatements.Statement(
            "delivered-payload-clear",
            $"""
            UPDATE {PostgreSqlIdentifier.Format(tables.SweepRunRowDetail)} AS detail
            SET "CapturedPayload" = NULL
            WHERE detail."Id" = @rowDetailId
              AND detail."CapturedPayload" IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)} AS status
                  WHERE status."SweepRunRowDetailId" = detail."Id"
                    AND status."State" <> @succeeded
              )
            """,
            new SqlParams { ["rowDetailId"] = rowDetailId, ["succeeded"] = Succeeded },
            tables.SweepRunRowDetail,
            tables.SweepRowHandlerStatus
        );

    /// <summary>
    /// Clears a row detail's captured snapshot once every handler for that row has succeeded.
    /// </summary>
    internal static Task<int> ClearDeliveredPayloadAsync(DbContext db, long rowDetailId) =>
        ExecuteAsync(
            db,
            ClearDeliveredPayloadStatement(CohortStoreTables.FromModel(db.Model), rowDetailId),
            CancellationToken.None
        );

    /// <summary>
    /// Q5: one page of claims held since <paramref name="leaseCutoff"/> or earlier, in
    /// (<c>ClaimedAt</c>, <c>Id</c>) order after <paramref name="after"/> (from the start when
    /// null).
    /// </summary>
    internal static SweepStatement ExpiredClaimsStatement(
        CohortStoreTables tables,
        DateTimeOffset leaseCutoff,
        ExpiredClaim? after,
        int batchSize
    ) =>
        CohortStatements.Statement(
            "expired-claims",
            $"""
            SELECT
                status."Id" AS "StatusId",
                status."SweepRunRowDetailId" AS "RowDetailId",
                status."Attempt",
                status."ClaimToken",
                status."ClaimedAt",
                detail."SweepId"
            FROM {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)} AS status
            INNER JOIN {PostgreSqlIdentifier.Format(tables.SweepRunRowDetail)} AS detail
                ON detail."Id" = status."SweepRunRowDetailId"
            WHERE status."State" = @inFlight AND status."ClaimedAt" <= @leaseCutoff
              AND (@fromStart OR (status."ClaimedAt", status."Id") > (@afterClaimedAt, @afterStatusId))
            ORDER BY status."ClaimedAt", status."Id"
            LIMIT @batchSize
            """,
            new SqlParams
            {
                ["inFlight"] = InFlight,
                ["leaseCutoff"] = leaseCutoff,
                ["batchSize"] = batchSize,
                ["fromStart"] = after is null,
                ["afterClaimedAt"] = after?.ClaimedAt ?? leaseCutoff,
                ["afterStatusId"] = after?.StatusId ?? 0L,
            },
            tables.SweepRowHandlerStatus,
            tables.SweepRunRowDetail
        );

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
        var statement = ExpiredClaimsStatement(CohortStoreTables.FromModel(db.Model), leaseCutoff, after, batchSize);
        return db.Database
            .SqlQueryRaw<ExpiredClaim>(
                statement.Sql,
                statement.Parameters.ToDbParameters(db.Database.GetDbConnection())
            )
            .ToListAsync(ct);
    }

    /// <summary>
    /// Q6: locks one status row if it is still in flight under <paramref name="claimToken"/>,
    /// skipping one a claim or another recovery holds.
    /// </summary>
    internal static SweepStatement ReclaimLockStatement(CohortStoreTables tables, long statusId, Guid claimToken) =>
        CohortStatements.Statement(
            "reclaim-lock",
            $"""
            SELECT "Id" AS "Value" FROM {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)}
            WHERE "Id" = @statusId AND "State" = @inFlight AND "ClaimToken" = @claimToken
            FOR UPDATE SKIP LOCKED
            """,
            new SqlParams { ["statusId"] = statusId, ["inFlight"] = InFlight, ["claimToken"] = claimToken },
            tables.SweepRowHandlerStatus
        );

    /// <summary>
    /// Reclaims one expired claim in one transaction. Locks the status row (skipping one a
    /// claim or another recovery holds), then tries the delivery lock on the context's
    /// connection, which the caller has opened. Neither waits, so recovery can't deadlock with
    /// a claim, which takes the row lock first and then waits on the delivery lock. A claim
    /// with attempts left goes back to the queue; one on its last attempt is dead-lettered
    /// with the row's later handlers. The delivery lock is released before this returns.
    /// </summary>
    internal static async Task<ReclaimOutcome> TryReclaimAsync(
        DbContext db,
        ExpiredClaim claim,
        int maxAttempts,
        ILogger logger,
        CancellationToken ct
    )
    {
        var connection = db.Database.GetDbConnection();
        var now = DateTimeOffset.UtcNow;
        var tables = CohortStoreTables.FromModel(db.Model);
        var key = RetentionRunAdvisoryLock.RowHandlerKeyFor(tables.SweepRowHandlerStatus, claim.StatusId);
        var locked = false;
        try
        {
            await using var transaction = await db.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
            var reclaimLock = ReclaimLockStatement(tables, claim.StatusId, claim.ClaimToken);
            var stillClaimed = await db.Database
                .SqlQueryRaw<long>(reclaimLock.Sql, reclaimLock.Parameters.ToDbParameters(connection))
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
                    OwnedUpdate.DeadLetter,
                    claim.StatusId,
                    claim.ClaimToken,
                    now,
                    ExhaustedClaimReason,
                    ct
                );
                await DeadLetterDependentsAsync(db, claim.RowDetailId, claim.StatusId, now, ct);
                outcome = ReclaimOutcome.DeadLettered;
            }
            else
            {
                await UpdateOwnedAsync(db, OwnedUpdate.Requeue, claim.StatusId, claim.ClaimToken, now, null, ct);
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
        ExecuteAsync(db, RequeueInterruptedStatement(CohortStoreTables.FromModel(db.Model), claimToken), ct);

    /// <summary>
    /// T5: puts back in the queue every claim <paramref name="claimToken"/> still holds.
    /// </summary>
    internal static SweepStatement RequeueInterruptedStatement(CohortStoreTables tables, Guid claimToken) =>
        CohortStatements.Statement(
            "requeue-interrupted",
            $"""
            UPDATE {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)}
            SET "State" = @pending, "ClaimedAt" = NULL, "ClaimToken" = NULL
            WHERE "ClaimToken" = @claimToken AND "State" = @inFlight
            """,
            new SqlParams { ["pending"] = Pending, ["claimToken"] = claimToken, ["inFlight"] = InFlight },
            tables.SweepRowHandlerStatus
        );

    /// <summary>
    /// T2: claims up to <paramref name="limit"/> due rows under <paramref name="claimToken"/> and
    /// returns them with their row details, locking the due rows with <c>SKIP LOCKED</c>.
    /// </summary>
    internal static SweepStatement ClaimStatement(
        CohortStoreTables tables,
        Guid claimToken,
        int limit,
        DateTimeOffset now
    )
    {
        var status = PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus);
        var detail = PostgreSqlIdentifier.Format(tables.SweepRunRowDetail);
        return CohortStatements.Statement(
            "claim-handlers",
            $"""
            WITH due AS (
                SELECT status."Id"
                FROM {status} AS status
                INNER JOIN {detail} AS detail
                    ON detail."Id" = status."SweepRunRowDetailId"
                INNER JOIN {PostgreSqlIdentifier.Format(tables.SweepRun)} AS run
                    ON run."SweepId" = detail."SweepId"
                WHERE status."State" = @pending AND status."NextAttemptAt" <= @now
                  AND (
                      status."DispatchPhase" = @immediate
                      OR (run."SettledAt" IS NOT NULL AND run."Status" <> @started)
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {status} AS blocker
                      WHERE blocker."SweepRunRowDetailId" = status."SweepRunRowDetailId"
                        AND blocker."Id" < status."Id"
                        AND blocker."State" IN (@pending, @inFlight)
                  )
                ORDER BY status."NextAttemptAt", status."Id"
                FOR UPDATE OF status SKIP LOCKED
                LIMIT @limit
            ),
            claimed AS (
                UPDATE {status} AS status
                SET "State" = @inFlight, "ClaimedAt" = @now, "ClaimToken" = @claimToken, "Attempt" = status."Attempt" + 1
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
            INNER JOIN {detail} AS detail
                ON detail."Id" = claimed."SweepRunRowDetailId"
            ORDER BY claimed."Id"
            """,
            new SqlParams
            {
                ["pending"] = Pending,
                ["inFlight"] = InFlight,
                ["now"] = now,
                ["immediate"] = (int)RowHandlerDispatchPhase.Immediate,
                ["started"] = (int)SweepRunStatus.Started,
                ["limit"] = limit,
                ["claimToken"] = claimToken,
            },
            tables.SweepRowHandlerStatus,
            tables.SweepRunRowDetail,
            tables.SweepRun
        );
    }

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
        var tables = CohortStoreTables.FromModel(leaseDb.Model);
        var statement = ClaimStatement(tables, claimToken, limit, DateTimeOffset.UtcNow);
        await using var transaction = await leaseDb.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);

        var claimed = await leaseDb.Database
            .SqlQueryRaw<ClaimedHandlerRow>(
                statement.Sql,
                statement.Parameters.ToDbParameters(leaseDb.Database.GetDbConnection())
            )
            .ToListAsync(ct);

        if (claimed.Count > 0)
        {
            // Taken while the claimed rows are still row-locked by this transaction, so no
            // recovery can see them in flight without their delivery locks held.
            await RetentionRunAdvisoryLock.AcquireAllAsync(
                leaseDb.Database.GetDbConnection(),
                transaction.GetDbTransaction(),
                [.. claimed.Select(row => RetentionRunAdvisoryLock.RowHandlerKeyFor(tables.SweepRowHandlerStatus, row.StatusId))],
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
            OwnedUpdate.Succeeded,
            claimed.StatusId,
            claimed.ClaimToken,
            DateTimeOffset.UtcNow,
            null,
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
            OwnedUpdate.Retry,
            claimed.StatusId,
            claimed.ClaimToken,
            nextAttemptAt,
            lastError,
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
            OwnedUpdate.DeadLetter,
            claimed.StatusId,
            claimed.ClaimToken,
            now,
            lastError,
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
        ExecuteAsync(
            db,
            DeadLetterDependentsStatement(
                CohortStoreTables.FromModel(db.Model),
                rowDetailId,
                statusId,
                now,
                DependentDeadLetterReason
            ),
            ct
        );

    /// <summary>
    /// T6: dead-letters every pending or in-flight handler of the row detail after
    /// <paramref name="statusId"/>, which can never run now, with <paramref name="reason"/> as
    /// their error.
    /// </summary>
    internal static SweepStatement DeadLetterDependentsStatement(
        CohortStoreTables tables,
        long rowDetailId,
        long statusId,
        DateTimeOffset now,
        string reason
    ) =>
        CohortStatements.Statement(
            "dead-letter-dependents",
            $"""
            UPDATE {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)}
            SET {DeadLetterAssignments}
            WHERE "SweepRunRowDetailId" = @rowDetailId AND "Id" > @statusId AND "State" IN (@pending, @inFlight)
            """,
            new SqlParams
            {
                ["state"] = DeadLettered,
                ["at"] = now,
                ["lastError"] = reason,
                ["rowDetailId"] = rowDetailId,
                ["statusId"] = statusId,
                ["pending"] = Pending,
                ["inFlight"] = InFlight,
            },
            tables.SweepRowHandlerStatus
        );

    /// <summary>
    /// T4: updates this handler row only while the delivery still owns its claim: a reclaim
    /// replaces the token, so a stale delivery matches nothing. <paramref name="at"/> is the
    /// completion time, or the next attempt's for a retry or a requeue; <paramref name="lastError"/>
    /// is only a retry's or a dead letter's. Raw SQL rather than
    /// ExecuteUpdateAsync: Cohort builds against EF Core 9, and EF Core 10 replaced the setter
    /// types, so a compiled ExecuteUpdateAsync call fails to load on EF Core 10 hosts.
    /// </summary>
    internal static SweepStatement OwnedUpdateStatement(
        CohortStoreTables tables,
        OwnedUpdate form,
        long statusId,
        Guid claimToken,
        DateTimeOffset at,
        string? lastError
    )
    {
        var (name, assignments, state) = form switch
        {
            OwnedUpdate.Succeeded => (
                "handler-status-succeeded",
                """
                "State" = @state, "CompletedAt" = @at, "ClaimedAt" = NULL, "ClaimToken" = NULL, "LastError" = NULL
                """,
                Succeeded
            ),
            OwnedUpdate.Retry => (
                "handler-status-retry",
                """
                "State" = @state, "NextAttemptAt" = @at, "ClaimedAt" = NULL, "ClaimToken" = NULL, "LastError" = @lastError
                """,
                Pending
            ),
            OwnedUpdate.DeadLetter => ("handler-status-dead-letter", DeadLetterAssignments, DeadLettered),
            OwnedUpdate.Requeue => (
                "handler-status-requeue",
                """
                "State" = @state, "NextAttemptAt" = @at, "ClaimedAt" = NULL, "ClaimToken" = NULL
                """,
                Pending
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(form)),
        };
        var parameters = new SqlParams { ["state"] = state, ["at"] = at };
        if (form is OwnedUpdate.Retry or OwnedUpdate.DeadLetter)
        {
            parameters["lastError"] = lastError;
        }

        parameters["statusId"] = statusId;
        parameters["inFlight"] = InFlight;
        parameters["claimToken"] = claimToken;
        return CohortStatements.Statement(
            name,
            $"""
            UPDATE {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)}
            SET {assignments}
            WHERE "Id" = @statusId AND "State" = @inFlight AND "ClaimToken" = @claimToken
            """,
            parameters,
            tables.SweepRowHandlerStatus
        );
    }

    private static Task<int> UpdateOwnedAsync(
        DbContext db,
        OwnedUpdate form,
        long statusId,
        Guid claimToken,
        DateTimeOffset at,
        string? lastError,
        CancellationToken ct
    ) =>
        ExecuteAsync(
            db,
            OwnedUpdateStatement(CohortStoreTables.FromModel(db.Model), form, statusId, claimToken, at, lastError),
            ct
        );

    private static Task<int> ExecuteAsync(DbContext db, SweepStatement statement, CancellationToken ct) =>
        db.Database.ExecuteSqlRawAsync(
            statement.Sql,
            statement.Parameters.ToDbParameters(db.Database.GetDbConnection()),
            ct
        );

    private sealed class RemainingWork
    {
        public long InFlight { get; init; }
        public long Pending { get; init; }
        public long DeadLetteredRequeueable { get; init; }
    }
}

/// <summary>
/// Which assignments <see cref="RowHandlerQueueSql.OwnedUpdateStatement"/> makes to a handler row
/// whose claim the caller still owns.
/// </summary>
internal enum OwnedUpdate
{
    /// <summary>The delivery succeeded.</summary>
    Succeeded,

    /// <summary>The delivery failed and goes back in the queue until its next attempt.</summary>
    Retry,

    /// <summary>The delivery failed for good.</summary>
    DeadLetter,

    /// <summary>An expired claim with attempts left goes back in the queue.</summary>
    Requeue,
}

/// <summary>
/// What <see cref="RowHandlerQueueSql.TryReclaimAsync"/> did with an expired claim.
/// </summary>
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
