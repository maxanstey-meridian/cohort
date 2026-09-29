using System.Collections.Concurrent;
using System.Reflection;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Handlers;
using Cohort.Infrastructure.Sweep;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cohort.Hosting;

// The only interpolations in this file's raw SQL are Cohort's own table names, quoted from
// the EF model; every value is a positional parameter.
#pragma warning disable EF1002

/// <summary>
/// Durable after-commit delivery of <see cref="IRetentionHandler{TEntity}.OnAfterAsync"/>.
/// Each poll claims at most <c>MaxParallelism</c> rows (capped by <c>BatchSize</c>) so no
/// claim waits unheartbeated behind another row, renews each claim before invoking its
/// handler, and heartbeats it while the handler runs.
/// </summary>
internal sealed class RetentionRowDispatcher(
    IServiceScopeFactory scopeFactory,
    CohortOptionsSnapshot options,
    ILogger<RetentionRowDispatcher> logger
) : BackgroundService, IRetentionRowDispatcher
{
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan PayloadScrubInterval = TimeSpan.FromHours(1);

    // Keep persisted retry timestamps inside both DateTimeOffset and PostgreSQL
    // timestamptz ranges, with headroom for provider conversions at the boundary.
    internal static readonly DateTimeOffset RetryScheduleUpperBound =
        DateTimeOffset.MaxValue.AddYears(-1);

    private const int Pending = (int)SweepRowHandlerDispatchState.Pending;
    private const int InFlight = (int)SweepRowHandlerDispatchState.InFlight;
    private const int DeadLettered = (int)SweepRowHandlerDispatchState.DeadLettered;

    private DateTimeOffset lastPayloadScrubAt = DateTimeOffset.MinValue;

    public async Task<RowDispatcherFlushResult> FlushAsync(CancellationToken ct = default)
    {
        await ValidateReadinessAsync(ct);
        await RecoverAbandonedRunsAsync(DateTimeOffset.UtcNow, ct);
        await ScrubExpiredPayloadsAsync(ct);
        await DrainQueueAsync(RetryScheduleUpperBound, ct);

        // Pending rows left here were not dispatchable (deferred phase with an unsettled
        // sweep, or queued behind a sibling); in-flight rows are held under a live lease.
        // Raw SQL, not LINQ: EF emits an unqualified COUNT that a search_path decoy can shadow.
        return await WithDbAsync(
            async db =>
            {
                var remaining = await db.Database
                    .SqlQueryRaw<RemainingWork>(
                        $$"""
                        SELECT
                            pg_catalog.count(*) FILTER (WHERE "State" = {0}) AS "InFlight",
                            pg_catalog.count(*) FILTER (WHERE "State" = {1}) AS "Pending"
                        FROM {{Table(db).SweepRowHandlerStatus}}
                        """,
                        InFlight,
                        Pending
                    )
                    .SingleAsync(ct);
                return new RowDispatcherFlushResult(remaining.InFlight, remaining.Pending);
            },
            ct
        );
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ValidateReadinessAsync(stoppingToken);
                var now = DateTimeOffset.UtcNow;
                await RecoverAbandonedRunsAsync(now, stoppingToken);
                if (now - lastPayloadScrubAt >= PayloadScrubInterval)
                {
                    lastPayloadScrubAt = now;
                    await ScrubExpiredPayloadsAsync(stoppingToken);
                }

                await DrainQueueAsync(now, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Cohort row handler dispatcher iteration failed.");
            }

            try
            {
                await OperationalTime.DelayAsync(
                    options.Current.RowHandlerDispatch.PollInterval,
                    stoppingToken
                );
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Backstop for captured snapshots that outlive <c>PayloadRetention</c> even while
    /// handler work is still queued: the personal data must not outlive its retention.
    /// </summary>
    private Task ScrubExpiredPayloadsAsync(CancellationToken ct)
    {
        var cutoff = OperationalTime.SubtractSaturating(
            DateTimeOffset.UtcNow,
            options.Current.RowHandlerDispatch.PayloadRetention
        );
        return WithDbAsync(
            db =>
                db.Database.ExecuteSqlRawAsync(
                    $$"""
                    UPDATE {{Table(db).SweepRunRowDetail}}
                    SET "CapturedPayload" = NULL
                    WHERE "CapturedPayload" IS NOT NULL AND "At" < {0}
                    """,
                    [cutoff],
                    ct
                ),
            ct
        );
    }

    /// <summary>
    /// Clears a row detail's captured snapshot once every handler for that row is
    /// terminal. Snapshots exist only to feed OnAfterAsync; keeping them longer retains
    /// exactly the personal data the sweep was meant to remove.
    /// </summary>
    private Task ClearSettledPayloadAsync(long rowDetailId)
    {
        return WithDbAsync(
            db =>
            {
                var tables = Table(db);
                return db.Database.ExecuteSqlRawAsync(
                    $$"""
                    UPDATE {{tables.SweepRunRowDetail}} AS detail
                    SET "CapturedPayload" = NULL
                    WHERE detail."Id" = {0}
                      AND detail."CapturedPayload" IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM {{tables.SweepRowHandlerStatus}} AS status
                          WHERE status."SweepRunRowDetailId" = detail."Id"
                            AND status."State" IN ({1}, {2})
                      )
                    """,
                    [rowDetailId, Pending, InFlight]
                );
            },
            CancellationToken.None
        );
    }

    /// <summary>
    /// Marks runs left Started past <c>SweepSettleTimeout</c> as failed, but only after
    /// taking their run lock proves no live process still owns them.
    /// </summary>
    private Task RecoverAbandonedRunsAsync(DateTimeOffset now, CancellationToken ct)
    {
        var cutoff = OperationalTime.SubtractSaturating(
            now,
            options.Current.RowHandlerDispatch.SweepSettleTimeout
        );
        return WithDbAsync(
            async db =>
            {
                var sweepRun = Table(db).SweepRun;
                var staleSweepIds = await db.Database
                    .SqlQueryRaw<Guid>(
                        $$"""
                        SELECT "SweepId" AS "Value" FROM {{sweepRun}}
                        WHERE "Status" = {0} AND "StartedAt" <= {1}
                        ORDER BY "StartedAt"
                        """,
                        (int)SweepRunStatus.Started,
                        cutoff
                    )
                    .ToListAsync(ct);
                if (staleSweepIds.Count == 0)
                {
                    return;
                }

                // The run lock is session-scoped, so hold one connection throughout.
                await db.Database.OpenConnectionAsync(ct);
                try
                {
                    var connection = db.Database.GetDbConnection();
                    foreach (var sweepId in staleSweepIds)
                    {
                        if (!await RetentionRunAdvisoryLock.TryAcquireAsync(connection, RetentionRunAdvisoryLock.KeyFor(sweepId), ct))
                        {
                            continue;
                        }

                        try
                        {
                            var recovered = await db.Database.ExecuteSqlRawAsync(
                                $$"""
                                UPDATE {{sweepRun}}
                                SET "Status" = {0}, "SettledAt" = {1}, "Duration" = {1} - "StartedAt", "Error" = {2}
                                WHERE "SweepId" = {3} AND "Status" = {4}
                                """,
                                [
                                    (int)SweepRunStatus.Failed,
                                    now,
                                    "Run owner exited before writing a terminal audit event.",
                                    sweepId,
                                    (int)SweepRunStatus.Started,
                                ],
                                ct
                            );
                            if (recovered > 0)
                            {
                                logger.LogWarning(
                                    "Cohort recovered abandoned retention run {SweepId} as failed.",
                                    sweepId
                                );
                            }
                        }
                        finally
                        {
                            await ReleaseRunLockAsync(connection, sweepId);
                        }
                    }
                }
                finally
                {
                    await db.Database.CloseConnectionAsync();
                }
            },
            ct
        );
    }

    private async Task ReleaseRunLockAsync(System.Data.Common.DbConnection connection, Guid sweepId)
    {
        try
        {
            using var cleanup = new CancellationTokenSource(CleanupTimeout);
            await RetentionRunAdvisoryLock.ReleaseAsync(connection, RetentionRunAdvisoryLock.KeyFor(sweepId), cleanup.Token);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Cohort could not release the run lock for {SweepId}.", sweepId);
        }
    }

    private async Task DrainQueueAsync(DateTimeOffset dueCutoff, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            // One snapshot per claim: the lease cutoff, claim size, heartbeat and retry
            // policy for these rows all come from the same options.
            var dispatch = options.Current.RowHandlerDispatch;
            var claimToken = Guid.NewGuid();
            var claimed = await ClaimAsync(dispatch, claimToken, dueCutoff, ct);
            if (claimed.Count == 0)
            {
                return;
            }

            try
            {
                await Parallel.ForEachAsync(
                    claimed,
                    new ParallelOptions
                    {
                        CancellationToken = ct,
                        MaxDegreeOfParallelism = dispatch.MaxParallelism,
                    },
                    (row, rowCt) => ProcessClaimedRowAsync(row, dispatch, rowCt)
                );
            }
            catch (Exception)
            {
                try
                {
                    using var cleanup = new CancellationTokenSource(CleanupTimeout);
                    await WithDbAsync(
                        db =>
                            db.Set<SweepRowHandlerStatusEntity>()
                                .Where(s =>
                                    s.ClaimToken == claimToken
                                    && s.State == SweepRowHandlerDispatchState.InFlight
                                )
                                .ExecuteUpdateAsync(
                                    s => s
                                        .SetProperty(x => x.State, SweepRowHandlerDispatchState.Pending)
                                        .SetProperty(x => x.ClaimedAt, (DateTimeOffset?)null)
                                        .SetProperty(x => x.ClaimToken, (Guid?)null),
                                    cleanup.Token
                                ),
                        cleanup.Token
                    );
                }
                catch (Exception cleanupException)
                {
                    logger.LogWarning(
                        cleanupException,
                        "Cohort could not requeue owned row-handler claims after dispatch failed; claim leases will recover them."
                    );
                }
                throw;
            }
        }
    }

    private Task<List<ClaimedHandlerRow>> ClaimAsync(
        RowHandlerDispatchOptions dispatch,
        Guid claimToken,
        DateTimeOffset dueCutoff,
        CancellationToken ct
    )
    {
        return WithDbAsync(
            async db =>
            {
                var tables = Table(db);
                var now = DateTimeOffset.UtcNow;
                var leaseCutoff = OperationalTime.SubtractSaturating(now, dispatch.ClaimTimeout);
                await using var transaction = await db.Database.BeginTransactionAsync(ct);

                // Expired claims that already used their last attempt are dead-lettered
                // (with every later handler for the same row) instead of reclaimed.
                await db.Database.ExecuteSqlRawAsync(
                    $$"""
                    WITH exhausted AS (
                        SELECT "Id", "SweepRunRowDetailId"
                        FROM {{tables.SweepRowHandlerStatus}}
                        WHERE "State" = {1} AND "Attempt" >= {3} AND "ClaimedAt" <= {4}
                        FOR UPDATE SKIP LOCKED
                    ),
                    dead_lettered AS (
                        UPDATE {{tables.SweepRowHandlerStatus}} AS status
                        SET "State" = {2}, "ClaimedAt" = NULL, "ClaimToken" = NULL, "CompletedAt" = {5},
                            "LastError" = 'Handler claim expired after reaching RowHandlerDispatch:MaxAttempts.'
                        FROM exhausted
                        WHERE status."Id" = exhausted."Id"
                        RETURNING status."Id", status."SweepRunRowDetailId"
                    ),
                    dependent_dead_lettered AS (
                        UPDATE {{tables.SweepRowHandlerStatus}} AS dependent
                        SET "State" = {2}, "ClaimedAt" = NULL, "ClaimToken" = NULL, "CompletedAt" = {5},
                            "LastError" = 'Skipped because an earlier handler for the same row exhausted its claim attempts.'
                        FROM dead_lettered
                        WHERE dependent."SweepRunRowDetailId" = dead_lettered."SweepRunRowDetailId"
                          AND dependent."Id" > dead_lettered."Id"
                          AND dependent."State" IN ({0}, {1})
                        RETURNING dependent."Id", dependent."SweepRunRowDetailId"
                    ),
                    affected AS (
                        SELECT "Id", "SweepRunRowDetailId" FROM dead_lettered
                        UNION
                        SELECT "Id", "SweepRunRowDetailId" FROM dependent_dead_lettered
                    )
                    UPDATE {{tables.SweepRunRowDetail}} AS detail
                    SET "CapturedPayload" = NULL
                    WHERE detail."Id" IN (SELECT "SweepRunRowDetailId" FROM affected)
                      AND NOT EXISTS (
                          SELECT 1 FROM {{tables.SweepRowHandlerStatus}} AS unsettled
                          WHERE unsettled."SweepRunRowDetailId" = detail."Id"
                            AND unsettled."State" IN ({0}, {1})
                            AND unsettled."Id" NOT IN (SELECT "Id" FROM affected)
                      )
                    """,
                    [Pending, InFlight, DeadLettered, dispatch.MaxAttempts, leaseCutoff, now],
                    ct
                );

                // Due rows: pending past their backoff, or in flight under an expired lease
                // with attempts left. Deferred-phase rows wait for their run to settle, and
                // a row's handlers run one at a time in status order.
                var claimed = await db.Database
                    .SqlQueryRaw<ClaimedHandlerRow>(
                        $$"""
                        WITH due AS (
                            SELECT status."Id"
                            FROM {{tables.SweepRowHandlerStatus}} AS status
                            INNER JOIN {{tables.SweepRunRowDetail}} AS detail
                                ON detail."Id" = status."SweepRunRowDetailId"
                            INNER JOIN {{tables.SweepRun}} AS run
                                ON run."SweepId" = detail."SweepId"
                            WHERE (
                                  (status."State" = {0} AND status."NextAttemptAt" <= {2})
                                  OR (status."State" = {1} AND status."ClaimedAt" <= {3} AND status."Attempt" < {4})
                              )
                              AND (
                                  status."DispatchPhase" = {5}
                                  OR (run."SettledAt" IS NOT NULL AND run."Status" <> {6})
                              )
                              AND NOT EXISTS (
                                  SELECT 1 FROM {{tables.SweepRowHandlerStatus}} AS blocker
                                  WHERE blocker."SweepRunRowDetailId" = status."SweepRunRowDetailId"
                                    AND blocker."Id" < status."Id"
                                    AND blocker."State" IN ({0}, {1})
                              )
                            ORDER BY status."NextAttemptAt", status."Id"
                            FOR UPDATE OF status SKIP LOCKED
                            LIMIT {7}
                        ),
                        claimed AS (
                            UPDATE {{tables.SweepRowHandlerStatus}} AS status
                            SET "State" = {1}, "ClaimedAt" = {8}, "ClaimToken" = {9}, "Attempt" = status."Attempt" + 1
                            FROM due
                            WHERE status."Id" = due."Id"
                            RETURNING status."Id", status."SweepRunRowDetailId", status."HandlerType", status."Attempt", status."ClaimToken"
                        )
                        SELECT
                            claimed."Id" AS "StatusId",
                            detail."Id" AS "RowDetailId",
                            claimed."HandlerType",
                            claimed."Attempt",
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
                        dueCutoff,
                        leaseCutoff,
                        dispatch.MaxAttempts,
                        (int)RowHandlerDispatchPhase.Immediate,
                        (int)SweepRunStatus.Started,
                        Math.Min(dispatch.MaxParallelism, dispatch.BatchSize),
                        now,
                        claimToken
                    )
                    .ToListAsync(ct);

                await transaction.CommitAsync(ct);
                return claimed;
            },
            ct
        );
    }

    private async ValueTask ProcessClaimedRowAsync(
        ClaimedHandlerRow claimed,
        RowHandlerDispatchOptions dispatch,
        CancellationToken ct
    )
    {
        if (string.IsNullOrWhiteSpace(claimed.CapturedPayload))
        {
            // Without the snapshot (scrubbed by the PayloadRetention backstop) the handler
            // can never succeed, so dead-letter with the real reason instead of burning
            // the retry budget on a misleading deserialisation error.
            await MarkDeadLetteredAsync(
                claimed,
                "Captured row snapshot was scrubbed by the payload-retention backstop (RowHandlerDispatch:PayloadRetention) before this handler ran; the work can no longer complete. Increase PayloadRetention or drain handler work sooner.",
                ct
            );
            return;
        }

        // Ownership check: the claim may have been reclaimed or dead-lettered since it was
        // taken. Renewing it also restarts the lease for the handler.
        if (!await RenewClaimAsync(claimed, ct))
        {
            logger.LogInformation(
                "Cohort skipped row handler status {StatusId}: its claim is no longer owned by this dispatcher.",
                claimed.StatusId
            );
            return;
        }

        var handlerCompleted = false;
        using var handlerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var heartbeat = HeartbeatAsync(claimed, dispatch.ClaimTimeout / 3, handlerCts, heartbeatCts.Token);

        try
        {
            await using var scope = scopeFactory.CreateAsyncScope();
            var entityType = ResolveEntityType(
                claimed,
                scope.ServiceProvider.GetRequiredService<RetentionRegistry>()
            );
            var handlers = RetentionHandlerSupport.ResolveHandlers(scope.ServiceProvider, entityType);
            var claimedIdentity = RetentionTypeIdentity.Normalize(claimed.HandlerType);
            // Identity first; type-name fallback so rows queued before a handler gained
            // an explicit identity still resolve.
            var handler =
                handlers.FirstOrDefault(candidate =>
                    string.Equals(candidate.HandlerIdentity, claimedIdentity, StringComparison.Ordinal)
                )
                ?? handlers.FirstOrDefault(candidate =>
                    string.Equals(candidate.HandlerTypeName, claimedIdentity, StringComparison.Ordinal)
                );
            if (handler is null)
            {
                // Retrying cannot heal an unregistered identity: the handler was renamed
                // without an explicit identity, or removed.
                await MarkDeadLetteredAsync(
                    claimed,
                    "The queued retention row handler is not registered. If it was renamed, register it with an explicit identity so queued work survives renames.",
                    ct
                );
                await ClearSettledPayloadAsync(claimed.RowDetailId);
                return;
            }

            var snapshot = RetentionSnapshotSerializer.Deserialize(
                claimed.CapturedPayload,
                entityType,
                handlers.Select(resolved => resolved.Instance.GetType().Assembly)
            );
            await InvokeOnAfterAsync(entityType, handler.Instance, claimed, snapshot, handlerCts.Token);
            handlerCompleted = true;
            await StopHeartbeatAsync(heartbeat, heartbeatCts);
            // Captured outside the lambda: EF would otherwise emit an unqualified now(),
            // resolvable through the search_path and on the database clock.
            var completedAt = DateTimeOffset.UtcNow;
            await SettleAsync(
                claimed,
                s => s
                    .SetProperty(x => x.State, SweepRowHandlerDispatchState.Succeeded)
                    .SetProperty(x => x.CompletedAt, completedAt)
                    .SetProperty(x => x.ClaimedAt, (DateTimeOffset?)null)
                    .SetProperty(x => x.ClaimToken, (Guid?)null)
                    .SetProperty(x => x.LastError, (string?)null),
                CancellationToken.None
            );
            await ClearSettledPayloadAsync(claimed.RowDetailId);
        }
        catch (OperationCanceledException) when (!handlerCompleted && ct.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException) when (!handlerCompleted && handlerCts.IsCancellationRequested)
        {
            // The heartbeat lost the claim and cancelled the handler; surface why.
            await heartbeat;
            throw;
        }
        catch (RetentionRowDispatchClaimLostException)
        {
            throw;
        }
        catch (Exception ex)
        {
            await MarkFailureAsync(claimed, dispatch, ex, ct);
            // No-op while a sibling handler (or this one's retry) is still unsettled.
            await ClearSettledPayloadAsync(claimed.RowDetailId);
        }
        finally
        {
            await StopHeartbeatAsync(heartbeat, heartbeatCts);
        }
    }

    private static async Task StopHeartbeatAsync(Task heartbeat, CancellationTokenSource heartbeatCts)
    {
        heartbeatCts.Cancel();
        try
        {
            await heartbeat;
        }
        catch (OperationCanceledException) when (heartbeatCts.IsCancellationRequested) { }
    }

    private async Task HeartbeatAsync(
        ClaimedHandlerRow claimed,
        TimeSpan interval,
        CancellationTokenSource handlerCts,
        CancellationToken ct
    )
    {
        try
        {
            while (true)
            {
                await OperationalTime.DelayAsync(interval, ct);
                if (!await RenewClaimAsync(claimed, ct))
                {
                    throw new RetentionRowDispatchClaimLostException(claimed.StatusId);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            handlerCts.Cancel();
            throw ex as RetentionRowDispatchClaimLostException
                ?? new RetentionRowDispatchClaimLostException(claimed.StatusId, ex);
        }
    }

    private Task<bool> RenewClaimAsync(ClaimedHandlerRow claimed, CancellationToken ct)
    {
        // Leases are judged on the app clock, so renew on it too (and never via a bare now()).
        var claimedAt = DateTimeOffset.UtcNow;
        return WithDbAsync(
            async db =>
                await Owned(db, claimed)
                    .ExecuteUpdateAsync(s => s.SetProperty(x => x.ClaimedAt, claimedAt), ct)
                == 1,
            ct
        );
    }

    private async Task MarkFailureAsync(
        ClaimedHandlerRow claimed,
        RowHandlerDispatchOptions dispatch,
        Exception ex,
        CancellationToken ct
    )
    {
        var diagnostic = RetentionFailureDiagnostic.Create(ex);
        logger.LogError(
            ex.GetBaseException(),
            "Cohort row handler failed for sweep {SweepId}. Diagnostic {DiagnosticId}.",
            claimed.SweepId,
            diagnostic.DiagnosticIdText
        );

        if (claimed.Attempt >= dispatch.MaxAttempts)
        {
            await MarkDeadLetteredAsync(claimed, diagnostic.ToString(), ct);
            return;
        }

        var nextAttemptAt = CalculateNextAttemptAt(
            DateTimeOffset.UtcNow,
            dispatch.BaseBackoff,
            claimed.Attempt
        );
        await SettleAsync(
            claimed,
            s => s
                .SetProperty(x => x.State, SweepRowHandlerDispatchState.Pending)
                .SetProperty(x => x.NextAttemptAt, nextAttemptAt)
                .SetProperty(x => x.ClaimedAt, (DateTimeOffset?)null)
                .SetProperty(x => x.ClaimToken, (Guid?)null)
                .SetProperty(x => x.LastError, diagnostic.ToString()),
            ct
        );
    }

    /// <summary>
    /// Dead-letters this handler row and every later handler for the same row detail:
    /// handlers for one row run in order, so later ones can never run.
    /// </summary>
    private Task MarkDeadLetteredAsync(ClaimedHandlerRow claimed, string lastError, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        return WithDbAsync(
            async db =>
            {
                await using var transaction = await db.Database.BeginTransactionAsync(ct);
                var settled = await Owned(db, claimed).ExecuteUpdateAsync(DeadLetter(now, lastError), ct);
                if (settled != 1)
                {
                    throw new RetentionRowDispatchClaimLostException(claimed.StatusId);
                }

                await db.Set<SweepRowHandlerStatusEntity>()
                    .Where(s =>
                        s.SweepRunRowDetailId == claimed.RowDetailId
                        && s.Id > claimed.StatusId
                        && (
                            s.State == SweepRowHandlerDispatchState.Pending
                            || s.State == SweepRowHandlerDispatchState.InFlight
                        )
                    )
                    .ExecuteUpdateAsync(
                        DeadLetter(now, "Skipped because an earlier handler for the same row dead-lettered."),
                        ct
                    );
                await transaction.CommitAsync(ct);
            },
            ct
        );
    }

    private static System.Linq.Expressions.Expression<
        Func<Microsoft.EntityFrameworkCore.Query.SetPropertyCalls<SweepRowHandlerStatusEntity>,
            Microsoft.EntityFrameworkCore.Query.SetPropertyCalls<SweepRowHandlerStatusEntity>>
    > DeadLetter(DateTimeOffset now, string lastError) =>
        s => s
            .SetProperty(x => x.State, SweepRowHandlerDispatchState.DeadLettered)
            .SetProperty(x => x.CompletedAt, now)
            .SetProperty(x => x.ClaimedAt, (DateTimeOffset?)null)
            .SetProperty(x => x.ClaimToken, (Guid?)null)
            .SetProperty(x => x.LastError, lastError);

    private Task SettleAsync(
        ClaimedHandlerRow claimed,
        System.Linq.Expressions.Expression<
            Func<Microsoft.EntityFrameworkCore.Query.SetPropertyCalls<SweepRowHandlerStatusEntity>,
                Microsoft.EntityFrameworkCore.Query.SetPropertyCalls<SweepRowHandlerStatusEntity>>
        > update,
        CancellationToken ct
    )
    {
        return WithDbAsync(
            async db =>
            {
                if (await Owned(db, claimed).ExecuteUpdateAsync(update, ct) != 1)
                {
                    throw new RetentionRowDispatchClaimLostException(claimed.StatusId);
                }
            },
            ct
        );
    }

    private static IQueryable<SweepRowHandlerStatusEntity> Owned(DbContext db, ClaimedHandlerRow claimed) =>
        db.Set<SweepRowHandlerStatusEntity>()
            .Where(s =>
                s.Id == claimed.StatusId
                && s.State == SweepRowHandlerDispatchState.InFlight
                && s.ClaimToken == claimed.ClaimToken
            );

    // Once per poll or flush, not per statement: the result is cached after the first pass.
    private async Task ValidateReadinessAsync(CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<RetentionRuntimeReadinessValidator>().ValidateAsync(ct);
    }

    private async Task<TResult> WithDbAsync<TResult>(
        Func<DbContext, Task<TResult>> action,
        CancellationToken ct
    )
    {
        ct.ThrowIfCancellationRequested();
        await using var scope = scopeFactory.CreateAsyncScope();
        return await action(scope.ServiceProvider.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext));
    }

    private Task WithDbAsync(Func<DbContext, Task> action, CancellationToken ct) =>
        WithDbAsync(
            async db =>
            {
                await action(db);
                return true;
            },
            ct
        );

    private static QuotedTables Table(DbContext db) => new(CohortStoreTables.FromModel(db.Model));

    private static Type ResolveEntityType(ClaimedHandlerRow claimed, RetentionRegistry registry)
    {
        return registry.Scan().Values
                .FirstOrDefault(entry => entry.RetentionEntityId == claimed.RetentionEntityId)
                ?.EntityType
            ?? throw new InvalidOperationException(
                $"Could not resolve retention handler entity type '{claimed.EntityType}': it is not a registered retained entity in the current model."
            );
    }

    // MakeGenericType/GetMethod would otherwise run on every dispatched row; the entity
    // set is small and fixed, so the closed types are cached for the process lifetime.
    private static readonly ConcurrentDictionary<Type, (Type ContextType, MethodInfo OnAfter)> DispatchReflection = new();

    private static async Task InvokeOnAfterAsync(
        Type entityType,
        object handler,
        ClaimedHandlerRow claimed,
        IReadOnlyDictionary<string, object?> snapshot,
        CancellationToken ct
    )
    {
        var (contextType, onAfter) = DispatchReflection.GetOrAdd(
            entityType,
            static type => (
                typeof(RetentionAfterContext<>).MakeGenericType(type),
                typeof(IRetentionHandler<>).MakeGenericType(type).GetMethod(nameof(IRetentionHandler<object>.OnAfterAsync))!
            )
        );
        var context = Activator.CreateInstance(
            contextType,
            claimed.SweepId,
            claimed.RecordId,
            claimed.Category,
            claimed.Strategy,
            claimed.TenantId,
            claimed.At,
            claimed.Attempt,
            snapshot
        )!;
        await (Task)onAfter.Invoke(handler, [context, ct])!;
    }

    internal static DateTimeOffset CalculateNextAttemptAt(
        DateTimeOffset now,
        TimeSpan baseBackoff,
        int attempt
    )
    {
        if (attempt <= 0 || baseBackoff <= TimeSpan.Zero)
        {
            return now > RetryScheduleUpperBound ? RetryScheduleUpperBound : now;
        }

        if (now >= RetryScheduleUpperBound)
        {
            return RetryScheduleUpperBound;
        }

        var availableTicks = (RetryScheduleUpperBound - now).Ticks;
        var shifts = attempt - 1;
        if (shifts >= 63 || baseBackoff.Ticks > (availableTicks >> shifts))
        {
            return RetryScheduleUpperBound;
        }

        return now.AddTicks(baseBackoff.Ticks << shifts);
    }

    private sealed class QuotedTables(CohortStoreTables tables)
    {
        public string SweepRun { get; } = PostgreSqlIdentifier.Format(tables.SweepRun);
        public string SweepRunRowDetail { get; } = PostgreSqlIdentifier.Format(tables.SweepRunRowDetail);
        public string SweepRowHandlerStatus { get; } = PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus);
    }

    private sealed class RemainingWork
    {
        public long InFlight { get; init; }
        public long Pending { get; init; }
    }

    // Materialized by SqlQueryRaw; property names match the claim query's column names.
    private sealed class ClaimedHandlerRow
    {
        public long StatusId { get; init; }
        public long RowDetailId { get; init; }
        public string HandlerType { get; init; } = "";
        public int Attempt { get; init; }
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
}
