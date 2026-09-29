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
/// Each poll claims at most <c>MaxParallelism</c> rows (capped by <c>BatchSize</c>), so every
/// claim starts running at once. A claim is a lease of <c>ClaimTimeout</c>: the handler is
/// cancelled when it runs out, and another dispatcher may reclaim the row after it. The
/// claim token fences settlement, so a delivery can only settle the claim it took.
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
    private const int Succeeded = (int)SweepRowHandlerDispatchState.Succeeded;
    private const int DeadLettered = (int)SweepRowHandlerDispatchState.DeadLettered;

    private DateTimeOffset lastPayloadScrubAt = DateTimeOffset.MinValue;

    public async Task<RowDispatcherFlushResult> FlushAsync(CancellationToken ct = default)
    {
        await ValidateReadinessAsync(ct);
        await RecoverAbandonedRunsAsync(DateTimeOffset.UtcNow, ct);
        await ScrubExpiredPayloadsAsync(ct);
        await DrainQueueAsync(RetryScheduleUpperBound, ct);

        // Pending rows left here were not dispatchable (deferred phase with an unsettled
        // sweep, or queued behind a sibling); in-flight rows are held under a lease.
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
            // One snapshot per claim: the lease cutoff, claim size, handler timeout and
            // retry policy for these rows all come from the same options.
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
                            db.Database.ExecuteSqlRawAsync(
                                $$"""
                                UPDATE {{Table(db).SweepRowHandlerStatus}}
                                SET "State" = {0}, "ClaimedAt" = NULL, "ClaimToken" = NULL
                                WHERE "ClaimToken" = {1} AND "State" = {2}
                                """,
                                [Pending, claimToken, InFlight],
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

        // The handler must end before its lease does: past it, another dispatcher may reclaim
        // the row and run it again. A tenth of the lease covers claim latency and clock skew.
        var deadline = claimed.ClaimedAt + dispatch.ClaimTimeout - dispatch.ClaimTimeout / 10;
        using var handlerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var remaining = deadline - DateTimeOffset.UtcNow;
        handlerCts.CancelAfter(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);

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
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException) when (handlerCts.IsCancellationRequested)
        {
            logger.LogError(
                "Cohort row handler for sweep {SweepId} exceeded RowHandlerDispatch:ClaimTimeout and was cancelled.",
                claimed.SweepId
            );
            await MarkFailureAsync(
                claimed,
                dispatch,
                "Handler exceeded RowHandlerDispatch:ClaimTimeout and was cancelled.",
                ct
            );
            await ClearSettledPayloadAsync(claimed.RowDetailId);
            return;
        }
        catch (Exception ex)
        {
            var diagnostic = RetentionFailureDiagnostic.Create(ex);
            logger.LogError(
                ex.GetBaseException(),
                "Cohort row handler failed for sweep {SweepId}. Diagnostic {DiagnosticId}.",
                claimed.SweepId,
                diagnostic.DiagnosticIdText
            );
            await MarkFailureAsync(claimed, dispatch, diagnostic.ToString(), ct);
            // No-op while a sibling handler (or this one's retry) is still unsettled.
            await ClearSettledPayloadAsync(claimed.RowDetailId);
            return;
        }

        // Outside the handler's try: failing to record a success must not be mistaken for the
        // handler failing, nor abort the rest of the batch. The row stays in flight and its
        // lease recovers it.
        try
        {
            await SettleAsync(
                claimed,
                """
                "State" = {0}, "CompletedAt" = {1}, "ClaimedAt" = NULL, "ClaimToken" = NULL, "LastError" = NULL
                """,
                [Succeeded, DateTimeOffset.UtcNow],
                CancellationToken.None
            );
            await ClearSettledPayloadAsync(claimed.RowDetailId);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Cohort could not record the success of row handler status {StatusId}; its lease will recover it.",
                claimed.StatusId
            );
        }
    }

    private async Task MarkFailureAsync(
        ClaimedHandlerRow claimed,
        RowHandlerDispatchOptions dispatch,
        string lastError,
        CancellationToken ct
    )
    {
        if (claimed.Attempt >= dispatch.MaxAttempts)
        {
            await MarkDeadLetteredAsync(claimed, lastError, ct);
            return;
        }

        var nextAttemptAt = CalculateNextAttemptAt(
            DateTimeOffset.UtcNow,
            dispatch.BaseBackoff,
            claimed.Attempt
        );
        await SettleAsync(
            claimed,
            """
            "State" = {0}, "NextAttemptAt" = {1}, "ClaimedAt" = NULL, "ClaimToken" = NULL, "LastError" = {2}
            """,
            [Pending, nextAttemptAt, lastError],
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
                var settled = await UpdateOwnedAsync(db, claimed, DeadLetterAssignments, [DeadLettered, now, lastError], ct);
                if (settled != 1)
                {
                    LogSuperseded(claimed);
                    return;
                }

                await db.Database.ExecuteSqlRawAsync(
                    $$"""
                    UPDATE {{Table(db).SweepRowHandlerStatus}}
                    SET {{DeadLetterAssignments}}
                    WHERE "SweepRunRowDetailId" = {3} AND "Id" > {4} AND "State" IN ({5}, {6})
                    """,
                    [
                        DeadLettered,
                        now,
                        "Skipped because an earlier handler for the same row dead-lettered.",
                        claimed.RowDetailId,
                        claimed.StatusId,
                        Pending,
                        InFlight,
                    ],
                    ct
                );
                await transaction.CommitAsync(ct);
            },
            ct
        );
    }

    // Values: {0} state, {1} completed-at, {2} last error.
    private const string DeadLetterAssignments =
        """
        "State" = {0}, "CompletedAt" = {1}, "ClaimedAt" = NULL, "ClaimToken" = NULL, "LastError" = {2}
        """;

    private Task SettleAsync(ClaimedHandlerRow claimed, string assignments, object[] values, CancellationToken ct)
    {
        return WithDbAsync(
            async db =>
            {
                if (await UpdateOwnedAsync(db, claimed, assignments, values, ct) != 1)
                {
                    LogSuperseded(claimed);
                }
            },
            ct
        );
    }

    // At-least-once: whichever pass took the expired claim over owns the row's outcome now.
    private void LogSuperseded(ClaimedHandlerRow claimed) =>
        logger.LogWarning(
            "Cohort discarded the outcome of row handler status {StatusId} attempt {Attempt}: its claim expired and was taken over or dead-lettered.",
            claimed.StatusId,
            claimed.Attempt
        );

    /// <summary>
    /// Updates this handler row only while this delivery still owns its claim: a reclaim
    /// replaces the token, so a stale delivery matches nothing. Raw SQL rather
    /// than ExecuteUpdateAsync: Cohort builds against EF Core 9, and EF Core 10 replaced the
    /// setter types, so a compiled ExecuteUpdateAsync call fails to load on EF Core 10 hosts.
    /// </summary>
    private static Task<int> UpdateOwnedAsync(
        DbContext db,
        ClaimedHandlerRow claimed,
        string assignments,
        object[] values,
        CancellationToken ct
    )
    {
        var n = values.Length;
        return db.Database.ExecuteSqlRawAsync(
            $$"""
            UPDATE {{Table(db).SweepRowHandlerStatus}}
            SET {{assignments}}
            WHERE "Id" = {{{n}}} AND "State" = {{{n + 1}}} AND "ClaimToken" = {{{n + 2}}}
            """,
            [.. values, claimed.StatusId, InFlight, claimed.ClaimToken],
            ct
        );
    }

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
}
