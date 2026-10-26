using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
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

/// <summary>
/// Durable after-commit delivery of <see cref="IRetentionHandler{TEntity}.OnAfterAsync"/>.
/// Each poll claims at most <c>MaxParallelism</c> due rows (capped by <c>BatchSize</c>), so every
/// claim starts running at once. A claim is a lease of <c>ClaimTimeout</c>: the handler is
/// cancelled when it runs out. While it runs, the claiming dispatcher holds a session advisory
/// lock for the row on its batch's lease connection, so an expired claim is reclaimed only once
/// that session has gone (the owner crashed), never while a handler that ignored its token is
/// still running. The claim token fences settlement, so a delivery can only settle the claim
/// it took. The kill switch stops the background poller; explicit flushes and requeues run.
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

    private static readonly RetentionExecutionContext RowDispatch =
        new RetentionExecutionContext.System(RetentionSystemWork.RowDispatch);

    private DateTimeOffset lastPayloadScrubAt = DateTimeOffset.MinValue;

    public async Task<RowDispatcherFlushResult> FlushAsync(CancellationToken ct = default)
    {
        await ValidateReadinessAsync(ct);
        await RecoverAbandonedRunsAsync(DateTimeOffset.UtcNow, ct);
        await RecoverExpiredClaimsAsync(ct);
        await ScrubExpiredPayloadsAsync(ct);
        await DrainQueueAsync(stopOnKillSwitch: false, ct);

        // Pending rows left here were not dispatchable (waiting out their backoff, deferred
        // phase with an unsettled sweep, or queued behind a sibling); in-flight rows are held
        // under a claim.
        return await WithDbAsync(db => RowHandlerQueueSql.CountRemainingAsync(db, ct), ct);
    }

    public async Task<long> RequeueDeadLetteredAsync(
        Guid? sweepId = null,
        Guid? handlerId = null,
        CancellationToken ct = default
    )
    {
        await ValidateReadinessAsync(ct);
        // Every dead-lettered handler of a matching row goes back together: the claim's
        // ordering only waits on pending or in-flight siblings, so requeuing a later handler
        // alone would let it run ahead of an earlier one. Attempt is kept: a handler resumes
        // its remaining attempts, or gets one more delivery if it used them all, and
        // ctx.Attempt > 1 still tells it the work may already be done.
        var requeued = await WithDbAsync(
            db => RowHandlerQueueSql.RequeueDeadLetteredAsync(db, sweepId, handlerId, DateTimeOffset.UtcNow, ct),
            ct
        );
        logger.LogInformation(
            "Cohort requeued {RequeuedCount} dead-lettered row handler statuses (sweep {SweepId}, handler {HandlerId}).",
            requeued,
            sweepId,
            handlerId
        );
        return requeued;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await PollAsync(stoppingToken);
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
    /// One background poll. The kill switch stops delivery: no claims are reclaimed or taken,
    /// and a drain in progress stops after the batch it is running. The payload-retention
    /// scrub still runs, so the switch never keeps personal data past its retention.
    /// </summary>
    internal async Task PollAsync(CancellationToken ct)
    {
        await ValidateReadinessAsync(ct);
        var now = DateTimeOffset.UtcNow;
        if (now - lastPayloadScrubAt >= PayloadScrubInterval)
        {
            lastPayloadScrubAt = now;
            await ScrubExpiredPayloadsAsync(ct);
        }

        if (options.Current.KillSwitch)
        {
            return;
        }

        await RecoverAbandonedRunsAsync(now, ct);
        await RecoverExpiredClaimsAsync(ct);
        await DrainQueueAsync(stopOnKillSwitch: true, ct);
    }

    /// <summary>
    /// Backstop for captured snapshots that outlive <c>PayloadRetention</c> even while
    /// handler work is still queued or dead-lettered: the personal data must not outlive its
    /// retention.
    /// </summary>
    private Task ScrubExpiredPayloadsAsync(CancellationToken ct)
    {
        var cutoff = OperationalTime.SubtractSaturating(
            DateTimeOffset.UtcNow,
            options.Current.RowHandlerDispatch.PayloadRetention
        );
        return WithDbAsync(db => RowHandlerQueueSql.ScrubExpiredPayloadsAsync(db, cutoff, ct), ct);
    }

    /// <summary>
    /// Clears a row detail's captured snapshot once every handler for that row has
    /// succeeded. The snapshot exists to feed OnAfterAsync until it succeeds, so dead-lettered
    /// work keeps it, to be requeued, until the <c>PayloadRetention</c> scrub.
    /// </summary>
    private Task ClearDeliveredPayloadAsync(long rowDetailId)
    {
        return WithDbAsync(
            db => RowHandlerQueueSql.ClearDeliveredPayloadAsync(db, rowDetailId),
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
            db => RetentionRunLedgerSql.RecoverAbandonedAsync(db, cutoff, now, logger, ct),
            ct
        );
    }

    /// <summary>
    /// Reclaims claims held past <c>ClaimTimeout</c>, but only after taking the delivery lock
    /// proves that the dispatcher which claimed them has gone. A claim whose dispatcher still
    /// holds the lock is running a handler that ignored its cancellation: reclaiming it would
    /// run the handler twice at once, or let the row's next handler start before it ends.
    /// A reclaimed claim with attempts left goes back to the queue, and the next claim counts
    /// the attempt; one that used its last attempt is dead-lettered, with the row's later
    /// handlers. Expired claims are paged by (<c>ClaimedAt</c>, <c>Id</c>) until a page comes
    /// back short, so claims still held by running handlers, which stay the oldest, can't
    /// hide a crashed dispatcher's claims behind them.
    /// </summary>
    private Task RecoverExpiredClaimsAsync(CancellationToken ct)
    {
        var dispatch = options.Current.RowHandlerDispatch;
        var now = DateTimeOffset.UtcNow;
        var leaseCutoff = OperationalTime.SubtractSaturating(now, dispatch.ClaimTimeout);
        return WithDbAsync(
            async db =>
            {
                ExpiredClaim? last = null;
                List<ExpiredClaim> page;
                do
                {
                    page = await RowHandlerQueueSql.ReadExpiredClaimsAsync(db, leaseCutoff, last, dispatch.BatchSize, ct);
                    if (page.Count == 0)
                    {
                        return;
                    }

                    await db.Database.OpenConnectionAsync(ct);
                    try
                    {
                        var connection = db.Database.GetDbConnection();
                        foreach (var claim in page)
                        {
                            await ReclaimAsync(db, connection, claim, dispatch, ct);
                        }
                    }
                    finally
                    {
                        await db.Database.CloseConnectionAsync();
                    }

                    last = page[^1];
                }
                while (page.Count == dispatch.BatchSize);
            },
            ct
        );
    }

    private async Task ReclaimAsync(
        DbContext db,
        DbConnection connection,
        ExpiredClaim claim,
        RowHandlerDispatchOptions dispatch,
        CancellationToken ct
    )
    {
        switch (await RowHandlerQueueSql.TryReclaimAsync(db, connection, claim, dispatch.MaxAttempts, logger, ct))
        {
            case ReclaimOutcome.OwnerStillRunning:
                logger.LogWarning(
                    "Cohort row handler status {StatusId} has outlived RowHandlerDispatch:ClaimTimeout and its dispatcher is still running it; not reclaimed.",
                    claim.StatusId
                );
                break;
            case ReclaimOutcome.DeadLettered:
                LogDeadLettered(
                    claim.StatusId,
                    claim.SweepId,
                    RowHandlerQueueSql.ExhaustedClaimReason,
                    snapshotKept: true
                );
                break;
        }
    }

    private async Task DrainQueueAsync(bool stopOnKillSwitch, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && !(stopOnKillSwitch && options.Current.KillSwitch))
        {
            // One snapshot per claim: the lease cutoff, claim size, handler timeout and
            // retry policy for these rows all come from the same options.
            var dispatch = options.Current.RowHandlerDispatch;
            if (!await WithDbAsync(leaseDb => RunBatchAsync(leaseDb, dispatch, ct), ct))
            {
                return;
            }
        }
    }

    /// <summary>
    /// Claims one batch and runs it, holding each claimed row's delivery lock on this batch's
    /// lease connection until that delivery's outcome is recorded. The lease connection is
    /// never in a transaction while handlers run. Returns false when nothing was due.
    /// </summary>
    private async Task<bool> RunBatchAsync(DbContext leaseDb, RowHandlerDispatchOptions dispatch, CancellationToken ct)
    {
        var claimToken = Guid.NewGuid();
        await leaseDb.Database.OpenConnectionAsync(ct);
        var connection = leaseDb.Database.GetDbConnection();
        Exception? primaryException = null;
        try
        {
            var claimed = await RowHandlerQueueSql.ClaimAsync(
                leaseDb,
                claimToken,
                Math.Min(dispatch.MaxParallelism, dispatch.BatchSize),
                ct
            );
            if (claimed.Count == 0)
            {
                return false;
            }

            using var lease = new DeliveryLease(
                connection,
                CohortStoreTables.FromModel(leaseDb.Model).SweepRowHandlerStatus,
                logger
            );
            try
            {
                await Parallel.ForEachAsync(
                    claimed,
                    new ParallelOptions
                    {
                        CancellationToken = ct,
                        MaxDegreeOfParallelism = dispatch.MaxParallelism,
                    },
                    async (row, rowCt) =>
                    {
                        await ProcessClaimedRowAsync(row, dispatch, rowCt);
                        // After the outcome is recorded, so recovery can never reclaim the
                        // row between "handler returned" and "settled".
                        await lease.ReleaseAsync(row.StatusId);
                    }
                );
            }
            catch (Exception)
            {
                // Every delivery records its own outcome, so only cancellation (shutdown or a
                // cancelled flush) ends a batch early. Its interrupted deliveries go back to
                // the queue before their locks are released; the attempt they were cancelled
                // in still counts.
                try
                {
                    using var cleanup = new CancellationTokenSource(CleanupTimeout);
                    await WithDbAsync(
                        db => RowHandlerQueueSql.RequeueInterruptedAsync(db, claimToken, cleanup.Token),
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

            return true;
        }
        catch (Exception ex)
        {
            primaryException = ex;
            throw;
        }
        finally
        {
            // Session locks survive a failed claim transaction too, so release whatever the
            // session holds on every path.
            await OperationalConnectionCleanup.RunAsync(
                connection,
                cleanupToken => RetentionRunAdvisoryLock.ReleaseAllAsync(connection, cleanupToken),
                _ => leaseDb.Database.CloseConnectionAsync(),
                primaryException,
                logger
            );
        }
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
            await RecordOutcomeAsync(
                claimed,
                () => MarkDeadLetteredAsync(
                    claimed,
                    "Captured row snapshot was scrubbed by the payload-retention backstop (RowHandlerDispatch:PayloadRetention) before this handler ran; the work can no longer complete. Increase PayloadRetention or drain handler work sooner.",
                    snapshotKept: false,
                    ct
                )
            );
            return;
        }

        // The handler must end before its lease does: past it, another dispatcher may reclaim
        // the row and run it again. A tenth of the lease covers claim latency and clock skew.
        var deadline = claimed.ClaimedAt + dispatch.ClaimTimeout - dispatch.ClaimTimeout / 10;
        using var handlerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var remaining = deadline - DateTimeOffset.UtcNow;
        handlerCts.CancelAfter(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);

        Func<Task> recordOutcome;
        try
        {
            // Each delivery runs as the row's tenant, so host handler code never runs under a
            // system scope against tenanted rows.
            recordOutcome = await scopeFactory.RunInScopeAsync<Func<Task>>(
                RetentionExecutionScopes.ForTenant(claimed.TenantId),
                async services =>
                {
                    var entityType = services.GetRequiredService<RetentionRegistry>()
                        .Scan()
                        .Values
                        .FirstOrDefault(entry => entry.RetentionEntityId == claimed.RetentionEntityId)
                        ?.EntityType;
                    if (entityType is null)
                    {
                        // Retrying cannot heal an unregistered entity id either: the entity
                        // lost its [Retain] or its [RetentionEntityId] changed. The snapshot
                        // stays, so the work can be requeued once the id is back.
                        return () => MarkDeadLetteredAsync(
                            claimed,
                            $"entity id {claimed.RetentionEntityId} is not registered; restore its [RetentionEntityId] and requeue",
                            snapshotKept: true,
                            ct
                        );
                    }

                    var handlers = RetentionHandlerSupport.ResolveHandlers(services, entityType);
                    var handler = handlers.FirstOrDefault(candidate =>
                        string.Equals(candidate.HandlerIdentity, claimed.HandlerType, StringComparison.Ordinal)
                    );
                    if (handler is null)
                    {
                        // Retrying cannot heal an unregistered handler id: the handler was
                        // removed, or its [RowHandlerId] changed. The snapshot stays, so the
                        // work can be requeued once the handler is back.
                        return () => MarkDeadLetteredAsync(
                            claimed,
                            $"handler id {claimed.HandlerType} is not registered; re-register it and requeue",
                            snapshotKept: true,
                            ct
                        );
                    }
                    else
                    {
                        var snapshot = RetentionSnapshotSerializer.Deserialize(
                            claimed.CapturedPayload,
                            entityType,
                            handlers.Select(resolved => resolved.Instance.GetType().Assembly)
                        );
                        await InvokeOnAfterAsync(entityType, handler.Instance, claimed, snapshot, handlerCts.Token);
                        return async () =>
                        {
                            await SettleAsync(
                                claimed,
                                db => RowHandlerQueueSql.SettleSucceededAsync(db, claimed, CancellationToken.None),
                                CancellationToken.None
                            );
                            await ClearDeliveredPayloadAsync(claimed.RowDetailId);
                        };
                    }
                },
                handlerCts.Token
            );
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
            recordOutcome = () => RecordFailureAsync(
                claimed,
                dispatch,
                "Handler exceeded RowHandlerDispatch:ClaimTimeout and was cancelled.",
                ct
            );
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
            recordOutcome = () => RecordFailureAsync(claimed, dispatch, diagnostic.ToString(), ct);
        }

        await RecordOutcomeAsync(claimed, recordOutcome);
    }

    /// <summary>
    /// Writes a delivery's outcome. The delivery has already happened, so failing to record
    /// it is neither the handler's failure nor a reason to abort the rest of the batch (and
    /// cancel its siblings mid-delivery): the row stays in flight, attempt spent, and its
    /// lease recovers it.
    /// </summary>
    private async Task RecordOutcomeAsync(ClaimedHandlerRow claimed, Func<Task> record)
    {
        try
        {
            await record();
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Cohort could not record the outcome of row handler status {StatusId} attempt {Attempt}; its lease will recover it.",
                claimed.StatusId,
                claimed.Attempt
            );
        }
    }

    private async Task RecordFailureAsync(
        ClaimedHandlerRow claimed,
        RowHandlerDispatchOptions dispatch,
        string lastError,
        CancellationToken ct
    )
    {
        if (claimed.Attempt >= dispatch.MaxAttempts)
        {
            await MarkDeadLetteredAsync(claimed, lastError, snapshotKept: true, ct);
            return;
        }

        var nextAttemptAt = CalculateNextAttemptAt(
            DateTimeOffset.UtcNow,
            dispatch.BaseBackoff,
            dispatch.MaxBackoff,
            claimed.Attempt
        );
        await SettleAsync(
            claimed,
            db => RowHandlerQueueSql.SettleForRetryAsync(db, claimed, nextAttemptAt, lastError, ct),
            ct
        );
    }

    /// <summary>
    /// Dead-letters this handler row and every later handler for the same row detail:
    /// handlers for one row run in order, so later ones can never run. The row's snapshot
    /// stays until <c>PayloadRetention</c>, so the work can be requeued.
    /// </summary>
    private Task MarkDeadLetteredAsync(
        ClaimedHandlerRow claimed,
        string lastError,
        bool snapshotKept,
        CancellationToken ct
    )
    {
        return WithDbAsync(
            async db =>
            {
                if (!await RowHandlerQueueSql.DeadLetterAsync(db, claimed, lastError, ct))
                {
                    LogSuperseded(claimed);
                    return;
                }

                LogDeadLettered(claimed.StatusId, claimed.SweepId, lastError, snapshotKept);
            },
            ct
        );
    }

    // An Error, because hosts that only run the background poller have no other signal that
    // work stopped retrying. LastError is a sanitized diagnostic or a Cohort-defined reason.
    // A snapshot the scrub already cleared can't be requeued, so that log doesn't offer it.
    private void LogDeadLettered(long statusId, Guid sweepId, string lastError, bool snapshotKept)
    {
        if (snapshotKept)
        {
            logger.LogError(
                "Cohort dead-lettered row handler status {StatusId} for sweep {SweepId}: {LastError} While its snapshot is retained (RowHandlerDispatch:PayloadRetention), IRetentionRowDispatcher.RequeueDeadLetteredAsync runs it again.",
                statusId,
                sweepId,
                lastError
            );
            return;
        }

        logger.LogError(
            "Cohort dead-lettered row handler status {StatusId} for sweep {SweepId}: {LastError} Its snapshot is gone, so it can't be requeued.",
            statusId,
            sweepId,
            lastError
        );
    }

    private Task SettleAsync(ClaimedHandlerRow claimed, Func<DbContext, Task<bool>> settle, CancellationToken ct)
    {
        return WithDbAsync(
            async db =>
            {
                if (!await settle(db))
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

    // Once per poll or flush, not per statement: the result is cached after the first pass.
    private Task ValidateReadinessAsync(CancellationToken ct) =>
        scopeFactory.RunInScopeAsync(
            RowDispatch,
            services => services.GetRequiredService<RetentionRuntimeReadinessValidator>().ValidateAsync(ct),
            ct
        );

    private Task<TResult> WithDbAsync<TResult>(
        Func<DbContext, Task<TResult>> action,
        CancellationToken ct
    )
    {
        ct.ThrowIfCancellationRequested();
        // Claims, settlement, recovery and scrubbing span every tenant's queued work.
        return scopeFactory.RunInScopeAsync(
            RowDispatch,
            services => action(services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext)),
            ct
        );
    }

    private Task WithDbAsync(Func<DbContext, Task> action, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return scopeFactory.RunInScopeAsync(
            RowDispatch,
            services => action(services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext)),
            ct
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
        TimeSpan maxBackoff,
        int attempt
    )
    {
        if (now >= RetryScheduleUpperBound)
        {
            return RetryScheduleUpperBound;
        }

        var delay = BackoffDelay(baseBackoff, maxBackoff, attempt);
        return delay.Ticks > (RetryScheduleUpperBound - now).Ticks
            ? RetryScheduleUpperBound
            : now + delay;
    }

    /// <summary>
    /// The wait after failed attempt <paramref name="attempt"/>: <paramref name="baseBackoff"/>
    /// doubled for each earlier failure, capped at <paramref name="maxBackoff"/>.
    /// </summary>
    internal static TimeSpan BackoffDelay(TimeSpan baseBackoff, TimeSpan maxBackoff, int attempt)
    {
        if (attempt <= 0 || baseBackoff <= TimeSpan.Zero || maxBackoff <= TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        var shifts = attempt - 1;
        if (shifts >= 63 || baseBackoff.Ticks > (maxBackoff.Ticks >> shifts))
        {
            return maxBackoff;
        }

        return TimeSpan.FromTicks(baseBackoff.Ticks << shifts);
    }

    /// <summary>
    /// The longest a handler's work can stay queued while it still has attempts: the backoff
    /// after every failed attempt but the last, plus every attempt running its whole
    /// <c>ClaimTimeout</c>. Call it only with options the validator's bounds already accept.
    /// </summary>
    internal static TimeSpan RetryScheduleSpan(RowHandlerDispatchOptions dispatch)
    {
        var span = dispatch.ClaimTimeout * dispatch.MaxAttempts;
        for (var attempt = 1; attempt < dispatch.MaxAttempts; attempt++)
        {
            span += BackoffDelay(dispatch.BaseBackoff, dispatch.MaxBackoff, attempt);
        }

        return span;
    }

    /// <summary>
    /// The longest a row's queued work can stay retrying when the row has
    /// <paramref name="handlerCount"/> handlers: they run one after another, each through its
    /// whole <see cref="RetryScheduleSpan"/>, and each attempt can be picked up a
    /// <c>PollInterval</c> late. Saturates at <see cref="TimeSpan.MaxValue"/>. Call it only
    /// with options the validator's bounds already accept.
    /// </summary>
    internal static TimeSpan RowRetryScheduleSpan(RowHandlerDispatchOptions dispatch, int handlerCount)
    {
        var perHandler = (Int128)RetryScheduleSpan(dispatch).Ticks
            + (Int128)dispatch.PollInterval.Ticks * dispatch.MaxAttempts;
        var total = perHandler * handlerCount;
        return total >= TimeSpan.MaxValue.Ticks ? TimeSpan.MaxValue : TimeSpan.FromTicks((long)total);
    }

    /// <summary>
    /// The batch's lease connection, shared by its deliveries to release their own locks. A
    /// connection runs one command at a time, so releases take turns.
    /// </summary>
    private sealed class DeliveryLease(
        DbConnection connection,
        RelationalObjectName statusTable,
        ILogger logger
    ) : IDisposable
    {
        private readonly SemaphoreSlim gate = new(1, 1);

        public async Task ReleaseAsync(long statusId)
        {
            using var cleanup = new CancellationTokenSource(CleanupTimeout);
            try
            {
                await gate.WaitAsync(cleanup.Token);
                try
                {
                    await RetentionRunAdvisoryLock.ReleaseAsync(
                        connection,
                        RetentionRunAdvisoryLock.RowHandlerKeyFor(statusTable, statusId),
                        cleanup.Token
                    );
                }
                finally
                {
                    gate.Release();
                }
            }
            catch (Exception ex)
            {
                // The batch releases everything its session holds when it ends.
                logger.LogWarning(
                    ex,
                    "Cohort could not release the delivery lock for row handler status {StatusId}; the batch releases it when it ends.",
                    statusId
                );
            }
        }

        public void Dispose() => gate.Dispose();
    }
}
