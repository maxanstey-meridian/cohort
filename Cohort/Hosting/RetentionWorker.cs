using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Cohort.Hosting;

/// <summary>
/// Runs the schedule as a set of obligations: each pass loop catches up the most recent
/// occurrence at or before now on the database clock, the clock that stamps every run. A sweep
/// covers everything due at any earlier occurrence, so only that one ever needs catching up.
/// An occurrence is settled once every pass in its plan has a <c>Succeeded</c> run since it,
/// or has used up <see cref="CohortOptions.OccurrenceMaxAttempts"/>; until then the worker
/// retries the unfinished passes <see cref="CohortOptions.OccurrenceRetryDelay"/> apart.
/// Every answer comes from the <c>sweep_run</c> ledger, read under the worker lock.
/// </summary>
internal sealed class RetentionWorker(
    IServiceScopeFactory scopeFactory,
    CohortOptionsSnapshot options,
    IOptionsMonitor<CohortOptions> optionsMonitor,
    ILogger<RetentionWorker> logger
) : BackgroundService
{
    private static readonly TimeSpan IdlePollInterval = TimeSpan.FromMilliseconds(200);

    // Only the schedule loop touches it.
    private bool paused;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // A retention worker must outlive individual failures: a transient database
        // outage or a misconfigured category at 02:00 should cost a retry, not all future
        // sweeps (or the host, under StopHost behavior).
        SettledOccurrence? settled = null;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                settled = await RunScheduleLoopOnceAsync(settled, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Cohort worker schedule loop failed. The worker stays alive and will try again.");
                await DelayUntilNextPollAsync(stoppingToken);
            }
        }
    }

    private async Task<SettledOccurrence?> RunScheduleLoopOnceAsync(
        SettledOccurrence? settled,
        CancellationToken stoppingToken
    )
    {
        var current = options.Current;
        if (current.KillSwitch && !paused)
        {
            logger.LogInformation("Cohort worker is paused: the kill switch is on, so no occurrence starts.");
        }
        paused = current.KillSwitch;
        if (current.KillSwitch || string.IsNullOrWhiteSpace(current.Schedule))
        {
            await DelayUntilNextPollAsync(stoppingToken);
            return settled;
        }

        IterationResult iteration;
        try
        {
            iteration = await RunScheduledIterationAsync(current.Schedule, current.DryRun, settled, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Nothing reached the ledger (readiness, the tenant source or the lock connection
            // failed), so no attempt is used up: an outage retries until the next occurrence.
            logger.LogError(
                ex,
                "Cohort worker iteration failed. The worker stays alive and retries after {RetryDelay}, or at the next occurrence if that comes first.",
                current.OccurrenceRetryDelay
            );
            // The database clock may be out of reach, so the replica's bounds the wait; the
            // next iteration reads the database's again.
            var nextOnReplica = CohortScheduleParser.GetNextOccurrence(current.Schedule, DateTimeOffset.UtcNow);
            await SleepUntilAsync(EarlierOf(nextOnReplica, DateTimeOffset.UtcNow + current.OccurrenceRetryDelay), current, stoppingToken);
            return settled;
        }

        switch (iteration.Outcome)
        {
            case OccurrenceOutcome.Settled:
                // No next occurrence ever: nothing is due until the schedule changes.
                await SleepUntilAsync(iteration.NextWake ?? DateTimeOffset.MaxValue, current, stoppingToken);
                return iteration.Occurrence is { } occurrence ? new SettledOccurrence(occurrence, current.DryRun) : settled;
            case OccurrenceOutcome.Stopped:
                // The kill switch is on, or already off again: the loop's next turn sees which.
                return settled;
            default:
                // A newer occurrence supersedes this one, so never sleep past it.
                await SleepUntilAsync(
                    EarlierOf(iteration.NextWake, DateTimeOffset.UtcNow + current.OccurrenceRetryDelay),
                    current,
                    stoppingToken
                );
                return settled;
        }
    }

    private static DateTimeOffset EarlierOf(DateTimeOffset? next, DateTimeOffset retryAt) =>
        next is { } upcoming && upcoming < retryAt ? upcoming : retryAt;

    /// <summary>
    /// Sleeps until <paramref name="wakeAt"/> on the replica's clock, or until the kill switch,
    /// schedule or dry-run setting changes from <paramref name="basis"/>, so turning the kill
    /// switch off catches up straight away.
    /// </summary>
    private async Task SleepUntilAsync(DateTimeOffset wakeAt, CohortOptions basis, CancellationToken ct)
    {
        var changed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = optionsMonitor.OnChange(updated =>
        {
            if (Differs(updated))
            {
                changed.TrySetResult();
            }
        });
        // A change made before the registration existed (the kill switch that cancelled the
        // iteration just now, say) would otherwise go unseen until the wake time.
        if (Differs(options.Current))
        {
            return;
        }

        while (!ct.IsCancellationRequested)
        {
            var remaining = wakeAt - DateTimeOffset.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                return;
            }

            var delay = Task.Delay(
                // Bounded chunks: a gap beyond Task.Delay's ~49.7-day ceiling would throw.
                OperationalTime.GetDelayChunk(remaining),
                ct
            );
            if (await Task.WhenAny(delay, changed.Task) == changed.Task)
            {
                return;
            }
            await delay;
        }

        bool Differs(CohortOptions updated) =>
            updated.KillSwitch != basis.KillSwitch
            || updated.DryRun != basis.DryRun
            || !string.Equals(updated.Schedule, basis.Schedule, StringComparison.Ordinal);
    }

    /// <summary>
    /// One iteration of <paramref name="schedule"/>: reads the database clock once, catches
    /// up the occurrence at or before it unless that is <paramref name="settled"/>, and says
    /// when the next one falls on the replica's clock.
    /// </summary>
    internal Task<IterationResult> RunScheduledIterationAsync(
        string schedule,
        bool dryRun,
        SettledOccurrence? settled,
        CancellationToken ct
    ) =>
        RunIterationAsync(
            async (db, token) =>
            {
                var databaseNow = await DatabaseClock.NowAsync(db, token);
                var replicaNow = DateTimeOffset.UtcNow;
                var occurrence = CohortScheduleParser.GetPreviousOccurrence(schedule, databaseNow);
                var next = CohortScheduleParser.GetNextOccurrence(schedule, databaseNow);
                return (
                    occurrence is { } due && settled != new SettledOccurrence(due, dryRun) ? due : null,
                    occurrence,
                    // The same instant on the replica's clock, which is the one Task.Delay keeps.
                    next is { } upcoming ? replicaNow + (upcoming - databaseNow) : null
                );
            },
            dryRun,
            ct
        );

    /// <summary>Catches up <paramref name="occurrence"/> as given, whatever either clock says.</summary>
    internal async Task<OccurrenceOutcome> RunIterationAsync(
        DateTimeOffset occurrence,
        bool dryRun,
        CancellationToken ct
    ) =>
        (await RunIterationAsync(
            (_, _) => Task.FromResult<(DateTimeOffset?, DateTimeOffset?, DateTimeOffset?)>((occurrence, occurrence, null)),
            dryRun,
            ct
        )).Outcome;

    // The iteration scope coordinates across tenants: the database clock, the worker lock, the
    // occurrence ledger and the tenant source. Each pass runs in its own scope, as its tenant,
    // inside this work. The kill switch cancels the iteration through the same path as
    // shutdown: the batch in flight rolls back, its run settles Cancelled, and no later pass
    // reaches Started.
    private async Task<IterationResult> RunIterationAsync(
        ChooseOccurrence choose,
        bool dryRun,
        CancellationToken ct
    )
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var registration = optionsMonitor.OnChange(updated =>
        {
            if (updated.KillSwitch)
            {
                Stop(stop);
            }
        });
        // Closes the race with a switch flipped before the registration existed.
        if (options.Current.KillSwitch)
        {
            Stop(stop);
        }

        try
        {
            return await scopeFactory.RunInScopeAsync(
                new RetentionExecutionContext.System(RetentionSystemWork.WorkerCoordination),
                services => RunIterationAsync(services, choose, dryRun, stop.Token),
                stop.Token
            );
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            logger.LogInformation(
                "Cohort worker kill switch engaged; stopped the pass in progress and skipped the rest of this iteration."
            );
            return new IterationResult(OccurrenceOutcome.Stopped, null, null);
        }

        // Disposing the registration doesn't wait for a callback already running, which can
        // then reach a source the iteration has disposed.
        static void Stop(CancellationTokenSource stop)
        {
            try
            {
                stop.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // The iteration is over: there is nothing left to stop.
            }
        }
    }

    private async Task<IterationResult> RunIterationAsync(
        IServiceProvider services,
        ChooseOccurrence choose,
        bool dryRun,
        CancellationToken ct
    )
    {
        await services
            .GetRequiredService<RetentionRuntimeReadinessValidator>()
            .ValidateAsync(ct);
        var db = services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext);

        // The advisory lock is session-scoped, so this coordination connection stays open for
        // the whole iteration; each pass runs on its own scope's connection.
        await db.Database.OpenConnectionAsync(ct);
        // Two replicas firing at the same cron instant must not both sweep: double mutations
        // are mostly benign, but doubled audit runs and doubled handler side effects are not.
        var workerLockKey = RetentionRunAdvisoryLock.WorkerKeyFor(CohortStoreTables.FromModel(db.Model).SweepRun);
        var lockAcquired = false;
        Exception? primaryException = null;
        try
        {
            var (due, occurrence, nextWake) = await choose(db, ct);
            if (due is not { } owed)
            {
                return new IterationResult(OccurrenceOutcome.Settled, occurrence, nextWake);
            }

            lockAcquired = await RetentionRunAdvisoryLock.TryAcquireAsync(
                db.Database.GetDbConnection(),
                workerLockKey,
                ct
            );
            if (!lockAcquired)
            {
                logger.LogInformation(
                    "Cohort worker skipped this occurrence: another instance holds the sweep advisory lock."
                );
                return new IterationResult(OccurrenceOutcome.LockBusy, owed, nextWake);
            }

            return new IterationResult(await RunLockedIterationAsync(services, db, owed, dryRun, ct), owed, nextWake);
        }
        catch (Exception ex)
        {
            primaryException = ex;
            throw;
        }
        finally
        {
            await OperationalConnectionCleanup.RunAsync(
                db.Database.GetDbConnection(),
                lockAcquired
                    ? cleanupToken =>
                        RetentionRunAdvisoryLock.ReleaseAsync(
                            db.Database.GetDbConnection(),
                            workerLockKey,
                            cleanupToken
                        )
                    : null,
                cleanupToken => db.Database.CloseConnectionAsync().WaitAsync(cleanupToken),
                primaryException,
                logger
            );
        }
    }

    private async Task<OccurrenceOutcome> RunLockedIterationAsync(
        IServiceProvider services,
        DbContext db,
        DateTimeOffset occurrence,
        bool dryRun,
        CancellationToken ct
    )
    {
        var maxAttempts = options.Current.OccurrenceMaxAttempts;
        var plan = await BuildPassPlanAsync(services, db, ct);
        var ledger = await LoadLedgerAsync(db, occurrence, dryRun, ct);
        if (await AnyLiveRunAsync(db, ledger, ct))
        {
            // The worker lock's session was lost (a timeout, a failover) while its owner's
            // passes ran on their own connections. Running beside them would double their
            // handler side effects, which is what the lock exists to prevent.
            logger.LogInformation(
                "Cohort worker skipped this occurrence: a run of it is still in progress elsewhere."
            );
            return OccurrenceOutcome.LockBusy;
        }

        var pending = plan.Where(pass => ledger.IsPending(pass.TenantId, maxAttempts)).ToArray();
        foreach (var pass in pending)
        {
            await RunPassAsync(pass, dryRun, ct);
        }

        // Read back rather than infer: a pass that failed before its run existed used no attempt.
        ledger = await LoadLedgerAsync(db, occurrence, dryRun, ct);
        // Only the passes this iteration ran: one exhausted earlier was reported then.
        var exhausted = pending
            .Where(pass => !ledger.IsCompleted(pass.TenantId) && !ledger.IsPending(pass.TenantId, maxAttempts))
            .Select(pass => pass.TenantId)
            .ToArray();
        if (exhausted.Length > 0)
        {
            logger.LogError(
                "Cohort worker gave up on occurrence {Occurrence} for tenants {TenantIds}: each pass ran {MaxAttempts} times without succeeding. The next occurrence tries again.",
                occurrence,
                exhausted,
                maxAttempts
            );
        }

        return plan.Any(pass => ledger.IsPending(pass.TenantId, maxAttempts))
            ? OccurrenceOutcome.Pending
            : OccurrenceOutcome.Settled;
    }

    /// <summary>
    /// Whether any run of the occurrence left <c>Started</c> still has its owner: the run lock
    /// is held for as long as the run is, and goes with its session, so a lock this connection
    /// can take marks a run whose owner died. The same test the dispatcher's recovery uses.
    /// </summary>
    private static async Task<bool> AnyLiveRunAsync(DbContext db, OccurrenceLedger ledger, CancellationToken ct)
    {
        var connection = db.Database.GetDbConnection();
        foreach (var sweepId in ledger.StartedSweepIds)
        {
            var key = RetentionRunAdvisoryLock.KeyFor(sweepId);
            if (!await RetentionRunAdvisoryLock.TryAcquireAsync(connection, key, ct))
            {
                return true;
            }

            await RetentionRunAdvisoryLock.ReleaseAsync(connection, key, ct);
        }

        return false;
    }

    /// <summary>
    /// The passes one occurrence owes, in run order: each tenant from the source, then each
    /// refused tenant, then the tenantless pass. A tenant the source returned with conflicting
    /// contexts, or one that owns retained rows the source didn't return, is refused rather
    /// than swept under a guessed context. Tenantless tables hold one shared row set; sweeping
    /// them inside the per-tenant loop retired nothing after the first pass but attributed the
    /// audit run (and resolved retention rules) under whichever tenant happened to come first.
    /// </summary>
    private async Task<IReadOnlyList<PlannedPass>> BuildPassPlanAsync(
        IServiceProvider services,
        DbContext db,
        CancellationToken ct
    )
    {
        var registry = services.GetRequiredService<RetentionRegistry>();
        var entries = registry.Scan().Values;
        var plan = new List<PlannedPass>();
        if (entries.Any(entry => entry.Tenant is not null))
        {
            var (tenants, conflicting) = MaterializeTenants(
                await services.GetRequiredService<IRetentionTenantSource>().GetTenantsAsync(ct)
            );
            var gaps = await RetentionTenantCoverage.FindGapsAsync(
                db,
                registry,
                [.. tenants.Select(tenant => tenant.Id), .. conflicting],
                ct
            );
            if (gaps.EntitiesWithEmptyTenant.Count > 0)
            {
                logger.LogError(
                    "Cohort worker found rows with an empty tenant ID (Guid.Empty) in the tenanted tables of {EntityTypes}. No tenant pass can sweep them; give them the tenant they belong to.",
                    string.Join(", ", gaps.EntitiesWithEmptyTenant.Select(type => type.FullName))
                );
            }

            plan.AddRange(tenants.Select(tenant => new SweepPass(tenant, SweepEntityScope.TenantedOnly)));
            plan.AddRange(conflicting.Select(id => new RefusedPass(id, RetentionPassRefusal.ConflictingTenantContexts)));
            plan.AddRange(gaps.MissingTenantIds.Select(id => new RefusedPass(id, RetentionPassRefusal.NotInTenantSource)));
        }

        if (entries.Any(entry => entry.Tenant is null))
        {
            plan.Add(new SweepPass(TenantContext.Tenantless, SweepEntityScope.TenantlessOnly));
        }

        return plan;
    }

    private static async Task<OccurrenceLedger> LoadLedgerAsync(
        DbContext db,
        DateTimeOffset occurrence,
        bool dryRun,
        CancellationToken ct
    )
    {
        // A pass is keyed by its tenant (Guid.Empty for the tenantless pass), and only runs of
        // the same kind count: a replica still configured to dry run must not stand in for the
        // real sweep. A run still Started has lost its owner, unless the owner's worker lock
        // went with a lost session while the run carried on: the caller tells the two apart by
        // the run lock. Cancelled runs (the kill switch, or a shutdown) use no attempt.
        var sweepRun = PostgreSqlIdentifier.Format(CohortStoreTables.FromModel(db.Model).SweepRun);
        await using var command = new SqlParams
        {
            ["scheduled"] = (int)SweepTriggerKind.Scheduled,
            ["occurrence"] = occurrence,
            ["dryRun"] = dryRun,
            ["started"] = (int)SweepRunStatus.Started,
            ["succeeded"] = (int)SweepRunStatus.Succeeded,
            ["cancelled"] = (int)SweepRunStatus.Cancelled,
        }.CreateCommand(
            db.Database.GetDbConnection(),
            null,
            $"""
            SELECT "TenantId",
                   bool_or("Status" = @succeeded) AS completed,
                   count(*) FILTER (WHERE "Status" <> @cancelled) AS attempts,
                   array_agg("SweepId") FILTER (WHERE "Status" = @started) AS started
            FROM {sweepRun}
            WHERE "TriggerKind" = @scheduled AND "DryRun" = @dryRun AND "StartedAt" >= @occurrence
            GROUP BY "TenantId"
            """
        );
        var passes = new Dictionary<Guid, (bool Completed, long Attempts)>();
        var started = new List<Guid>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            passes[reader.GetGuid(0)] = (reader.GetBoolean(1), reader.GetInt64(2));
            if (!reader.IsDBNull(3))
            {
                started.AddRange(reader.GetFieldValue<Guid[]>(3));
            }
        }

        return new OccurrenceLedger(passes, started);
    }

    private async Task RunPassAsync(PlannedPass pass, bool dryRun, CancellationToken ct)
    {
        switch (pass)
        {
            case SweepPass sweep:
                await RunSweepPassAsync(sweep, dryRun, ct);
                break;
            case RefusedPass refused:
                await RecordRefusedPassAsync(refused, dryRun, ct);
                break;
        }
    }

    private async Task RunSweepPassAsync(SweepPass pass, bool dryRun, CancellationToken ct)
    {
        var tenant = pass.Tenant;
        var mode = dryRun ? "dry run" : "sweep";
        try
        {
            var result = await scopeFactory.RunInScopeAsync(
                RetentionExecutionScopes.For(tenant),
                services => services.GetRequiredService<RetentionSweepEngine>().RunAsync(
                    tenant,
                    null,
                    SweepTriggerKind.Scheduled,
                    pass.Scope,
                    dryRun,
                    ct
                ),
                ct
            );

            logger.LogInformation(
                "Cohort worker completed {Mode} for {Pass} with {EntityCount} entity counts.",
                mode,
                Describe(tenant),
                result.Counts.Count
            );
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Cohort worker {Mode} failed for {Pass}; continuing with remaining passes.",
                mode,
                Describe(tenant)
            );
        }
    }

    private async Task RecordRefusedPassAsync(RefusedPass pass, bool dryRun, CancellationToken ct)
    {
        try
        {
            // The run belongs to the tenant, so it is written as that tenant. The run logs the
            // refusal, with the tenant and what to do about it.
            await scopeFactory.RunInScopeAsync(
                RetentionExecutionScopes.ForTenant(pass.TenantId),
                services => services.GetRequiredService<RetentionSweepEngine>()
                    .RecordRefusedPassAsync(pass.TenantId, pass.Refusal, dryRun, ct),
                ct
            );
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Cohort worker could not record the refused pass for tenant {TenantId}; continuing with remaining passes.",
                pass.TenantId
            );
        }
    }

    private static string Describe(TenantContext tenant) =>
        tenant.IsTenantless ? "the tenantless pass" : $"tenant {tenant.Id}";

    /// <summary>
    /// The source's tenants once each, in source order, and the ids it returned with
    /// conflicting contexts, which are left out of the tenants. A null entry can't be
    /// identified, so it fails the iteration, which the schedule loop retries.
    /// </summary>
    private static (IReadOnlyList<TenantContext> Tenants, IReadOnlyList<Guid> Conflicting) MaterializeTenants(
        IReadOnlyList<TenantContext> source
    )
    {
        ArgumentNullException.ThrowIfNull(source);

        var firstById = new Dictionary<Guid, TenantContext>();
        var tenants = new List<TenantContext>(source.Count);
        var conflicting = new List<Guid>();
        foreach (var tenant in source)
        {
            if (tenant is null)
            {
                throw new InvalidOperationException(
                    "IRetentionTenantSource returned a null tenant context."
                );
            }

            if (firstById.TryAdd(tenant.Id, tenant))
            {
                tenants.Add(tenant);
            }
            else if (!ContextsMatch(firstById[tenant.Id], tenant) && !conflicting.Contains(tenant.Id))
            {
                conflicting.Add(tenant.Id);
            }
        }

        tenants.RemoveAll(tenant => conflicting.Contains(tenant.Id));
        return (tenants, conflicting);
    }

    private static bool ContextsMatch(TenantContext first, TenantContext duplicate)
    {
        return string.Equals(first.Jurisdiction, duplicate.Jurisdiction, StringComparison.Ordinal)
            && first.Tags.Count == duplicate.Tags.Count
            && first.Tags.All(pair =>
                duplicate.Tags.TryGetValue(pair.Key, out var value)
                && string.Equals(pair.Value, value, StringComparison.Ordinal)
            );
    }

    private static async Task DelayUntilNextPollAsync(CancellationToken ct)
    {
        try
        {
            await Task.Delay(IdlePollInterval, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutdown during an idle wait is not an error.
        }
    }

    /// <summary>The occurrence an iteration settled, and of which kind of run.</summary>
    internal sealed record SettledOccurrence(DateTimeOffset Occurrence, bool DryRun);

    /// <summary>
    /// Picks the occurrence an iteration catches up, on the coordination connection: the one
    /// owed (null when there is none, or it is already settled), the latest at or before now,
    /// and when the next falls on the replica's clock (null when unknown or never).
    /// </summary>
    private delegate Task<(DateTimeOffset? Due, DateTimeOffset? Occurrence, DateTimeOffset? NextWake)> ChooseOccurrence(
        DbContext db,
        CancellationToken ct
    );

    /// <summary>One pass of an occurrence, keyed in the ledger by its tenant.</summary>
    private abstract record PlannedPass(Guid TenantId);

    private sealed record SweepPass(TenantContext Tenant, SweepEntityScope Scope) : PlannedPass(Tenant.Id);

    private sealed record RefusedPass(Guid TenantId, RetentionPassRefusal Refusal) : PlannedPass(TenantId);

    private sealed class OccurrenceLedger(
        IReadOnlyDictionary<Guid, (bool Completed, long Attempts)> passes,
        IReadOnlyList<Guid> startedSweepIds
    )
    {
        /// <summary>The occurrence's runs still <c>Started</c>, live or orphaned.</summary>
        public IReadOnlyList<Guid> StartedSweepIds => startedSweepIds;

        public bool IsCompleted(Guid tenantId) => passes.TryGetValue(tenantId, out var pass) && pass.Completed;

        /// <summary>Neither succeeded nor out of attempts.</summary>
        public bool IsPending(Guid tenantId, int maxAttempts) =>
            !passes.TryGetValue(tenantId, out var pass) || (!pass.Completed && pass.Attempts < maxAttempts);
    }
}

/// <summary>
/// What one iteration left of its occurrence: the outcome, the latest occurrence at or before
/// the database clock (null when unknown or there is none), and when the next one falls on the
/// replica's clock (null when unknown or there is none).
/// </summary>
internal sealed record IterationResult(OccurrenceOutcome Outcome, DateTimeOffset? Occurrence, DateTimeOffset? NextWake);

/// <summary>What one iteration left of its occurrence.</summary>
internal enum OccurrenceOutcome
{
    /// <summary>Every pass succeeded or used up its attempts, or nothing was owed.</summary>
    Settled,

    /// <summary>A pass may still succeed: retry after the retry delay.</summary>
    Pending,

    /// <summary>
    /// Another replica holds the worker lock, or a run of the occurrence is still live:
    /// check again after the retry delay.
    /// </summary>
    LockBusy,

    /// <summary>The kill switch stopped the iteration: the loop decides what comes next.</summary>
    Stopped,
}
