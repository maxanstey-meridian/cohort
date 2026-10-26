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
/// Every answer comes from the <c>sweep_run</c> ledger: the worker lock keeps replicas from
/// planning an occurrence side by side, and each pass re-reads its own ledger under its pass
/// lock, which is what keeps it from running twice.
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
                PlanOccurrence(schedule, dryRun, settled, await DatabaseClock.NowAsync(db, token), DateTimeOffset.UtcNow),
            dryRun,
            ct
        );

    /// <summary>
    /// What one database-clock reading owes: the occurrence at or before it, unless that is
    /// <paramref name="settled"/>, and when the next one falls on the replica's clock, the one
    /// <c>Task.Delay</c> keeps. Runs are stamped by the database clock, so a replica whose own
    /// clock runs ahead or behind still counts each run towards the occurrence it served.
    /// </summary>
    internal static (DateTimeOffset? Due, DateTimeOffset? Occurrence, DateTimeOffset? NextWake) PlanOccurrence(
        string schedule,
        bool dryRun,
        SettledOccurrence? settled,
        DateTimeOffset databaseNow,
        DateTimeOffset replicaNow
    )
    {
        var occurrence = CohortScheduleParser.GetPreviousOccurrence(schedule, databaseNow);
        var next = CohortScheduleParser.GetNextOccurrence(schedule, databaseNow);
        return (
            occurrence is { } due && settled != new SettledOccurrence(due, dryRun) ? due : null,
            occurrence,
            next is { } upcoming ? replicaNow + (upcoming - databaseNow) : null
        );
    }

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
        Exception? primaryException = null;
        try
        {
            var (due, occurrence, nextWake) = await choose(db, ct);
            if (due is not { } owed)
            {
                return new IterationResult(OccurrenceOutcome.Settled, occurrence, nextWake);
            }

            var lockAcquired = await RetentionRunAdvisoryLock.TryAcquireAsync(
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
            // Every session lock goes, not just the worker key: a lock the server granted to a
            // command that was then cancelled would otherwise return to the pool with this
            // connection.
            await OperationalConnectionCleanup.RunAsync(
                db.Database.GetDbConnection(),
                cleanupToken => RetentionRunAdvisoryLock.ReleaseAllAsync(db.Database.GetDbConnection(), cleanupToken),
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
        var ran = new List<PlannedPass>();
        var busy = false;
        foreach (var pass in plan.Where(pass => ledger.IsPending(pass.TenantId, maxAttempts)))
        {
            // The pass lock, not the worker lock, keeps a pass from running twice: this
            // replica's worker-lock session may have been lost (a timeout, a failover) while
            // another took the lock and planned the same passes.
            var scheduled = new ScheduledPass(pass.TenantId, occurrence, dryRun, maxAttempts);
            var outcome = await RunPassAsync(pass, scheduled, ct);
            if (outcome == ScheduledPassOutcome.Busy)
            {
                // Stop rather than run later passes first: tenantless runs after the tenants.
                logger.LogInformation(
                    "Cohort worker stopped this occurrence: another replica is running the pass for {Pass}.",
                    Describe(pass)
                );
                busy = true;
                break;
            }

            // A pass another replica settled meanwhile is that replica's to report.
            if (outcome == ScheduledPassOutcome.Ran)
            {
                ran.Add(pass);
            }
        }

        // Read back rather than infer: a pass that failed before its run existed used no attempt.
        ledger = await LoadLedgerAsync(db, occurrence, dryRun, ct);
        // Only the passes this iteration ran: one exhausted earlier was reported then.
        var exhausted = ran
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

        if (busy)
        {
            return OccurrenceOutcome.LockBusy;
        }

        return plan.Any(pass => ledger.IsPending(pass.TenantId, maxAttempts))
            ? OccurrenceOutcome.Pending
            : OccurrenceOutcome.Settled;
    }

    /// <summary>
    /// The passes one occurrence owes, in run order: each tenant from the source, then each
    /// refused tenant, then the tenantless pass. A tenant the source returned with conflicting
    /// contexts, or one that owns retained rows a sweep could still act on that the source
    /// didn't return, is refused rather than swept under a guessed context. Tenantless tables hold one shared row set; sweeping
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
                services.GetRequiredService<RetentionRuntimeReadinessValidator>().ValidatedCapabilities,
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

    private static Task<OccurrenceLedger> LoadLedgerAsync(
        DbContext db,
        DateTimeOffset occurrence,
        bool dryRun,
        CancellationToken ct
    ) =>
        RetentionRunLedgerSql.LoadOccurrenceAsync(
            db.Database.GetDbConnection(),
            CohortStoreTables.FromModel(db.Model).SweepRun,
            occurrence,
            dryRun,
            tenantId: null,
            ct
        );

    private Task<ScheduledPassOutcome> RunPassAsync(PlannedPass pass, ScheduledPass scheduled, CancellationToken ct) =>
        pass switch
        {
            SweepPass sweep => RunSweepPassAsync(sweep, scheduled, ct),
            RefusedPass refused => RecordRefusedPassAsync(refused, scheduled, ct),
            _ => throw new ArgumentOutOfRangeException(nameof(pass)),
        };

    private async Task<ScheduledPassOutcome> RunSweepPassAsync(SweepPass pass, ScheduledPass scheduled, CancellationToken ct)
    {
        var tenant = pass.Tenant;
        var mode = scheduled.DryRun ? "dry run" : "sweep";
        try
        {
            var (outcome, result) = await scopeFactory.RunInScopeAsync(
                RetentionExecutionScopes.For(tenant),
                services => services.GetRequiredService<RetentionSweepEngine>().RunScheduledPassAsync(
                    tenant,
                    pass.Scope,
                    scheduled,
                    ct
                ),
                ct
            );

            if (result is not null)
            {
                logger.LogInformation(
                    "Cohort worker completed {Mode} for {Pass} with {EntityCount} entity counts.",
                    mode,
                    Describe(tenant),
                    result.Counts.Count
                );
            }

            return outcome;
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
            // The failure is this pass's own: its run, if one started, is in the ledger.
            return ScheduledPassOutcome.Ran;
        }
    }

    private async Task<ScheduledPassOutcome> RecordRefusedPassAsync(RefusedPass pass, ScheduledPass scheduled, CancellationToken ct)
    {
        try
        {
            // The run belongs to the tenant, so it is written as that tenant. The run logs the
            // refusal, with the tenant and what to do about it.
            return await scopeFactory.RunInScopeAsync(
                RetentionExecutionScopes.ForTenant(pass.TenantId),
                services => services.GetRequiredService<RetentionSweepEngine>()
                    .RecordRefusedPassAsync(pass.Refusal, scheduled, ct),
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
            return ScheduledPassOutcome.Ran;
        }
    }

    private static string Describe(TenantContext tenant) =>
        tenant.IsTenantless ? "the tenantless pass" : $"tenant {tenant.Id}";

    private static string Describe(PlannedPass pass) =>
        pass is SweepPass sweep ? Describe(sweep.Tenant) : $"tenant {pass.TenantId}";

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
    /// Another replica holds the worker lock or is running one of this occurrence's passes:
    /// check again after the retry delay.
    /// </summary>
    LockBusy,

    /// <summary>The kill switch stopped the iteration: the loop decides what comes next.</summary>
    Stopped,
}
