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
/// occurrence at or before now. A sweep covers everything due at any earlier occurrence, so
/// only that one ever needs catching up. An occurrence is settled once every pass in its plan
/// has a <c>Succeeded</c> run since it, or has used up <see cref="CohortOptions.OccurrenceMaxAttempts"/>;
/// until then the worker retries the unfinished passes <see cref="CohortOptions.OccurrenceRetryDelay"/>
/// apart. Every answer comes from the <c>sweep_run</c> ledger, read under the worker lock.
/// </summary>
internal sealed class RetentionWorker(
    IServiceScopeFactory scopeFactory,
    CohortOptionsSnapshot options,
    IOptionsMonitor<CohortOptions> optionsMonitor,
    ILogger<RetentionWorker> logger
) : BackgroundService
{
    private static readonly TimeSpan IdlePollInterval = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan NotDueRecheckInterval = TimeSpan.FromSeconds(1);

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
        if (current.KillSwitch || string.IsNullOrWhiteSpace(current.Schedule))
        {
            await DelayUntilNextPollAsync(stoppingToken);
            return settled;
        }

        var now = DateTimeOffset.UtcNow;
        DateTimeOffset? previous;
        DateTimeOffset? next;
        try
        {
            previous = CohortScheduleParser.GetPreviousOccurrence(current.Schedule, now);
            next = CohortScheduleParser.GetNextOccurrence(current.Schedule, now);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Cohort worker schedule is invalid at runtime.");
            await DelayUntilNextPollAsync(stoppingToken);
            return settled;
        }

        // Catch-up applies on a fresh install too: sweeping later than scheduled is never
        // early, and the schedule only sets when the load lands.
        if (previous is not { } occurrence || settled == new SettledOccurrence(occurrence, current.DryRun))
        {
            if (next is null)
            {
                await DelayUntilNextPollAsync(stoppingToken);
            }
            else
            {
                await SleepUntilAsync(next.Value, current, stoppingToken);
            }
            return settled;
        }

        OccurrenceOutcome outcome;
        try
        {
            outcome = await RunIterationAsync(occurrence, current.DryRun, stoppingToken);
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
                "Cohort worker iteration for occurrence {Occurrence} failed. The worker stays alive and retries it after {RetryDelay}, or at the next occurrence if that comes first.",
                occurrence,
                current.OccurrenceRetryDelay
            );
            outcome = OccurrenceOutcome.Pending;
        }

        if (outcome == OccurrenceOutcome.Settled)
        {
            return new SettledOccurrence(occurrence, current.DryRun);
        }

        // A newer occurrence supersedes this one, so never sleep past it.
        var retryAt =
            DateTimeOffset.UtcNow
            + (outcome == OccurrenceOutcome.NotDue ? NotDueRecheckInterval : current.OccurrenceRetryDelay);
        await SleepUntilAsync(next is { } upcoming && upcoming < retryAt ? upcoming : retryAt, current, stoppingToken);
        return settled;
    }

    /// <summary>
    /// Sleeps until <paramref name="wakeAt"/>, or until the kill switch, schedule or dry-run
    /// setting changes from <paramref name="basis"/>, so turning the kill switch off catches
    /// up straight away.
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

    // The iteration scope coordinates across tenants: the worker lock, the occurrence ledger
    // and the tenant source. Each pass runs in its own scope, as its tenant, inside this work.
    // The kill switch cancels the iteration through the same path as shutdown: the batch in
    // flight rolls back, its run settles Cancelled, and no later pass reaches Started.
    internal async Task<OccurrenceOutcome> RunIterationAsync(
        DateTimeOffset occurrence,
        bool dryRun,
        CancellationToken ct
    )
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var registration = optionsMonitor.OnChange(updated =>
        {
            if (updated.KillSwitch)
            {
                stop.Cancel();
            }
        });
        // Closes the race with a switch flipped before the registration existed.
        if (options.Current.KillSwitch)
        {
            stop.Cancel();
        }

        try
        {
            return await scopeFactory.RunInScopeAsync(
                new RetentionExecutionContext.System(RetentionSystemWork.WorkerCoordination),
                services => RunIterationAsync(services, occurrence, dryRun, stop.Token),
                stop.Token
            );
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            logger.LogInformation(
                "Cohort worker kill switch engaged; stopped the pass in progress and skipped the rest of this iteration."
            );
            return OccurrenceOutcome.Pending;
        }
    }

    private async Task<OccurrenceOutcome> RunIterationAsync(
        IServiceProvider services,
        DateTimeOffset occurrence,
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
            // Runs are stamped by the database clock, so an occurrence is due only once that
            // clock reaches it. A replica whose clock runs ahead would otherwise sweep early,
            // and its runs, started before the occurrence, would never count towards it.
            if (await DatabaseClock.NowAsync(db, ct) < occurrence)
            {
                return OccurrenceOutcome.NotDue;
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
                return OccurrenceOutcome.LockBusy;
            }

            return await RunLockedIterationAsync(services, db, occurrence, dryRun, ct);
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

        foreach (var pass in plan)
        {
            if (!ledger.IsPending(pass.TenantId, maxAttempts))
            {
                continue;
            }

            await RunPassAsync(pass, dryRun, ct);
        }

        // Read back rather than infer: a pass that failed before its run existed used no attempt.
        ledger = await LoadLedgerAsync(db, occurrence, dryRun, ct);
        var exhausted = plan
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
        // real sweep. Every scheduled run is written under the worker lock, which this worker
        // holds, so a run still Started lost its owner. Cancelled runs (the kill switch, or a
        // shutdown) use no attempt.
        var sweepRun = PostgreSqlIdentifier.Format(CohortStoreTables.FromModel(db.Model).SweepRun);
        await using var command = new SqlParams
        {
            ["scheduled"] = (int)SweepTriggerKind.Scheduled,
            ["occurrence"] = occurrence,
            ["dryRun"] = dryRun,
            ["succeeded"] = (int)SweepRunStatus.Succeeded,
            ["cancelled"] = (int)SweepRunStatus.Cancelled,
        }.CreateCommand(
            db.Database.GetDbConnection(),
            null,
            $"""
            SELECT "TenantId",
                   bool_or("Status" = @succeeded) AS completed,
                   count(*) FILTER (WHERE "Status" <> @cancelled) AS attempts
            FROM {sweepRun}
            WHERE "TriggerKind" = @scheduled AND "DryRun" = @dryRun AND "StartedAt" >= @occurrence
            GROUP BY "TenantId"
            """
        );
        var passes = new Dictionary<Guid, (bool Completed, long Attempts)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            passes[reader.GetGuid(0)] = (reader.GetBoolean(1), reader.GetInt64(2));
        }

        return new OccurrenceLedger(passes);
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
        logger.LogError(
            "Cohort worker refused the pass for tenant {TenantId} ({Refusal}) and records it as a Failed scheduled run.",
            pass.TenantId,
            pass.Refusal
        );
        try
        {
            // The run belongs to the tenant, so it is written as that tenant.
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

    private sealed record SettledOccurrence(DateTimeOffset Occurrence, bool DryRun);

    /// <summary>One pass of an occurrence, keyed in the ledger by its tenant.</summary>
    private abstract record PlannedPass(Guid TenantId);

    private sealed record SweepPass(TenantContext Tenant, SweepEntityScope Scope) : PlannedPass(Tenant.Id);

    private sealed record RefusedPass(Guid TenantId, RetentionPassRefusal Refusal) : PlannedPass(TenantId);

    private sealed class OccurrenceLedger(IReadOnlyDictionary<Guid, (bool Completed, long Attempts)> passes)
    {
        public bool IsCompleted(Guid tenantId) => passes.TryGetValue(tenantId, out var pass) && pass.Completed;

        /// <summary>Neither succeeded nor out of attempts.</summary>
        public bool IsPending(Guid tenantId, int maxAttempts) =>
            !passes.TryGetValue(tenantId, out var pass) || (!pass.Completed && pass.Attempts < maxAttempts);
    }
}

/// <summary>What one iteration left of its occurrence.</summary>
internal enum OccurrenceOutcome
{
    /// <summary>Every pass succeeded or used up its attempts.</summary>
    Settled,

    /// <summary>A pass may still succeed: retry after the retry delay.</summary>
    Pending,

    /// <summary>Another replica holds the worker lock: check again after the retry delay.</summary>
    LockBusy,

    /// <summary>The database clock hasn't reached the occurrence yet: check again shortly.</summary>
    NotDue,
}
