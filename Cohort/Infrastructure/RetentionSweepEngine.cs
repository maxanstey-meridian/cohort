using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Audit;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure;

internal sealed class RetentionSweepEngine(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionRegistry registry,
    IRetentionRuleProvider ruleProvider,
    RetentionRuntimeReadinessValidator readinessValidator,
    EfRetentionAuditWriter auditWriter,
    IEnumerable<SweepStrategy> sweepStrategies,
    IEnumerable<IRetentionDependentsRegistration> dependentsRegistrations,
    IRetentionExecutionSettings options,
    RetentionAuditNotifier auditNotifier,
    ILogger<RetentionSweepEngine> logger
)
{
    private readonly IReadOnlyDictionary<Strategy, SweepStrategy> strategies =
        sweepStrategies.ToDictionary(strategy => strategy.HandlesStrategy);

    /// <summary>
    /// Runs one audited, <see cref="SweepTriggerKind.Manual"/> sweep over the scope's entities.
    /// A dry run counts what a sweep would change and writes the same audit trail without
    /// mutating anything: the audited counterpart of <see cref="IRetentionPreview"/>, which
    /// writes no audit at all. <paramref name="at"/> null sweeps as of the database clock; a
    /// real sweep refuses a later time before any run exists.
    /// </summary>
    public async Task<RetentionSweepResult> RunAsync(
        TenantContext tenant,
        DateTimeOffset? at,
        SweepEntityScope scope,
        bool dryRun,
        CancellationToken ct = default
    )
    {
        await readinessValidator.ValidateAsync(ct);
        return await RunSweepAsync(tenant, at, SweepTriggerKind.Manual, scope, dryRun, passLockKey: null, ct);
    }

    /// <summary>
    /// Runs one scheduled pass under its pass lock, unless another replica holds it
    /// (<see cref="ScheduledPassOutcome.Busy"/>) or the ledger, re-read under it, says the pass
    /// succeeded or ran out of attempts meanwhile
    /// (<see cref="ScheduledPassOutcome.AlreadySettled"/>).
    /// The only way a <see cref="SweepTriggerKind.Scheduled"/> sweep is written.
    /// </summary>
    public async Task<(ScheduledPassOutcome Outcome, RetentionSweepResult? Result)> RunScheduledPassAsync(
        TenantContext tenant,
        SweepEntityScope scope,
        ScheduledPass pass,
        CancellationToken ct
    )
    {
        RetentionSweepResult? result = null;
        var outcome = await RunUnderPassLockAsync(
            pass,
            async passLockKey => result = await RunSweepAsync(tenant, null, SweepTriggerKind.Scheduled, scope, pass.DryRun, passLockKey, ct),
            ct
        );
        return (outcome, result);
    }

    /// <summary>
    /// Records a scheduled pass the worker refused as a run that settles Failed with the
    /// refusal's machine code, so the ledger and audit observers show the tenant wasn't swept
    /// and why. Nothing is resolved or mutated. Guarded by the pass lock like a sweep pass, so
    /// two replicas can't both record it and use up two attempts.
    /// </summary>
    public Task<ScheduledPassOutcome> RecordRefusedPassAsync(
        RetentionPassRefusal refusal,
        ScheduledPass pass,
        CancellationToken ct
    ) =>
        RunUnderPassLockAsync(
            pass,
            async passLockKey =>
            {
                var startedAt = await DatabaseClock.NowAsync(db, ct);
                var run = CreateRun(
                    new SweepEvent.Started(Guid.NewGuid(), startedAt, SweepTriggerKind.Scheduled, pass.DryRun, pass.TenantId),
                    passLockKey
                );
                await run.ExecuteRefusalAsync(refusal.MachineCode(), refusal.Describe(pass.TenantId), ct);
            },
            ct
        );

    private async Task<RetentionSweepResult> RunSweepAsync(
        TenantContext tenant,
        DateTimeOffset? at,
        SweepTriggerKind trigger,
        SweepEntityScope scope,
        bool dryRun,
        long? passLockKey,
        CancellationToken ct
    )
    {
        var (now, startedAt) = await DatabaseClock.ResolveRunTimeAsync(db, at, dryRun, ct);
        var run = CreateRun(new SweepEvent.Started(Guid.NewGuid(), startedAt, trigger, dryRun, tenant.Id), passLockKey);
        await run.ExecuteAsync(
            async () => await run.RunEntitiesAsync(await BuildExecutionPlanAsync(tenant, now, scope, run, ct), strategies, dependentsRegistrations, ct),
            ct
        );
        return run.CreateSweepResult();
    }

    private RetentionRun CreateRun(SweepEvent.Started started, long? passLockKey) =>
        new(db, auditWriter, auditNotifier, logger, started, options.SweepBatchSize, passLockKey: passLockKey);

    /// <summary>
    /// Holds the pass's lock on this scope's connection, the one its run writes on, and re-reads
    /// the pass's ledger under it before <paramref name="run"/> starts. Every other replica's
    /// run of the pass would hold the same lock, so a run the ledger still shows Started has
    /// lost its owner, and a pass that settled while this replica waited isn't rerun. The lock
    /// is only ever tried, never waited on, so it can't deadlock with the run or worker locks.
    /// </summary>
    private async Task<ScheduledPassOutcome> RunUnderPassLockAsync(
        ScheduledPass pass,
        Func<long, Task> run,
        CancellationToken ct
    )
    {
        // Readiness refuses a multiplexed connection, which can't hold a session lock.
        await readinessValidator.ValidateAsync(ct);
        var sweepRun = CohortStoreTables.FromModel(db.Model).SweepRun;
        var passLockKey = RetentionRunAdvisoryLock.ScheduledPassKeyFor(sweepRun, pass.TenantId, pass.DryRun);
        await db.Database.OpenConnectionAsync(ct);
        var connection = db.Database.GetDbConnection();
        var locked = false;
        Exception? primaryException = null;
        try
        {
            // Not cancellable: a try never waits, and a cancelled command the server granted
            // would leave the lock unreleased on a pooled session.
            locked = await RetentionRunAdvisoryLock.TryAcquireAsync(connection, passLockKey, CancellationToken.None);
            if (!locked)
            {
                return ScheduledPassOutcome.Busy;
            }

            var ledger = await RetentionRunLedgerSql.LoadOccurrenceAsync(
                connection,
                sweepRun,
                pass.Occurrence,
                pass.DryRun,
                pass.TenantId,
                ct
            );
            if (!ledger.IsPending(pass.TenantId, pass.MaxAttempts))
            {
                return ScheduledPassOutcome.AlreadySettled;
            }

            await run(passLockKey);
            return ScheduledPassOutcome.Ran;
        }
        catch (Exception ex)
        {
            primaryException = ex;
            throw;
        }
        finally
        {
            await OperationalConnectionCleanup.RunAsync(
                connection,
                locked ? cleanupToken => RetentionRunAdvisoryLock.ReleaseAsync(connection, passLockKey, cleanupToken) : null,
                _ => db.Database.CloseConnectionAsync(),
                primaryException,
                logger
            );
        }
    }

    private async Task<IReadOnlyList<SweepScope>> BuildExecutionPlanAsync(
        TenantContext tenant,
        DateTimeOffset now,
        SweepEntityScope scope,
        RetentionRun run,
        CancellationToken ct
    )
    {
        var plan = new List<SweepScope>();
        var timeZone = options.TimeZone;
        foreach (var entry in registry.Scan().Values)
        {
            if (!scope.Includes(entry))
            {
                continue;
            }

            var context = new RetentionResolutionContext(entry.Category, tenant, now);
            try
            {
                var rule = await RetentionRuleProviderResolution.ResolveAsync(
                    ruleProvider,
                    readinessValidator.ValidatedCapabilities,
                    context,
                    ct
                );
                plan.Add(SweepScope.ForSweep(entry, rule, context, timeZone));
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                run.RecordEntityFailure(entry.EntityType, ex, "failed to prepare");
            }
        }

        return plan;
    }
}

/// <summary>
/// One scheduled pass of an occurrence: its tenant (<see cref="Guid.Empty"/> for the
/// tenantless pass), its kind, and the attempts it may use.
/// </summary>
internal sealed record ScheduledPass(Guid TenantId, DateTimeOffset Occurrence, bool DryRun, int MaxAttempts);

/// <summary>What became of a scheduled pass.</summary>
internal enum ScheduledPassOutcome
{
    /// <summary>The pass ran (or recorded its refusal), whatever the run's status.</summary>
    Ran,

    /// <summary>The ledger, re-read under the pass lock, shows it succeeded or used up.</summary>
    AlreadySettled,

    /// <summary>Another replica holds the pass lock: it is running this pass.</summary>
    Busy,
}

/// <summary>
/// Which retained entities a sweep covers. Every internal caller must choose explicitly
/// so tenantless rows cannot be swept as an accidental side effect of a tenanted pass.
/// </summary>
internal enum SweepEntityScope
{
    TenantedOnly,
    TenantlessOnly,
}

internal static class SweepEntityScopes
{
    /// <summary>
    /// The entities a run under <paramref name="tenant"/> covers: tenantless ones for
    /// <see cref="TenantContext.Tenantless"/>, tenanted ones otherwise.
    /// </summary>
    internal static SweepEntityScope For(TenantContext tenant) =>
        tenant.IsTenantless ? SweepEntityScope.TenantlessOnly : SweepEntityScope.TenantedOnly;

    /// <summary>Whether <paramref name="entry"/> is one of the entities the scope covers.</summary>
    internal static bool Includes(this SweepEntityScope scope, RetentionEntry entry) =>
        scope == SweepEntityScope.TenantedOnly ? entry.Tenant is not null : entry.Tenant is null;
}
