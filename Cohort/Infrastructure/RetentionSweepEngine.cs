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
    IRetentionExecutionSettings options,
    RetentionAuditNotifier auditNotifier,
    ILogger<RetentionSweepEngine> logger
)
{
    private readonly IReadOnlyDictionary<Strategy, SweepStrategy> strategies =
        sweepStrategies.ToDictionary(strategy => strategy.HandlesStrategy);

    /// <summary>
    /// Runs one audited sweep over the scope's entities. A dry run counts what a sweep would
    /// change and writes the same audit trail without mutating anything: the audited
    /// counterpart of <see cref="IRetentionPreview"/>, which writes no audit at all.
    /// </summary>
    public async Task<RetentionSweepResult> RunAsync(
        TenantContext tenant,
        DateTimeOffset now,
        SweepTriggerKind trigger,
        SweepEntityScope scope,
        bool dryRun,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(tenant);
        await readinessValidator.ValidateAsync(ct);

        var run = new RetentionRun(
            db,
            auditWriter,
            auditNotifier,
            logger,
            new SweepEvent.Started(Guid.NewGuid(), DateTimeOffset.UtcNow, trigger, dryRun, tenant.Id),
            options.SweepBatchSize
        );
        await run.ExecuteAsync(
            async () => await run.RunEntitiesAsync(await BuildExecutionPlanAsync(tenant, now, scope, run, ct), strategies, ct),
            ct
        );
        return run.CreateSweepResult();
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
        foreach (var entry in registry.Scan().Values)
        {
            if (
                (scope == SweepEntityScope.TenantedOnly && entry.Tenant is null)
                || (scope == SweepEntityScope.TenantlessOnly && entry.Tenant is not null)
            )
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
                plan.Add(SweepScope.ForSweep(entry, rule, context));
            }
            catch (OperationCanceledException)
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
/// Which retained entities a sweep covers. Every internal caller must choose explicitly
/// so tenantless rows cannot be swept as an accidental side effect of a tenanted pass.
/// </summary>
internal enum SweepEntityScope
{
    TenantedOnly,
    TenantlessOnly,
}
