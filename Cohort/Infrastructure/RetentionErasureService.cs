using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Audit;
using Cohort.Infrastructure.Sweep;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure;

internal sealed class RetentionErasureService(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionRegistry registry,
    IRetentionRuleProvider ruleProvider,
    RetentionRuntimeReadinessValidator readinessValidator,
    RetentionValidationState validationState,
    EfRetentionAuditWriter auditWriter,
    RetentionAuditNotifier auditNotifier,
    IEnumerable<SweepStrategy> sweepStrategies,
    IRetentionExecutionSettings options,
    ILogger<RetentionErasureService> logger
)
{
    private readonly IReadOnlyDictionary<Strategy, SweepStrategy> strategies =
        sweepStrategies.ToDictionary(strategy => strategy.HandlesStrategy);

    public async Task<ErasureResult> EraseAsync(
        TenantContext tenant,
        ErasureScope scope,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        // Npgsql only writes UTC DateTimeOffsets to timestamptz.
        now = now.ToUniversalTime();
        await readinessValidator.ValidateAsync(ct);
        ValidateSubject(scope);

        var run = new RetentionRun(
            db,
            auditWriter,
            auditNotifier,
            logger,
            new SweepEvent.Started(
                Guid.NewGuid(),
                DateTimeOffset.UtcNow,
                SweepTriggerKind.Erasure,
                scope.DryRun,
                tenant.Id,
                scope.Kind
            ),
            options.SweepBatchSize
        );
        // Built before the run exists: a request refused by policy leaves no attempted run.
        var plan = await BuildExecutionPlanAsync(tenant, scope, now, run, ct);
        await run.ExecuteAsync(() => run.RunEntitiesAsync(plan, strategies, ct), ct);
        return run.CreateErasureResult(scope);
    }

    /// <summary>
    /// Refuses, before any run exists, a kind no retained entity declares (it would erase
    /// nothing) and a subject that isn't of its kind's CLR type. Neither message includes
    /// the subject's value.
    /// </summary>
    private void ValidateSubject(ErasureScope scope)
    {
        var kinds = validationState.For(db.Model).ErasureSubjectKinds;
        if (!kinds.TryGetValue(scope.Kind, out var subjectType))
        {
            var declared = kinds.Count == 0
                ? "none"
                : string.Join(", ", kinds.Keys.Order(StringComparer.Ordinal).Select(kind => $"'{kind}'"));
            throw new InvalidOperationException(
                $"Erasure subject kind '{scope.Kind}' isn't declared by any retained entity's [ErasureSubject], so the erasure would match nothing. Declared kinds: {declared}."
            );
        }

        if (!subjectType.IsInstanceOfType(scope.Subject))
        {
            throw new InvalidOperationException(
                $"Erasure subject kind '{scope.Kind}' identifies subjects by {subjectType.Name}, but the scope's subject is a {scope.Subject.GetType().Name}."
            );
        }
    }

    private async Task<IReadOnlyList<SweepScope>> BuildExecutionPlanAsync(
        TenantContext tenant,
        ErasureScope scope,
        DateTimeOffset now,
        RetentionRun run,
        CancellationToken ct
    )
    {
        var plan = new List<SweepScope>();
        foreach (var entry in registry.Scan().Values)
        {
            if (entry.Tenant is null)
            {
                continue;
            }

            var subjectMetadata = validationState
                .For(db.Model)
                .ErasureSubjects[entry.EntityType]
                .FirstOrDefault(metadata => string.Equals(metadata.Kind, scope.Kind, StringComparison.Ordinal));
            if (subjectMetadata is null)
            {
                continue;
            }

            RetentionRule rule;
            try
            {
                rule = await RetentionRuleProviderResolution.ResolveAsync(
                    ruleProvider,
                    readinessValidator.ValidatedCapabilities,
                    new RetentionResolutionContext(entry.Category, tenant, now),
                    ct
                );
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                run.RecordEntityFailure(entry.EntityType, ex, "failed to prepare");
                continue;
            }

            if (rule.Strategy == Strategy.SoftDelete && !scope.AllowSoftDeleteAsErasure)
            {
                throw new InvalidOperationException(
                    $"Erasure for entity {entry.EntityType.FullName} (category '{entry.Category}') resolves to the SoftDelete strategy, which only sets the soft-delete flag and leaves personal data in place. If that genuinely satisfies the erasure request, opt in with new ErasureScope(kind, subject, allowSoftDeleteAsErasure: true)."
                );
            }

            try
            {
                plan.Add(
                    SweepScope.ForErasure(entry, rule, subjectMetadata.CreatePredicate(scope.Subject), tenant, now)
                );
            }
            catch (Exception ex)
            {
                run.RecordEntityFailure(entry.EntityType, ex, "failed to prepare");
            }
        }

        return plan;
    }
}
