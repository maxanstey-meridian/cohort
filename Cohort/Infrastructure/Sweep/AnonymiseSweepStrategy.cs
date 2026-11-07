using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Sweep;

internal sealed class AnonymiseSweepStrategy(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    AnonymiseAssignmentResolver assignments,
    IEnumerable<IRetentionDependentsRegistration> dependentsRegistrations,
    RetentionHoldGuard holdGuard,
    IServiceScopeFactory scopeFactory,
    ILogger<AnonymiseSweepStrategy> logger
) : SweepStrategy(db, dependentsRegistrations, holdGuard, scopeFactory, logger)
{
    public override Strategy HandlesStrategy => Strategy.Anonymise;

    protected override void Validate(RetentionEntry entry)
    {
        if (entry.AnonymiseFields.Count == 0)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose anonymise metadata for anonymisation."
            );
        }

        // The startup validator only enforces the marker for rules it can resolve at boot.
        // A runtime-resolved Anonymise rule on an entity without it would anonymise the same
        // rows again on every run, because the mutation never takes them out of scope.
        if (entry.AnonymisedAt is null)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} resolves to the Anonymise strategy but has no AnonymisedAt marker (a nullable DateTimeOffset named AnonymisedAt by convention, or marked with [RetentionAnonymisedAt]). Without it anonymisation cannot tell scrubbed rows from pending ones and would anonymise the same rows again on every run."
            );
        }
    }

    protected override SweepMutation CreateMutation(SweepScope scope)
    {
        var entry = scope.Entry;
        var staticValues = assignments.CreateStaticAssignments(entry, scope.Tenant.Id, scope.Now);

        return new SweepMutation(
            (parameters, row) =>
            {
                var values = row is null
                    ? assignments.ToProviderValues(entry, staticValues)
                    : assignments.CreatePerRowAssignmentValues(entry, scope.Tenant, scope.Now, row, staticValues);
                for (var index = 0; index < values.Count; index++)
                {
                    parameters[$"value{index}"] = values[index];
                }
                parameters["anonymisedAt"] = scope.Now;
            },
            RowByRow: assignments.RequiresPerRowExecution(entry)
        );
    }
}
