using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Sweep;

internal sealed class SoftDeleteSweepStrategy(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    IEnumerable<IRetentionHandlerRegistration> handlerRegistrations,
    IEnumerable<IRetentionDependentsRegistration> dependentsRegistrations,
    RetentionHoldGuard holdGuard,
    IServiceScopeFactory scopeFactory,
    ILogger<SoftDeleteSweepStrategy> logger
) : SweepStrategy(db, handlerRegistrations, dependentsRegistrations, holdGuard, scopeFactory, logger)
{
    public override Strategy HandlesStrategy => Strategy.SoftDelete;

    protected override void Validate(RetentionEntry entry)
    {
        if (entry.SoftDelete is null)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose soft-delete metadata for soft-delete operations."
            );
        }
    }

    protected override SweepMutation CreateMutation(SweepScope scope)
    {
        var entry = scope.Entry;
        var softDelete = entry.SoftDelete!;
        if (softDelete.DeletedAtColumn is null)
        {
            return new(static (_, _) => { });
        }

        var deletedAt = SweepStatements.DeletedAtValue(entry, softDelete, scope.Now);
        return new((parameters, _) => parameters["deletedAt"] = deletedAt);
    }
}
