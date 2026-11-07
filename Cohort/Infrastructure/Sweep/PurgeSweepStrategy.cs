using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Sweep;

internal sealed class PurgeSweepStrategy(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    IEnumerable<IRetentionDependentsRegistration> dependentsRegistrations,
    RetentionHoldGuard holdGuard,
    IServiceScopeFactory scopeFactory,
    ILogger<PurgeSweepStrategy> logger
) : SweepStrategy(db, dependentsRegistrations, holdGuard, scopeFactory, logger)
{
    public override Strategy HandlesStrategy => Strategy.Purge;

    protected override SweepMutation CreateMutation(SweepScope scope) => new(static (_, _) => { });
}
