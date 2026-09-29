using Cohort.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Sweep;

internal sealed class PurgeSweepStrategy(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    IServiceProvider services,
    ILogger<PurgeSweepStrategy> logger
) : SweepStrategy(db, services, logger)
{
    public override Strategy HandlesStrategy => Strategy.Purge;

    protected override string EligibilitySql(RetentionEntry entry) => "";

    protected override SweepMutation CreateMutation(SweepScope scope) =>
        new($"DELETE FROM {PostgreSqlIdentifier.Format(scope.Entry.Table)} AS target", static (_, _) => { });
}
