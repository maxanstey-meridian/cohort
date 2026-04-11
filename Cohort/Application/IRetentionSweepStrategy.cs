using System.Data;

using Cohort.Domain;

namespace Cohort.Application;

public interface IRetentionSweepStrategy
{
    public Task<int> SweepAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        IDbConnection conn,
        CancellationToken ct
    );
}
