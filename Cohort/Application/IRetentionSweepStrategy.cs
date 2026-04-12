using System.Data.Common;

using Cohort.Domain;

namespace Cohort.Application;

public interface IRetentionSweepStrategy
{
    public Strategy HandlesStrategy { get; }

    public Task<int> SweepAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        CancellationToken ct
    );
}
