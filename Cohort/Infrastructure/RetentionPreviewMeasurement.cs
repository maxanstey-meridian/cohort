using System.Data.Common;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;

namespace Cohort.Infrastructure;

internal static class RetentionPreviewMeasurement
{
    internal static async Task<(long Affected, long HeldCount, long NullAnchorCount)> MeasureAsync(
        SweepStrategy strategy,
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext context,
        DbConnection connection,
        CancellationToken ct
    )
    {
        var scope = SweepScope.ForSweep(entry, rule, context);
        return (
            await strategy.CountAsync(scope, SweepCount.Eligible, connection, ct),
            await strategy.CountAsync(scope, SweepCount.Held, connection, ct),
            await strategy.CountAsync(scope, SweepCount.NullAnchor, connection, ct)
        );
    }
}
