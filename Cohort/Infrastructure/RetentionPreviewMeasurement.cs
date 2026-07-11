using System.Data.Common;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;

namespace Cohort.Infrastructure;

internal static class RetentionPreviewMeasurement
{
    internal static async Task<(long Affected, long HeldCount, long NullAnchorCount)> MeasureAsync(
        IRetentionSweepStrategy strategy,
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext context,
        DbConnection connection,
        CancellationToken ct
    )
    {
        var affected = await strategy.PreviewAsync(entry, rule, context, connection, ct);
        var heldCount = await strategy.CountHeldAsync(entry, rule, context, connection, ct);
        var nullAnchorCount = await strategy.CountNullAnchorsAsync(
            entry,
            rule,
            context,
            connection,
            ct
        );
        return (affected, heldCount, nullAnchorCount);
    }
}
