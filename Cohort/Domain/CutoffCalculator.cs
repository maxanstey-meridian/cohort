namespace Cohort.Domain;

internal static class CutoffCalculator
{
    public static TimeSpan ResolveEffectivePeriod(TimeSpan period, TimeSpan? legalMin)
    {
        return legalMin is { } min && min > period ? min : period;
    }

    public static DateTimeOffset Compute(DateTimeOffset now, TimeSpan period, TimeSpan? legalMin)
    {
        var effectivePeriod = ResolveEffectivePeriod(period, legalMin);
        if (effectivePeriod < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(period),
                effectivePeriod,
                "Effective retention period must be non-negative; refusing to compute a cutoff in the future."
            );
        }

        return now - effectivePeriod;
    }
}
