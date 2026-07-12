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

        return SubtractSaturating(now, effectivePeriod);
    }

    public static TimeSpan ResolveErasureMinimumAge(TimeSpan? legalMin)
    {
        return legalMin is { } minimum && minimum > TimeSpan.Zero
            ? minimum
            : TimeSpan.Zero;
    }

    public static DateTimeOffset? ComputeErasureCutoff(DateTimeOffset now, TimeSpan? legalMin)
    {
        var minimumAge = ResolveErasureMinimumAge(legalMin);
        return minimumAge > TimeSpan.Zero ? SubtractSaturating(now, minimumAge) : null;
    }

    private static DateTimeOffset SubtractSaturating(DateTimeOffset value, TimeSpan duration)
    {
        return duration.Ticks > value.UtcTicks - DateTimeOffset.MinValue.UtcTicks
            ? DateTimeOffset.MinValue
            : value - duration;
    }
}
