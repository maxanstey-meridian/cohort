namespace Cohort.Domain;

internal static class CutoffCalculator
{
    public static TimeSpan ResolveEffectivePeriod(TimeSpan period, TimeSpan? legalMin)
    {
        return legalMin is { } min && min > period ? min : period;
    }

    public static DateTimeOffset Compute(DateTimeOffset now, TimeSpan period, TimeSpan? legalMin)
    {
        return now - ResolveEffectivePeriod(period, legalMin);
    }
}
