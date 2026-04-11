namespace Cohort.Domain;

internal static class CutoffCalculator
{
    public static DateTimeOffset Compute(DateTimeOffset now, TimeSpan period, TimeSpan? legalMin)
    {
        var effective = legalMin is { } min && min > period ? min : period;
        return now - effective;
    }
}
