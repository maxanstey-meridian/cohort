namespace Cohort.Domain;

public sealed record RetentionRule(
    TimeSpan Period,
    Strategy Strategy,
    TimeSpan? LegalMin = null,
    AuditRowDetail AuditRowDetail = AuditRowDetail.SummaryOnly,
    RetentionRuleProvenance? Provenance = null
)
{
    public TimeSpan Period { get; init; } = RequireNonNegative(Period, nameof(Period));

    public TimeSpan? LegalMin { get; init; } =
        LegalMin is { } legalMin ? RequireNonNegative(legalMin, nameof(LegalMin)) : null;

    private static TimeSpan RequireNonNegative(TimeSpan value, string paramName)
    {
        if (value < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                paramName,
                value,
                $"{paramName} must be non-negative. A negative retention period computes a cutoff in the future, which would treat every row — including rows that are not yet expired — as eligible for the sweep."
            );
        }

        return value;
    }
}
