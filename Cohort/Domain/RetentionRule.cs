namespace Cohort.Domain;

public sealed record RetentionRule
{
    public RetentionRule(
        TimeSpan Period,
        Strategy Strategy,
        TimeSpan? LegalMin = null,
        AuditRowDetail AuditRowDetail = AuditRowDetail.SummaryOnly,
        RetentionRuleProvenance? Provenance = null
    )
    {
        this.Period = RequireNonNegative(Period, nameof(Period));
        this.Strategy = RequireDefined(Strategy, nameof(Strategy));
        this.LegalMin = LegalMin is { } legalMin
            ? RequireNonNegative(legalMin, nameof(LegalMin))
            : null;
        this.AuditRowDetail = RequireDefined(AuditRowDetail, nameof(AuditRowDetail));
        if (this.AuditRowDetail == global::Cohort.Domain.AuditRowDetail.Inherit)
        {
            throw new ArgumentOutOfRangeException(
                nameof(AuditRowDetail),
                AuditRowDetail,
                "AuditRowDetail.Inherit is only valid on entity metadata, not a resolved retention rule."
            );
        }

        this.Provenance = Provenance;
    }

    public TimeSpan Period { get; }

    public Strategy Strategy { get; }

    public TimeSpan? LegalMin { get; }

    public AuditRowDetail AuditRowDetail { get; }

    public RetentionRuleProvenance? Provenance { get; }

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

    private static TEnum RequireDefined<TEnum>(TEnum value, string paramName)
        where TEnum : struct, Enum
    {
        if (!Enum.IsDefined(value))
        {
            throw new ArgumentOutOfRangeException(
                paramName,
                value,
                $"{paramName} must be defined."
            );
        }

        return value;
    }
}
