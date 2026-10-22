namespace Cohort.Domain;

public sealed record RetentionRule
{
    public RetentionRule(
        RetentionPeriod Period,
        Strategy Strategy,
        RetentionPeriod? LegalMin = null,
        AuditRowDetail AuditRowDetail = AuditRowDetail.SummaryOnly,
        RetentionRuleProvenance? Provenance = null
    )
    {
        this.Period = Period ?? throw new ArgumentNullException(nameof(Period));
        this.Strategy = RequireDefined(Strategy, nameof(Strategy));
        this.LegalMin = LegalMin;
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

    public RetentionPeriod Period { get; }

    public Strategy Strategy { get; }

    public RetentionPeriod? LegalMin { get; }

    public AuditRowDetail AuditRowDetail { get; }

    public RetentionRuleProvenance? Provenance { get; }

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
