namespace Cohort.Domain;

public sealed record RetentionRule(
    TimeSpan Period,
    Strategy Strategy,
    TimeSpan? LegalMin = null,
    AuditRowDetail AuditRowDetail = AuditRowDetail.SummaryOnly
);
