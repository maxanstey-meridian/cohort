namespace Cohort.Domain;

public sealed record RetentionRule(TimeSpan Period, TimeSpan? LegalMin = null);
