namespace Cohort.Domain;

public sealed record RetentionHold(
    Guid HoldId,
    Guid RetentionEntityId,
    string RecordId,
    Guid? TenantId,
    string Reason,
    DateTimeOffset CreatedAt,
    DateTimeOffset? ExpiresAt,
    DateTimeOffset? RemovedAt
);
