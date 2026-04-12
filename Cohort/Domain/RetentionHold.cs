namespace Cohort.Domain;

public sealed record RetentionHold(
    Guid HoldId,
    string TableName,
    Guid RecordId,
    Guid TenantId,
    string Reason,
    DateTimeOffset CreatedAt,
    DateTimeOffset? ExpiresAt,
    DateTimeOffset? RemovedAt
);
