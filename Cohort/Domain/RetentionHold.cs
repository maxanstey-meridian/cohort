namespace Cohort.Domain;

public sealed record RetentionHold(
    Guid HoldId,
    string TableName,
    string RecordId,
    Guid TenantId,
    string Reason,
    DateTimeOffset CreatedAt,
    DateTimeOffset? ExpiresAt,
    DateTimeOffset? RemovedAt
);
