namespace Cohort.Domain;

public sealed record RetentionHoldRequest(
    Guid HoldId,
    string TableName,
    Guid RecordId,
    Guid TenantId,
    string Reason,
    DateTimeOffset CreatedAt,
    DateTimeOffset? ExpiresAt = null
);
