namespace Cohort.Domain;

public sealed record RetentionHoldRequest
{
    public RetentionHoldRequest(
        Guid holdId,
        Guid retentionEntityId,
        string recordId,
        Guid? tenantId,
        string reason,
        DateTimeOffset createdAt,
        DateTimeOffset? expiresAt = null
    )
    {
        HoldId = ValidateHoldId(holdId);
        RetentionEntityId = ValidateId(retentionEntityId, nameof(RetentionEntityId), "Retention entity ID");
        RecordId = ValidateRequired(recordId, nameof(RecordId), "Record ID");
        TenantId = ValidateTenantId(tenantId);
        Reason = ValidateRequired(reason, nameof(Reason), "Reason");
        CreatedAt = createdAt;
        ExpiresAt = ValidateExpiry(expiresAt, createdAt);
    }

    public Guid HoldId { get; }

    public Guid RetentionEntityId { get; }

    public string RecordId { get; }

    public Guid? TenantId { get; }

    public string Reason { get; }

    public DateTimeOffset CreatedAt { get; }

    public DateTimeOffset? ExpiresAt { get; }

    private static string ValidateRequired(string value, string parameterName, string label) =>
        string.IsNullOrWhiteSpace(value)
            ? throw new ArgumentException($"{label} cannot be blank.", parameterName)
            : value;

    private static Guid ValidateHoldId(Guid holdId) =>
        holdId == Guid.Empty
            ? throw new ArgumentException("Hold ID cannot be empty.", nameof(HoldId))
            : holdId;

    private static Guid ValidateId(Guid id, string parameterName, string label) =>
        id == Guid.Empty
            ? throw new ArgumentException($"{label} cannot be empty.", parameterName)
            : id;

    private static Guid? ValidateTenantId(Guid? tenantId) =>
        tenantId == Guid.Empty
            ? throw new ArgumentException("Tenant ID cannot be empty.", nameof(TenantId))
            : tenantId;

    private static DateTimeOffset? ValidateExpiry(
        DateTimeOffset? expiresAt,
        DateTimeOffset createdAt
    ) =>
        expiresAt < createdAt
            ? throw new ArgumentOutOfRangeException(
                nameof(ExpiresAt),
                expiresAt,
                "Expiry cannot be before creation."
            )
            : expiresAt;
}
