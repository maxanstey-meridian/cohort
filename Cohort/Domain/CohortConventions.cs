namespace Cohort.Domain;

/// <summary>
/// Property names Cohort resolves by convention when no marker attribute
/// (<see cref="RetentionRecordIdAttribute"/>, <see cref="RetentionTenantAttribute"/>, ...) is present.
/// </summary>
public sealed class CohortConventions
{
    public string RecordIdPropertyName { get; init; } = "Id";
    public string TenantPropertyName { get; init; } = "TenantId";
    public string SoftDeletePropertyName { get; init; } = "IsDeleted";
    public string DeletedAtPropertyName { get; init; } = "DeletedAt";
    public string AnonymisedAtPropertyName { get; init; } = "AnonymisedAt";
}
