namespace Cohort.Hosting;

public sealed class CohortConventions
{
    public string RecordIdPropertyName { get; init; } = "Id";
    public string TenantPropertyName { get; init; } = "TenantId";
    public string SoftDeletePropertyName { get; init; } = "IsDeleted";
    public string DeletedAtPropertyName { get; init; } = "DeletedAt";
}
