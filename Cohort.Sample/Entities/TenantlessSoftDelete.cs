using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("tenantless-softdelete", nameof(CreatedAt))]
public sealed class TenantlessSoftDelete
{
    public Guid Id { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Payload { get; set; } = "";
    public bool IsDeleted { get; set; }
    public DateTimeOffset? DeletedAt { get; set; }
}
