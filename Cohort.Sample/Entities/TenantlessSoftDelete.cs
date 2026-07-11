using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("tenantless-softdelete", nameof(CreatedAt))]
[RetentionEntityId("36d4a1a6-f2d8-40a8-84ea-5a062fc82889")]
[RetentionTenantless]
public sealed class TenantlessSoftDelete
{
    public Guid Id { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Payload { get; set; } = "";
    public bool IsDeleted { get; set; }
    public DateTimeOffset? DeletedAt { get; set; }
}
