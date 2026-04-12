using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("soft-delete", nameof(CreatedAt))]
public sealed class SoftDeleteRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Body { get; set; } = "";
    public bool IsDeleted { get; set; }
    public DateTimeOffset? DeletedAt { get; set; }
}
