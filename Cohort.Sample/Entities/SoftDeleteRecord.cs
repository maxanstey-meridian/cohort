using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("soft-delete", nameof(CreatedAt))]
[RetentionEntityId("6107ff39-bf33-413c-889e-6347c909ba15")]
public sealed class SoftDeleteRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject]
    public Guid? SubjectId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Body { get; set; } = "";
    public bool IsDeleted { get; set; }
    public DateTimeOffset? DeletedAt { get; set; }
}
