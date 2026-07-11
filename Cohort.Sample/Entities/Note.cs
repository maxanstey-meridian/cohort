using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("short-lived", nameof(CreatedAt))]
[RetentionEntityId("a3f467fe-c5d0-4f17-9897-83c373cc1dc8")]
public sealed class Note
{
    public static readonly Guid RetentionIdentity = Guid.Parse(
        "a3f467fe-c5d0-4f17-9897-83c373cc1dc8"
    );

    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject]
    public Guid? SubjectId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Body { get; set; } = "";
}
