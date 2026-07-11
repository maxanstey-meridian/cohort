using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("tenantless-purge", nameof(CreatedAt))]
[RetentionEntityId("992a65db-d658-4b76-aaf5-b11ca52c4a8f")]
[RetentionTenantless]
public sealed class TenantlessLog
{
    public Guid Id { get; set; }
    public DateTimeOffset CreatedAt { get; set; }

    public string Payload { get; set; } = "";

    [ErasureSubject]
    public Guid? SubjectId { get; set; }
}
