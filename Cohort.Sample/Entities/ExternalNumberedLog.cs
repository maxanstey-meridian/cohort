using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("tenantless-purge", nameof(CreatedAt))]
[RetentionEntityId("d0991164-8823-4f4e-aac1-f9d8d1753764")]
[RetentionTenantless]
public sealed class ExternalNumberedLog
{
    public Guid Id { get; set; }

    [RetentionRecordId]
    public int ExternalId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
    public string Payload { get; set; } = "";
}
