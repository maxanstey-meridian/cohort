using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("tenantless-purge", nameof(CreatedAt))]
public sealed class TenantlessLog
{
    public Guid Id { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Payload { get; set; } = "";
}
