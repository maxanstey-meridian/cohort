using Cohort.Domain;

namespace Cohort.Sample.Entities;

[ExemptFromRetention("Cohort legal hold storage")]
public sealed class HeldRecord
{
    public Guid HoldId { get; set; }
    public string TableName { get; set; } = "";
    public Guid RecordId { get; set; }
    public Guid TenantId { get; set; }
    public string Reason { get; set; } = "";
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? ExpiresAt { get; set; }
    public DateTimeOffset? RemovedAt { get; set; }
}
