using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("per-row-audit-override", nameof(CreatedAt), AuditRowDetail = AuditRowDetail.PerRow)]
[RetentionEntityId("42670ee7-c26a-4a2a-a2ab-d9571db7d4f6")]
public sealed class PerRowAuditedLog
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Payload { get; set; } = "";
}
