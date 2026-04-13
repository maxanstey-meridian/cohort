using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("per-row-audit-override", nameof(CreatedAt), AuditRowDetail = AuditRowDetail.PerRow)]
public sealed class PerRowAuditedLog
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Payload { get; set; } = "";
}
