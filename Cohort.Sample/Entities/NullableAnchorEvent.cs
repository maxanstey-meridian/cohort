using Cohort.Domain;

namespace Cohort.Sample.Entities;

// Dogfoods the NULL-anchor reporting path: rows whose OccurredAt is NULL never match a
// cutoff and are retained indefinitely, so sweeps surface them via NullAnchorCount.
[Retain("nullable-anchor-purge", nameof(OccurredAt))]
[RetentionEntityId("314fd4f7-f771-4b94-ab6e-7fc0a09a6ef5")]
public sealed class NullableAnchorEvent
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset? OccurredAt { get; set; }
    public string Payload { get; set; } = "";
}
