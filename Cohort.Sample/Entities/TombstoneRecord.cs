using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("tombstone-anonymise", nameof(CreatedAt))]
[RetentionEntityId("6ebbc096-d3b8-4077-8f21-bf9b4d53c869")]
public sealed class TombstoneRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject]
    public Guid? SubjectId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    [AnonymiseWith(typeof(GuidTombstoneFactory))]
    public Guid ExternalId { get; set; }

    [AnonymiseWith(typeof(OriginalValueTombstoneFactory))]
    public string DisplayName { get; set; } = "";

    [Anonymise(AnonymiseMethod.Null)]
    public string? ContactEmail { get; set; }

    public string Notes { get; set; } = "";

    public DateTimeOffset? AnonymisedAt { get; set; }
}
