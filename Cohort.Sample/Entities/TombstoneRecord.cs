using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("tombstone-anonymise", nameof(CreatedAt))]
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
}
