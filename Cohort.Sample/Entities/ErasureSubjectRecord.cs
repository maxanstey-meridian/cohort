using Cohort.Domain;

namespace Cohort.Sample.Entities;

[ExemptFromRetention("Sample erasure subject fixture")]
public sealed class ErasureSubjectRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject]
    public Guid? SubjectId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
    public string Body { get; set; } = "";
}
