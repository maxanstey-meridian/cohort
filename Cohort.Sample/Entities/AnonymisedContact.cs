using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("anonymise", nameof(CreatedAt))]
[RetentionEntityId("fd4a533e-e6a9-44ea-948e-cbf881f35e57")]
public sealed class AnonymisedContact
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject]
    public Guid? SubjectId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }

    [Anonymise(AnonymiseMethod.Null)]
    public string? EmailAddress { get; set; }

    [Anonymise(AnonymiseMethod.EmptyString)]
    public string GivenName { get; set; } = "";

    [Anonymise(AnonymiseMethod.FixedLiteral, "[redacted]")]
    public string Surname { get; set; } = "";

    public string Notes { get; set; } = "";

    public DateTimeOffset? AnonymisedAt { get; set; }
}
