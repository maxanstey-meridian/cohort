using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("anonymise", nameof(CreatedAt))]
public sealed class AnonymisedContact
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }

    [Anonymise(AnonymiseMethod.Null)]
    public string? EmailAddress { get; set; }

    [Anonymise(AnonymiseMethod.EmptyString)]
    public string GivenName { get; set; } = "";

    [Anonymise(AnonymiseMethod.FixedLiteral, "[redacted]")]
    public string Surname { get; set; } = "";

    public string Notes { get; set; } = "";
}
