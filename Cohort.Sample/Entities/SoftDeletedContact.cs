using Cohort.Domain;

namespace Cohort.Sample.Entities;

/// <summary>
/// A contact whose category may resolve to soft delete or to anonymise, so it carries both
/// markers: a row is retired only once every strategy its category declares has acted on it.
/// </summary>
[Retain("soft-delete-or-anonymise", nameof(CreatedAt))]
[RetentionEntityId("ecf152c9-4629-4973-926e-42950cad0fd1")]
public sealed class SoftDeletedContact
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }

    [Anonymise(AnonymiseMethod.Null)]
    public string? EmailAddress { get; set; }

    public bool IsDeleted { get; set; }
    public DateTimeOffset? DeletedAt { get; set; }
    public DateTimeOffset? AnonymisedAt { get; set; }
}
