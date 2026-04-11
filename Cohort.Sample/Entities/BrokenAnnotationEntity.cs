using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("broken-sample", nameof(Body))]
public sealed class BrokenAnnotationEntity
{
    public Guid Id { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Body { get; set; } = "";
}
