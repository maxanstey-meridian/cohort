using Cohort.Domain;

namespace Cohort.Sample.Entities;

[Retain("short-lived", nameof(CreatedAt))]
public sealed class Note
{
    public Guid Id { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Body { get; set; } = "";
}
