using Cohort.Domain;

namespace Cohort.Sample.Entities;

[ExemptFromRetention("statutory archive")]
public sealed class ExemptDocument
{
    public Guid Id { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Title { get; set; } = "";
}
