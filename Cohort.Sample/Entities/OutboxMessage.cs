namespace Cohort.Sample.Entities;

/// <summary>
/// The sample host's own outbox: a message written by a dependents step commits with the row it
/// retires and rolls back with it. Unretained, so Cohort never sweeps it.
/// </summary>
public sealed class OutboxMessage
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public string Type { get; set; } = "";
    public string Payload { get; set; } = "";
    public DateTimeOffset CreatedAt { get; set; }
}
