namespace Cohort.Sample.Entities;

/// <summary>
/// A stand-in outbox table that pins Cohort's half of the contract: a message written by a
/// dependents step commits with the row it retires and rolls back with it. The sample only
/// deposits: nothing delivers the messages, a job a real host leaves to a maintained outbox.
/// Unretained, so Cohort never sweeps it.
/// </summary>
public sealed class OutboxMessage
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public string Type { get; set; } = "";
    public string Payload { get; set; } = "";
    public DateTimeOffset CreatedAt { get; set; }
}
