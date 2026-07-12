namespace Cohort.Application;

/// <summary>
/// Receives best-effort notifications after Cohort audit events have committed.
/// Delivery is not durable; use the authoritative audit tables, CDC, or a host-owned
/// outbox when guaranteed export is required.
/// </summary>
public interface IRetentionAuditObserver
{
    public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct);
}
