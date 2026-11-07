namespace Cohort.Application;

/// <summary>
/// Recovers abandoned runs: a run left Started longer than <c>Cohort:SweepSettleTimeout</c>
/// whose owner is gone (no process still holds its run lock) is settled as Failed, so it no
/// longer reads as in progress and history pruning can delete it. Cohort's background service
/// does this once a minute; a host that runs no hosted services calls this instead.
/// </summary>
public interface IRetentionRunRecovery
{
    /// <summary>
    /// Recovers every abandoned run now. An explicit call ignores the kill switch.
    /// </summary>
    public Task RecoverAsync(CancellationToken ct = default);
}
