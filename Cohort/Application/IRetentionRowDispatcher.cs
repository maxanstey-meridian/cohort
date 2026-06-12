namespace Cohort.Application;

public interface IRetentionRowDispatcher
{
    /// <summary>
    /// Drains every currently dispatchable handler row to a terminal state, ignoring
    /// retry backoff: a failing handler is retried back-to-back within this one call
    /// until it succeeds or exhausts MaxAttempts and dead-letters. Work the call cannot
    /// dispatch is left untouched and reported on the result: rows claimed by another
    /// dispatcher whose lease has not expired, and AfterSweepSettled rows whose sweep
    /// run has not settled. Callers that need full settlement should re-flush until
    /// <see cref="RowDispatcherFlushResult.Settled"/> is true.
    /// </summary>
    public Task<RowDispatcherFlushResult> FlushAsync(CancellationToken ct = default);
}

/// <summary>
/// What a flush left behind. <paramref name="InFlightRemaining"/> counts rows another
/// dispatcher still holds under a live claim lease; <paramref name="PendingRemaining"/>
/// counts rows the flush could not dispatch (deferred-phase rows whose sweep has not
/// settled, or rows queued behind an in-flight sibling).
/// </summary>
public sealed record RowDispatcherFlushResult(int InFlightRemaining, int PendingRemaining)
{
    public bool Settled => InFlightRemaining == 0 && PendingRemaining == 0;
}
