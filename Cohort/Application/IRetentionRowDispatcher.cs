namespace Cohort.Application;

public interface IRetentionRowDispatcher
{
    /// <summary>
    /// Runs every handler row that is due now, honouring retry backoff: a failed attempt
    /// waits out its backoff like it would under the background poller, so one flush during
    /// an outage spends one attempt per row, not the whole retry budget. Work the call cannot
    /// dispatch is left untouched and reported on the result: rows waiting out their
    /// backoff, rows claimed by a dispatcher that is still running them, and
    /// AfterSweepSettled rows whose sweep run has not settled. A flush runs only work that is
    /// due, so looping on it until <see cref="RowDispatcherFlushResult.Settled"/> spins through
    /// every backoff wait; a caller that needs a final state waits out the backoff, or, in
    /// tests, sets a short <c>BaseBackoff</c> and <c>MaxBackoff</c> or <c>MaxAttempts</c> of 1.
    /// Not affected by the kill switch, which stops only the background poller.
    /// </summary>
    public Task<RowDispatcherFlushResult> FlushAsync(CancellationToken ct = default);

    /// <summary>
    /// Requeues dead-lettered handler work whose captured snapshot is still retained (it is
    /// kept until <c>RowHandlerDispatch:PayloadRetention</c>). Every dead-lettered handler of
    /// each matching row is requeued, so the row's handlers still run in order. A requeue
    /// resumes each handler's remaining attempts, or gives one more delivery to a handler that
    /// used them all: its <c>Attempt</c> count carries on, so <c>ctx.Attempt</c> still tells it
    /// the work may already be done. Not affected by the kill switch.
    /// </summary>
    /// <param name="sweepId">Only rows from this run, or every run when null.</param>
    /// <param name="handlerId">
    /// Only rows on which the handler with this <c>[RowHandlerId]</c> dead-lettered, or
    /// every row when null. The row's other dead-lettered handlers are requeued with it.
    /// </param>
    /// <param name="ct">Cancels the requeue.</param>
    /// <returns>The number of handler statuses requeued.</returns>
    public Task<long> RequeueDeadLetteredAsync(
        Guid? sweepId = null,
        Guid? handlerId = null,
        CancellationToken ct = default
    );
}

/// <summary>
/// What a flush left behind. <paramref name="InFlightRemaining"/> counts rows a dispatcher
/// still holds under a claim; <paramref name="PendingRemaining"/> counts rows the flush could
/// not dispatch (rows waiting out their retry backoff, deferred-phase rows whose sweep has not
/// settled, or rows queued behind an in-flight sibling). <paramref name="DeadLetteredRequeueable"/>
/// counts dead-lettered handler work whose snapshot is still retained, so
/// <see cref="IRetentionRowDispatcher.RequeueDeadLetteredAsync"/> can run it again.
/// </summary>
public sealed record RowDispatcherFlushResult(
    long InFlightRemaining,
    long PendingRemaining,
    long DeadLetteredRequeueable
)
{
    /// <summary>Nothing is left to dispatch. Dead-lettered work doesn't count.</summary>
    public bool Settled => InFlightRemaining == 0 && PendingRemaining == 0;
}
