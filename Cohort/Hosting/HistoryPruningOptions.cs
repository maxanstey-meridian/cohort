namespace Cohort.Hosting;

/// <summary>
/// Pruning of Cohort's own run history and hold records. Every retention is opt-in: a
/// <c>null</c> retention keeps that history forever, which is the default.
/// </summary>
public sealed class HistoryPruningOptions
{
    /// <summary>
    /// How long a run that settled <c>Succeeded</c>, with all of its row-handler work
    /// succeeded, is kept after <c>SettledAt</c>. The run's entity summaries, row details
    /// and handler statuses are deleted with it. <c>null</c> keeps such runs forever.
    /// Must be at least 1 day.
    /// </summary>
    public TimeSpan? SucceededRunRetention { get; init; }

    /// <summary>
    /// How long a run that settled <c>Failed</c>, <c>PartiallyFailed</c> or
    /// <c>Cancelled</c>, or a succeeded run with dead-lettered row-handler work, is kept
    /// after <c>SettledAt</c>. <c>null</c> keeps such runs forever. Must be at least 1 day.
    /// </summary>
    public TimeSpan? FailedRunRetention { get; init; }

    /// <summary>
    /// How long a hold that stopped protecting (removed or expired, whichever came first)
    /// is kept. <c>null</c> keeps inactive holds forever. Must be at least 1 day.
    /// </summary>
    public TimeSpan? InactiveHoldRetention { get; init; }

    internal static TimeSpan MinimumRetention { get; } = TimeSpan.FromDays(1);

    internal bool Enabled =>
        SucceededRunRetention is not null
        || FailedRunRetention is not null
        || InactiveHoldRetention is not null;
}
