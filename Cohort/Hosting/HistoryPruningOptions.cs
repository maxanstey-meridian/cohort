namespace Cohort.Hosting;

/// <summary>
/// Pruning of Cohort's own run history and hold records. Every retention is opt-in: a
/// <c>null</c> retention keeps that history forever, which is the default.
/// </summary>
public sealed class HistoryPruningOptions
{
    /// <summary>
    /// How long a run that settled <c>Succeeded</c> is kept after <c>SettledAt</c>. The run's
    /// entity summaries and row details are deleted with it. <c>null</c> keeps such runs
    /// forever. Must be at least 1 day.
    /// </summary>
    public TimeSpan? SucceededRunRetention { get; init; }

    /// <summary>
    /// How long a run that settled <c>Failed</c>, <c>PartiallyFailed</c> or
    /// <c>Cancelled</c> is kept after <c>SettledAt</c>. A run whose dependents step skipped
    /// rows settles <c>PartiallyFailed</c>. <c>null</c> keeps such runs forever. Must be at
    /// least 1 day.
    /// </summary>
    public TimeSpan? FailedRunRetention { get; init; }

    /// <summary>
    /// How long a hold that stopped protecting (removed or expired, whichever came first)
    /// is kept. <c>null</c> keeps inactive holds forever. Must be at least 1 day.
    /// </summary>
    public TimeSpan? InactiveHoldRetention { get; init; }

    internal static TimeSpan MinimumRetention { get; } = TimeSpan.FromDays(1);

    /// <summary>Whether a retention for settled runs is set.</summary>
    internal bool PrunesRuns => SucceededRunRetention is not null || FailedRunRetention is not null;

    /// <summary>Whether a retention for inactive holds is set.</summary>
    internal bool PrunesHolds => InactiveHoldRetention is not null;

    internal bool Enabled => PrunesRuns || PrunesHolds;
}
