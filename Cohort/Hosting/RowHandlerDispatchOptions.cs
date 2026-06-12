namespace Cohort.Hosting;

public sealed class RowHandlerDispatchOptions
{
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(10);

    public int BatchSize { get; init; } = 50;

    public int MaxAttempts { get; init; } = 10;

    public int MaxParallelism { get; init; } = 4;

    public TimeSpan BaseBackoff { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Visibility timeout for claimed handler work. A row left InFlight longer than this
    /// (e.g. the process crashed between claiming and completing) is reclaimed by the next
    /// dispatch pass, and the reclaim counts as an attempt. Values below 30 seconds are
    /// clamped up to avoid reclaiming work that is genuinely still running.
    /// </summary>
    public TimeSpan ClaimTimeout { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// How long after a sweep started its AfterSweepSettled handler work is dispatched
    /// even if the run never recorded completion (e.g. the process crashed mid-sweep).
    /// Values below 1 minute are clamped up.
    /// </summary>
    public TimeSpan SweepSettleTimeout { get; init; } = TimeSpan.FromHours(6);

    /// <summary>
    /// Backstop retention for captured row snapshots. CapturedPayload can contain
    /// pre-anonymisation personal data; it is cleared as soon as every handler for the
    /// row reaches a terminal state, and any stragglers are scrubbed once older than
    /// this. Values below 1 hour are clamped up.
    /// </summary>
    public TimeSpan PayloadRetention { get; init; } = TimeSpan.FromDays(30);
}
