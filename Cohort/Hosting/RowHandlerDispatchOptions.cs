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
    /// dispatch pass, and the reclaim counts as an attempt. Must be at least 30 seconds
    /// to avoid reclaiming work that is genuinely still running.
    /// </summary>
    public TimeSpan ClaimTimeout { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// A run left Started longer than this is recovered after Cohort proves that no
    /// process still owns its run lock. Must be at least 1 minute.
    /// </summary>
    public TimeSpan SweepSettleTimeout { get; init; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Backstop retention for captured row snapshots. CapturedPayload can contain
    /// pre-anonymisation personal data; it is cleared as soon as every handler for the
    /// row reaches a terminal state, and any stragglers are scrubbed once older than
    /// this. Must be at least 1 hour.
    /// </summary>
    public TimeSpan PayloadRetention { get; init; } = TimeSpan.FromDays(30);
}
