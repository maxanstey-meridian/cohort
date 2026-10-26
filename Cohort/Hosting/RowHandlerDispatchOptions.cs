namespace Cohort.Hosting;

public sealed class RowHandlerDispatchOptions
{
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(10);

    public int BatchSize { get; init; } = 50;

    /// <summary>
    /// Deliveries a handler gets before its work is dead-lettered. With the default backoff,
    /// 25 attempts span about 15 hours. A requeue of dead-lettered work gives one more.
    /// </summary>
    public int MaxAttempts { get; init; } = 25;

    public int MaxParallelism { get; init; } = 4;

    /// <summary>
    /// The wait after the first failed attempt. Each later failure doubles it, up to
    /// <see cref="MaxBackoff"/>.
    /// </summary>
    public TimeSpan BaseBackoff { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The longest wait between two attempts. Must be at least <see cref="BaseBackoff"/> and
    /// at most 1 day.
    /// </summary>
    public TimeSpan MaxBackoff { get; init; } = TimeSpan.FromHours(1);

    /// <summary>
    /// How long one delivery may hold its claim. The handler is cancelled when it elapses
    /// and the delivery counts as a failed attempt. A row left InFlight longer than this is
    /// reclaimed only once its dispatcher's session has gone (the process crashed
    /// mid-delivery); a handler that is still running keeps it. The reclaim counts as an
    /// attempt. Must be at least 30 seconds.
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
    /// row succeeds. Dead-lettered work keeps its snapshot so it can be requeued, and any
    /// snapshot older than this is scrubbed. Must be at least 1 hour, and longer than a
    /// handler's whole retry schedule (every backoff wait plus every attempt's
    /// <see cref="ClaimTimeout"/>), so the scrub never ends work that is still retrying.
    /// </summary>
    public TimeSpan PayloadRetention { get; init; } = TimeSpan.FromDays(30);
}
