namespace Cohort.Hosting;

public sealed class RowHandlerDispatchOptions
{
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(10);

    public int BatchSize { get; init; } = 50;

    /// <summary>
    /// Deliveries a handler gets before its work is dead-lettered. With the defaults, 25
    /// attempts take up to about 17.5 hours: every backoff wait plus every attempt running its
    /// whole <see cref="ClaimTimeout"/>. A requeue resumes the attempts a handler has left, or
    /// gives one more delivery to a handler that used them all.
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
    /// Backstop retention for captured row snapshots, counted from the row's mutation.
    /// CapturedPayload can contain pre-anonymisation personal data; it is cleared as soon as
    /// every handler for the row succeeds. Dead-lettered work keeps its snapshot so it can be
    /// requeued, and any snapshot older than this is scrubbed. Must be at least 1 hour and
    /// longer than one handler's retry schedule (every backoff wait plus every attempt's
    /// <see cref="ClaimTimeout"/>). With row handlers registered it must also be longer than
    /// N times (that schedule plus <see cref="MaxAttempts"/> times <see cref="PollInterval"/>),
    /// N being the most handlers on one entity, since a row's handlers run one after another. Work
    /// the scrub can still end: AfterSweepSettled work whose run takes long to settle, and
    /// work held up while the kill switch pauses the poller.
    /// </summary>
    public TimeSpan PayloadRetention { get; init; } = TimeSpan.FromDays(30);
}
