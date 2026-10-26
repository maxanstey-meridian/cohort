using Cohort.Domain;

namespace Cohort.Hosting;

public sealed class CohortOptions
{
    public const string SectionName = "Cohort";

    /// <summary>
    /// The scheduled worker's cron expression (5 fields, or 6 with seconds), in UTC. Null
    /// leaves the worker idle. Occurrences are counted on the database clock, the clock that
    /// stamps every run.
    /// </summary>
    public string? Schedule { get; init; }

    /// <summary>
    /// Makes the scheduled worker dry-run instead of sweep. Explicit sweep and erasure
    /// requests carry their own dry-run flag and are not affected.
    /// </summary>
    public bool DryRun { get; init; }

    /// <summary>
    /// Stops the scheduled worker: the pass in progress is cancelled (its current batch rolls
    /// back and the run settles <c>Cancelled</c>), the rest of that occurrence is skipped, and
    /// no occurrence starts while it is on. Turning it off catches up the most recent
    /// occurrence straight away.
    /// </summary>
    public bool KillSwitch { get; init; }

    /// <summary>
    /// How long the worker waits before retrying the unfinished passes of an occurrence (a
    /// pass with no <c>Succeeded</c> run, or an iteration that failed outright). The next
    /// occurrence supersedes it when that comes sooner. Greater than zero, at most 1 day.
    /// </summary>
    public TimeSpan OccurrenceRetryDelay { get; init; } = TimeSpan.FromMinutes(15);

    /// <summary>
    /// How many runs one pass of an occurrence gets before the worker gives up on it until
    /// the next occurrence. Cancelled runs (the kill switch or a shutdown) don't count. 1 to 100.
    /// </summary>
    public int OccurrenceMaxAttempts { get; init; } = 3;

    /// <summary>
    /// The time zone id (IANA, for example <c>Europe/London</c>; Windows ids are accepted
    /// where ICU is available) whose calendar retention periods are counted in: each period is
    /// counted back from today's date in this zone, so a row is kept at least until the end of
    /// its anniversary day there. Defaults to UTC. The schedule is always UTC.
    /// </summary>
    public string TimeZone { get; init; } = "UTC";

    /// <summary>
    /// Maximum rows a strategy selects, locks, and mutates per transaction. Each batch
    /// commits independently, so a large backlog is retired incrementally instead of in
    /// one unbounded transaction.
    /// </summary>
    public int SweepBatchSize { get; init; } = 5000;

    public AuditObserverOptions AuditObservers { get; init; } = new();

    public RowHandlerDispatchOptions RowHandlerDispatch { get; init; } = new();

    /// <summary>
    /// Opt-in pruning of settled run history and inactive holds. Nothing is pruned unless
    /// a retention is configured.
    /// </summary>
    public HistoryPruningOptions HistoryPruning { get; init; } = new();

    public CohortConventions Conventions { get; init; } = new();
}
