using Cohort.Domain;

namespace Cohort.Hosting;

public sealed class CohortOptions
{
    public const string SectionName = "Cohort";

    public string? Schedule { get; init; }

    /// <summary>
    /// Makes the scheduled worker dry-run instead of sweep. Explicit sweep and erasure
    /// requests carry their own dry-run flag and are not affected.
    /// </summary>
    public bool DryRun { get; init; }

    public bool KillSwitch { get; init; }

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
