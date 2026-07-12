namespace Cohort.Hosting;

public sealed class CohortOptions
{
    public const string SectionName = "Cohort";

    public string? Schedule { get; init; }

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

    public CohortConventions Conventions { get; init; } = new();
}
