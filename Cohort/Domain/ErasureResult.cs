namespace Cohort.Domain;

public sealed record ErasureResult(
    Guid SweepId,
    DateTimeOffset StartedAt,
    DateTimeOffset CompletedAt,
    ErasureScope Scope,
    IReadOnlyList<EntitySweepCount> Counts,
    bool DryRun = false,
    IReadOnlyList<string>? EntityFailures = null
)
{
    public IReadOnlyList<string> EntityFailures { get; init; } = EntityFailures ?? [];

    /// <summary>
    /// True when the erasure left none of the subject's rows behind: no entity failed, and no
    /// row was held, skipped, unreachable through a NULL anchor, kept by its rule, deferred or
    /// blocked. A dry run reports whether the real run would be complete.
    /// </summary>
    public bool Complete =>
        EntityFailures.Count == 0
        && Counts.All(count =>
            count.HeldCount
                + count.SkippedCount
                + count.NullAnchorCount
                + count.RetainedCount
                + count.DeferredCount
                + count.BlockedCount
            == 0
        );
}
