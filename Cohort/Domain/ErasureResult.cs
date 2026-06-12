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
}
