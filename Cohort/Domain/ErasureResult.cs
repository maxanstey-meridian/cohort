namespace Cohort.Domain;

public sealed record ErasureResult(
    Guid SweepId,
    DateTimeOffset StartedAt,
    DateTimeOffset CompletedAt,
    ErasureScope Scope,
    IReadOnlyList<EntitySweepCount> Counts
);
