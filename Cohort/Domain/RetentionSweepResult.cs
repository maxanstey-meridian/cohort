namespace Cohort.Domain;

public sealed record RetentionSweepResult(
    Guid SweepId,
    DateTimeOffset StartedAt,
    DateTimeOffset CompletedAt,
    IReadOnlyList<EntitySweepCount> Counts
);

public sealed record EntitySweepCount(
    Type EntityType,
    string Category,
    Guid TenantId,
    Strategy Strategy,
    int Affected
);
