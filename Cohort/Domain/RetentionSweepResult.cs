namespace Cohort.Domain;

public sealed record RetentionSweepResult(
    Guid SweepId,
    DateTimeOffset StartedAt,
    DateTimeOffset CompletedAt,
    IReadOnlyList<EntitySweepCount> Counts,
    IReadOnlyList<string>? EntityFailures = null
)
{
    public IReadOnlyList<string> EntityFailures { get; init; } = EntityFailures ?? [];
}

public sealed record EntitySweepCount(
    Type EntityType,
    string Category,
    Guid TenantId,
    Strategy Strategy,
    int Affected,
    int HeldCount = 0,
    int SkippedCount = 0
);
