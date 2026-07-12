using System.Reflection;

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
    long Affected,
    long HeldCount = 0,
    long SkippedCount = 0,
    long NullAnchorCount = 0
)
{
    public Guid RetentionEntityId { get; } =
        EntityType.GetCustomAttribute<RetentionEntityIdAttribute>(inherit: false)?.Id
        ?? throw new ArgumentException(
            "EntityType must declare a stable retention entity identity.",
            nameof(EntityType)
        );
}
