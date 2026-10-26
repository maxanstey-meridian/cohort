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

/// <summary>
/// What one entity's run did and what it left in scope. <see cref="RetainedCount"/> is
/// erasure only: the subject's rows the resolved rule keeps (younger than the legal minimum,
/// or every row under an Exempt rule). <see cref="DeferredCount"/> is real runs only: rows
/// still due once the run finished with the entity that it neither changed nor skipped
/// (locked by another transaction when a sweep reached them, or come into scope mid-run
/// behind the rows the run had already passed).
/// <see cref="BlockedCount"/> is rows a database constraint stopped Cohort changing.
/// </summary>
public sealed record EntitySweepCount(
    Type EntityType,
    string Category,
    Guid TenantId,
    Strategy Strategy,
    long Affected,
    long HeldCount,
    long SkippedCount,
    long NullAnchorCount,
    long RetainedCount,
    long DeferredCount,
    long BlockedCount
)
{
    public Guid RetentionEntityId { get; } =
        EntityType.GetCustomAttribute<RetentionEntityIdAttribute>(inherit: false)?.Id
        ?? throw new ArgumentException(
            "EntityType must declare a stable retention entity identity.",
            nameof(EntityType)
        );
}
