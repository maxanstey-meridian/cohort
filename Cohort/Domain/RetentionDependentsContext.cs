namespace Cohort.Domain;

/// <summary>
/// What a dependents step knows about the run retiring its row: the same facts as
/// <see cref="RetentionBeforeContext"/>, without a snapshot, because the step hands nothing on.
/// </summary>
public sealed class RetentionDependentsContext
{
    public RetentionDependentsContext(
        Guid sweepId,
        string category,
        Strategy strategy,
        Guid tenantId,
        DateTimeOffset at
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(category);

        SweepId = sweepId;
        Category = category;
        Strategy = strategy;
        TenantId = tenantId;
        At = at;
    }

    public Guid SweepId { get; }

    public string Category { get; }

    public Strategy Strategy { get; }

    public Guid TenantId { get; }

    public DateTimeOffset At { get; }
}
