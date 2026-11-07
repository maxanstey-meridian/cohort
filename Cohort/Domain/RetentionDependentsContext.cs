namespace Cohort.Domain;

/// <summary>
/// What a dependents step knows about the run retiring its row: the run, the row's category,
/// the strategy about to mutate it, the run's tenant, and the database time the batch stamps
/// its rows with. A step that writes to an outbox can put these in its message.
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
