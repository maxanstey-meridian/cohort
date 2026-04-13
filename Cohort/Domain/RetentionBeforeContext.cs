namespace Cohort.Domain;

public sealed class RetentionBeforeContext
{
    public RetentionBeforeContext(
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
        Snapshot = new Dictionary<string, object?>(StringComparer.Ordinal);
    }

    public Guid SweepId { get; }

    public string Category { get; }

    public Strategy Strategy { get; }

    public Guid TenantId { get; }

    public DateTimeOffset At { get; }

    public IDictionary<string, object?> Snapshot { get; }
}
