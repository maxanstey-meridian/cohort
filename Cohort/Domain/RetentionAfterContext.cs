using System.Collections.ObjectModel;

namespace Cohort.Domain;

public sealed class RetentionAfterContext<TEntity>
{
    public RetentionAfterContext(
        Guid sweepId,
        string recordId,
        string category,
        Strategy strategy,
        Guid tenantId,
        DateTimeOffset at,
        int attempt,
        IReadOnlyDictionary<string, object?> snapshot
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordId);
        ArgumentException.ThrowIfNullOrWhiteSpace(category);
        ArgumentNullException.ThrowIfNull(snapshot);

        SweepId = sweepId;
        RecordId = recordId;
        Category = category;
        Strategy = strategy;
        TenantId = tenantId;
        At = at;
        Attempt = attempt;
        Snapshot = new ReadOnlyDictionary<string, object?>(
            new Dictionary<string, object?>(snapshot, StringComparer.Ordinal)
        );
    }

    public Guid SweepId { get; }

    public string RecordId { get; }

    public string Category { get; }

    public Strategy Strategy { get; }

    public Guid TenantId { get; }

    public DateTimeOffset At { get; }

    public int Attempt { get; }

    public IReadOnlyDictionary<string, object?> Snapshot { get; }
}
