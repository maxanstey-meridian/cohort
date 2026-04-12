using Cohort.Domain;

namespace Cohort.Application;

public abstract record SweepEvent
{
    public sealed record Started(
        Guid SweepId,
        DateTimeOffset At,
        SweepTriggerKind Trigger,
        bool DryRun,
        Guid TenantId
    ) : SweepEvent;

    public sealed record EntitySummary(
        Guid SweepId,
        DateTimeOffset At,
        Type EntityType,
        string Category,
        Guid TenantId,
        Strategy Strategy,
        TimeSpan ResolvedPeriod,
        int Affected,
        int HeldCount
    ) : SweepEvent;

    public sealed record RowDetail(
        Guid SweepId,
        DateTimeOffset At,
        Type EntityType,
        string EntityId,
        string Category,
        Strategy Strategy,
        Guid TenantId
    ) : SweepEvent;

    public sealed record Completed(
        Guid SweepId,
        DateTimeOffset At,
        TimeSpan Duration,
        int TotalAffected
    ) : SweepEvent;
}

public enum SweepTriggerKind
{
    Scheduled,
    Erasure,
}
