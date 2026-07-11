using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Tests;

public sealed class SweepEventTests
{
    private static readonly Guid SweepId = Guid.NewGuid();
    private static readonly Guid RetentionEntityId = Guid.NewGuid();
    private static readonly Guid TenantId = Guid.NewGuid();
    private static readonly DateTimeOffset At = new(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);

    [Theory]
    [InlineData("started")]
    [InlineData("summary")]
    [InlineData("progress")]
    [InlineData("row-detail")]
    [InlineData("completed")]
    [InlineData("failed")]
    [InlineData("partially-failed")]
    [InlineData("cancelled")]
    public void Constructors_Reject_Empty_Sweep_Ids(string eventKind)
    {
        var act = () => CreateWithEmptySweepId(eventKind);

        act.Should().Throw<ArgumentException>().WithParameterName("SweepId");
    }

    [Theory]
    [InlineData("summary")]
    [InlineData("progress")]
    [InlineData("row-detail")]
    public void Entity_Events_Reject_Null_Entity_Types(string eventKind)
    {
        var act = () => CreateEntityEventWithNullType(eventKind);

        act.Should().Throw<ArgumentNullException>().WithParameterName("EntityType");
    }

    [Theory]
    [InlineData("summary")]
    [InlineData("progress")]
    [InlineData("row-detail")]
    public void Entity_Events_Reject_Empty_Retention_Entity_Ids(string eventKind)
    {
        var act = () => CreateEntityEvent(eventKind, retentionEntityId: Guid.Empty);

        act.Should().Throw<ArgumentException>().WithParameterName("RetentionEntityId");
    }

    [Theory]
    [InlineData("summary", null)]
    [InlineData("summary", "")]
    [InlineData("summary", "   ")]
    [InlineData("progress", null)]
    [InlineData("progress", "")]
    [InlineData("progress", "   ")]
    [InlineData("row-detail", null)]
    [InlineData("row-detail", "")]
    [InlineData("row-detail", "   ")]
    public void Entity_Events_Reject_Blank_Categories(string eventKind, string? category)
    {
        var act = () => CreateEntityEvent(eventKind, category: category!);

        act.Should().Throw<ArgumentException>().WithParameterName("Category");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Row_Detail_Rejects_Blank_Entity_Ids(string? entityId)
    {
        var act = () => ValidRowDetail(entityId: entityId!);

        act.Should().Throw<ArgumentException>().WithParameterName("EntityId");
    }

    [Theory]
    [InlineData("failed", null)]
    [InlineData("failed", "")]
    [InlineData("failed", "   ")]
    [InlineData("partially-failed", null)]
    [InlineData("partially-failed", "")]
    [InlineData("partially-failed", "   ")]
    [InlineData("cancelled", null)]
    [InlineData("cancelled", "")]
    [InlineData("cancelled", "   ")]
    public void Terminal_Failure_Events_Reject_Blank_Errors(string eventKind, string? error)
    {
        var act = () => CreateFailureEvent(eventKind, error!);

        act.Should().Throw<ArgumentException>().WithParameterName("Error");
    }

    [Theory]
    [InlineData("summary")]
    [InlineData("progress")]
    [InlineData("row-detail")]
    public void Entity_Events_Reject_Undefined_Strategies(string eventKind)
    {
        var act = () => CreateEntityEvent(eventKind, strategy: (Strategy)999);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("Strategy");
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(999)]
    public void Started_Rejects_Undefined_Triggers(int trigger)
    {
        var act = () => new SweepEvent.Started(SweepId, At, (SweepTriggerKind)trigger, false, TenantId);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("Trigger");
    }

    [Theory]
    [InlineData("summary")]
    [InlineData("progress")]
    public void Entity_Events_Reject_Negative_Resolved_Periods(string eventKind)
    {
        var act = () => CreateEntityEvent(eventKind, resolvedPeriod: TimeSpan.FromTicks(-1));

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("ResolvedPeriod");
    }

    [Theory]
    [InlineData("summary", "Affected")]
    [InlineData("summary", "HeldCount")]
    [InlineData("summary", "SkippedCount")]
    [InlineData("summary", "NullAnchorCount")]
    [InlineData("progress", "Affected")]
    [InlineData("progress", "SkippedCount")]
    public void Entity_Events_Reject_Negative_Counts(string eventKind, string count)
    {
        var act = () => CreateEntityEventWithNegativeCount(eventKind, count);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(count);
    }

    [Theory]
    [InlineData("completed")]
    [InlineData("partially-failed")]
    [InlineData("cancelled")]
    public void Terminal_Events_Reject_Negative_Durations(string eventKind)
    {
        var act = () => CreateTerminalEvent(eventKind, TimeSpan.FromTicks(-1), 0);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("Duration");
    }

    [Theory]
    [InlineData("completed")]
    [InlineData("partially-failed")]
    [InlineData("cancelled")]
    public void Terminal_Events_Reject_Negative_Totals(string eventKind)
    {
        var act = () => CreateTerminalEvent(eventKind, TimeSpan.Zero, -1);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("TotalAffected");
    }

    [Theory]
    [InlineData("duration")]
    [InlineData("total")]
    public void Failed_Rejects_Negative_Optional_Values(string optionalValue)
    {
        var act = () => optionalValue == "duration"
            ? new SweepEvent.Failed(SweepId, At, "error", TimeSpan.FromTicks(-1), null)
            : new SweepEvent.Failed(SweepId, At, "error", null, -1);

        act.Should()
            .Throw<ArgumentOutOfRangeException>()
            .WithParameterName(optionalValue == "duration" ? "Duration" : "TotalAffected");
    }

    [Theory]
    [InlineData("started")]
    [InlineData("summary")]
    [InlineData("progress")]
    [InlineData("row-detail")]
    [InlineData("completed")]
    [InlineData("failed-without-optionals")]
    [InlineData("failed-with-optionals")]
    [InlineData("partially-failed")]
    [InlineData("cancelled")]
    public void Constructors_Accept_Valid_Boundary_Values(string eventKind)
    {
        var act = () => CreateValidBoundaryEvent(eventKind);

        act.Should().NotThrow();
    }

    private static SweepEvent CreateWithEmptySweepId(string eventKind) => eventKind switch
    {
        "started" => new SweepEvent.Started(Guid.Empty, At, SweepTriggerKind.Scheduled, false, TenantId),
        "summary" => ValidSummary(sweepId: Guid.Empty),
        "progress" => ValidProgress(sweepId: Guid.Empty),
        "row-detail" => ValidRowDetail(sweepId: Guid.Empty),
        "completed" => new SweepEvent.Completed(Guid.Empty, At, TimeSpan.Zero, 0),
        "failed" => new SweepEvent.Failed(Guid.Empty, At, "error"),
        "partially-failed" => new SweepEvent.PartiallyFailed(Guid.Empty, At, TimeSpan.Zero, 0, "error"),
        "cancelled" => new SweepEvent.Cancelled(Guid.Empty, At, "error", TimeSpan.Zero, 0),
        _ => throw new ArgumentOutOfRangeException(nameof(eventKind)),
    };

    private static SweepEvent CreateEntityEvent(
        string eventKind,
        Type? entityType = null,
        Guid? retentionEntityId = null,
        string category = "category",
        Strategy strategy = Strategy.Purge,
        TimeSpan? resolvedPeriod = null
    )
    {
        entityType ??= typeof(object);
        retentionEntityId ??= RetentionEntityId;
        resolvedPeriod ??= TimeSpan.Zero;

        return eventKind switch
        {
            "summary" => ValidSummary(entityType, retentionEntityId.Value, category, strategy, resolvedPeriod.Value),
            "progress" => ValidProgress(entityType, retentionEntityId.Value, category, strategy, resolvedPeriod.Value),
            "row-detail" => ValidRowDetail(entityType, retentionEntityId.Value, category, strategy),
            _ => throw new ArgumentOutOfRangeException(nameof(eventKind)),
        };
    }

    private static SweepEvent CreateEntityEventWithNullType(string eventKind) => eventKind switch
    {
        "summary" => new SweepEvent.EntitySummary(
            SweepId,
            At,
            null!,
            RetentionEntityId,
            "category",
            TenantId,
            Strategy.Purge,
            TimeSpan.Zero,
            0,
            0
        ),
        "progress" => new SweepEvent.EntityProgress(
            SweepId,
            At,
            null!,
            RetentionEntityId,
            "category",
            TenantId,
            Strategy.Purge,
            TimeSpan.Zero,
            0,
            0
        ),
        "row-detail" => new SweepEvent.RowDetail(
            SweepId,
            At,
            null!,
            RetentionEntityId,
            "entity-id",
            "category",
            Strategy.Purge,
            TenantId
        ),
        _ => throw new ArgumentOutOfRangeException(nameof(eventKind)),
    };

    private static SweepEvent CreateEntityEventWithNegativeCount(string eventKind, string count) =>
        eventKind switch
        {
            "summary" => new SweepEvent.EntitySummary(
                SweepId,
                At,
                typeof(object),
                RetentionEntityId,
                "category",
                TenantId,
                Strategy.Purge,
                TimeSpan.Zero,
                count == "Affected" ? -1 : 0,
                count == "HeldCount" ? -1 : 0,
                count == "SkippedCount" ? -1 : 0,
                count == "NullAnchorCount" ? -1 : 0
            ),
            "progress" => new SweepEvent.EntityProgress(
                SweepId,
                At,
                typeof(object),
                RetentionEntityId,
                "category",
                TenantId,
                Strategy.Purge,
                TimeSpan.Zero,
                count == "Affected" ? -1 : 0,
                count == "SkippedCount" ? -1 : 0
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(eventKind)),
        };

    private static SweepEvent CreateFailureEvent(string eventKind, string error) => eventKind switch
    {
        "failed" => new SweepEvent.Failed(SweepId, At, error),
        "partially-failed" => new SweepEvent.PartiallyFailed(SweepId, At, TimeSpan.Zero, 0, error),
        "cancelled" => new SweepEvent.Cancelled(SweepId, At, error, TimeSpan.Zero, 0),
        _ => throw new ArgumentOutOfRangeException(nameof(eventKind)),
    };

    private static SweepEvent CreateTerminalEvent(string eventKind, TimeSpan duration, long total) =>
        eventKind switch
        {
            "completed" => new SweepEvent.Completed(SweepId, At, duration, total),
            "partially-failed" => new SweepEvent.PartiallyFailed(SweepId, At, duration, total, "error"),
            "cancelled" => new SweepEvent.Cancelled(SweepId, At, "error", duration, total),
            _ => throw new ArgumentOutOfRangeException(nameof(eventKind)),
        };

    private static SweepEvent CreateValidBoundaryEvent(string eventKind) => eventKind switch
    {
        "started" => new SweepEvent.Started(SweepId, At, SweepTriggerKind.Scheduled, false, TenantId),
        "summary" => ValidSummary(),
        "progress" => ValidProgress(),
        "row-detail" => ValidRowDetail(),
        "completed" => new SweepEvent.Completed(SweepId, At, TimeSpan.Zero, 0),
        "failed-without-optionals" => new SweepEvent.Failed(SweepId, At, "error"),
        "failed-with-optionals" => new SweepEvent.Failed(SweepId, At, "error", TimeSpan.Zero, 0),
        "partially-failed" => new SweepEvent.PartiallyFailed(SweepId, At, TimeSpan.Zero, 0, "error"),
        "cancelled" => new SweepEvent.Cancelled(SweepId, At, "error", TimeSpan.Zero, 0),
        _ => throw new ArgumentOutOfRangeException(nameof(eventKind)),
    };

    private static SweepEvent.EntitySummary ValidSummary(
        Type? entityType = null,
        Guid? retentionEntityId = null,
        string category = "category",
        Strategy strategy = Strategy.Purge,
        TimeSpan? resolvedPeriod = null,
        Guid? sweepId = null
    ) =>
        new(
            sweepId ?? SweepId,
            At,
            entityType ?? typeof(object),
            retentionEntityId ?? RetentionEntityId,
            category,
            TenantId,
            strategy,
            resolvedPeriod ?? TimeSpan.Zero,
            0,
            0
        );

    private static SweepEvent.EntityProgress ValidProgress(
        Type? entityType = null,
        Guid? retentionEntityId = null,
        string category = "category",
        Strategy strategy = Strategy.Purge,
        TimeSpan? resolvedPeriod = null,
        Guid? sweepId = null
    ) =>
        new(
            sweepId ?? SweepId,
            At,
            entityType ?? typeof(object),
            retentionEntityId ?? RetentionEntityId,
            category,
            TenantId,
            strategy,
            resolvedPeriod ?? TimeSpan.Zero,
            0,
            0
        );

    private static SweepEvent.RowDetail ValidRowDetail(
        Type? entityType = null,
        Guid? retentionEntityId = null,
        string category = "category",
        Strategy strategy = Strategy.Purge,
        string entityId = "entity-id",
        Guid? sweepId = null
    ) =>
        new(
            sweepId ?? SweepId,
            At,
            entityType ?? typeof(object),
            retentionEntityId ?? RetentionEntityId,
            entityId,
            category,
            strategy,
            TenantId
        );
}
