using Cohort.Domain;

namespace Cohort.Application;

public abstract record SweepEvent
{
    private SweepEvent() { }

    public sealed record Started : SweepEvent
    {
        public Started(
            Guid SweepId,
            DateTimeOffset At,
            SweepTriggerKind Trigger,
            bool DryRun,
            Guid TenantId
        )
        {
            this.SweepId = RequiredId(SweepId, nameof(SweepId));
            this.At = At;
            this.Trigger = Defined(Trigger, nameof(Trigger));
            this.DryRun = DryRun;
            this.TenantId = TenantId;
        }

        public Guid SweepId { get; }
        public DateTimeOffset At { get; }
        public SweepTriggerKind Trigger { get; }
        public bool DryRun { get; }
        public Guid TenantId { get; }
    }

    public sealed record EntitySummary : SweepEvent
    {
        public EntitySummary(
            Guid SweepId,
            DateTimeOffset At,
            Type EntityType,
            Guid RetentionEntityId,
            string Category,
            Guid TenantId,
            Strategy Strategy,
            TimeSpan ResolvedPeriod,
            long Affected,
            long HeldCount,
            long SkippedCount = 0,
            long NullAnchorCount = 0,
            RetentionRuleProvenance? Provenance = null
        )
        {
            this.SweepId = RequiredId(SweepId, nameof(SweepId));
            this.At = At;
            this.EntityType = EntityType ?? throw new ArgumentNullException(nameof(EntityType));
            this.RetentionEntityId = RequiredId(RetentionEntityId, nameof(RetentionEntityId));
            this.Category = RequiredText(Category, nameof(Category));
            this.TenantId = TenantId;
            this.Strategy = Defined(Strategy, nameof(Strategy));
            this.ResolvedPeriod = NonNegative(ResolvedPeriod, nameof(ResolvedPeriod));
            this.Affected = NonNegative(Affected, nameof(Affected));
            this.HeldCount = NonNegative(HeldCount, nameof(HeldCount));
            this.SkippedCount = NonNegative(SkippedCount, nameof(SkippedCount));
            this.NullAnchorCount = NonNegative(NullAnchorCount, nameof(NullAnchorCount));
            this.Provenance = Provenance;
        }

        public Guid SweepId { get; }
        public DateTimeOffset At { get; }
        public Type EntityType { get; }
        public Guid RetentionEntityId { get; }
        public string Category { get; }
        public Guid TenantId { get; }
        public Strategy Strategy { get; }
        public TimeSpan ResolvedPeriod { get; }
        public long Affected { get; }
        public long HeldCount { get; }
        public long SkippedCount { get; }
        public long NullAnchorCount { get; }
        public RetentionRuleProvenance? Provenance { get; }
    }

    public sealed record EntityProgress : SweepEvent
    {
        public EntityProgress(
            Guid SweepId,
            DateTimeOffset At,
            Type EntityType,
            Guid RetentionEntityId,
            string Category,
            Guid TenantId,
            Strategy Strategy,
            TimeSpan ResolvedPeriod,
            long Affected,
            long SkippedCount,
            RetentionRuleProvenance? Provenance = null
        )
        {
            this.SweepId = RequiredId(SweepId, nameof(SweepId));
            this.At = At;
            this.EntityType = EntityType ?? throw new ArgumentNullException(nameof(EntityType));
            this.RetentionEntityId = RequiredId(RetentionEntityId, nameof(RetentionEntityId));
            this.Category = RequiredText(Category, nameof(Category));
            this.TenantId = TenantId;
            this.Strategy = Defined(Strategy, nameof(Strategy));
            this.ResolvedPeriod = NonNegative(ResolvedPeriod, nameof(ResolvedPeriod));
            this.Affected = NonNegative(Affected, nameof(Affected));
            this.SkippedCount = NonNegative(SkippedCount, nameof(SkippedCount));
            this.Provenance = Provenance;
        }

        public Guid SweepId { get; }
        public DateTimeOffset At { get; }
        public Type EntityType { get; }
        public Guid RetentionEntityId { get; }
        public string Category { get; }
        public Guid TenantId { get; }
        public Strategy Strategy { get; }
        public TimeSpan ResolvedPeriod { get; }
        public long Affected { get; }
        public long SkippedCount { get; }
        public RetentionRuleProvenance? Provenance { get; }
    }

    public sealed record RowDetail : SweepEvent
    {
        public RowDetail(
            Guid SweepId,
            DateTimeOffset At,
            Type EntityType,
            Guid RetentionEntityId,
            string RecordId,
            string Category,
            Strategy Strategy,
            Guid TenantId
        )
        {
            this.SweepId = RequiredId(SweepId, nameof(SweepId));
            this.At = At;
            this.EntityType = EntityType ?? throw new ArgumentNullException(nameof(EntityType));
            this.RetentionEntityId = RequiredId(RetentionEntityId, nameof(RetentionEntityId));
            this.RecordId = RequiredText(RecordId, nameof(RecordId));
            this.Category = RequiredText(Category, nameof(Category));
            this.Strategy = Defined(Strategy, nameof(Strategy));
            this.TenantId = TenantId;
        }

        public Guid SweepId { get; }
        public DateTimeOffset At { get; }
        public Type EntityType { get; }
        public Guid RetentionEntityId { get; }
        public string RecordId { get; }
        public string Category { get; }
        public Strategy Strategy { get; }
        public Guid TenantId { get; }
    }

    public sealed record Completed : SweepEvent
    {
        public Completed(Guid SweepId, DateTimeOffset At, TimeSpan Duration, long TotalAffected)
        {
            this.SweepId = RequiredId(SweepId, nameof(SweepId));
            this.At = At;
            this.Duration = NonNegative(Duration, nameof(Duration));
            this.TotalAffected = NonNegative(TotalAffected, nameof(TotalAffected));
        }

        public Guid SweepId { get; }
        public DateTimeOffset At { get; }
        public TimeSpan Duration { get; }
        public long TotalAffected { get; }
    }

    public sealed record Failed : SweepEvent
    {
        public Failed(
            Guid SweepId,
            DateTimeOffset At,
            string Error,
            TimeSpan? Duration = null,
            long? TotalAffected = null
        )
        {
            this.SweepId = RequiredId(SweepId, nameof(SweepId));
            this.At = At;
            this.Error = RequiredText(Error, nameof(Error));
            this.Duration = OptionalNonNegative(Duration, nameof(Duration));
            this.TotalAffected = OptionalNonNegative(TotalAffected, nameof(TotalAffected));
        }

        public Guid SweepId { get; }
        public DateTimeOffset At { get; }
        public string Error { get; }
        public TimeSpan? Duration { get; }
        public long? TotalAffected { get; }
    }

    public sealed record PartiallyFailed : SweepEvent
    {
        public PartiallyFailed(
            Guid SweepId,
            DateTimeOffset At,
            TimeSpan Duration,
            long TotalAffected,
            string Error
        )
        {
            this.SweepId = RequiredId(SweepId, nameof(SweepId));
            this.At = At;
            this.Duration = NonNegative(Duration, nameof(Duration));
            this.TotalAffected = NonNegative(TotalAffected, nameof(TotalAffected));
            this.Error = RequiredText(Error, nameof(Error));
        }

        public Guid SweepId { get; }
        public DateTimeOffset At { get; }
        public TimeSpan Duration { get; }
        public long TotalAffected { get; }
        public string Error { get; }
    }

    public sealed record Cancelled : SweepEvent
    {
        public Cancelled(
            Guid SweepId,
            DateTimeOffset At,
            string Error,
            TimeSpan Duration,
            long TotalAffected
        )
        {
            this.SweepId = RequiredId(SweepId, nameof(SweepId));
            this.At = At;
            this.Error = RequiredText(Error, nameof(Error));
            this.Duration = NonNegative(Duration, nameof(Duration));
            this.TotalAffected = NonNegative(TotalAffected, nameof(TotalAffected));
        }

        public Guid SweepId { get; }
        public DateTimeOffset At { get; }
        public string Error { get; }
        public TimeSpan Duration { get; }
        public long TotalAffected { get; }
    }

    private static Guid RequiredId(Guid value, string parameterName) =>
        value != Guid.Empty
            ? value
            : throw new ArgumentException("The identifier cannot be empty.", parameterName);

    private static string RequiredText(string value, string parameterName) =>
        !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new ArgumentException("The value cannot be null, empty, or whitespace.", parameterName);

    private static TEnum Defined<TEnum>(TEnum value, string parameterName)
        where TEnum : struct, Enum =>
        Enum.IsDefined(value)
            ? value
            : throw new ArgumentOutOfRangeException(parameterName, value, "The enum value is undefined.");

    private static TimeSpan NonNegative(TimeSpan value, string parameterName) =>
        value >= TimeSpan.Zero
            ? value
            : throw new ArgumentOutOfRangeException(parameterName, value, "The value cannot be negative.");

    private static long NonNegative(long value, string parameterName) =>
        value >= 0
            ? value
            : throw new ArgumentOutOfRangeException(parameterName, value, "The value cannot be negative.");

    private static TimeSpan? OptionalNonNegative(TimeSpan? value, string parameterName) =>
        value is null ? null : NonNegative(value.Value, parameterName);

    private static long? OptionalNonNegative(long? value, string parameterName) =>
        value is null ? null : NonNegative(value.Value, parameterName);
}

public enum SweepRunStatus
{
    Started,
    Succeeded,
    PartiallyFailed,
    Failed,
    Cancelled,
}

public enum SweepTriggerKind
{
    Scheduled,
    Erasure,
    Manual,
}
