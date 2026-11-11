using Cohort.Domain;

namespace Cohort.Application;

public abstract record SweepEvent
{
    private SweepEvent(Guid sweepId, DateTimeOffset at)
    {
        SweepId = sweepId;
        At = at;
    }

    /// <summary>The run this event belongs to.</summary>
    public Guid SweepId { get; }

    public DateTimeOffset At { get; }

    public sealed record Started : SweepEvent
    {
        public Started(
            Guid SweepId,
            DateTimeOffset At,
            SweepTriggerKind Trigger,
            bool DryRun,
            Guid TenantId,
            string? ErasureSubjectKind = null
        )
            : base(RequiredId(SweepId, nameof(SweepId)), At)
        {
            this.Trigger = Defined(Trigger, nameof(Trigger));
            this.DryRun = DryRun;
            this.TenantId = TenantId;
            this.ErasureSubjectKind = this.Trigger == SweepTriggerKind.Erasure
                ? RequiredText(ErasureSubjectKind ?? "", nameof(ErasureSubjectKind))
                : ErasureSubjectKind is null
                    ? null
                    : throw new ArgumentException(
                        "Only an erasure run has an erasure subject kind.",
                        nameof(ErasureSubjectKind)
                    );
        }

        public SweepTriggerKind Trigger { get; }
        public bool DryRun { get; }
        public Guid TenantId { get; }

        /// <summary>
        /// The kind of subject an erasure run erased (never the subject itself); null for
        /// every other run.
        /// </summary>
        public string? ErasureSubjectKind { get; }
    }

    /// <param name="SweepId">The run this event belongs to.</param>
    /// <param name="At">When the event was recorded.</param>
    /// <param name="EntityType">The retained entity type.</param>
    /// <param name="RetentionEntityId">The entity type's stable retention identity.</param>
    /// <param name="Category">The entity's retention category.</param>
    /// <param name="TenantId">The tenant the run covered.</param>
    /// <param name="Strategy">The strategy of the rule the run applied.</param>
    /// <param name="ResolvedPeriod">
    /// The age a row had to reach: whichever of the period and the legal minimum set the
    /// cutoff, or the legal minimum alone for an erasure.
    /// </param>
    /// <param name="Affected">Rows the run changed.</param>
    /// <param name="HeldCount">Rows left alone because a hold covers them.</param>
    /// <param name="SkippedCount">Rows the run skipped.</param>
    /// <param name="NullAnchorCount">Rows whose anchor is null, which a cutoff never matches.</param>
    /// <param name="RetainedCount">Erasure only: the subject's rows the resolved rule keeps.</param>
    /// <param name="DeferredCount">
    /// Real runs only: rows still due after the entity's batches that the run neither
    /// changed nor skipped.
    /// </param>
    /// <param name="BlockedCount">Rows a database constraint stopped Cohort changing.</param>
    /// <param name="Provenance">Where the resolved rule came from, when the provider says.</param>
    public sealed record EntitySummary(
        Guid SweepId,
        DateTimeOffset At,
        Type EntityType,
        Guid RetentionEntityId,
        string Category,
        Guid TenantId,
        Strategy Strategy,
        RetentionPeriod ResolvedPeriod,
        long Affected,
        long HeldCount,
        long SkippedCount,
        long NullAnchorCount,
        long RetainedCount,
        long DeferredCount,
        long BlockedCount,
        RetentionRuleProvenance? Provenance = null
    ) : SweepEvent(SweepId, At);

    /// <param name="SweepId">The run this event belongs to.</param>
    /// <param name="At">When the event was recorded.</param>
    /// <param name="EntityType">The retained entity type.</param>
    /// <param name="RetentionEntityId">The entity type's stable retention identity.</param>
    /// <param name="Category">The entity's retention category.</param>
    /// <param name="TenantId">The tenant the run covered.</param>
    /// <param name="Strategy">The strategy of the rule the run applied.</param>
    /// <param name="ResolvedPeriod">
    /// The age a row had to reach: whichever of the period and the legal minimum set the
    /// cutoff, or the legal minimum alone for an erasure.
    /// </param>
    /// <param name="Affected">Rows the run has changed so far.</param>
    /// <param name="SkippedCount">Rows the run has skipped so far.</param>
    /// <param name="BlockedCount">Rows a database constraint stopped Cohort changing.</param>
    /// <param name="Provenance">Where the resolved rule came from, when the provider says.</param>
    public sealed record EntityProgress(
        Guid SweepId,
        DateTimeOffset At,
        Type EntityType,
        Guid RetentionEntityId,
        string Category,
        Guid TenantId,
        Strategy Strategy,
        RetentionPeriod ResolvedPeriod,
        long Affected,
        long SkippedCount,
        long BlockedCount,
        RetentionRuleProvenance? Provenance = null
    ) : SweepEvent(SweepId, At);

    public sealed record RowDetail(
        Guid SweepId,
        DateTimeOffset At,
        Type EntityType,
        Guid RetentionEntityId,
        string RecordId,
        string Category,
        Strategy Strategy,
        Guid TenantId
    ) : SweepEvent(SweepId, At);

    public sealed record Completed(Guid SweepId, DateTimeOffset At, TimeSpan Duration, long TotalAffected)
        : SweepEvent(SweepId, At);

    public sealed record Failed(
        Guid SweepId,
        DateTimeOffset At,
        string Error,
        TimeSpan? Duration = null,
        long? TotalAffected = null
    ) : SweepEvent(SweepId, At);

    public sealed record PartiallyFailed(
        Guid SweepId,
        DateTimeOffset At,
        TimeSpan Duration,
        long TotalAffected,
        string Error
    ) : SweepEvent(SweepId, At);

    public sealed record Cancelled(
        Guid SweepId,
        DateTimeOffset At,
        string Error,
        TimeSpan Duration,
        long TotalAffected
    ) : SweepEvent(SweepId, At);

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
