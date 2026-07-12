using System.Data.Common;
using Cohort.Domain;

namespace Cohort.Infrastructure.Sweep;

internal interface IRetentionSweepStrategy
{
    public Strategy HandlesStrategy { get; }

    public Task<long> PreviewAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        CancellationToken ct
    );

    public Task<SweepExecutionResult> SweepAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        CancellationToken ct,
        SweepMutationContext? execution = null
    );

    /// <summary>
    /// Counts rows that are past the effective cutoff (and otherwise eligible for this
    /// strategy) but excluded by an active legal hold. Measured directly so audit
    /// summaries report holds rather than inferring them from candidate arithmetic.
    /// </summary>
    public Task<long> CountHeldAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        CancellationToken ct
    );

    /// <summary>
    /// Counts rows this strategy could otherwise act on whose anchor column is NULL.
    /// Such rows never match a cutoff comparison and are retained indefinitely; the
    /// count is surfaced on every sweep summary so they cannot accumulate invisibly.
    /// </summary>
    public Task<long> CountNullAnchorsAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        CancellationToken ct
    );

    public Task<long> PreviewEraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        CancellationToken ct
    );

    /// <summary>
    /// Counts subject-matching rows that satisfy the erasure legal-minimum predicate,
    /// when one exists, but are excluded by an active legal hold.
    /// </summary>
    public Task<long> CountHeldForEraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        CancellationToken ct
    );

    public Task<long> CountNullAnchorsForEraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DbConnection conn,
        CancellationToken ct
    );

    public Task<SweepExecutionResult> EraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        CancellationToken ct,
        SweepMutationContext? execution = null
    );
}

internal sealed record SweepExecutionResult(
    IReadOnlyList<string> AffectedRecordIds,
    int HeldCount,
    bool RowDetailsPersisted = false,
    int SkippedCount = 0,
    int CandidateCount = 0,
    IReadOnlyList<string>? SkippedRecordIds = null
)
{
    /// <summary>
    /// Record ids the strategy selected but deliberately did not mutate (e.g. rows whose
    /// OnBefore handler failed). The engine excludes them from later batches of the same
    /// run: a skipped row stays eligible, so reselecting it would re-fail it forever and
    /// re-insert its audit row detail under the same sweep id.
    /// </summary>
    public IReadOnlyList<string> SkippedRecordIds { get; init; } = SkippedRecordIds ?? [];
}

internal sealed record SweepMutationContext(
    Guid SweepId,
    DateTimeOffset At,
    int? BatchSize = null
);

internal sealed record ErasureSubjectPredicate
{
    public ErasureSubjectPredicate(IReadOnlyList<ErasureSubjectMatch> matches)
    {
        ArgumentNullException.ThrowIfNull(matches);
        if (matches.Count == 0)
        {
            throw new ArgumentException(
                "Erasure subject predicates must contain at least one subject match.",
                nameof(matches)
            );
        }

        Matches = matches;
    }

    public IReadOnlyList<ErasureSubjectMatch> Matches { get; }
}

internal sealed record ErasureSubjectMatch(
    string SubjectMember,
    string SubjectColumn,
    object SubjectValue
);
