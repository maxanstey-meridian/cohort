using System.Data.Common;

using Cohort.Domain;

namespace Cohort.Application;

public interface IRetentionSweepStrategy
{
    public Strategy HandlesStrategy { get; }

    public Task<int> PreviewAsync(
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

    public Task<int> PreviewEraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset now,
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

public sealed record SweepExecutionResult(
    IReadOnlyList<string> AffectedRecordIds,
    int HeldCount,
    bool RowDetailsPersisted = false,
    int SkippedCount = 0
);

public sealed record SweepMutationContext(Guid SweepId, DateTimeOffset At);

public sealed record ErasureSubjectPredicate
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

public sealed record ErasureSubjectMatch(string SubjectMember, string SubjectColumn, object SubjectValue);
