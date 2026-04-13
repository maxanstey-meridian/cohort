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
        CancellationToken ct
    );

    public Task<int> PreviewEraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectMatch match,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        CancellationToken ct
    );

    public Task<SweepExecutionResult> EraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectMatch match,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        CancellationToken ct
    );
}

public sealed record SweepExecutionResult(IReadOnlyList<string> AffectedRecordIds, int HeldCount);

public sealed record ErasureSubjectMatch(string SubjectMember, string SubjectColumn, object SubjectValue);
