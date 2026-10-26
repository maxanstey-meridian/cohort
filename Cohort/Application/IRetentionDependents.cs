using Cohort.Domain;

namespace Cohort.Application;

/// <summary>
/// Retires what depends on a row (deletes or blanks it) in the row's own transaction, before
/// Cohort mutates the row. Register one per entity with <c>AddRetentionDependents</c>; the
/// class carries a <see cref="RowHandlerIdAttribute"/>, under which its failures are recorded.
/// </summary>
public interface IRetentionDependents<TEntity>
{
    public Task RetireAsync(TEntity row, RetentionDependentsContext ctx, CancellationToken ct);
}
