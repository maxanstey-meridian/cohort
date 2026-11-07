using Cohort.Domain;

namespace Cohort.Application;

/// <summary>
/// Retires what depends on a row (deletes or blanks it) in the row's own transaction, before
/// Cohort mutates the row. The host's <c>AddCohort&lt;TContext&gt;</c> context, injected by
/// the step, is on the run's connection and in its transaction, so whatever the step writes
/// through it, a message to the host's outbox included, commits with the row's mutation and
/// rolls back with it. Register one per entity
/// with <c>AddRetentionDependents</c>, passing <c>retiresReferencingRows: false</c> for a step
/// that doesn't retire the rows that reference its entity (one that only writes to an outbox,
/// say). A step that throws skips its row, recorded in the row detail's <c>Failure</c>.
/// </summary>
public interface IRetentionDependents<TEntity>
{
    public Task RetireAsync(TEntity row, RetentionDependentsContext ctx, CancellationToken ct);
}
