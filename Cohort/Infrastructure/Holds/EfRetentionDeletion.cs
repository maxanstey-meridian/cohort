using System.Data;
using System.Data.Common;

using Cohort.Application;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure.Holds;

internal sealed class EfRetentionDeletion(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionTargetResolver targetResolver,
    RetentionRuntimeReadinessValidator readinessValidator,
    RetentionHoldGuard holdGuard
) : IRetentionDeletion
{
    private CohortStoreTables? tables;

    // Read lazily, after readiness, so a model with no Cohort mapping reaches readiness's refusal.
    private CohortStoreTables Tables => tables ??= CohortStoreTables.FromModel(db.Model);

    public async Task<RetentionDeletionOutcome> ExecuteAsync(
        IReadOnlyCollection<RetentionTarget> targets,
        Func<CancellationToken, Task> deletion,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(targets);
        ArgumentNullException.ThrowIfNull(deletion);
        if (targets.Count == 0)
        {
            throw new ArgumentException("At least one retention target is required.", nameof(targets));
        }

        var requestedTargets = targets.ToArray();
        if (requestedTargets.Any(target => target is null))
        {
            throw new ArgumentException("Retention targets cannot contain null.", nameof(targets));
        }

        if (db.Database.CurrentTransaction is not null)
        {
            throw new InvalidOperationException(
                "Retention deletion must own the scoped DbContext transaction."
            );
        }

        // Pending changes would be saved inside the hold-checked transaction without being
        // checked, or leak out unsaved. They belong to the host, so refuse rather than discard
        // them. Detected explicitly: HasChanges alone misses an edit when the host turned
        // automatic change detection off.
        db.ChangeTracker.DetectChanges();
        if (db.ChangeTracker.HasChanges())
        {
            throw new InvalidOperationException(
                "Retention deletion needs a DbContext with no unsaved changes; save or discard them before calling ExecuteAsync."
            );
        }

        await readinessValidator.ValidateAsync(ct);
        // The hold checks rely on each statement taking a fresh snapshot after the advisory lock.
        // The callback is host code that Cohort can't know is safe to run twice, so the deletion
        // runs once, on a context with a retrying execution strategy too: a transient failure
        // rolls back and reaches the caller, who calls again if the callback can be repeated.
        var callbackReturned = false;
        try
        {
            return await CohortTransaction.RunOnceAsync(
                db,
                IsolationLevel.ReadCommitted,
                async (transaction, token) =>
                {
                    var outcome = await DeleteAsync(requestedTargets, deletion, transaction, token);
                    callbackReturned = outcome == RetentionDeletionOutcome.Executed;
                    return outcome;
                },
                ct
            );
        }
        catch when (callbackReturned)
        {
            // Only the commit is left to fail: the callback's saves roll back with it, so what it
            // left tracked is no longer true.
            db.ChangeTracker.Clear();
            throw;
        }
    }

    private async Task<RetentionDeletionOutcome> DeleteAsync(
        RetentionTarget[] requestedTargets,
        Func<CancellationToken, Task> deletion,
        IDbContextTransaction transaction,
        CancellationToken ct
    )
    {
        var dbTransaction = transaction.GetDbTransaction();
        var resolvedTargets = new List<ResolvedTarget>(requestedTargets.Length);

        foreach (var target in requestedTargets)
        {
            var entry = targetResolver.ResolveTarget(target.RetentionEntityId);
            RetentionTargetResolver.ValidateTenantOwnership(
                entry,
                target.TenantId,
                "Retention deletion"
            );
            var recordId = await targetResolver.CanonicaliseRecordIdAsync(
                entry,
                target.RecordId,
                dbTransaction,
                "Retention deletion",
                ct
            );
            resolvedTargets.Add(new ResolvedTarget(entry, recordId, target.TenantId));
        }

        resolvedTargets = resolvedTargets
            .Distinct()
            .OrderBy(target => target.Entry.RetentionEntityId)
            .ThenBy(target => target.TenantId)
            .ThenBy(target => target.RecordId, StringComparer.Ordinal)
            .ToList();

        var connection = db.Database.GetDbConnection();
        await RetentionEntityLockSql.AcquireAsync(
            connection,
            dbTransaction,
            resolvedTargets
                .Select(target => new RetentionEntityLockSql.Target(
                    target.Entry.RetentionEntityId,
                    target.TenantId,
                    target.RecordId
                ))
                .ToArray(),
            ct
        );

        await holdGuard.EnsureTenantlessHoldsVisibleAsync(connection, dbTransaction, "Retention deletion", ct);

        foreach (var target in resolvedTargets)
        {
            if (await HasActiveHoldAsync(target, dbTransaction, ct))
            {
                return RetentionDeletionOutcome.Protected;
            }
        }

        // The listed targets are unheld, but the callback can remove more than it listed (an
        // unlisted row, a client or database cascade, raw SQL). Any row an active hold covers that
        // disappears inside this transaction is a violation, whatever removed it, and so is a hold
        // on an entity that covers related rows appearing meanwhile. Rows already orphaned before
        // the callback are not this deletion's doing.
        var heldBefore = await holdGuard.ReadHeldRowVersionsAsync(connection, dbTransaction, ct);
        try
        {
            await deletion(ct);
            db.ChangeTracker.DetectChanges();
            if (db.ChangeTracker.HasChanges())
            {
                throw new InvalidOperationException(
                    "The retention deletion callback returned with unsaved changes. Save them inside the callback; nothing was committed."
                );
            }

            var violated = await holdGuard.ReadRemovedHeldRowsAsync(heldBefore, connection, dbTransaction, ct);
            if (violated.Count > 0)
            {
                throw new InvalidOperationException(
                    $"The retention deletion callback deleted rows protected by active holds ({string.Join(", ", violated)}). Nothing was committed."
                );
            }
        }
        catch
        {
            // The transaction rolls back on dispose. Anything still tracked (an unsaved Remove, or
            // the entries of a SaveChanges that failed) would otherwise apply on the host's next
            // save, outside the lock and the hold check.
            db.ChangeTracker.Clear();
            throw;
        }

        return RetentionDeletionOutcome.Executed;
    }

    private async Task<bool> HasActiveHoldAsync(
        ResolvedTarget target,
        DbTransaction transaction,
        CancellationToken ct
    )
    {
        // Covered through a relation counts as held, exactly as a sweep judges it.
        var statement = RetentionHoldSql.BuildAnyCoveringHold(
            Tables.RetentionHolds,
            target.Entry,
            target.RecordId,
            target.TenantId
        );
        await using var command = statement.Parameters.CreateCommand(
            db.Database.GetDbConnection(),
            transaction,
            statement.Sql
        );
        return await command.ExecuteScalarAsync(ct) is not null;
    }

    private sealed record ResolvedTarget(
        RetentionEntry Entry,
        string RecordId,
        Guid? TenantId
    );
}
