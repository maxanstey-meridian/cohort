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
    private readonly CohortStoreTables tables = CohortStoreTables.FromModel(db.Model);

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
        await using var transaction = await db.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
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
        // unlisted row, a client or database cascade, raw SQL). Any active hold whose row
        // disappears inside this transaction is a violation, whatever removed it. Holds already
        // orphaned before the callback are not this deletion's doing.
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

            await transaction.CommitAsync(ct);
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
        var parameters = new SqlParams
        {
            ["retentionEntityId"] = target.Entry.RetentionEntityId,
            ["recordId"] = target.RecordId,
        };
        if (target.Entry.Tenant is not null)
        {
            parameters["tenantId"] = target.TenantId;
        }

        await using var command = parameters.CreateCommand(
            db.Database.GetDbConnection(),
            transaction,
            RetentionHoldSql.BuildActiveHoldForRecord(
                tables.RetentionHolds,
                tenanted: target.Entry.Tenant is not null
            )
        );
        return await command.ExecuteScalarAsync(ct) is not null;
    }

    private sealed record ResolvedTarget(
        RetentionEntry Entry,
        string RecordId,
        Guid? TenantId
    );
}
