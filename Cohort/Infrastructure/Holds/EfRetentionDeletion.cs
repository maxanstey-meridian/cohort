using System.Data;
using System.Data.Common;

using Cohort.Application;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure.Holds;

internal sealed class EfRetentionDeletion(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionRegistry registry,
    RetentionTargetResolver targetResolver,
    RetentionRuntimeReadinessValidator readinessValidator,
    IServiceScopeFactory scopeFactory
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
        // them.
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

        await RetentionEntityLockSql.AcquireAsync(
            db.Database.GetDbConnection(),
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

        // Any target's callback can reach a tenantless row (a cascade, raw SQL), so the check
        // follows the model, not the targets.
        if (registry.Scan().Values.Any(entry => entry.Tenant is null))
        {
            await EnsureTenantlessHoldsVisibleAsync(dbTransaction, ct);
        }

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
        var orphanedBefore = await ReadActiveHoldsWithoutRowAsync(dbTransaction, ct);
        try
        {
            await deletion(ct);
            if (db.ChangeTracker.HasChanges())
            {
                throw new InvalidOperationException(
                    "The retention deletion callback returned with unsaved changes. Save them inside the callback; nothing was committed."
                );
            }

            var violated = (await ReadActiveHoldsWithoutRowAsync(dbTransaction, ct))
                .Except(orphanedBefore)
                .Order()
                .ToArray();
            if (violated.Length > 0)
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

    /// <summary>
    /// The hold checks see only the holds the caller's scope can read. A tenant scope whose
    /// holds policy hides NULL-tenant holds would pass a held tenantless row as unheld, so the
    /// caller must see every active tenantless hold that system work sees.
    /// </summary>
    private async Task EnsureTenantlessHoldsVisibleAsync(DbTransaction transaction, CancellationToken ct)
    {
        var hidden = await ReadSystemTenantlessHoldsAsync(ct);
        if (hidden.Count == 0)
        {
            return;
        }

        hidden.ExceptWith(await ReadActiveTenantlessHoldsAsync(db, transaction, ct));
        if (hidden.Count == 0)
        {
            return;
        }

        // A hold that ended between the two reads was not hidden; one still active now was.
        hidden.IntersectWith(await ReadSystemTenantlessHoldsAsync(ct));
        if (hidden.Count > 0)
        {
            throw new RetentionConfigurationException([
                $"Retention deletion's scope cannot see {hidden.Count} active hold(s) on tenantless rows that Cohort's System(Readiness) work can, so it cannot tell whether a tenantless row it deletes is held. The holds table's row-level security must let every scope read holds whose \"TenantId\" is NULL. Nothing was deleted.",
            ]);
        }
    }

    private Task<HashSet<Guid>> ReadSystemTenantlessHoldsAsync(CancellationToken ct) =>
        scopeFactory.RunInScopeAsync(
            new RetentionExecutionContext.System(RetentionSystemWork.Readiness),
            async services =>
            {
                var systemDb = services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext);
                await systemDb.Database.OpenConnectionAsync(ct);
                try
                {
                    return await ReadActiveTenantlessHoldsAsync(systemDb, null, ct);
                }
                finally
                {
                    await systemDb.Database.CloseConnectionAsync();
                }
            },
            ct
        );

    private async Task<HashSet<Guid>> ReadActiveTenantlessHoldsAsync(
        DbContext context,
        DbTransaction? transaction,
        CancellationToken ct
    )
    {
        await using var command = new SqlParams().CreateCommand(
            context.Database.GetDbConnection(),
            transaction,
            RetentionHoldSql.BuildActiveTenantlessHoldIds(tables.RetentionHolds)
        );
        var holdIds = new HashSet<Guid>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            holdIds.Add(reader.GetGuid(0));
        }

        return holdIds;
    }

    private async Task<HashSet<Guid>> ReadActiveHoldsWithoutRowAsync(
        DbTransaction transaction,
        CancellationToken ct
    )
    {
        var (sql, parameters) = RetentionHoldSql.BuildActiveHoldsWithoutRow(
            tables.RetentionHolds,
            registry.Scan().Values.ToList()
        );
        await using var command = parameters.CreateCommand(
            db.Database.GetDbConnection(),
            transaction,
            sql
        );
        var holdIds = new HashSet<Guid>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            holdIds.Add(reader.GetGuid(0));
        }

        return holdIds;
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
