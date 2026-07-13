using Cohort.Application;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure.Holds;

internal sealed class EfRetentionDeletion(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionTargetResolver targetResolver,
    RetentionRuntimeReadinessValidator readinessValidator
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

        await readinessValidator.ValidateAsync(ct);
        await using var transaction = await db.Database.BeginTransactionAsync(ct);
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

        foreach (var target in resolvedTargets)
        {
            if (await HasActiveHoldAsync(target, dbTransaction, ct))
            {
                return RetentionDeletionOutcome.Protected;
            }
        }

        await deletion(ct);
        await transaction.CommitAsync(ct);
        return RetentionDeletionOutcome.Executed;
    }

    private async Task<bool> HasActiveHoldAsync(
        ResolvedTarget target,
        System.Data.Common.DbTransaction transaction,
        CancellationToken ct
    )
    {
        await using var command = db.Database.GetDbConnection().CreateCommand();
        command.Transaction = transaction;
        var tenantPredicate = target.Entry.Tenant is not null
            ? "AND \"TenantId\" = @tenantId"
            : "AND \"TenantId\" IS NULL";
        command.CommandText = $"""
            SELECT 1
            FROM {PostgreSqlIdentifier.Format(tables.RetentionHolds)}
            WHERE "RetentionEntityId" = @retentionEntityId
              AND "RecordId" = @recordId
              {tenantPredicate}
              AND "CreatedAt" <= pg_catalog.statement_timestamp()
              AND ("ExpiresAt" IS NULL OR "ExpiresAt" > pg_catalog.statement_timestamp())
              AND ("RemovedAt" IS NULL OR "RemovedAt" > pg_catalog.statement_timestamp())
            LIMIT 1
            """;
        command.Parameters.Add(
            RetentionHoldSql.CreateParameter(
                command,
                "retentionEntityId",
                target.Entry.RetentionEntityId
            )
        );
        command.Parameters.Add(
            RetentionHoldSql.CreateParameter(command, "recordId", target.RecordId)
        );
        if (target.Entry.Tenant is not null)
        {
            command.Parameters.Add(
                RetentionHoldSql.CreateParameter(command, "tenantId", target.TenantId!.Value)
            );
        }

        return await command.ExecuteScalarAsync(ct) is not null;
    }

    private sealed record ResolvedTarget(
        RetentionEntry Entry,
        string RecordId,
        Guid? TenantId
    );
}
