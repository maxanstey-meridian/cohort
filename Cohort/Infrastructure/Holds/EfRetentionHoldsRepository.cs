using System.Data;
using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure.Holds;

internal sealed class EfRetentionHoldsRepository(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionTargetResolver targetResolver,
    RetentionRuntimeReadinessValidator readinessValidator,
    RetentionRegistry registry,
    RetentionValidationState validationState
)
    : IRetentionHoldsRepository
{
    private CohortStoreTables? tables;

    // Read lazily, after readiness, so a model with no Cohort mapping reaches readiness's refusal.
    private CohortStoreTables Tables => tables ??= CohortStoreTables.FromModel(db.Model);

    public async Task CreateAsync(RetentionHoldRequest request, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Readiness first: resolving the target reads the Cohort mapping, which readiness refuses
        // with its aggregated message when it is missing.
        await readinessValidator.ValidateAsync(RetentionStatementGroup.Holds, ct);
        // A hold with an unknown stable identity or record-id format the sweep-side NOT EXISTS
        // match never hits would look persisted while protecting nothing. Fail loudly instead.
        var entry = targetResolver.ResolveTarget(request.RetentionEntityId);
        RetentionTargetResolver.ValidateTenantOwnership(entry, request.TenantId, "Retention hold");

        await db.Database.OpenConnectionAsync(ct);
        try
        {
            var existingTransaction = db.Database.CurrentTransaction;
            if (existingTransaction is not null && entry.CoveredPaths.Count > 0)
            {
                await RequireReadCommittedAsync(existingTransaction.GetDbTransaction(), request, ct);
            }

            await using var ownedTransaction = existingTransaction is null
                ? await db.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct)
                : null;
            var transaction = (existingTransaction ?? ownedTransaction)!.GetDbTransaction();
            var recordId = await targetResolver.CanonicaliseRecordIdAsync(
                entry,
                request.RecordId,
                transaction,
                "Retention hold",
                ct
            );
            await RetentionEntityLockSql.AcquireAsync(
                db.Database.GetDbConnection(),
                transaction,
                entry.RetentionEntityId,
                request.TenantId,
                recordId,
                ct
            );
            await ValidateTargetRowAsync(entry, recordId, request, transaction, ct);
            await LockCoveredRowsAsync(entry, recordId, request.TenantId, transaction, ct);

            // An expiry the database clock has already reached would store a hold that never
            // protects. The statement refuses it by row count, not a SQL error, so a caller-owned
            // transaction stays usable.
            var create = RetentionHoldSql.BuildCreateHold(
                Tables,
                request.HoldId,
                request.RetentionEntityId,
                recordId,
                request.TenantId,
                request.Reason,
                request.CreatedAt,
                request.ExpiresAt
            );
            await using var command = create.Parameters.CreateCommand(
                db.Database.GetDbConnection(),
                transaction,
                create.Sql
            );
            if (await command.ExecuteNonQueryAsync(ct) == 0)
            {
                throw new InvalidOperationException(
                    $"Retention hold '{request.HoldId}' expires at {request.ExpiresAt:O}, which is not after the database clock. It would persist while protecting nothing."
                );
            }

            if (ownedTransaction is not null)
            {
                await ownedTransaction.CommitAsync(ct);
            }
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }
    }

    /// <summary>
    /// A hold that covers related rows locks them (<see cref="LockCoveredRowsAsync"/>) with
    /// statements that must each read the current rows, so it can't join a transaction-wide
    /// snapshot: a row linked after the snapshot would not be locked.
    /// </summary>
    private async Task RequireReadCommittedAsync(
        DbTransaction transaction,
        RetentionHoldRequest request,
        CancellationToken ct
    )
    {
        var statement = CohortStatements.TransactionIsolation();
        await using var command = statement.Parameters.CreateCommand(
            db.Database.GetDbConnection(),
            transaction,
            statement.Sql
        );
        var isolation = (string?)await command.ExecuteScalarAsync(ct);
        // PostgreSQL accepts READ UNCOMMITTED and runs it as READ COMMITTED.
        if (isolation is not ("read committed" or "read uncommitted"))
        {
            throw new InvalidOperationException(
                $"Retention hold for entity '{request.RetentionEntityId}' covers related rows, so it needs a READ COMMITTED transaction, but the caller's is {isolation?.ToUpperInvariant()}: under a transaction-wide snapshot a row linked after the transaction began would not be locked. Nothing was changed."
            );
        }
    }

    /// <summary>
    /// Row-locks, <c>FOR SHARE</c>, every row the new hold will cover through a relation, after the
    /// target row is validated and before the hold is inserted, so a sweep, erasure, or deletion
    /// either finishes with such a row before the hold commits or sees the hold (see
    /// <see cref="RetentionHoldSql.BuildCoveredRowLocks"/>).
    /// </summary>
    private async Task LockCoveredRowsAsync(
        RetentionEntry entry,
        string recordId,
        Guid? tenantId,
        DbTransaction transaction,
        CancellationToken ct
    )
    {
        foreach (var statement in RetentionHoldSql.BuildCoveredRowLocks(entry, registry.Scan().Values, recordId, tenantId))
        {
            await using var command = statement.Parameters.CreateCommand(
                db.Database.GetDbConnection(),
                transaction,
                statement.Sql
            );
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private async Task ValidateTargetRowAsync(
        RetentionEntry entry,
        string recordId,
        RetentionHoldRequest request,
        DbTransaction transaction,
        CancellationToken ct
    )
    {
        var statement = RetentionHoldSql.BuildTargetRowLock(entry, recordId);
        await using var command = statement.Parameters.CreateCommand(
            db.Database.GetDbConnection(),
            transaction,
            statement.Sql
        );

        var rowTenant = await command.ExecuteScalarAsync(ct);
        if (rowTenant is null)
        {
            throw new InvalidOperationException(
                $"Retention hold for entity '{request.RetentionEntityId}', target record '{recordId}' does not exist."
            );
        }

        if (entry.Tenant is null || (rowTenant is Guid tenantId && tenantId == request.TenantId))
        {
            return;
        }

        var actualTenant = rowTenant is DBNull ? "NULL" : rowTenant.ToString();
        throw new InvalidOperationException(
            $"Retention hold for entity '{request.RetentionEntityId}', record '{request.RecordId}' was requested under tenant '{request.TenantId}', but the row belongs to tenant '{actualTenant}'. Sweeps only honour holds whose TenantId matches the row's tenant, so this hold would persist while protecting nothing."
        );
    }

    public async Task RemoveAsync(Guid holdId, DateTimeOffset removedAt, CancellationToken ct)
    {
        await readinessValidator.ValidateAsync(RetentionStatementGroup.Holds, ct);
        var remove = RetentionHoldSql.BuildRemoveHold(Tables, holdId, removedAt.ToUniversalTime());
        var removed = await db.Database.ExecuteSqlRawAsync(
            remove.Sql,
            remove.Parameters.ToDbParameters(db.Database.GetDbConnection()),
            ct
        );
        if (removed == 0)
        {
            throw new InvalidOperationException(
                $"Retention hold '{holdId}' could not be removed because it does not exist or is already removed."
            );
        }
    }

    public async Task<IReadOnlyList<RetentionHold>> ListActiveAsync(CancellationToken ct)
    {
        await readinessValidator.ValidateAsync(RetentionStatementGroup.Holds, ct);
        await db.Database.OpenConnectionAsync(ct);
        try
        {
            var list = RetentionHoldSql.BuildActiveHolds(Tables);
            await using var command = list.Parameters.CreateCommand(
                db.Database.GetDbConnection(),
                db.Database.CurrentTransaction?.GetDbTransaction(),
                list.Sql
            );

            return await ReadHoldsAsync(command, ct);
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }
    }

    private static async Task<IReadOnlyList<RetentionHold>> ReadHoldsAsync(DbCommand command, CancellationToken ct)
    {
        var holds = new List<RetentionHold>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            holds.Add(
                new RetentionHold(
                    reader.GetGuid(0),
                    reader.GetGuid(1),
                    reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetGuid(3),
                    reader.GetString(4),
                    reader.GetFieldValue<DateTimeOffset>(5),
                    reader.IsDBNull(6) ? null : reader.GetFieldValue<DateTimeOffset>(6),
                    reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7)
                )
            );
        }

        return holds;
    }

    public async Task<bool> HasActiveHoldAsync(
        Guid retentionEntityId,
        string recordId,
        Guid? tenantId,
        CancellationToken ct
    ) =>
        await ReadCoveringRecordAsync(
            retentionEntityId,
            recordId,
            tenantId,
            RetentionHoldSql.BuildAnyCoveringHold,
            async command => await command.ExecuteScalarAsync(ct) is not null,
            ct
        );

    public Task<IReadOnlyList<RetentionHold>> ListCoveringAsync(
        Guid retentionEntityId,
        string recordId,
        Guid? tenantId,
        CancellationToken ct
    ) =>
        ReadCoveringRecordAsync(
            retentionEntityId,
            recordId,
            tenantId,
            RetentionHoldSql.BuildCoveringHolds,
            command => ReadHoldsAsync(command, ct),
            ct
        );

    /// <summary>
    /// Validates the target as every record read does, then reads the holds that cover it with the
    /// statement <paramref name="build"/> makes, on the caller's transaction if there is one.
    /// </summary>
    private async Task<TResult> ReadCoveringRecordAsync<TResult>(
        Guid retentionEntityId,
        string recordId,
        Guid? tenantId,
        Func<RelationalObjectName, RetentionEntry, object?, object?, SweepStatement> build,
        Func<DbCommand, Task<TResult>> read,
        CancellationToken ct
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordId);
        // Readiness first, as in CreateAsync: resolving the target reads the Cohort mapping.
        await readinessValidator.ValidateAsync(RetentionStatementGroup.Holds, ct);
        var entry = targetResolver.ResolveTarget(retentionEntityId);
        RetentionTargetResolver.ValidateTenantOwnership(entry, tenantId, "Retention hold");

        await db.Database.OpenConnectionAsync(ct);
        try
        {
            var transaction = db.Database.CurrentTransaction?.GetDbTransaction();
            var canonicalRecordId = await targetResolver.CanonicaliseRecordIdAsync(
                entry,
                recordId,
                transaction,
                "Retention hold",
                ct
            );
            var statement = build(Tables.RetentionHolds, entry, canonicalRecordId, tenantId);
            await using var command = statement.Parameters.CreateCommand(
                db.Database.GetDbConnection(),
                transaction,
                statement.Sql
            );
            return await read(command);
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }
    }

    public async Task<IReadOnlyList<RetentionHold>> ListCoveringSubjectAsync(
        string subjectKind,
        object subject,
        Guid? tenantId,
        CancellationToken ct
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subjectKind);
        ArgumentNullException.ThrowIfNull(subject);
        if (tenantId == Guid.Empty)
        {
            throw new ArgumentException("A tenant ID cannot be empty; use null for tenantless entities.", nameof(tenantId));
        }

        var entities = tenantId is null ? SweepEntityScope.TenantlessOnly : SweepEntityScope.TenantedOnly;
        // After readiness, like erasure: startup validation fills the state the check reads.
        await readinessValidator.ValidateAsync(RetentionStatementGroup.Holds, ct);
        var validation = validationState.For(db.Model);
        ErasureSubjectValidation.Validate(validation, registry, subjectKind, subject, entities, "the hold lookup");

        var starts = registry.Scan().Values
            .Where(entry => entities.Includes(entry))
            .Select(entry => (
                Entry: entry,
                Metadata: validation.ErasureSubjects[entry.EntityType]
                    .FirstOrDefault(metadata => string.Equals(metadata.Kind, subjectKind, StringComparison.Ordinal))
            ))
            .Where(start => start.Metadata is not null)
            .Select(start => (start.Entry, start.Metadata!.CreatePredicate(subject)))
            .ToList();

        await db.Database.OpenConnectionAsync(ct);
        try
        {
            // Validated above, so some entity declares the kind; null (no starts) reads no holds.
            if (RetentionHoldSql.BuildCoveringHoldsForSubject(Tables.RetentionHolds, starts, tenantId) is not { } statement)
            {
                return [];
            }

            await using var command = statement.Parameters.CreateCommand(
                db.Database.GetDbConnection(),
                db.Database.CurrentTransaction?.GetDbTransaction(),
                statement.Sql
            );
            return await ReadHoldsAsync(command, ct);
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }
    }
}
