using System.Data;
using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure.Holds;

// Raw SQL interpolates only identifiers quoted from the EF model; values are parameters.
#pragma warning disable EF1002

internal sealed class EfRetentionHoldsRepository(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionTargetResolver targetResolver,
    RetentionRuntimeReadinessValidator readinessValidator,
    RetentionRegistry registry,
    RetentionValidationState validationState
)
    : IRetentionHoldsRepository
{
    private readonly CohortStoreTables tables = CohortStoreTables.FromModel(db.Model);

    public async Task CreateAsync(RetentionHoldRequest request, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);

        // A hold with an unknown stable identity or record-id format the sweep-side NOT EXISTS
        // match never hits would look persisted while protecting nothing. Fail loudly instead.
        var entry = targetResolver.ResolveTarget(request.RetentionEntityId);
        RetentionTargetResolver.ValidateTenantOwnership(entry, request.TenantId, "Retention hold");
        await readinessValidator.ValidateAsync(ct);

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

            await using var command = new SqlParams
            {
                ["holdId"] = request.HoldId,
                ["retentionEntityId"] = request.RetentionEntityId,
                ["recordId"] = recordId,
                ["tenantId"] = request.TenantId,
                ["reason"] = request.Reason,
                ["createdAt"] = request.CreatedAt,
                ["expiresAt"] = request.ExpiresAt,
            }.CreateCommand(
                db.Database.GetDbConnection(),
                transaction,
                // An expiry the database clock has already reached would store a hold that never
                // protects. Refuse it by row count, not a SQL error, so a caller-owned transaction
                // stays usable. CAST: an untyped NULL can't be inferred in IS NULL.
                $"""
                INSERT INTO {PostgreSqlIdentifier.Format(tables.RetentionHolds)}
                    ("HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt")
                SELECT
                    @holdId, @retentionEntityId, @recordId, @tenantId, @reason,
                    -- A hold is active from the moment it commits; never let an application clock
                    -- ahead of Postgres defer that.
                    LEAST(@createdAt, {DatabaseClock.Sql}),
                    CAST(@expiresAt AS timestamptz), NULL
                WHERE CAST(@expiresAt AS timestamptz) IS NULL
                   OR CAST(@expiresAt AS timestamptz) > {DatabaseClock.Sql}
                """
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
        await using var command = new SqlParams().CreateCommand(
            db.Database.GetDbConnection(),
            transaction,
            "SELECT pg_catalog.current_setting('transaction_isolation')"
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
        foreach (var sql in RetentionHoldSql.BuildCoveredRowLocks(entry, registry.Scan().Values))
        {
            var parameters = new SqlParams { ["recordId"] = recordId };
            if (entry.Tenant is not null)
            {
                parameters["tenantId"] = tenantId;
            }

            await using var command = parameters.CreateCommand(db.Database.GetDbConnection(), transaction, sql);
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
        var targetValue = entry.Tenant is null
            ? "1"
            : $"target.{PostgreSqlIdentifier.Quote(entry.Tenant.TenantColumn)}";
        // FOR SHARE reads the row's current version, not the caller's snapshot: under
        // REPEATABLE READ a row deleted since the snapshot raises a serialization failure
        // instead of taking a hold that protects nothing.
        await using var command = new SqlParams { ["recordId"] = recordId }.CreateCommand(
            db.Database.GetDbConnection(),
            transaction,
            $"""
            SELECT {targetValue}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {RecordIdSql.EqualsParameter("target", entry.RecordId, "recordId")}
            LIMIT 1
            FOR SHARE
            """
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
        await readinessValidator.ValidateAsync(ct);
        var removed = await db.Database.ExecuteSqlRawAsync(
            $"""
            UPDATE {PostgreSqlIdentifier.Format(tables.RetentionHolds)}
            SET "RemovedAt" = @removedAt
            WHERE "HoldId" = @holdId
              AND "RemovedAt" IS NULL
            """,
            new SqlParams { ["holdId"] = holdId, ["removedAt"] = removedAt.ToUniversalTime() }.ToDbParameters(db.Database.GetDbConnection()),
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
        await readinessValidator.ValidateAsync(ct);
        await db.Database.OpenConnectionAsync(ct);
        try
        {
            await using var command = new SqlParams().CreateCommand(
                db.Database.GetDbConnection(),
                db.Database.CurrentTransaction?.GetDbTransaction(),
                $"""
                SELECT "HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt"
                FROM {PostgreSqlIdentifier.Format(tables.RetentionHolds)} AS hold
                WHERE {RetentionHoldSql.ActivePredicate("hold")}
                ORDER BY "RetentionEntityId", "RecordId", "HoldId"
                """
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
        Func<RelationalObjectName, RetentionEntry, string, Guid?, (string Sql, SqlParams Parameters)> build,
        Func<DbCommand, Task<TResult>> read,
        CancellationToken ct
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordId);
        var entry = targetResolver.ResolveTarget(retentionEntityId);
        RetentionTargetResolver.ValidateTenantOwnership(entry, tenantId, "Retention hold");
        await readinessValidator.ValidateAsync(ct);

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
            var (sql, parameters) = build(tables.RetentionHolds, entry, canonicalRecordId, tenantId);
            await using var command = parameters.CreateCommand(db.Database.GetDbConnection(), transaction, sql);
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
        await readinessValidator.ValidateAsync(ct);
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
            var (sql, parameters) = RetentionHoldSql.BuildCoveringHoldsForSubject(tables.RetentionHolds, starts, tenantId);
            await using var command = parameters.CreateCommand(
                db.Database.GetDbConnection(),
                db.Database.CurrentTransaction?.GetDbTransaction(),
                sql
            );
            return await ReadHoldsAsync(command, ct);
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }
    }
}
