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
    RetentionRegistry registry
)
    : IRetentionHoldsRepository
{
    public async Task CreateAsync(RetentionHoldRequest request, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);

        // A hold with an unknown stable identity or record-id format the sweep-side NOT EXISTS match
        // never hits would look persisted while protecting nothing. Fail loudly instead.
        var entry = ResolveTarget(request.RetentionEntityId);
        ValidateTenantOwnership(entry, request.TenantId);

        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            var existingTransaction = db.Database.CurrentTransaction;
            await using var ownedTransaction = existingTransaction is null
                ? await db.Database.BeginTransactionAsync(ct)
                : null;
            var transaction = (existingTransaction ?? ownedTransaction)!.GetDbTransaction();
            var recordId = await CanonicaliseRecordIdAsync(
                entry,
                request.RecordId,
                transaction,
                ct
            );
            await RetentionEntityLockSql.AcquireAsync(
                connection,
                transaction,
                entry.EntityId,
                request.TenantId,
                recordId,
                ct
            );
            await ValidateTenantMatchesTargetRowAsync(
                entry,
                recordId,
                request,
                transaction,
                ct
            );

            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"""
                INSERT INTO {RetentionHoldSql.QuoteIdentifier(RetentionHoldSql.TableName)} (
                    "HoldId",
                    "RetentionEntityId",
                    "RecordId",
                    "TenantId",
                    "Reason",
                    "CreatedAt",
                    "ExpiresAt",
                    "RemovedAt"
                )
                VALUES (
                    @holdId,
                    @retentionEntityId,
                    @recordId,
                    @tenantId,
                    @reason,
                    @createdAt,
                    @expiresAt,
                    NULL
                )
                """;
            command.Parameters.Add(
                RetentionHoldSql.CreateParameter(command, "holdId", request.HoldId)
            );
            command.Parameters.Add(
                RetentionHoldSql.CreateParameter(command, "retentionEntityId", request.RetentionEntityId)
            );
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "recordId", recordId));
            command.Parameters.Add(
                RetentionHoldSql.CreateParameter(command, "tenantId", (object?)request.TenantId ?? DBNull.Value)
            );
            command.Parameters.Add(
                RetentionHoldSql.CreateParameter(command, "reason", request.Reason)
            );
            command.Parameters.Add(
                RetentionHoldSql.CreateParameter(command, "createdAt", request.CreatedAt)
            );
            command.Parameters.Add(
                RetentionHoldSql.CreateParameter(
                    command,
                    "expiresAt",
                    (object?)request.ExpiresAt ?? DBNull.Value
                )
            );

            await command.ExecuteNonQueryAsync(ct);
            if (ownedTransaction is not null)
            {
                await ownedTransaction.CommitAsync(ct);
            }
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }
    }

    /// <summary>
    /// The sweep-side exclusion on tenanted tables only honours holds whose TenantId
    /// matches the row's tenant, so a hold created under the wrong tenant would look
    /// persisted while protecting nothing. When the target row already exists on a
    /// retained tenanted entity, its tenant must match the request. A row that does not
    /// exist yet is allowed — holds may legitimately be created ahead of their row.
    /// </summary>
    private async Task ValidateTenantMatchesTargetRowAsync(
        RetentionEntry entry,
        string recordId,
        RetentionHoldRequest request,
        DbTransaction transaction,
        CancellationToken ct
    )
    {
        if (entry.Tenant is null)
        {
            return;
        }

        var connection = db.Database.GetDbConnection();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            SELECT target.{RetentionHoldSql.QuoteIdentifier(entry.Tenant.TenantColumn)}
            FROM {RetentionHoldSql.QuoteIdentifier(entry.TableName)} AS target
            WHERE {RecordIdSql.EqualsParameter("target", entry.RecordId, "recordId")}
            LIMIT 1
            """;
        command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "recordId", recordId));

        var rowTenant = await command.ExecuteScalarAsync(ct);
        if (rowTenant is null)
        {
            return;
        }

        if (rowTenant is Guid tenantId && tenantId == request.TenantId)
        {
            return;
        }

        var actualTenant = rowTenant is DBNull ? "NULL" : rowTenant.ToString();
        throw new InvalidOperationException(
            $"Retention hold for entity '{request.RetentionEntityId}', record '{request.RecordId}' was requested under tenant '{request.TenantId}', but the row belongs to tenant '{actualTenant}'. Sweeps only honour holds whose TenantId matches the row's tenant, so this hold would persist while protecting nothing."
        );
    }

    private RetentionEntry ResolveTarget(Guid retentionEntityId)
    {
        return registry.Scan().Values.SingleOrDefault(entry => entry.EntityId == retentionEntityId)
            ?? throw new InvalidOperationException(
                $"Retention entity ID '{retentionEntityId}' does not match a retained entity in the EF model."
            );
    }

    private static void ValidateTenantOwnership(RetentionEntry entry, Guid? tenantId)
    {
        if (entry.Tenant is not null && (tenantId is null || tenantId == Guid.Empty))
        {
            throw new InvalidOperationException(
                $"Retention hold for tenanted entity '{entry.EntityId}' requires a non-empty tenant ID."
            );
        }

        if (entry.Tenant is null && tenantId is not null)
        {
            throw new InvalidOperationException(
                $"Retention hold for tenantless entity '{entry.EntityId}' requires a null tenant ID."
            );
        }
    }

    private async Task<string> CanonicaliseRecordIdAsync(
        RetentionEntry entry,
        string recordId,
        DbTransaction? transaction,
        CancellationToken ct
    )
    {
        var keyClrType =
            Nullable.GetUnderlyingType(entry.RecordId.RecordIdType)
            ?? entry.RecordId.RecordIdType;
        if (keyClrType == typeof(Guid) && !Guid.TryParse(recordId, out _))
        {
            throw new InvalidOperationException(
                $"Retention hold record id '{recordId}' for entity '{entry.EntityId}' is not a valid Guid. The hold would never match its row."
            );
        }

        if (PostgresStoreTypeSql.Validate(entry.RecordId.RecordIdStoreType) is not { } storeType)
        {
            return keyClrType == typeof(Guid) ? Guid.Parse(recordId).ToString("D") : recordId;
        }

        await using var command = db.Database.GetDbConnection().CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT CAST(CAST(@recordId AS {storeType}) AS text)";
        command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "recordId", recordId));

        try
        {
            return (string)(await command.ExecuteScalarAsync(ct))!;
        }
        catch (DbException exception) when (exception.SqlState?.StartsWith("22", StringComparison.Ordinal) == true)
        {
            throw new InvalidOperationException(
                $"Retention hold record id '{recordId}' for entity '{entry.EntityId}' is not valid for provider type '{storeType}'. The hold would never match its row.",
                exception
            );
        }
    }

    public async Task RemoveAsync(Guid holdId, DateTimeOffset removedAt, CancellationToken ct)
    {
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            await using var command = connection.CreateCommand();
            command.Transaction = db.Database.CurrentTransaction?.GetDbTransaction();
            command.CommandText = $"""
                UPDATE {RetentionHoldSql.QuoteIdentifier(RetentionHoldSql.TableName)}
                SET "RemovedAt" = @removedAt
                WHERE "HoldId" = @holdId
                  AND "RemovedAt" IS NULL
                """;
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "holdId", holdId));
            command.Parameters.Add(
                RetentionHoldSql.CreateParameter(command, "removedAt", removedAt)
            );

            var affected = await command.ExecuteNonQueryAsync(ct);
            if (affected == 0)
            {
                throw new InvalidOperationException(
                    $"Retention hold '{holdId}' could not be removed because it does not exist or is already removed."
                );
            }
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }
    }

    public async Task<IReadOnlyList<RetentionHold>> ListActiveAsync(
        DateTimeOffset asOf,
        CancellationToken ct
    )
    {
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            await using var command = connection.CreateCommand();
            command.Transaction = db.Database.CurrentTransaction?.GetDbTransaction();
            command.CommandText = $"""
                SELECT "HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt"
                FROM {RetentionHoldSql.QuoteIdentifier(RetentionHoldSql.TableName)}
                WHERE "CreatedAt" <= @asOf
                  AND ("ExpiresAt" IS NULL OR "ExpiresAt" > @asOf)
                  AND ("RemovedAt" IS NULL OR "RemovedAt" > @asOf)
                ORDER BY "RetentionEntityId", "RecordId", "HoldId"
                """;
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "asOf", asOf));

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
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }
    }

    public async Task<bool> HasActiveHoldAsync(
        Guid retentionEntityId,
        string recordId,
        Guid? tenantId,
        DateTimeOffset asOf,
        CancellationToken ct
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordId);
        var entry = ResolveTarget(retentionEntityId);
        ValidateTenantOwnership(entry, tenantId);
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            var transaction = db.Database.CurrentTransaction?.GetDbTransaction();
            var canonicalRecordId = await CanonicaliseRecordIdAsync(
                entry,
                recordId,
                transaction,
                ct
            );
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            var tenantPredicate = entry.Tenant is not null
                ? "AND \"TenantId\" = @tenantId"
                : "AND \"TenantId\" IS NULL";
            command.CommandText = $"""
                SELECT 1
                FROM {RetentionHoldSql.QuoteIdentifier(RetentionHoldSql.TableName)}
                WHERE "RetentionEntityId" = @retentionEntityId
                  AND "RecordId" = @recordId
                  {tenantPredicate}
                  AND "CreatedAt" <= @asOf
                  AND ("ExpiresAt" IS NULL OR "ExpiresAt" > @asOf)
                  AND ("RemovedAt" IS NULL OR "RemovedAt" > @asOf)
                LIMIT 1
                """;
            command.Parameters.Add(
                RetentionHoldSql.CreateParameter(command, "retentionEntityId", retentionEntityId)
            );
            command.Parameters.Add(
                RetentionHoldSql.CreateParameter(command, "recordId", canonicalRecordId)
            );
            if (entry.Tenant is not null)
            {
                command.Parameters.Add(
                    RetentionHoldSql.CreateParameter(command, "tenantId", tenantId!.Value)
                );
            }
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "asOf", asOf));

            var result = await command.ExecuteScalarAsync(ct);
            return result is not null;
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }
    }
}
