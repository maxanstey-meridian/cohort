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
    RetentionRuntimeReadinessValidator readinessValidator
)
    : IRetentionHoldsRepository
{
    private readonly CohortStoreTables tables = CohortStoreTables.FromModel(db.Model);

    public async Task CreateAsync(RetentionHoldRequest request, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);

        // A hold with an unknown stable identity or record-id format the sweep-side NOT EXISTS match
        // never hits would look persisted while protecting nothing. Fail loudly instead.
        var entry = targetResolver.ResolveTarget(request.RetentionEntityId);
        RetentionTargetResolver.ValidateTenantOwnership(entry, request.TenantId, "Retention hold");
        await readinessValidator.ValidateAsync(ct);

        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;
        Exception? primaryException = null;

        try
        {
            if (shouldCloseConnection)
            {
                await db.Database.OpenConnectionAsync(ct);
            }

            var existingTransaction = db.Database.CurrentTransaction;
            await using var ownedTransaction = existingTransaction is null
                ? await db.Database.BeginTransactionAsync(ct)
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
                connection,
                transaction,
                entry.RetentionEntityId,
                request.TenantId,
                recordId,
                ct
            );
            await ValidateTargetRowAsync(
                entry,
                recordId,
                request,
                transaction,
                ct
            );

            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"""
                INSERT INTO {PostgreSqlIdentifier.Format(tables.RetentionHolds)} (
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
        catch (Exception ex)
        {
            primaryException = ex;
            throw;
        }
        finally
        {
            await CloseOwnedConnectionAsync(shouldCloseConnection, primaryException);
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
        var connection = db.Database.GetDbConnection();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        var targetValue = entry.Tenant is null
            ? "1"
            : $"target.{RetentionHoldSql.QuoteIdentifier(entry.Tenant.TenantColumn)}";
        command.CommandText = $"""
            SELECT {targetValue}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {RecordIdSql.EqualsParameter("target", entry.RecordId, "recordId")}
            LIMIT 1
            """;
        command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "recordId", recordId));

        var rowTenant = await command.ExecuteScalarAsync(ct);
        if (rowTenant is null)
        {
            throw new InvalidOperationException(
                $"Retention hold for entity '{request.RetentionEntityId}', target record '{recordId}' does not exist."
            );
        }

        if (entry.Tenant is null)
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

    public async Task RemoveAsync(Guid holdId, DateTimeOffset removedAt, CancellationToken ct)
    {
        await readinessValidator.ValidateAsync(ct);
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;
        Exception? primaryException = null;

        try
        {
            if (shouldCloseConnection)
            {
                await db.Database.OpenConnectionAsync(ct);
            }

            await using var command = connection.CreateCommand();
            command.Transaction = db.Database.CurrentTransaction?.GetDbTransaction();
            command.CommandText = $"""
                UPDATE {PostgreSqlIdentifier.Format(tables.RetentionHolds)}
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
        catch (Exception ex)
        {
            primaryException = ex;
            throw;
        }
        finally
        {
            await CloseOwnedConnectionAsync(shouldCloseConnection, primaryException);
        }
    }

    public async Task<IReadOnlyList<RetentionHold>> ListActiveAsync(
        DateTimeOffset asOf,
        CancellationToken ct
    )
    {
        await readinessValidator.ValidateAsync(ct);
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;
        Exception? primaryException = null;

        try
        {
            if (shouldCloseConnection)
            {
                await db.Database.OpenConnectionAsync(ct);
            }

            await using var command = connection.CreateCommand();
            command.Transaction = db.Database.CurrentTransaction?.GetDbTransaction();
            command.CommandText = $"""
                SELECT "HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt"
                FROM {PostgreSqlIdentifier.Format(tables.RetentionHolds)}
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
        catch (Exception ex)
        {
            primaryException = ex;
            throw;
        }
        finally
        {
            await CloseOwnedConnectionAsync(shouldCloseConnection, primaryException);
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
        var entry = targetResolver.ResolveTarget(retentionEntityId);
        RetentionTargetResolver.ValidateTenantOwnership(entry, tenantId, "Retention hold");
        await readinessValidator.ValidateAsync(ct);
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;
        Exception? primaryException = null;

        try
        {
            if (shouldCloseConnection)
            {
                await db.Database.OpenConnectionAsync(ct);
            }

            var transaction = db.Database.CurrentTransaction?.GetDbTransaction();
            var canonicalRecordId = await targetResolver.CanonicaliseRecordIdAsync(
                entry,
                recordId,
                transaction,
                "Retention hold",
                ct
            );
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            var tenantPredicate = entry.Tenant is not null
                ? "AND \"TenantId\" = @tenantId"
                : "AND \"TenantId\" IS NULL";
            command.CommandText = $"""
                SELECT 1
                FROM {PostgreSqlIdentifier.Format(tables.RetentionHolds)}
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
        catch (Exception ex)
        {
            primaryException = ex;
            throw;
        }
        finally
        {
            await CloseOwnedConnectionAsync(shouldCloseConnection, primaryException);
        }
    }

    private Task CloseOwnedConnectionAsync(
        bool shouldCloseConnection,
        Exception? primaryException
    ) =>
        OperationalConnectionCleanup.RunAsync(
            null,
            shouldCloseConnection
                ? cleanupToken => db.Database.CloseConnectionAsync().WaitAsync(cleanupToken)
                : null,
            primaryException,
            null
        );
}
