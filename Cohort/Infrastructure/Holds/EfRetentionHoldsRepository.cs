using System.Data;

using Cohort.Application;
using Cohort.Domain;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Infrastructure.Holds;

public sealed class EfRetentionHoldsRepository(DbContext db) : IRetentionHoldsRepository
{
    public async Task CreateAsync(RetentionHoldRequest request, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.RecordId);

        // A hold with a table name or record-id format the sweep-side NOT EXISTS match
        // never hits would look persisted while protecting nothing. Fail loudly instead.
        var targetEntityType = FindEntityTypeForTable(request.TableName);
        var recordId = NormaliseRecordId(targetEntityType, request.RecordId);

        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"""
                INSERT INTO {RetentionHoldSql.QuoteIdentifier(RetentionHoldSql.TableName)} (
                    "HoldId",
                    "TableName",
                    "RecordId",
                    "TenantId",
                    "Reason",
                    "CreatedAt",
                    "ExpiresAt",
                    "RemovedAt"
                )
                VALUES (
                    @holdId,
                    @tableName,
                    @recordId,
                    @tenantId,
                    @reason,
                    @createdAt,
                    @expiresAt,
                    NULL
                )
                """;
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "holdId", request.HoldId));
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "tableName", request.TableName));
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "recordId", recordId));
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "tenantId", request.TenantId));
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "reason", request.Reason));
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
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }
    }

    private Microsoft.EntityFrameworkCore.Metadata.IEntityType FindEntityTypeForTable(
        string tableName
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var entityType = db.Model
            .GetEntityTypes()
            .FirstOrDefault(candidate =>
                string.Equals(candidate.GetTableName(), tableName, StringComparison.Ordinal)
            );

        if (entityType is null)
        {
            throw new InvalidOperationException(
                $"Retention hold table name '{tableName}' does not match any table mapped by the EF model. Holds are matched by exact physical table name; a mismatched name would persist a hold that protects nothing."
            );
        }

        return entityType;
    }

    private static string NormaliseRecordId(
        Microsoft.EntityFrameworkCore.Metadata.IEntityType entityType,
        string recordId
    )
    {
        // The sweep-side exclusion compares CAST(id AS text) = "RecordId". Postgres casts
        // uuid to lowercase hyphenated text, so Guid-keyed holds must be stored in exactly
        // that format or they silently never match.
        var primaryKeyProperties = entityType.FindPrimaryKey()?.Properties;
        if (primaryKeyProperties is not [var keyProperty])
        {
            return recordId;
        }

        var keyClrType = Nullable.GetUnderlyingType(keyProperty.ClrType) ?? keyProperty.ClrType;
        if (keyClrType != typeof(Guid))
        {
            return recordId;
        }

        if (!Guid.TryParse(recordId, out var parsed))
        {
            throw new InvalidOperationException(
                $"Retention hold record id '{recordId}' for table '{entityType.GetTableName()}' is not a valid Guid, but the table's primary key is Guid-typed. The hold would never match its row."
            );
        }

        return parsed.ToString("D");
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
            command.CommandText =
                $"""
                UPDATE {RetentionHoldSql.QuoteIdentifier(RetentionHoldSql.TableName)}
                SET "RemovedAt" = @removedAt
                WHERE "HoldId" = @holdId
                  AND "RemovedAt" IS NULL
                """;
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "holdId", holdId));
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "removedAt", removedAt));

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
            command.CommandText =
                $"""
                SELECT "HoldId", "TableName", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt"
                FROM {RetentionHoldSql.QuoteIdentifier(RetentionHoldSql.TableName)}
                WHERE "CreatedAt" <= @asOf
                  AND ("ExpiresAt" IS NULL OR "ExpiresAt" > @asOf)
                  AND ("RemovedAt" IS NULL OR "RemovedAt" > @asOf)
                ORDER BY "TableName", "RecordId", "HoldId"
                """;
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "asOf", asOf));

            var holds = new List<RetentionHold>();
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                holds.Add(
                    new RetentionHold(
                        reader.GetGuid(0),
                        reader.GetString(1),
                        reader.GetString(2),
                        reader.GetGuid(3),
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
        string tableName,
        string recordId,
        Guid tenantId,
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
            command.CommandText =
                $"""
                SELECT 1
                FROM {RetentionHoldSql.QuoteIdentifier(RetentionHoldSql.TableName)}
                WHERE "TableName" = @tableName
                  AND "RecordId" = @recordId
                  AND "TenantId" = @tenantId
                  AND "CreatedAt" <= @asOf
                  AND ("ExpiresAt" IS NULL OR "ExpiresAt" > @asOf)
                  AND ("RemovedAt" IS NULL OR "RemovedAt" > @asOf)
                LIMIT 1
                """;
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "tableName", tableName));
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "recordId", recordId));
            command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "tenantId", tenantId));
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
