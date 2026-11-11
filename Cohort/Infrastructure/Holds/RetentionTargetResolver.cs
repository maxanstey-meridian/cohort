using System.Data.Common;

using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure.Holds;

internal sealed class RetentionTargetResolver(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionRegistry registry
)
{
    private const string CastSavepoint = "cohort_record_id_cast";

    internal RetentionEntry ResolveTarget(Guid retentionEntityId)
    {
        return registry.Scan().Values.SingleOrDefault(entry => entry.RetentionEntityId == retentionEntityId)
            ?? throw new InvalidOperationException(
                $"Retention entity ID '{retentionEntityId}' does not match a retained entity in the EF model."
            );
    }

    internal static void ValidateTenantOwnership(
        RetentionEntry entry,
        Guid? tenantId,
        string operation
    )
    {
        if (entry.Tenant is not null && (tenantId is null || tenantId == Guid.Empty))
        {
            throw new InvalidOperationException(
                $"{operation} for tenanted entity '{entry.RetentionEntityId}' requires a non-empty tenant ID."
            );
        }

        if (entry.Tenant is null && tenantId is not null)
        {
            throw new InvalidOperationException(
                $"{operation} for tenantless entity '{entry.RetentionEntityId}' requires a null tenant ID."
            );
        }
    }

    /// <summary>
    /// S18: the canonical text of a record id through the stored row or the store type. Prefers
    /// the stored row's own text: equality under the column type (citext, unscaled numeric) can
    /// admit spellings whose cast-to-text differs from what sweeps compare. Null when the store
    /// type isn't a plain type name to cast to: the run canonicalises in memory and issues no
    /// statement.
    /// </summary>
    internal static SweepStatement? CanonicaliseStatement(RetentionEntry entry, object? recordId)
    {
        if (PostgresStoreTypeSql.CastType(entry.RecordId.RecordIdStoreType) is not { } storeType)
        {
            return null;
        }

        return new SweepStatement(
            "canonicalise-target-record-id",
            entry.EntityType,
            $"""
            SELECT COALESCE(
                (
                    SELECT {RecordIdSql.TextExpression("target", entry.RecordId)}
                    FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
                    WHERE {RecordIdSql.EqualsParameter("target", entry.RecordId, "recordId")}
                    LIMIT 1
                ),
                CAST(CAST(@recordId AS {storeType}) AS text)
            )
            """,
            new SqlParams { ["recordId"] = recordId },
            [entry.Table]
        );
    }

    internal async Task<string> CanonicaliseRecordIdAsync(
        RetentionEntry entry,
        string recordId,
        ContextTransaction transaction,
        string operation,
        CancellationToken ct
    )
    {
        var keyClrType =
            Nullable.GetUnderlyingType(entry.RecordId.RecordIdType)
            ?? entry.RecordId.RecordIdType;
        if (keyClrType == typeof(Guid) && !Guid.TryParse(recordId, out _))
        {
            throw new InvalidOperationException(
                $"{operation} record id '{recordId}' for entity '{entry.RetentionEntityId}' is not a valid Guid. The target would never match its row."
            );
        }

        if (CanonicaliseStatement(entry, recordId) is not { } statement)
        {
            return keyClrType == typeof(Guid) ? Guid.Parse(recordId).ToString("D") : recordId;
        }

        var connection = db.Database.GetDbConnection();
        await using var command = statement.Parameters.CreateCommand(connection, transaction.Commands, statement.Sql);

        // A failed cast aborts the enclosing transaction, which may be the host's own. Plain
        // SQL, not DbTransaction.SaveAsync: an enlisted connection has no transaction object to
        // call it on, and a host's connection wrapper may not implement it.
        var inTransaction = transaction is not ContextTransaction.None;
        if (inTransaction)
        {
            await ExecuteAsync(connection, transaction.Commands, $"SAVEPOINT {CastSavepoint}", ct);
        }

        try
        {
            var canonical = (string)(await command.ExecuteScalarAsync(ct))!;
            if (inTransaction)
            {
                await ExecuteAsync(connection, transaction.Commands, $"RELEASE SAVEPOINT {CastSavepoint}", ct);
            }

            return canonical;
        }
        catch (DbException exception) when (exception.SqlState?.StartsWith("22", StringComparison.Ordinal) == true)
        {
            if (inTransaction)
            {
                await ExecuteAsync(
                    connection,
                    transaction.Commands,
                    $"ROLLBACK TO SAVEPOINT {CastSavepoint}",
                    CancellationToken.None
                );
            }

            throw new InvalidOperationException(
                $"{operation} record id '{recordId}' for entity '{entry.RetentionEntityId}' is not valid for provider type '{PostgresStoreTypeSql.CastType(entry.RecordId.RecordIdStoreType)}'. The target would never match its row.",
                exception
            );
        }
    }

    private static async Task ExecuteAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string sql,
        CancellationToken ct
    )
    {
        await using var command = new SqlParams().CreateCommand(connection, transaction, sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
