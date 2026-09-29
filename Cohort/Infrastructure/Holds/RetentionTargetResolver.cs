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

    internal async Task<string> CanonicaliseRecordIdAsync(
        RetentionEntry entry,
        string recordId,
        DbTransaction? transaction,
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

        if (PostgresStoreTypeSql.Validate(entry.RecordId.RecordIdStoreType) is not { } storeType)
        {
            return keyClrType == typeof(Guid) ? Guid.Parse(recordId).ToString("D") : recordId;
        }

        // Prefer the stored row's own text: equality under the column type (citext, unscaled
        // numeric) can admit spellings whose cast-to-text differs from what sweeps compare.
        await using var command = new SqlParams { ["recordId"] = recordId }.CreateCommand(
            db.Database.GetDbConnection(),
            transaction,
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
            """
        );

        // A failed cast aborts the enclosing transaction, which may be the host's own.
        if (transaction is not null)
        {
            await transaction.SaveAsync(CastSavepoint, ct);
        }

        try
        {
            var canonical = (string)(await command.ExecuteScalarAsync(ct))!;
            if (transaction is not null)
            {
                await transaction.ReleaseAsync(CastSavepoint, ct);
            }

            return canonical;
        }
        catch (DbException exception) when (exception.SqlState?.StartsWith("22", StringComparison.Ordinal) == true)
        {
            if (transaction is not null)
            {
                await transaction.RollbackAsync(CastSavepoint, CancellationToken.None);
            }

            throw new InvalidOperationException(
                $"{operation} record id '{recordId}' for entity '{entry.RetentionEntityId}' is not valid for provider type '{storeType}'. The target would never match its row.",
                exception
            );
        }
    }
}
