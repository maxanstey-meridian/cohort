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
                $"{operation} record id '{recordId}' for entity '{entry.RetentionEntityId}' is not valid for provider type '{storeType}'. The target would never match its row.",
                exception
            );
        }
    }
}
