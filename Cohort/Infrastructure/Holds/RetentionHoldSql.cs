using System.Data.Common;

namespace Cohort.Infrastructure.Holds;

internal static class RetentionHoldSql
{
    internal const string TableName = "retention_holds";

    internal static string BuildActiveHoldExclusion(
        string targetAlias,
        string recordIdColumn,
        string? tenantColumn = null
    )
    {
        var tenantLine = tenantColumn is not null
            ? @"
              AND hold.""TenantId"" = @tenantId"
            : @"
              AND hold.""TenantId"" IS NULL";

        // Hold activity is deliberately evaluated against the database wall clock, not the
        // sweep's logical 'now': a litigation hold protects rows from the moment it exists,
        // even when an operator runs a backdated sweep.
        return $"""
            NOT EXISTS (
                SELECT 1
                FROM {QuoteIdentifier(TableName)} AS hold
                WHERE hold."RetentionEntityId" = @retentionEntityId
                  AND hold."RecordId" = CAST({targetAlias}.{QuoteIdentifier(
                recordIdColumn
            )} AS text){tenantLine}
                  AND hold."CreatedAt" <= statement_timestamp()
                  AND (hold."ExpiresAt" IS NULL OR hold."ExpiresAt" > statement_timestamp())
                  AND (hold."RemovedAt" IS NULL OR hold."RemovedAt" > statement_timestamp())
            )
            """;
    }

    internal static DbParameter CreateParameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        return parameter;
    }

    internal static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }
}
