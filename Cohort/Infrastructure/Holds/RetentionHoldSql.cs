using System.Data.Common;

namespace Cohort.Infrastructure.Holds;

internal static class RetentionHoldSql
{
    internal const string TableName = "retention_holds";

    internal static string BuildActiveHoldExclusion(string targetAlias, string recordIdColumn, string? tenantColumn = null)
    {
        var tenantLine = tenantColumn is not null
            ? @"
              AND hold.""TenantId"" = @tenantId"
            : "";

        return
            $"""
            NOT EXISTS (
                SELECT 1
                FROM {QuoteIdentifier(TableName)} AS hold
                WHERE hold."TableName" = @holdTableName
                  AND hold."RecordId" = CAST({targetAlias}.{QuoteIdentifier(recordIdColumn)} AS text){tenantLine}
                  AND hold."CreatedAt" <= @holdAsOf
                  AND (hold."ExpiresAt" IS NULL OR hold."ExpiresAt" > @holdAsOf)
                  AND (hold."RemovedAt" IS NULL OR hold."RemovedAt" > @holdAsOf)
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
