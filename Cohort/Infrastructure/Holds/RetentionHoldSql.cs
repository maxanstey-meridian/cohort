using System.Data.Common;
using System.Reflection;

using Cohort.Domain;

namespace Cohort.Infrastructure.Holds;

internal static class RetentionHoldSql
{
    internal const string TableName = "retention_holds";

    internal static string GetRecordIdColumn(RetentionEntry entry)
    {
        var idMember = entry.EntityType.GetProperty("Id", BindingFlags.Public | BindingFlags.Instance);
        if (idMember is null || (idMember.PropertyType != typeof(Guid) && idMember.PropertyType != typeof(Guid?)))
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose a public Guid Id property to support legal hold filtering."
            );
        }

        return "Id";
    }

    internal static string BuildActiveHoldExclusion(string targetAlias, string recordIdColumn)
    {
        return
            $"""
            NOT EXISTS (
                SELECT 1
                FROM {QuoteIdentifier(TableName)} AS hold
                WHERE hold."TableName" = @holdTableName
                  AND hold."RecordId" = {targetAlias}.{QuoteIdentifier(recordIdColumn)}
                  AND hold."TenantId" = @tenantId
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
