using Cohort.Domain;
using Cohort.Infrastructure.Holds;

namespace Cohort.Infrastructure.Sweep;

internal static class AnonymiseSqlBuilder
{
    internal static string BuildPreviewCountCommandText(RetentionEntry entry, SqlFilter filter)
    {
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return
            $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE {filter.PredicateSql}
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            """;
    }

    internal static string BuildSetBasedCommandText(RetentionEntry entry, SqlFilter filter)
    {
        var assignments = entry.AnonymiseFields
            .Select((field, index) => $"{QuoteIdentifier(field.ColumnName)} = @value{index}");
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return
            $"""
            UPDATE {QuoteIdentifier(entry.TableName)} AS target
            SET {string.Join(", ", assignments)}
            WHERE {filter.PredicateSql}
              {tenantClause}
              AND CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) = ANY(@candidateIds)
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            RETURNING target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            """;
    }

    internal static string BuildPerRowCommandText(RetentionEntry entry, SqlFilter filter)
    {
        var assignments = new List<string>(entry.AnonymiseFields.Count);
        for (var index = 0; index < entry.AnonymiseFields.Count; index++)
        {
            var field = entry.AnonymiseFields[index];
            assignments.Add($"{QuoteIdentifier(field.ColumnName)} = @value{index}");
        }

        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return
            $"""
            UPDATE {QuoteIdentifier(entry.TableName)} AS target
            SET {string.Join(", ", assignments)}
            WHERE CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) = @recordId
              AND {filter.PredicateSql}
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            RETURNING target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            """;
    }

    internal static string BuildCandidateSelectionCommandText(RetentionEntry entry, SqlFilter filter)
    {
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return
            $"""
            SELECT target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE {filter.PredicateSql}
              {tenantClause}
            FOR UPDATE
            """;
    }

    internal static string BuildLoadUpdatableRowsCommandText(
        RetentionEntry entry,
        IReadOnlyList<AnonymiseFactoryField> originalValueFields
    )
    {
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);
        var selectedColumns = originalValueFields
            .Select(field => $"target.{QuoteIdentifier(field.ColumnName)}")
            .ToArray();
        var selectList = selectedColumns.Length == 0
            ? $"CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text)"
            : $"CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text), {string.Join(", ", selectedColumns)}";

        return
            $"""
            SELECT {selectList}
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) = ANY(@candidateIds)
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            ORDER BY CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text)
            """;
    }

    internal static string BuildLoadHandlerRowsCommandText(RetentionEntry entry)
    {
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return
            $"""
            SELECT *
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) = ANY(@candidateIds)
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            ORDER BY CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text)
            """;
    }

    internal static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    private static string BuildTenantClause(string? tenantColumn)
    {
        return tenantColumn is not null
            ? $"AND target.{QuoteIdentifier(tenantColumn)} = @tenantId"
            : "";
    }
}
