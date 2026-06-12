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

    internal static string BuildHeldCountCommandText(RetentionEntry entry, SqlFilter filter)
    {
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return
            $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE {filter.PredicateSql}
              {tenantClause}
              AND NOT {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            """;
    }

    internal static string BuildNullAnchorCountCommandText(RetentionEntry entry, SqlFilter filter)
    {
        // No hold exclusion: a held NULL-anchor row is just as invisible to retention.
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return
            $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE {filter.PredicateSql}
              {tenantClause}
            """;
    }

    private static string BuildAnonymisedAtAssignment(RetentionEntry entry)
    {
        return entry.AnonymisedAt is { } anonymisedAt
            ? $", {QuoteIdentifier(anonymisedAt.AnonymisedAtColumn)} = @anonymisedAt"
            : "";
    }

    internal static string BuildSetBasedCommandText(RetentionEntry entry, SqlFilter filter)
    {
        var assignments = entry.AnonymiseFields
            .Select((field, index) => $"{QuoteIdentifier(field.ColumnName)} = @value{index}");
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return
            $"""
            UPDATE {QuoteIdentifier(entry.TableName)} AS target
            SET {string.Join(", ", assignments)}{BuildAnonymisedAtAssignment(entry)}
            WHERE {filter.PredicateSql}
              {tenantClause}
              AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
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
            SET {string.Join(", ", assignments)}{BuildAnonymisedAtAssignment(entry)}
            WHERE {RecordIdSql.EqualsParameter("target", entry.RecordId, "recordId")}
              AND {filter.PredicateSql}
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            RETURNING target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            """;
    }

    internal static string BuildCandidateSelectionCommandText(
        RetentionEntry entry,
        SqlFilter filter,
        int? batchSize
    )
    {
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);
        var limitClause = batchSize is not null ? "\nLIMIT @batchSize" : "";

        // Held rows are excluded up front so they are neither locked nor re-selected by
        // every batch; the engine measures them separately for the audit summary.
        return
            $"""
            SELECT target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE {filter.PredicateSql}
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) ASC
            FOR UPDATE SKIP LOCKED{limitClause}
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
            WHERE {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) ASC
            """;
    }

    internal static string BuildLoadHandlerRowsCommandText(RetentionEntry entry)
    {
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return
            $"""
            SELECT *
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) ASC
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
