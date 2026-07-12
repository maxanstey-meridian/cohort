using Cohort.Infrastructure.Holds;

namespace Cohort.Infrastructure.Sweep;

internal static class AnonymiseSqlBuilder
{
    internal static string BuildPreviewCountCommandText(RetentionEntry entry, SqlFilter filter)
    {
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return $"""
            SELECT pg_catalog.count(*)
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {filter.PredicateSql}
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion(
                entry.CohortTables.RetentionHolds,
                "target",
                entry.RecordId.RecordIdColumn,
                entry.Tenant?.TenantColumn
            )}
            """;
    }

    internal static string BuildHeldCountCommandText(RetentionEntry entry, SqlFilter filter)
    {
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return $"""
            SELECT pg_catalog.count(*)
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {filter.PredicateSql}
              {tenantClause}
              AND NOT {RetentionHoldSql.BuildActiveHoldExclusion(
                entry.CohortTables.RetentionHolds,
                "target",
                entry.RecordId.RecordIdColumn,
                entry.Tenant?.TenantColumn
            )}
            """;
    }

    internal static string BuildNullAnchorCountCommandText(RetentionEntry entry, SqlFilter filter)
    {
        // No hold exclusion: a held NULL-anchor row is just as invisible to retention.
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return $"""
            SELECT pg_catalog.count(*)
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
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
        var assignments = entry.AnonymiseFields.Select(
            (field, index) => $"{QuoteIdentifier(field.ColumnName)} = @value{index}"
        );
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return $"""
            UPDATE {PostgreSqlIdentifier.Format(entry.Table)} AS target
            SET {string.Join(", ", assignments)}{BuildAnonymisedAtAssignment(entry)}
            WHERE {filter.PredicateSql}
              {tenantClause}
              AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {RetentionHoldSql.BuildActiveHoldExclusion(
                entry.CohortTables.RetentionHolds,
                "target",
                entry.RecordId.RecordIdColumn,
                entry.Tenant?.TenantColumn
            )}
            RETURNING {RecordIdSql.TextExpression("target", entry.RecordId)}
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

        return $"""
            UPDATE {PostgreSqlIdentifier.Format(entry.Table)} AS target
            SET {string.Join(", ", assignments)}{BuildAnonymisedAtAssignment(entry)}
            WHERE {RecordIdSql.EqualsParameter("target", entry.RecordId, "recordId")}
              AND {filter.PredicateSql}
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion(
                entry.CohortTables.RetentionHolds,
                "target",
                entry.RecordId.RecordIdColumn,
                entry.Tenant?.TenantColumn
            )}
            RETURNING {RecordIdSql.TextExpression("target", entry.RecordId)}
            """;
    }

    internal static string BuildCandidateSelectionCommandText(
        RetentionEntry entry,
        SqlFilter filter,
        int? batchSize,
        bool hasAttemptedRecordIds = false,
        bool excludeCommittedRowDetails = false
    )
    {
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);
        var limitClause = batchSize is not null ? "\nLIMIT @batchSize" : "";
        var attemptedClause = hasAttemptedRecordIds
            ? $"\n  AND NOT ({RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "attemptedRecordIds")})"
            : "";
        var committedClause = excludeCommittedRowDetails
            ? $"""

              AND NOT EXISTS (
                  SELECT 1
                  FROM {PostgreSqlIdentifier.Format(entry.CohortTables.SweepRunRowDetail)} AS prior_detail
                  WHERE prior_detail."SweepId" = @excludedSweepId
                    AND prior_detail."RetentionEntityId" = @excludedRetentionEntityId
                    AND prior_detail."RecordId" = {RecordIdSql.TextExpression("target", entry.RecordId)}
                    AND prior_detail."Category" = @excludedCategory
                    AND prior_detail."Strategy" = @excludedStrategy
                    AND prior_detail."TenantId" = @excludedTenantId
              )
              """
            : "";

        // Held rows are excluded up front so they are neither selected nor re-selected by
        // every batch; the engine measures them separately for the audit summary. Rows
        // already skipped by an earlier batch of this run are excluded too — they stay
        // eligible, so reselecting them would re-fail them forever.
        return $"""
            SELECT {RecordIdSql.TextExpression("target", entry.RecordId)}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {filter.PredicateSql}
              {tenantClause}{attemptedClause}{committedClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion(
                entry.CohortTables.RetentionHolds,
                "target",
                entry.RecordId.RecordIdColumn,
                entry.Tenant?.TenantColumn
            )}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(
                entry.RecordId.RecordIdColumn
            )} AS text) ASC
            {limitClause}
            """;
    }

    internal static string BuildCandidateLockCommandText(
        RetentionEntry entry,
        SqlFilter filter,
        bool skipLocked
    )
    {
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return $"""
            SELECT {RecordIdSql.TextExpression("target", entry.RecordId)}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {filter.PredicateSql}
              {tenantClause}
              AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {RetentionHoldSql.BuildActiveHoldExclusion(
                entry.CohortTables.RetentionHolds,
                "target",
                entry.RecordId.RecordIdColumn,
                entry.Tenant?.TenantColumn
            )}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(
                entry.RecordId.RecordIdColumn
            )} AS text) ASC
            FOR UPDATE{(skipLocked ? " SKIP LOCKED" : "")}
            """;
    }

    internal static string BuildLoadUpdatableRowsCommandText(
        RetentionEntry entry,
        IReadOnlyList<AnonymiseFactoryField> originalValueFields,
        SqlFilter filter
    )
    {
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);
        var selectedColumns = originalValueFields
            .Select(field => $"target.{QuoteIdentifier(field.ColumnName)}")
            .ToArray();
        var selectList =
            selectedColumns.Length == 0
                ? $"CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text)"
                : $"CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text), {string.Join(", ", selectedColumns)}";

        return $"""
            SELECT {selectList}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {filter.PredicateSql}
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion(
                entry.CohortTables.RetentionHolds,
                "target",
                entry.RecordId.RecordIdColumn,
                entry.Tenant?.TenantColumn
            )}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(
                entry.RecordId.RecordIdColumn
            )} AS text) ASC
            """;
    }

    internal static string BuildLoadHandlerRowsCommandText(RetentionEntry entry, SqlFilter filter)
    {
        var tenantClause = BuildTenantClause(entry.Tenant?.TenantColumn);

        return $"""
            SELECT *
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {filter.PredicateSql}
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion(
                entry.CohortTables.RetentionHolds,
                "target",
                entry.RecordId.RecordIdColumn,
                entry.Tenant?.TenantColumn
            )}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(
                entry.RecordId.RecordIdColumn
            )} AS text) ASC
            """;
    }

    internal static string QuoteIdentifier(string identifier)
    {
        return PostgreSqlIdentifier.Quote(identifier);
    }

    private static string BuildTenantClause(string? tenantColumn)
    {
        return tenantColumn is not null
            ? $"AND target.{QuoteIdentifier(tenantColumn)} = @tenantId"
            : "";
    }
}
