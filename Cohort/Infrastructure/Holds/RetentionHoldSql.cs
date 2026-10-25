using Cohort.Infrastructure.Sweep;

namespace Cohort.Infrastructure.Holds;

internal static class RetentionHoldSql
{
    /// <summary>
    /// The one definition of "this hold protects": created, not yet expired and not yet removed
    /// on the database clock. Every hold check (mutation, deletion, reporting) uses it, so none
    /// can disagree with another.
    /// </summary>
    internal static string ActivePredicate(string holdAlias)
    {
        const string now = DatabaseClock.Sql;
        return $"""
            {holdAlias}."CreatedAt" <= {now}
                  AND ({holdAlias}."ExpiresAt" IS NULL OR {holdAlias}."ExpiresAt" > {now})
                  AND ({holdAlias}."RemovedAt" IS NULL OR {holdAlias}."RemovedAt" > {now})
            """;
    }

    /// <summary>
    /// Selects one row when an active hold covers <c>@recordId</c> (a canonical record id) of
    /// <c>@retentionEntityId</c>, scoped to <c>@tenantId</c> when <paramref name="tenanted"/>,
    /// otherwise to tenantless holds.
    /// </summary>
    internal static string BuildActiveHoldForRecord(RelationalObjectName holdsTable, bool tenanted)
    {
        var tenantLine = tenanted
            ? @"hold.""TenantId"" = @tenantId"
            : @"hold.""TenantId"" IS NULL";
        return $"""
            SELECT 1
            FROM {PostgreSqlIdentifier.Format(holdsTable)} AS hold
            WHERE hold."RetentionEntityId" = @retentionEntityId
              AND hold."RecordId" = @recordId
              AND {tenantLine}
              AND {ActivePredicate("hold")}
            LIMIT 1
            """;
    }

    internal static string BuildActiveHoldExclusion(
        RelationalObjectName holdsTable,
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
                FROM {PostgreSqlIdentifier.Format(holdsTable)} AS hold
                WHERE hold."RetentionEntityId" = @retentionEntityId
                  AND hold."RecordId" = CAST({targetAlias}.{PostgreSqlIdentifier.Quote(
                recordIdColumn
            )} AS text){tenantLine}
                  AND {ActivePredicate("hold")}
            )
            """;
    }

    /// <summary>
    /// Active holds whose row no longer exists, across every retained entity. Diffing this before
    /// and after a host deletion callback shows which holds the callback broke, whatever removed
    /// the row (EF, a client or database cascade, raw SQL).
    /// </summary>
    internal static (string Sql, SqlParams Parameters) BuildActiveHoldsWithoutRow(
        RelationalObjectName holdsTable,
        IReadOnlyList<RetentionEntry> entries
    )
    {
        if (entries.Count == 0)
        {
            throw new ArgumentException(
                "At least one retained entity is required to find holds without a row.",
                nameof(entries)
            );
        }

        var parameters = new SqlParams();
        var branches = new List<string>(entries.Count);
        for (var index = 0; index < entries.Count; index++)
        {
            var entry = entries[index];
            parameters[$"entity{index}"] = entry.RetentionEntityId;
            branches.Add(
                $"""
                SELECT hold."HoldId"
                FROM {PostgreSqlIdentifier.Format(holdsTable)} AS hold
                WHERE hold."RetentionEntityId" = @entity{index}
                  AND {ActivePredicate("hold")}
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
                      WHERE {RecordIdSql.EqualsExpression("target", entry.RecordId, "hold.\"RecordId\"")}
                  )
                """
            );
        }

        return (string.Join("\nUNION ALL\n", branches), parameters);
    }
}
