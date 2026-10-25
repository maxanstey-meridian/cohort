using Cohort.Infrastructure.Sweep;

namespace Cohort.Infrastructure.Holds;

internal static class RetentionHoldSql
{
    private const string DatabaseClock = "pg_catalog.statement_timestamp()";

    /// <summary>
    /// The one definition of "this hold protects": created, not yet expired and not yet removed
    /// at <paramref name="atSql"/>, which defaults to the database clock. Every hold check
    /// (mutation, deletion, reporting) uses it, so none can disagree with another.
    /// </summary>
    internal static string ActivePredicate(string holdAlias, string atSql = DatabaseClock)
    {
        return $"""
            {holdAlias}."CreatedAt" <= {atSql}
                  AND ({holdAlias}."ExpiresAt" IS NULL OR {holdAlias}."ExpiresAt" > {atSql})
                  AND ({holdAlias}."RemovedAt" IS NULL OR {holdAlias}."RemovedAt" > {atSql})
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
