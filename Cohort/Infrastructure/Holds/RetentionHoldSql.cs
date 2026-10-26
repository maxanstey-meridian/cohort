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

    internal static string BuildActiveTenantlessHoldIds(RelationalObjectName holdsTable) =>
        $"""
        SELECT hold."HoldId"
        FROM {PostgreSqlIdentifier.Format(holdsTable)} AS hold
        WHERE hold."TenantId" IS NULL
          AND {ActivePredicate("hold")}
        """;

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
    /// Every active hold across every retained entity, plus every hold in
    /// <paramref name="baselineHoldIds"/> whether or not it is still active (so a hold that ended
    /// during the work is still compared by its row), with the physical version of the row it
    /// covers: its <c>tableoid</c>, <c>ctid</c> and <c>xmin</c> as text, or NULL when the row no
    /// longer exists (or the scope cannot see it). Concatenated, not a row value: a row of NULLs
    /// casts to a non-NULL <c>(,)</c>. PostgreSQL gives every UPDATE, even one writing the same
    /// values, a new tuple stamped with the writing (sub)transaction's <c>xmin</c>; the
    /// <c>xmin</c> keeps the version distinct even if VACUUM frees the baseline's line pointer and
    /// the new tuple reuses its <c>ctid</c>. So diffing this before and after host work inside one
    /// transaction shows which held rows that work changed or removed, whatever did it (EF, a
    /// client or database cascade, a trigger, raw SQL). A savepoint the work rolls back restores
    /// the old tuple, version and all. <see cref="RetentionHoldGuard"/> owns the diff.
    /// </summary>
    internal static (string Sql, SqlParams Parameters) BuildActiveHeldRowVersions(
        RelationalObjectName holdsTable,
        IReadOnlyList<RetentionEntry> entries,
        IReadOnlyCollection<Guid> baselineHoldIds
    )
    {
        if (entries.Count == 0)
        {
            throw new ArgumentException(
                "At least one retained entity is required to read held row versions.",
                nameof(entries)
            );
        }

        var parameters = new SqlParams { ["baselineHoldIds"] = baselineHoldIds.ToArray() };
        var branches = new List<string>(entries.Count);
        for (var index = 0; index < entries.Count; index++)
        {
            var entry = entries[index];
            parameters[$"entity{index}"] = entry.RetentionEntityId;
            branches.Add(
                $"""
                SELECT hold."HoldId",
                    CAST(target.tableoid AS text) || '/' || CAST(target.ctid AS text)
                        || '/' || CAST(target.xmin AS text)
                FROM {PostgreSqlIdentifier.Format(holdsTable)} AS hold
                LEFT JOIN {PostgreSqlIdentifier.Format(entry.Table)} AS target
                    ON {RecordIdSql.EqualsExpression("target", entry.RecordId, "hold.\"RecordId\"")}
                WHERE hold."RetentionEntityId" = @entity{index}
                  AND ({ActivePredicate("hold")} OR hold."HoldId" = ANY(@baselineHoldIds))
                """
            );
        }

        return (string.Join("\nUNION ALL\n", branches), parameters);
    }
}
