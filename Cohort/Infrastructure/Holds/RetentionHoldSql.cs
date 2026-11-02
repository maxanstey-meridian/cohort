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

    internal static string BuildActiveTenantlessHoldIds(RelationalObjectName holdsTable) =>
        $"""
        SELECT hold."HoldId"
        FROM {PostgreSqlIdentifier.Format(holdsTable)} AS hold
        WHERE hold."TenantId" IS NULL
          AND {ActivePredicate("hold")}
        """;

    /// <summary>
    /// One parenthesised boolean that is true only when no active hold covers the row of
    /// <paramref name="entry"/> aliased <paramref name="targetAlias"/>: a hold names the row
    /// itself, or names a row it is linked to through one of the entry's holder paths
    /// (<see cref="RetentionEntry.HolderPaths"/>). Hold activity is deliberately evaluated against
    /// the database wall clock, not the sweep's logical 'now': a litigation hold protects rows
    /// from the moment it exists, even when an operator runs a backdated sweep. The caller binds
    /// <c>@tenantId</c> for a tenanted entry; the returned parameters are <c>retentionEntityId</c>
    /// and one <c>holderEntity{n}</c> per entity a path ends at.
    /// </summary>
    internal static (string Sql, SqlParams Parameters) BuildCoveredExclusion(
        RelationalObjectName holdsTable,
        RetentionEntry entry,
        string targetAlias
    )
    {
        var parameters = new SqlParams { ["retentionEntityId"] = entry.RetentionEntityId };
        var holders = new NamedGuids(parameters, "holderEntity");
        var holds = PostgreSqlIdentifier.Format(holdsTable);
        var terms = new List<string>
        {
            $"""
            EXISTS (
                SELECT 1
                FROM {holds} AS hold
                WHERE hold."RetentionEntityId" = @retentionEntityId
                  AND hold."RecordId" = CAST({targetAlias}.{PostgreSqlIdentifier.Quote(entry.RecordId.RecordIdColumn)} AS text)
                  AND {TenantLine(entry.Tenant is not null)}
                  AND {ActivePredicate("hold")}
            )
            """,
        };
        foreach (var path in entry.HolderPaths)
        {
            var aliases = Aliases(path, targetAlias, "p");
            terms.Add(
                $"""
                EXISTS (
                    SELECT 1
                    FROM {PostgreSqlIdentifier.Format(path.Hops[0].Far.Table)} AS {aliases[1]}{JoinChain(path, aliases, 1)}
                    {HolderJoin(holds, path.End, aliases[^1], holders.NameFor(path.End.RetentionEntityId))}
                    WHERE {HopCondition(path.Hops[0], aliases[0], aliases[1])}
                )
                """
            );
        }

        return ($"NOT ({string.Join("\n OR ", terms)})", parameters);
    }

    /// <summary>
    /// Every active hold that covers one row of <paramref name="entry"/>, directly or through a
    /// holder path, once each: ordered by entity, record id, then hold id, like
    /// <c>ListActiveAsync</c>. A direct hold counts even when its row is gone (the record id is
    /// canonical text, matched by parameter); a path starts from the row itself, which must belong
    /// to <paramref name="tenantId"/> when the entity is tenanted.
    /// </summary>
    internal static (string Sql, SqlParams Parameters) BuildCoveringHolds(
        RelationalObjectName holdsTable,
        RetentionEntry entry,
        string canonicalRecordId,
        Guid? tenantId
    )
    {
        var (union, parameters) = CoveringHoldsForRecord(holdsTable, entry, canonicalRecordId, tenantId, "UNION");
        return (
            $"""
            SELECT *
            FROM (
            {union}
            ) AS covering
            ORDER BY "RetentionEntityId", "RecordId", "HoldId"
            """,
            parameters
        );
    }

    /// <summary>
    /// Selects one row when an active hold covers the row, the same coverage as
    /// <see cref="BuildCoveringHolds"/>. Existence only, so the branches are not de-duplicated.
    /// </summary>
    internal static (string Sql, SqlParams Parameters) BuildAnyCoveringHold(
        RelationalObjectName holdsTable,
        RetentionEntry entry,
        string canonicalRecordId,
        Guid? tenantId
    )
    {
        var (union, parameters) = CoveringHoldsForRecord(holdsTable, entry, canonicalRecordId, tenantId, "UNION ALL");
        return (
            $"""
            SELECT 1
            FROM (
            {union}
            ) AS covering
            LIMIT 1
            """,
            parameters
        );
    }

    private static (string Union, SqlParams Parameters) CoveringHoldsForRecord(
        RelationalObjectName holdsTable,
        RetentionEntry entry,
        string canonicalRecordId,
        Guid? tenantId,
        string setOperator
    )
    {
        var parameters = new SqlParams
        {
            ["retentionEntityId"] = entry.RetentionEntityId,
            ["recordId"] = canonicalRecordId,
        };
        if (entry.Tenant is not null)
        {
            parameters["tenantId"] = tenantId;
        }

        var holders = new NamedGuids(parameters, "holderEntity");
        var holds = PostgreSqlIdentifier.Format(holdsTable);
        var startRow = $"{RecordIdSql.EqualsParameter("target", entry.RecordId, "recordId")}{TenantOfStartRow(entry)}";
        var branches = new List<string>
        {
            $"""
            SELECT {HoldColumns}
            FROM {holds} AS hold
            WHERE hold."RetentionEntityId" = @retentionEntityId
              AND hold."RecordId" = @recordId
              AND {TenantLine(entry.Tenant is not null)}
              AND {ActivePredicate("hold")}
            """,
        };
        foreach (var path in entry.HolderPaths)
        {
            branches.Add(HolderPathBranch(holds, entry, path, holders, startRow));
        }

        return (string.Join($"\n{setOperator}\n", branches), parameters);
    }

    /// <summary>
    /// Every active hold that covers any row whose <c>[ErasureSubject]</c> columns match the
    /// subject, once each, in <paramref name="starts"/>'s entities (the tenant split's tenanted
    /// or tenantless entities), directly or through a holder path. Age, legal minimum and
    /// strategy eligibility play no part. Ordered like <see cref="BuildCoveringHolds"/>.
    /// </summary>
    internal static (string Sql, SqlParams Parameters) BuildCoveringHoldsForSubject(
        RelationalObjectName holdsTable,
        IReadOnlyList<(RetentionEntry Entry, ErasureSubjectPredicate Subject)> starts,
        Guid? tenantId
    )
    {
        if (starts.Count == 0)
        {
            throw new ArgumentException("At least one entity with the subject's kind is required.", nameof(starts));
        }

        var parameters = new SqlParams();
        if (tenantId is not null)
        {
            parameters["tenantId"] = tenantId;
        }

        var holders = new NamedGuids(parameters, "holderEntity");
        var holds = PostgreSqlIdentifier.Format(holdsTable);
        var branches = new List<string>();
        for (var index = 0; index < starts.Count; index++)
        {
            var (entry, subject) = starts[index];
            var matches = new List<string>();
            for (var match = 0; match < subject.Matches.Count; match++)
            {
                var name = $"subject{index}Value{match}";
                parameters[name] = subject.Matches[match].SubjectValue;
                matches.Add(subject.Matches[match].EqualsParameterSql("target", name));
            }

            var startRow = $"({string.Join(" OR ", matches)}){TenantOfStartRow(entry)}";
            branches.Add(
                $"""
                SELECT {HoldColumns}
                FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
                INNER JOIN {holds} AS hold
                    ON hold."RetentionEntityId" = @{holders.NameFor(entry.RetentionEntityId)}
                   AND hold."RecordId" = CAST(target.{PostgreSqlIdentifier.Quote(entry.RecordId.RecordIdColumn)} AS text)
                   AND {TenantLine(entry.Tenant is not null)}
                   AND {ActivePredicate("hold")}
                WHERE {startRow}
                """
            );
            foreach (var path in entry.HolderPaths)
            {
                branches.Add(HolderPathBranch(holds, entry, path, holders, startRow));
            }
        }

        return (
            $"""
            SELECT *
            FROM (
            {string.Join("\nUNION\n", branches)}
            ) AS covering
            ORDER BY "RetentionEntityId", "RecordId", "HoldId"
            """,
            parameters
        );
    }

    /// <summary>
    /// <c>SELECT</c>s the holds a holder path reaches from the rows <paramref name="startRow"/>
    /// picks out of <paramref name="entry"/>'s table (aliased <c>target</c>).
    /// </summary>
    private static string HolderPathBranch(
        string holds,
        RetentionEntry entry,
        HoldPath path,
        NamedGuids holders,
        string startRow
    )
    {
        var aliases = Aliases(path, "target", "p");
        return $"""
            SELECT {HoldColumns}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target{JoinChain(path, aliases, 0)}
            {HolderJoin(holds, path.End, aliases[^1], holders.NameFor(path.End.RetentionEntityId))}
            WHERE {startRow}
            """;
    }

    private const string HoldColumns =
        "hold.\"HoldId\", hold.\"RetentionEntityId\", hold.\"RecordId\", hold.\"TenantId\", hold.\"Reason\", hold.\"CreatedAt\", hold.\"ExpiresAt\", hold.\"RemovedAt\"";

    /// <summary>The hold's tenant against the tenant a statement's scope pins, or NULL for a tenantless row.</summary>
    private static string TenantLine(bool tenanted) =>
        tenanted ? "hold.\"TenantId\" = @tenantId" : "hold.\"TenantId\" IS NULL";

    private static string TenantOfStartRow(RetentionEntry entry) =>
        entry.Tenant is { } tenant ? $" AND target.{PostgreSqlIdentifier.Quote(tenant.TenantColumn)} = @tenantId" : "";

    /// <summary>
    /// Joins the active hold that names the row of <paramref name="holder"/> aliased
    /// <paramref name="holderAlias"/>. Its tenant is the scope's tenant; every hop already joined
    /// by tenant, so the holder row has the same one.
    /// </summary>
    private static string HolderJoin(string holds, HoldRelationEnd holder, string holderAlias, string holderParameter) =>
        $"""
        INNER JOIN {holds} AS hold
                ON hold."RetentionEntityId" = @{holderParameter}
               AND hold."RecordId" = CAST({holderAlias}.{PostgreSqlIdentifier.Quote(holder.RecordId.RecordIdColumn)} AS text)
               AND {TenantLine(holder.Tenant is not null)}
               AND {ActivePredicate("hold")}
        """;

    private static string[] Aliases(HoldPath path, string startAlias, string prefix)
    {
        var aliases = new string[path.Hops.Count + 1];
        aliases[0] = startAlias;
        for (var index = 1; index < aliases.Length; index++)
        {
            aliases[index] = $"{prefix}{index}";
        }

        return aliases;
    }

    /// <summary>
    /// <c>INNER JOIN</c>s for the hops from <paramref name="fromHop"/> on; <paramref name="aliases"/>
    /// names the rows along the path, the start row first.
    /// </summary>
    private static string JoinChain(HoldPath path, string[] aliases, int fromHop)
    {
        var joins = new System.Text.StringBuilder();
        for (var index = fromHop; index < path.Hops.Count; index++)
        {
            var hop = path.Hops[index];
            joins.Append(
                $"\nINNER JOIN {PostgreSqlIdentifier.Format(hop.Far.Table)} AS {aliases[index + 1]} ON {HopCondition(hop, aliases[index], aliases[index + 1])}"
            );
        }

        return joins.ToString();
    }

    /// <summary>
    /// A hop's join: the link column equals the record id of the end it targets, and a tenanted
    /// pair of ends must share the tenant (the link may have no foreign key, and a composite one
    /// doesn't guarantee it).
    /// </summary>
    private static string HopCondition(HoldHop hop, string nearAlias, string farAlias)
    {
        var link = hop.LinkSide == HoldLinkSide.Near
            ? $"{nearAlias}.{PostgreSqlIdentifier.Quote(hop.LinkColumn)} = {farAlias}.{PostgreSqlIdentifier.Quote(hop.Far.RecordId.RecordIdColumn)}"
            : $"{farAlias}.{PostgreSqlIdentifier.Quote(hop.LinkColumn)} = {nearAlias}.{PostgreSqlIdentifier.Quote(hop.Near.RecordId.RecordIdColumn)}";
        return hop.Near.Tenant is { } nearTenant && hop.Far.Tenant is { } farTenant
            ? $"{link} AND {farAlias}.{PostgreSqlIdentifier.Quote(farTenant.TenantColumn)} = {nearAlias}.{PostgreSqlIdentifier.Quote(nearTenant.TenantColumn)}"
            : link;
    }

    /// <summary>
    /// Names each distinct entity id once as a parameter, <c>{prefix}{n}</c>, so ids are never
    /// inlined into the SQL.
    /// </summary>
    private sealed class NamedGuids(SqlParams parameters, string prefix)
    {
        private readonly Dictionary<Guid, string> names = [];

        internal string NameFor(Guid id)
        {
            if (!names.TryGetValue(id, out var name))
            {
                name = $"{prefix}{names.Count}";
                names[id] = name;
                parameters[name] = id;
            }

            return name;
        }
    }

    /// <summary>
    /// Takes every row a hold on <paramref name="entry"/>'s row (<c>@recordId</c>, and
    /// <c>@tenantId</c> when tenanted) will cover, one statement per covered <em>entity</em>,
    /// each locking that entity's covered rows <c>FOR SHARE</c> in one sort, its sweep order (the
    /// order a sweep or erasure locks a batch in), so the two can't deadlock within one entity,
    /// even when two covered paths end at it. PostgreSQL refuses <c>FOR SHARE</c> at a query level
    /// with <c>UNION</c>, <c>DISTINCT</c>, or aggregates, and on the nullable side of an outer
    /// join, but allows a <c>UNION</c> in a <c>WHERE</c> subquery, so a statement is
    /// <c>target.&lt;record id&gt; IN (&lt;path select&gt; UNION …)</c>, each path select being the
    /// index-driven inner joins from the holder row to the covered row. An <c>OR</c> of
    /// <c>EXISTS</c> terms would plan as a full scan of the covered table. Unlike the join shape,
    /// a row whose link moves away while the walk waits on it stays share-locked until commit,
    /// which is harmless: the hold doesn't cover it. Statements run ordered by the longest covered
    /// path ending at the entity, then by entity name: every entity on a path to X comes before X
    /// (the graph is acyclic), so a row is locked before a later statement reads through it.
    /// <paramref name="entries"/> supplies each covered entity's anchor for the sort order.
    /// </summary>
    internal static IReadOnlyList<string> BuildCoveredRowLocks(RetentionEntry entry, IEnumerable<RetentionEntry> entries)
    {
        var byEntity = entries.ToDictionary(candidate => candidate.RetentionEntityId);
        var startRow = $"{RecordIdSql.EqualsParameter("h0", entry.RecordId, "recordId")}"
            + (entry.Tenant is { } tenant ? $" AND h0.{PostgreSqlIdentifier.Quote(tenant.TenantColumn)} = @tenantId" : "");
        return
        [
            .. entry.CoveredPaths
                .GroupBy(path => path.End.RetentionEntityId)
                .OrderBy(group => group.Max(path => path.Hops.Count))
                .ThenBy(group => group.First().End.EntityType.FullName, StringComparer.Ordinal)
                .Select(group =>
                {
                    var end = group.First().End;
                    var selects = group.Select(path =>
                    {
                        var aliases = Aliases(path, "h0", "c");
                        return $"""
                            SELECT {aliases[^1]}.{PostgreSqlIdentifier.Quote(end.RecordId.RecordIdColumn)}
                                FROM {PostgreSqlIdentifier.Format(entry.Table)} AS h0{JoinChain(path, aliases, 0)}
                                WHERE {startRow}
                            """;
                    });
                    return $"""
                        SELECT 1
                        FROM {PostgreSqlIdentifier.Format(end.Table)} AS target
                        WHERE target.{PostgreSqlIdentifier.Quote(end.RecordId.RecordIdColumn)} IN (
                            {string.Join("\n    UNION\n    ", selects)}
                        )
                        ORDER BY {SweepStrategy.OrderBySql(byEntity[end.RetentionEntityId])}
                        FOR SHARE OF target
                        """;
                }),
        ];
    }

    /// <summary>
    /// Every active hold across every retained entity, plus every hold in
    /// <paramref name="baselineHoldIds"/> whether or not it is still active (so a hold that ended
    /// during the work is still compared by its row), with the physical version of each row it
    /// covers: its <c>tableoid</c>, <c>ctid</c> and <c>xmin</c> as text, or NULL when the row no
    /// longer exists (or the scope cannot see it). One entry per (hold, covered entity, canonical
    /// record id): the row the hold names, which still yields NULL when it is gone, and every row a
    /// covered path reaches from it, <c>DISTINCT</c> because two paths, or a diamond of relations,
    /// can reach one row twice. A hold is joined to the row it names by the row's tenant, since the
    /// guard has no scope tenant. Concatenated, not a row value: a row of NULLs casts to a non-NULL
    /// <c>(,)</c>. PostgreSQL gives every UPDATE, even one writing the same values, a new tuple
    /// stamped with the writing (sub)transaction's <c>xmin</c>; the <c>xmin</c> keeps the version
    /// distinct even if VACUUM frees the baseline's line pointer and the new tuple reuses its
    /// <c>ctid</c>. So diffing this before and after host work inside one transaction shows which
    /// held rows that work changed or removed, whatever did it (EF, a client or database cascade, a
    /// trigger, raw SQL). A savepoint the work rolls back restores the old tuple, version and all.
    /// Columns: hold id, the hold's entity, the covered row's entity, its record id, its version.
    /// <see cref="RetentionHoldGuard"/> owns the diff.
    /// </summary>
    internal static (string Sql, SqlParams Parameters) BuildHeldRowVersions(
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
        var entities = new NamedGuids(parameters, "entity");
        var holds = PostgreSqlIdentifier.Format(holdsTable);
        var branches = new List<string>();
        foreach (var entry in entries)
        {
            var holder = entities.NameFor(entry.RetentionEntityId);
            var holdFilter = $"""
                hold."RetentionEntityId" = @{holder}
                  AND ({ActivePredicate("hold")} OR hold."HoldId" = ANY(@baselineHoldIds))
                """;
            branches.Add(
                $"""
                SELECT hold."HoldId" AS "HoldId", hold."RetentionEntityId" AS "HoldEntityId",
                    hold."RetentionEntityId" AS "CoveredEntityId", hold."RecordId" AS "RecordId",
                    {VersionOf("target")} AS "Version"
                FROM {holds} AS hold
                LEFT JOIN {PostgreSqlIdentifier.Format(entry.Table)} AS target
                    ON {RecordIdSql.EqualsExpression("target", entry.RecordId, "hold.\"RecordId\"")}
                   AND {HoldOfRow(entry.Tenant, "target")}
                WHERE {holdFilter}
                """
            );
            foreach (var path in entry.CoveredPaths)
            {
                var aliases = Aliases(path, "h0", "c");
                var covered = aliases[^1];
                branches.Add(
                    $"""
                    SELECT hold."HoldId" AS "HoldId", hold."RetentionEntityId" AS "HoldEntityId",
                        CAST(@{entities.NameFor(path.End.RetentionEntityId)} AS uuid) AS "CoveredEntityId",
                        CAST({covered}.{PostgreSqlIdentifier.Quote(path.End.RecordId.RecordIdColumn)} AS text) AS "RecordId",
                        {VersionOf(covered)} AS "Version"
                    FROM {holds} AS hold
                    INNER JOIN {PostgreSqlIdentifier.Format(entry.Table)} AS h0
                        ON {RecordIdSql.EqualsExpression("h0", entry.RecordId, "hold.\"RecordId\"")}
                       AND {HoldOfRow(entry.Tenant, "h0")}{JoinChain(path, aliases, 0)}
                    WHERE {holdFilter}
                    """
                );
            }
        }

        return ($"SELECT DISTINCT *\nFROM (\n{string.Join("\nUNION ALL\n", branches)}\n) AS versions", parameters);
    }

    /// <summary>
    /// Re-reads the version of each key in <paramref name="keys"/>, by record id, for the holds
    /// that still exist: a covered row that left the covered-path read (its link was cut, or the
    /// row was deleted) is still compared by its row. A row that is gone yields a NULL version.
    /// Same columns and version expression as <see cref="BuildHeldRowVersions"/>.
    /// </summary>
    internal static (string Sql, SqlParams Parameters) BuildHeldRowRereads(
        RelationalObjectName holdsTable,
        IReadOnlyList<RetentionEntry> entries,
        IReadOnlyCollection<HeldRowKey> keys
    )
    {
        var parameters = new SqlParams();
        var holds = PostgreSqlIdentifier.Format(holdsTable);
        var branches = new List<string>();
        var index = 0;
        foreach (var group in keys.GroupBy(key => key.RetentionEntityId))
        {
            var entry = entries.Single(candidate => candidate.RetentionEntityId == group.Key);
            parameters[$"rereadEntity{index}"] = group.Key;
            parameters[$"rereadHoldIds{index}"] = group.Select(key => key.HoldId).ToArray();
            parameters[$"rereadRecordIds{index}"] = group.Select(key => key.RecordId).ToArray();
            branches.Add(
                $"""
                SELECT hold."HoldId" AS "HoldId", hold."RetentionEntityId" AS "HoldEntityId",
                    CAST(@rereadEntity{index} AS uuid) AS "CoveredEntityId", k."RecordId" AS "RecordId",
                    {VersionOf("target")} AS "Version"
                FROM ROWS FROM (pg_catalog.unnest(@rereadHoldIds{index}), pg_catalog.unnest(@rereadRecordIds{index})) AS k("HoldId", "RecordId")
                INNER JOIN {holds} AS hold ON hold."HoldId" = k."HoldId"
                LEFT JOIN {PostgreSqlIdentifier.Format(entry.Table)} AS target
                    ON {RecordIdSql.EqualsExpression("target", entry.RecordId, "k.\"RecordId\"")}
                   AND {HoldOfRow(entry.Tenant, "target")}
                """
            );
            index++;
        }

        return (string.Join("\nUNION ALL\n", branches), parameters);
    }

    /// <summary>The hold's tenant against the tenant of the row it covers, or NULL for a tenantless row.</summary>
    private static string HoldOfRow(TenantConvention? tenant, string rowAlias) =>
        tenant is null
            ? "hold.\"TenantId\" IS NULL"
            : $"hold.\"TenantId\" = {rowAlias}.{PostgreSqlIdentifier.Quote(tenant.TenantColumn)}";

    private static string VersionOf(string alias) =>
        $"CAST({alias}.tableoid AS text) || '/' || CAST({alias}.ctid AS text) || '/' || CAST({alias}.xmin AS text)";

    /// <summary>
    /// Merges <paramref name="source"/> into <paramref name="target"/>, for a statement built from
    /// several of this class's pieces.
    /// </summary>
    internal static void Merge(SqlParams target, SqlParams source)
    {
        foreach (var (name, value) in source)
        {
            target[name] = value;
        }
    }
}
