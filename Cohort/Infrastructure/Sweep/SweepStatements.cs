using Cohort.Domain;
using Cohort.Infrastructure.Holds;

namespace Cohort.Infrastructure.Sweep;

// Raw SQL here interpolates only identifiers quoted from the EF model and validated store
// types; every value is a named parameter.

/// <summary>
/// One statement Cohort sends, against a host table, Cohort's own tables, or a PostgreSQL
/// function: its text, the values it binds, and every table it reads, the host's and Cohort's
/// own. <see cref="Entity"/> is the one entity whose scope or hold call issues it, or null for
/// a statement that spans entities or names none.
/// </summary>
internal sealed record SweepStatement(
    string Name,
    Type? Entity,
    string Sql,
    SqlParams Parameters,
    IReadOnlyList<RelationalObjectName> Relations
);

/// <summary>Which side of the cutoff a scope's anchor must fall; see <c>SweepStatements.ScopeSql</c>.</summary>
internal enum AnchorFilter
{
    BeforeCutoff,
    Null,
    OnOrAfterCutoff,
}

/// <summary>Where discovery continues from, which shapes the text of its statement.</summary>
internal enum SweepCursorKind
{
    /// <summary>From the start.</summary>
    None,

    /// <summary>After a row with an anchor.</summary>
    Anchored,

    /// <summary>After a row whose anchor is NULL.</summary>
    NullAnchor,
}

/// <summary>
/// Which mutation path a batch takes. <paramref name="RowByRow"/> comes from
/// <see cref="SweepStatements.TakesRowByRowPath"/>; <paramref name="HasDependentsStep"/> is whether
/// the entity has a dependents step, which adds <c>IsStillDue</c> to the row-by-row statements.
/// </summary>
internal readonly record struct SweepPath(bool RowByRow, bool HasDependentsStep);

/// <summary>
/// The values of a scope's statements that only a run has: a row's record key, a record-id
/// text, and the refused ids. A check passes placeholders of the right types.
/// </summary>
internal sealed record SweepRunValues(
    object? RecordKey,
    object? RecordIdText,
    object? RefusedRecordIds
);

/// <summary>
/// Every host-table statement of the sweep pipeline, built once for the run and for readiness
/// (<see cref="SweepStrategy"/> executes what a builder returns). A builder takes what shapes the
/// text as separate arguments, takes the values it binds as <c>object?</c>, and never runs host
/// code, so the same text comes out whoever calls it.
/// </summary>
internal static class SweepStatements
{
    // ------------------------------------------------------------------------------------
    // Path predicates: the run's own conditions for which statements it issues.
    // ------------------------------------------------------------------------------------

    /// <summary>Whether a run counts the rows whose anchor is NULL: only with a cutoff.</summary>
    internal static bool CountsNullAnchor(SweepScope scope) => scope.Cutoff is not null;

    /// <summary>
    /// Whether rows whose anchor is NULL are in the scope, so discovery continues past them: only
    /// without a cutoff (an erasure without a legal minimum).
    /// </summary>
    internal static bool HasNullAnchorsInScope(SweepScope scope) => scope.Cutoff is null;

    /// <summary>Whether a run counts the rows an erasure's legal minimum keeps.</summary>
    internal static bool CountsRetainedByRule(SweepScope scope) =>
        scope.Cutoff is not null && scope.Subject is not null;

    /// <summary>Whether a run counts the subject's rows an Exempt rule keeps from an erasure.</summary>
    internal static bool CountsSubjectRows(SweepScope scope) =>
        scope.Rule.Strategy == Strategy.Exempt && scope.Subject is not null;

    internal static SweepCursorKind CursorKindOf(SweepCursor? after) =>
        after is null ? SweepCursorKind.None
        : after.Anchor is null ? SweepCursorKind.NullAnchor
        : SweepCursorKind.Anchored;

    /// <summary>
    /// Whether a batch loads and changes its rows one at a time: with a dependents step, or when
    /// the strategy's new values depend on each row. Per-row execution counts only for
    /// Anonymise, the only strategy whose mutation sets it.
    /// </summary>
    internal static bool TakesRowByRowPath(
        Strategy strategy,
        bool hasDependentsStep,
        bool requiresPerRowExecution
    ) =>
        hasDependentsStep
        || (strategy == Strategy.Anonymise && requiresPerRowExecution);

    // ------------------------------------------------------------------------------------
    // Enumeration: every statement a scope can issue.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// Every S1 to S11 statement <paramref name="scope"/> can issue under its rule's strategy,
    /// over each branch's values. <paramref name="headParameters"/> holds the mutation head's
    /// values (<c>@deletedAt</c>, <c>@anonymisedAt</c>, <c>@value{n}</c>), filled by the caller.
    /// </summary>
    internal static IReadOnlyList<SweepStatement> ForScope(
        SweepScope scope,
        SweepPath path,
        SqlParams headParameters,
        SweepRunValues values
    )
    {
        var statements = new List<SweepStatement>();
        if (scope.Rule.Strategy == Strategy.Exempt)
        {
            if (CountsSubjectRows(scope))
            {
                statements.Add(CountSubjectRows(scope));
            }

            return statements;
        }

        var sweepId = Guid.Empty;
        const int batchSize = 1;
        string[] candidateIds = [];
        if (CountsNullAnchor(scope))
        {
            statements.Add(CountByAnchor(scope, SweepCount.NullAnchor));
        }

        if (CountsRetainedByRule(scope))
        {
            statements.Add(CountByAnchor(scope, SweepCount.RetainedByRule));
        }

        statements.Add(CountPredicted(scope));
        statements.Add(CountLeft(scope, sweepId, refusedRecordIds: null));
        statements.Add(CountLeft(scope, sweepId, values.RefusedRecordIds));
        statements.Add(Discover(scope, sweepId, batchSize, SweepCursorKind.None, null, null));
        statements.Add(
            Discover(
                scope,
                sweepId,
                batchSize,
                SweepCursorKind.Anchored,
                scope.Now,
                values.RecordIdText
            )
        );
        if (HasNullAnchorsInScope(scope))
        {
            statements.Add(
                Discover(
                    scope,
                    sweepId,
                    batchSize,
                    SweepCursorKind.NullAnchor,
                    null,
                    values.RecordIdText
                )
            );
        }

        statements.Add(LockCandidates(scope, candidateIds));
        if (!path.RowByRow)
        {
            statements.Add(MutateBatch(scope, headParameters, candidateIds));
            return statements;
        }

        statements.Add(LoadRows(scope, candidateIds));
        statements.Add(MutateRow(scope, headParameters, values.RecordKey));
        statements.Add(RecordIdSql.CanonicalizeStatement(scope.Entry, values.RecordKey));
        if (path.HasDependentsStep)
        {
            statements.Add(IsStillDue(scope, values.RecordKey));
        }

        return statements;
    }

    // ------------------------------------------------------------------------------------
    // Statements.
    // ------------------------------------------------------------------------------------

    /// <summary>S1: rows it could act on whose anchor is NULL, or rows the cutoff keeps.</summary>
    internal static SweepStatement CountByAnchor(SweepScope scope, SweepCount count)
    {
        var parameters = new SqlParams();
        var (name, where) = count switch
        {
            // No hold exclusion: a held NULL-anchor row is just as invisible to retention.
            SweepCount.NullAnchor => (
                "count-null-anchor",
                ScopeSql(scope, parameters, AnchorFilter.Null)
            ),
            // No hold exclusion: the rule keeps the row whether or not it is also held.
            SweepCount.RetainedByRule => (
                "count-retained-by-rule",
                ScopeSql(scope, parameters, AnchorFilter.OnOrAfterCutoff)
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(count), count, null),
        };
        return CountWhere(name, scope, where, parameters);
    }

    /// <summary>
    /// S2: what a dry run or a preview predicts, in one statement: one snapshot and one clock
    /// reading, so a hold that commits while it counts (a dry run takes no row locks) can't
    /// put a row in two counts or in none.
    /// <list type="bullet">
    /// <item><c>Eligible</c>: rows the strategy would act on now.</item>
    /// <item><c>Held</c>: rows it would act on but for an active hold.</item>
    /// <item><c>Blocked</c>: rows it would act on but for a blocking reference (held rows count
    /// as held). 0 when the scope has no blocking reference.</item>
    /// </list>
    /// </summary>
    internal static SweepStatement CountPredicted(SweepScope scope)
    {
        var parameters = new SqlParams();
        var scopeSql = ScopeSql(scope, parameters);
        var blocked = BlockedSql(scope, scopeSql, parameters);
        var due = DueSql(scope, parameters);
        var held = HoldExclusion(scope, parameters);
        var sql = $"""
            SELECT pg_catalog.count(*) FILTER (WHERE {due}),
                   pg_catalog.count(*) FILTER (WHERE {scopeSql} AND NOT {held}),
                   {blocked}
            FROM {PostgreSqlIdentifier.Format(scope.Entry.Table)} AS target
            WHERE {scopeSql}
            """;
        return Statement("count-predicted", scope, sql, parameters, references: true);
    }

    /// <summary>
    /// S3: what is left once run <paramref name="sweepId"/> has finished its batches for the
    /// entity, in one statement: one snapshot and one clock reading, so a hold that commits
    /// while the run counts can't put a row in two counts or in none.
    /// <list type="bullet">
    /// <item><c>Deferred</c>: rows still due, leaving out the rows the run already accounted
    /// for: a row skipped by a failing dependents step, through the row detail its skip wrote,
    /// and a row a constraint refused, through
    /// <paramref name="refusedRecordIds"/>. What remains was locked by another transaction when
    /// discovery passed it, or came into scope mid-run behind the run's position.</item>
    /// <item><c>Held</c>: rows it would act on but for an active hold.</item>
    /// <item><c>Referenced</c>: rows that would be due but for a blocking reference, leaving
    /// out <paramref name="refusedRecordIds"/> too, so no row is counted both refused and
    /// referenced. 0 when the scope has no blocking reference.</item>
    /// </list>
    /// The refused-ids clause is in the text only when <paramref name="refusedRecordIds"/> is not null.
    /// </summary>
    internal static SweepStatement CountLeft(SweepScope scope, Guid sweepId, object? refusedRecordIds)
    {
        var parameters = new SqlParams();
        var scopeSql = ScopeSql(scope, parameters);
        var notRefused = NotAnyRecordIdSql(scope.Entry, "refusedRecordIds", refusedRecordIds, parameters);
        var deferred =
            $"{DueSql(scope, parameters)}{NotRecordedByRunSql(scope, sweepId, parameters)}{notRefused}";
        var held = $"{scopeSql} AND NOT {HoldExclusion(scope, parameters)}";
        var referenced = BlockedSql(scope, scopeSql, parameters, notRefused);
        var sql = $"""
            SELECT pg_catalog.count(*) FILTER (WHERE {deferred}),
                   pg_catalog.count(*) FILTER (WHERE {held}),
                   {referenced}
            FROM {PostgreSqlIdentifier.Format(scope.Entry.Table)} AS target
            WHERE {scopeSql}
            """;
        return Statement(
            refusedRecordIds is null ? "count-left" : "count-left-after-refusals",
            scope,
            sql,
            parameters,
            rowDetail: true,
            references: true
        );
    }

    /// <summary>
    /// S4: every row of the scope's subject in its tenant, with no cutoff, eligibility or hold
    /// filter: what an Exempt rule keeps from an erasure.
    /// </summary>
    internal static SweepStatement CountSubjectRows(SweepScope scope)
    {
        if (scope.Subject is null)
        {
            throw new ArgumentException("Only an erasure scope has subject rows.", nameof(scope));
        }

        var parameters = new SqlParams();
        var clauses = SubjectAndTenantSql(scope, parameters);
        return CountWhere("count-subject-rows", scope, string.Join(" AND ", clauses), parameters);
    }

    private static SweepStatement CountWhere(
        string name,
        SweepScope scope,
        string where,
        SqlParams parameters
    ) =>
        Statement(
            name,
            scope,
            $"SELECT pg_catalog.count(*) FROM {PostgreSqlIdentifier.Format(scope.Entry.Table)} AS target WHERE {where}",
            parameters,
            holdExclusion: false
        );

    /// <summary>
    /// S5: up to <paramref name="limit"/> due rows after the cursor in discovery's order, with
    /// each row's anchor. A keyset rather than an exclusion list: each statement costs one batch
    /// however many rows the run has already passed over.
    /// </summary>
    internal static SweepStatement Discover(
        SweepScope scope,
        Guid sweepId,
        int limit,
        SweepCursorKind cursor,
        object? cursorAnchor,
        object? cursorRecordId
    )
    {
        var entry = scope.Entry;
        var parameters = new SqlParams { ["batchSize"] = limit };
        var where =
            DueSql(scope, parameters)
            + AfterCursorSql(scope, cursor, cursorAnchor, cursorRecordId, parameters)
            + NotRecordedByRunSql(scope, sweepId, parameters);

        var recordIdText = RecordIdSql.TextExpression("target", entry.RecordId);
        var anchorColumn = PostgreSqlIdentifier.Quote(entry.AnchorColumn);
        var sql = $"""
            SELECT {recordIdText}, target.{anchorColumn}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {where}
            ORDER BY {OrderBySql(entry)}
            LIMIT @batchSize
            """;
        var name = cursor switch
        {
            SweepCursorKind.None => "discover",
            SweepCursorKind.Anchored => "discover-after-anchor",
            _ => "discover-after-null-anchor",
        };
        return Statement(name, scope, sql, parameters, rowDetail: true, references: true);
    }

    /// <summary>
    /// S6: locks the discovered candidates <c>FOR UPDATE</c>, re-checking that each is still due.
    /// A sweep skips rows another run has locked and retires them next time; an erasure waits.
    /// </summary>
    internal static SweepStatement LockCandidates(SweepScope scope, object? candidateIds)
    {
        var entry = scope.Entry;
        var parameters = new SqlParams { ["candidateIds"] = candidateIds };
        var sql = $"""
            SELECT {RecordIdSql.TextExpression("target", entry.RecordId)}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {DueSql(scope, parameters)}
              AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
            ORDER BY {OrderBySql(entry)}
            FOR UPDATE{(scope.SkipLocked ? " SKIP LOCKED" : "")}
            """;
        return Statement("lock-candidates", scope, sql, parameters, references: true);
    }

    /// <summary>S7: changes the candidates in one statement, the bulk path.</summary>
    internal static SweepStatement MutateBatch(
        SweepScope scope,
        SqlParams headParameters,
        object? candidateIds
    )
    {
        var entry = scope.Entry;
        var parameters = new SqlParams { ["candidateIds"] = candidateIds };
        RetentionHoldSql.Merge(parameters, headParameters);
        var sql = $"""
            {HeadSql(scope.Rule.Strategy, entry)}
            WHERE {ScopeSql(scope, parameters)}
              AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {HoldExclusion(scope, parameters)}
            RETURNING {RecordIdSql.TextExpression("target", entry.RecordId)}
            """;
        return Statement("mutate-batch", scope, sql, parameters);
    }

    /// <summary>S8: loads the candidates' rows for the row-by-row path.</summary>
    internal static SweepStatement LoadRows(SweepScope scope, object? candidateIds)
    {
        var entry = scope.Entry;
        var parameters = new SqlParams { ["candidateIds"] = candidateIds };
        var columns = entry.MaterializationColumns.Select(column =>
            $"target.{PostgreSqlIdentifier.Quote(column)}"
        );
        var sql = $"""
            SELECT {string.Join(", ", columns)}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {ScopeSql(scope, parameters)}
              AND {HoldExclusion(scope, parameters)}
            ORDER BY {OrderBySql(entry)}
            """;
        return Statement("load-rows", scope, sql, parameters);
    }

    /// <summary>S9: changes one row on the row-by-row path.</summary>
    internal static SweepStatement MutateRow(
        SweepScope scope,
        SqlParams headParameters,
        object? recordKey
    )
    {
        var entry = scope.Entry;
        var parameters = new SqlParams { ["recordKey"] = recordKey };
        RetentionHoldSql.Merge(parameters, headParameters);
        var sql = $"""
            {HeadSql(scope.Rule.Strategy, entry)}
            WHERE {RecordIdSql.EqualsParameter("target", entry.RecordId, "recordKey")}
              AND {ScopeSql(scope, parameters)}
              AND {HoldExclusion(scope, parameters)}
            RETURNING {RecordIdSql.TextExpression("target", entry.RecordId)}
            """;
        return Statement("mutate-row", scope, sql, parameters);
    }

    /// <summary>
    /// S10: whether the row still matches the mutation statement's own WHERE: in scope, eligible
    /// and not held.
    /// </summary>
    internal static SweepStatement IsStillDue(SweepScope scope, object? recordKey)
    {
        var entry = scope.Entry;
        var parameters = new SqlParams { ["recordKey"] = recordKey };
        var sql = $"""
            SELECT EXISTS (
                SELECT 1
                FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
                WHERE {RecordIdSql.EqualsParameter("target", entry.RecordId, "recordKey")}
                  AND {ScopeSql(scope, parameters)}
                  AND {HoldExclusion(scope, parameters)}
            )
            """;
        return Statement("is-still-due", scope, sql, parameters);
    }

    /// <summary>
    /// The head of a strategy's mutation, aliased as <c>target</c>, from the entry and the
    /// strategy alone. Its values (<c>@deletedAt</c>, <c>@anonymisedAt</c>, <c>@value{n}</c>) are
    /// bound by the strategy's <see cref="SweepMutation"/>.
    /// </summary>
    internal static string HeadSql(Strategy strategy, RetentionEntry entry)
    {
        switch (strategy)
        {
            case Strategy.Purge:
                return $"DELETE FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target";
            case Strategy.SoftDelete:
                var softDelete = entry.SoftDelete!;
                return softDelete.DeletedAtColumn is null
                    ? $"UPDATE {PostgreSqlIdentifier.Format(entry.Table)} AS target SET {PostgreSqlIdentifier.Quote(softDelete.IsDeletedColumn)} = TRUE"
                    : $"UPDATE {PostgreSqlIdentifier.Format(entry.Table)} AS target SET {PostgreSqlIdentifier.Quote(softDelete.IsDeletedColumn)} = TRUE, {PostgreSqlIdentifier.Quote(softDelete.DeletedAtColumn)} = @deletedAt";
            case Strategy.Anonymise:
                var set = entry.AnonymiseFields.Select(
                    (field, index) => field.AssignmentSql($"value{index}")
                );
                var anonymisedAt = PostgreSqlIdentifier.Quote(
                    entry.AnonymisedAt!.AnonymisedAtColumn
                );
                return $"""
                    UPDATE {PostgreSqlIdentifier.Format(entry.Table)} AS target
                    SET {string.Join(", ", set)}, {anonymisedAt} = @anonymisedAt
                    """;
            default:
                throw new ArgumentOutOfRangeException(nameof(strategy), strategy, null);
        }
    }

    /// <summary>
    /// The value <c>@deletedAt</c> binds: a UTC <see cref="DateTime"/> for a <c>DateTime</c>
    /// member, which Npgsql sends as <c>timestamptz</c> only when its Kind is Utc, and the
    /// <see cref="DateTimeOffset"/> itself for a <c>DateTimeOffset</c> member.
    /// </summary>
    internal static object DeletedAtValue(
        RetentionEntry entry,
        SoftDeleteConvention softDelete,
        DateTimeOffset now
    )
    {
        var type = ReflectionMemberResolver
            .FindPropertyByName(entry.EntityType, softDelete.DeletedAtMember!)
            ?.PropertyType;
        return type == typeof(DateTime) || type == typeof(DateTime?) ? now.UtcDateTime
            : type == typeof(DateTimeOffset) || type == typeof(DateTimeOffset?) ? now
            : throw new InvalidOperationException(
                $"Soft-delete DeletedAt member '{softDelete.DeletedAtMember}' on {entry.EntityType.FullName} must be DateTime or DateTimeOffset (nullable allowed), got {type?.Name ?? "no property"}."
            );
    }

    // ------------------------------------------------------------------------------------
    // Fragments.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// The select item counting the rows that would be due but for a blocking reference, leaving
    /// out <paramref name="notRefused"/>; <c>0::bigint</c> when the scope has no blocking reference.
    /// </summary>
    private static string BlockedSql(
        SweepScope scope,
        string scopeSql,
        SqlParams parameters,
        string notRefused = ""
    ) =>
        scope.BlockingReferences.Count == 0
            ? "0::bigint"
            : $"pg_catalog.count(*) FILTER (WHERE {scopeSql} AND {HoldExclusion(scope, parameters)} AND NOT ({ReferencingForeignKeySql.NotReferencedSql(scope)}){notRefused})";

    /// <summary>
    /// Rows after the cursor in <see cref="OrderBySql"/>'s order, compared on
    /// the same expressions: the anchor as a timestamptz, then the record id's text. Ascending order puts NULL anchors
    /// last and a row comparison with NULL is never true, so they are spelt out; only an
    /// erasure without a legal minimum has them in scope.
    /// </summary>
    private static string AfterCursorSql(
        SweepScope scope,
        SweepCursorKind cursor,
        object? cursorAnchor,
        object? cursorRecordId,
        SqlParams parameters
    )
    {
        if (cursor == SweepCursorKind.None)
        {
            return "";
        }

        var entry = scope.Entry;
        var anchor = $"target.{PostgreSqlIdentifier.Quote(entry.AnchorColumn)}";
        var recordId = RecordIdSql.TextExpression("target", entry.RecordId);
        parameters["cursorRecordId"] = cursorRecordId;
        if (cursor == SweepCursorKind.NullAnchor)
        {
            return $"\n  AND {anchor} IS NULL AND {recordId} > @cursorRecordId";
        }

        parameters["cursorAnchor"] = cursorAnchor;
        var nullAnchors = HasNullAnchorsInScope(scope) ? $" OR {anchor} IS NULL" : "";
        return $"\n  AND (({anchor}, {recordId}) > (@cursorAnchor, @cursorRecordId){nullAnchors})";
    }

    /// <summary>
    /// Rows the strategy may act on now: in scope, not actively held, and not referenced
    /// through a blocking foreign key. Discovery, the row lock and the eligible and deferred
    /// counts share it, so a row excluded from one is excluded from all.
    /// </summary>
    private static string DueSql(SweepScope scope, SqlParams parameters) =>
        ReferencingForeignKeySql.NotReferencedSql(scope) is { Length: > 0 } notReferenced
            ? $"{ScopeSql(scope, parameters)} AND {HoldExclusion(scope, parameters)} AND {notReferenced}"
            : $"{ScopeSql(scope, parameters)} AND {HoldExclusion(scope, parameters)}";

    /// <summary>Excludes the record ids, or "" when there are none (<paramref name="recordIds"/> null).</summary>
    private static string NotAnyRecordIdSql(
        RetentionEntry entry,
        string parameterName,
        object? recordIds,
        SqlParams parameters
    )
    {
        if (recordIds is null)
        {
            return "";
        }

        parameters[parameterName] = recordIds;
        return $"\n  AND NOT ({RecordIdSql.EqualsAnyParameter("target", entry.RecordId, parameterName)})";
    }

    /// <summary>
    /// Excludes rows run <paramref name="sweepId"/> already recorded for this entity, such as
    /// rows a failing dependents step skipped: they stay eligible, so counting them as deferred would
    /// count them twice. Discovery's keyset already passes them, but a row whose anchor moved
    /// later after it was recorded would be met again, and its row detail recorded twice.
    /// </summary>
    private static string NotRecordedByRunSql(SweepScope scope, Guid sweepId, SqlParams parameters)
    {
        var entry = scope.Entry;
        parameters["excludedSweepId"] = sweepId;
        parameters["excludedRetentionEntityId"] = entry.RetentionEntityId;
        parameters["excludedCategory"] = entry.Category;
        parameters["excludedStrategy"] = (int)scope.Rule.Strategy;
        parameters["excludedTenantId"] = scope.Tenant.Id;
        var detailTable = PostgreSqlIdentifier.Format(entry.CohortTables.SweepRunRowDetail);
        var recordIdText = RecordIdSql.TextExpression("target", entry.RecordId);
        return $"""

              AND NOT EXISTS (
                  SELECT 1
                  FROM {detailTable} AS prior_detail
                  WHERE prior_detail."SweepId" = @excludedSweepId
                    AND prior_detail."RetentionEntityId" = @excludedRetentionEntityId
                    AND prior_detail."RecordId" = {recordIdText}
                    AND prior_detail."Category" = @excludedCategory
                    AND prior_detail."Strategy" = @excludedStrategy
                    AND prior_detail."TenantId" = @excludedTenantId
              )
            """;
    }

    /// <summary>
    /// The scope's rows, restricted to the tenant and to rows this strategy has not handled.
    /// <paramref name="anchor"/> picks which side of the cutoff the anchor must fall:
    /// before it (the rows retention acts on), NULL (rows no cutoff ever matches), or on or
    /// after it (rows the rule keeps; none without a cutoff).
    /// </summary>
    private static string ScopeSql(
        SweepScope scope,
        SqlParams parameters,
        AnchorFilter anchor = AnchorFilter.BeforeCutoff
    )
    {
        var entry = scope.Entry;
        var anchorColumn = $"target.{PostgreSqlIdentifier.Quote(entry.AnchorColumn)}";
        var clauses = SubjectAndTenantSql(scope, parameters);
        switch (anchor)
        {
            case AnchorFilter.Null:
                clauses.Add($"{anchorColumn} IS NULL");
                break;
            case AnchorFilter.BeforeCutoff when scope.Cutoff is { } cutoff:
                parameters["cutoff"] = cutoff;
                clauses.Add($"{anchorColumn} < @cutoff");
                break;
            case AnchorFilter.OnOrAfterCutoff when scope.Cutoff is { } cutoff:
                parameters["cutoff"] = cutoff;
                clauses.Add($"{anchorColumn} >= @cutoff");
                break;
            case AnchorFilter.OnOrAfterCutoff:
                clauses.Add("FALSE");
                break;
        }
        if (SweepEligibilitySql.Predicate(scope.Rule.Strategy, entry) is { Length: > 0 } eligibility)
        {
            clauses.Add(eligibility);
        }

        return clauses.Count == 0 ? "TRUE" : string.Join(" AND ", clauses);
    }

    /// <summary>The scope's subject (for an erasure) and tenant clauses.</summary>
    private static List<string> SubjectAndTenantSql(SweepScope scope, SqlParams parameters)
    {
        var entry = scope.Entry;
        var clauses = new List<string>();
        if (scope.Subject is { } subject)
        {
            var matches = new List<string>();
            for (var index = 0; index < subject.Matches.Count; index++)
            {
                parameters[$"subjectValue{index}"] = subject.Matches[index].SubjectValue;
                matches.Add(
                    subject.Matches[index].EqualsParameterSql("target", $"subjectValue{index}")
                );
            }
            clauses.Add($"({string.Join(" OR ", matches)})");
        }
        if (entry.Tenant is { } tenant)
        {
            parameters["tenantId"] = scope.Tenant.Id;
            clauses.Add($"target.{PostgreSqlIdentifier.Quote(tenant.TenantColumn)} = @tenantId");
        }

        return clauses;
    }

    /// <summary>
    /// Excludes covered rows: an active hold names the row, or a row it is linked to through a
    /// declared relation. One parenthesised term, so <c>NOT {HoldExclusion}</c> selects them.
    /// </summary>
    private static string HoldExclusion(SweepScope scope, SqlParams parameters)
    {
        var entry = scope.Entry;
        if (entry.Tenant is not null)
        {
            parameters["tenantId"] = scope.Tenant.Id;
        }

        var (sql, holdParameters) = RetentionHoldSql.BuildCoveredExclusion(
            entry.CohortTables.RetentionHolds,
            entry,
            "target"
        );
        RetentionHoldSql.Merge(parameters, holdParameters);
        return sql;
    }

    /// <summary>
    /// The order a batch is locked in, aliased <c>target</c>; hold creation locks the rows a new
    /// hold covers in the same order.
    /// </summary>
    internal static string OrderBySql(RetentionEntry entry) =>
        $"target.{PostgreSqlIdentifier.Quote(entry.AnchorColumn)} ASC, {RecordIdSql.TextExpression("target", entry.RecordId)} ASC";

    // ------------------------------------------------------------------------------------
    // Assembly.
    // ------------------------------------------------------------------------------------

    private static SweepStatement Statement(
        string name,
        SweepScope scope,
        string sql,
        SqlParams parameters,
        bool holdExclusion = true,
        bool rowDetail = false,
        bool references = false
    ) =>
        new(
            name,
            scope.Entry.EntityType,
            sql,
            parameters,
            ReadTables(scope, holdExclusion, rowDetail, references)
        );

    /// <summary>
    /// The tables a scope's statement reads: the entity's own; where the statement leaves out
    /// held rows, the holds table and the tables along the entity's hold paths
    /// (<see cref="RetentionEntry.HolderPaths"/>); where it leaves out the rows the run already
    /// recorded, the row-detail table; and, where it leaves out referenced rows, the child
    /// tables of the blocking foreign keys.
    /// </summary>
    private static RelationalObjectName[] ReadTables(
        SweepScope scope,
        bool holdExclusion,
        bool rowDetail,
        bool references
    )
    {
        var entry = scope.Entry;
        var tables = holdExclusion
            ? RetentionHoldSql
                .PathTables(entry, entry.HolderPaths, includeEntry: true)
                .Append(entry.CohortTables.RetentionHolds)
                .ToList()
            : [entry.Table];
        if (rowDetail)
        {
            tables.Add(entry.CohortTables.SweepRunRowDetail);
        }

        if (references)
        {
            tables.AddRange(scope.BlockingReferences.Select(reference => reference.ChildTable));
        }

        return tables.Distinct().ToArray();
    }
}
