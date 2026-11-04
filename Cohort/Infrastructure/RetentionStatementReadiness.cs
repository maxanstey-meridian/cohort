using System.Data.Common;
using System.Runtime.ExceptionServices;
using System.Text;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;
using Cohort.Infrastructure.Sweep;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using NpgsqlTypes;

namespace Cohort.Infrastructure;

/// <summary>
/// A value only a run has (a row's record key, a refused id, an erasure subject, an anonymise
/// value), which readiness binds as a NULL typed from <paramref name="Type"/>: the type PostgreSQL
/// needs to refuse a comparison it can't make, and no value for it to fold at plan time.
/// </summary>
internal sealed record ReadinessTypedNull(Type Type);

/// <summary>Turns a statement's <see cref="SqlParams"/> into the parameters readiness sends.</summary>
internal static class ReadinessParameters
{
    /// <summary>
    /// One <see cref="NpgsqlParameter"/> per entry, named as the entry is, so Npgsql rewrites and
    /// sends them as it does for the run. A value becomes a <c>NpgsqlParameter&lt;T?&gt;</c> over
    /// its own CLR type; a <see cref="ReadinessTypedNull"/> becomes one of the same shape with no
    /// value (a <c>DateTime</c> one is <c>timestamptz</c>, the type a Kind-Utc value gets); a
    /// plain null stays untyped, as the run binds it.
    /// </summary>
    internal static NpgsqlParameter[] From(SqlParams parameters) =>
        [.. parameters.Select(pair => Create(pair.Key, pair.Value))];

    private static NpgsqlParameter Create(string name, object? value) =>
        value switch
        {
            null or DBNull => new NpgsqlParameter(name, DBNull.Value),
            ReadinessTypedNull typed => Typed(name, typed.Type, null),
            _ => Typed(name, value.GetType(), value),
        };

    private static NpgsqlParameter Typed(string name, Type type, object? value)
    {
        var nullable =
            type.IsValueType && Nullable.GetUnderlyingType(type) is null
                ? typeof(Nullable<>).MakeGenericType(type)
                : type;
        var parameterType = typeof(NpgsqlParameter<>).MakeGenericType(nullable);
        var parameter = (NpgsqlParameter)
            parameterType.GetConstructor([typeof(string), nullable])!.Invoke([name, value]);
        if (value is null && Nullable.GetUnderlyingType(nullable) == typeof(DateTime))
        {
            parameter.NpgsqlDbType = NpgsqlDbType.TimestampTz;
        }

        return parameter;
    }
}

/// <summary>
/// The part of a statement's text PostgreSQL's error position points at. The position indexes the
/// text the server received, which is <c>EXPLAIN </c> followed by the statement with each
/// <c>@name</c> rewritten to <c>$n</c> by Npgsql, whose rewriter is internal. This mirrors it so
/// that the excerpt lands on the right column; it never changes what is sent.
/// </summary>
internal static class ReadinessExcerpt
{
    private const string Prefix = "EXPLAIN ";
    private const int CharactersBefore = 30;
    private const int CharactersAfter = 30;

    /// <summary>
    /// About 60 characters of the rewritten statement around <paramref name="position"/> (the
    /// server's 1-based character offset into <c>EXPLAIN </c> plus the rewritten text), with
    /// whitespace runs collapsed to one space. Empty when the position isn't inside the statement.
    /// </summary>
    internal static string At(string sql, IReadOnlyCollection<string> names, int position)
    {
        var text = Rewrite(sql, names);
        var characters = position - 1 - Prefix.Length;
        if (characters < 0)
        {
            return "";
        }

        var index = Utf16Offset(text, characters);
        if (index >= text.Length)
        {
            return "";
        }

        var start = Math.Max(0, index - CharactersBefore);
        var end = Math.Min(text.Length, index + CharactersAfter);
        return string.Join(
            ' ',
            text[start..end].Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries)
        );
    }

    /// <summary>
    /// The statement as Npgsql sends it: each <c>@name</c> or <c>:name</c> in
    /// <paramref name="names"/> becomes <c>$n</c>, numbered by first appearance with a repeat
    /// reusing its number. A name reads greedily (letters, digits, <c>_</c>, <c>.</c>); a name not
    /// in the collection stays as written; quoted identifiers, literals, comments, and dollar
    /// quotes are skipped.
    /// </summary>
    private static string Rewrite(string sql, IReadOnlyCollection<string> names)
    {
        var known = new HashSet<string>(names, StringComparer.OrdinalIgnoreCase);
        var numbers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var text = new StringBuilder(sql.Length);
        var at = 0;
        while (at < sql.Length)
        {
            var current = sql[at];
            var next = at + 1 < sql.Length ? sql[at + 1] : '\0';
            var previous = at > 0 ? sql[at - 1] : '\0';
            switch (current)
            {
                case '\'':
                    at = CopyQuoted(sql, at, text, backslashEscapes: IsEscapeString(sql, at));
                    break;
                case '"':
                    at = CopyQuoted(sql, at, text, backslashEscapes: false);
                    break;
                case '-' when next == '-':
                    at = CopyThrough(
                        sql,
                        at,
                        text,
                        sql.IndexOf('\n', at) is var lineEnd and >= 0 ? lineEnd + 1 : sql.Length
                    );
                    break;
                case '/' when next == '*':
                    at = CopyBlockComment(sql, at, text);
                    break;
                case '$'
                    when !IsIdentifierChar(previous)
                        && DollarQuoteEnd(sql, at) is var quoteEnd and >= 0:
                    at = CopyThrough(sql, at, text, quoteEnd);
                    break;
                case '@' or ':' when previous != current && IsParameterNameChar(next):
                    var end = at + 1;
                    while (end < sql.Length && IsParameterNameChar(sql[end]))
                    {
                        end++;
                    }

                    var name = sql[(at + 1)..end];
                    if (known.Contains(name))
                    {
                        if (!numbers.TryGetValue(name, out var number))
                        {
                            number = numbers.Count + 1;
                            numbers[name] = number;
                        }

                        text.Append('$').Append(number);
                    }
                    else
                    {
                        text.Append(sql, at, end - at);
                    }

                    at = end;
                    break;
                default:
                    text.Append(current);
                    at++;
                    break;
            }
        }

        return text.ToString();
    }

    private static bool IsParameterNameChar(char c) =>
        c == '_' || c == '.' || (c < 128 ? char.IsAsciiLetterOrDigit(c) : char.IsLetterOrDigit(c));

    private static bool IsIdentifierChar(char c) => c == '_' || c == '$' || char.IsLetterOrDigit(c);

    // E'...' reads a backslash as an escape.
    private static bool IsEscapeString(string sql, int quote) =>
        quote > 0
        && (sql[quote - 1] is 'e' or 'E')
        && (quote == 1 || !IsIdentifierChar(sql[quote - 2]));

    private static int CopyQuoted(string sql, int start, StringBuilder text, bool backslashEscapes)
    {
        var quote = sql[start];
        text.Append(quote);
        var at = start + 1;
        while (at < sql.Length)
        {
            var c = sql[at++];
            text.Append(c);
            if (backslashEscapes && c == '\\' && at < sql.Length)
            {
                text.Append(sql[at++]);
            }
            else if (c == quote)
            {
                if (at < sql.Length && sql[at] == quote)
                {
                    text.Append(sql[at++]);
                    continue;
                }

                return at;
            }
        }

        return at;
    }

    private static int CopyThrough(string sql, int start, StringBuilder text, int end)
    {
        text.Append(sql, start, end - start);
        return end;
    }

    // Block comments nest in PostgreSQL.
    private static int CopyBlockComment(string sql, int start, StringBuilder text)
    {
        var depth = 1;
        var at = start + 2;
        while (at < sql.Length && depth > 0)
        {
            if (sql[at] == '/' && at + 1 < sql.Length && sql[at + 1] == '*')
            {
                depth++;
                at += 2;
            }
            else if (sql[at] == '*' && at + 1 < sql.Length && sql[at + 1] == '/')
            {
                depth--;
                at += 2;
            }
            else
            {
                at++;
            }
        }

        text.Append(sql, start, at - start);
        return at;
    }

    // Where the dollar-quoted string starting at `start` ends, or -1 when `$` there isn't one
    // (`$1` is a positional parameter).
    private static int DollarQuoteEnd(string sql, int start)
    {
        var tagEnd = start + 1;
        while (tagEnd < sql.Length && (char.IsLetterOrDigit(sql[tagEnd]) || sql[tagEnd] == '_'))
        {
            tagEnd++;
        }

        if (
            tagEnd >= sql.Length
            || sql[tagEnd] != '$'
            || (tagEnd > start + 1 && char.IsDigit(sql[start + 1]))
        )
        {
            return -1;
        }

        var tag = sql[start..(tagEnd + 1)];
        var close = sql.IndexOf(tag, tagEnd + 1, StringComparison.Ordinal);
        return close < 0 ? sql.Length : close + tag.Length;
    }

    // The server counts characters (code points); .NET indexes UTF-16 units.
    private static int Utf16Offset(string text, int characters)
    {
        var index = 0;
        for (var counted = 0; counted < characters && index < text.Length; counted++)
        {
            index += char.IsHighSurrogate(text[index]) && index + 1 < text.Length ? 2 : 1;
        }

        return index;
    }
}

/// <summary>
/// One statement readiness explains: the text and parameters a run sends, with every entity and
/// operation that issues it and the tables it reads. Statements with the same text and parameter
/// types are one.
/// </summary>
internal sealed class ReadinessStatement(
    string name,
    string sql,
    SqlParams parameters,
    string parameterTypes
)
{
    internal string Name { get; } = name;

    internal string Sql { get; } = sql;

    internal SqlParams Parameters { get; } = parameters;

    internal string ParameterTypes { get; } = parameterTypes;

    /// <summary>The entities whose scope or hold call issues it; a null for a statement that spans entities.</summary>
    internal List<Type?> Entities { get; } = [];

    internal List<string> Operations { get; } = [];

    internal List<RelationalObjectName> Relations { get; } = [];

    internal ReadinessStatementInput Input =>
        new(
            Name,
            string.Join(',', Entities.Select(EntityName).Order(StringComparer.Ordinal)),
            Sql,
            ParameterTypes
        );

    internal static string EntityName(Type? entity) =>
        entity is null ? "" : entity.FullName ?? entity.Name;
}

/// <summary>A statement as a readiness key holds it: values only.</summary>
internal sealed record ReadinessStatementInput(
    string Name,
    string Entity,
    string Sql,
    string ParameterTypes
);

/// <summary>
/// Readiness for the statements Cohort sends, against the host's tables and its own. For one
/// <see cref="RetentionStatementGroup"/> (the statements one operation sends, checked under the
/// role of the scope that sends them) it builds every statement the operation can issue for the
/// host's model and the strategies its categories declare, through the builders the run itself
/// executes, and asks PostgreSQL to analyse and plan each one (<c>EXPLAIN</c>, which runs
/// nothing). What PostgreSQL refuses is what readiness refuses: a missing table or column, a
/// comparison or assignment the types can't make, a column the database generates, an unknown
/// cast type, a privilege the session's role lacks.
/// </summary>
internal sealed class RetentionStatementReadiness(
    DbContext db,
    RetentionRegistry registry,
    RetentionValidationState validationState,
    AnonymiseAssignmentResolver assignments,
    IEnumerable<IRetentionHandlerRegistration> handlerRegistrations,
    IEnumerable<IRetentionDependentsRegistration> dependentsRegistrations,
    IRetentionExecutionSettings settings
)
{
    private const string Savepoint = "cohort_readiness_statements";

    // The tenant a tenanted entity's statements are built for: any non-empty id.
    private static readonly Guid ReadinessTenantId = new("c0de0000-0000-0000-0000-000000000001");

    private static readonly TenantContext ReadinessTenant = new(
        ReadinessTenantId,
        null,
        new Dictionary<string, string>()
    );

    private static readonly RetentionPeriod OneYear = new(Years: 1);

    /// <summary>
    /// The groups a port operation sends: sweeps, erasures, previews, hold calls, and deletions.
    /// Each runs in a scope startup can't take on, so each is checked when its role first reaches
    /// readiness; startup only explains them under its own role, as an early report.
    /// </summary>
    internal static IReadOnlyList<RetentionStatementGroup> PortGroups { get; } =
        [
            RetentionStatementGroup.Sweep,
            RetentionStatementGroup.Erasure,
            RetentionStatementGroup.Preview,
            RetentionStatementGroup.Holds,
            RetentionStatementGroup.Deletion,
        ];

    /// <summary>
    /// What a refusal calls the operation a group's statements belong to ("PostgreSQL refused
    /// statements Cohort sends in sweeps").
    /// </summary>
    internal static string ScopeOf(RetentionStatementGroup group) =>
        group.Kind switch
        {
            RetentionStatementGroupKind.Sweep => "sweeps",
            RetentionStatementGroupKind.Erasure => "erasures",
            RetentionStatementGroupKind.Preview => "previews",
            RetentionStatementGroupKind.Holds => "hold calls",
            RetentionStatementGroupKind.Deletion => "deletions",
            RetentionStatementGroupKind.Scrub => "the payload scrub",
            RetentionStatementGroupKind.Dispatch => "row-handler dispatch",
            RetentionStatementGroupKind.Worker => "the scheduled worker",
            RetentionStatementGroupKind.Pruning => "history pruning",
            _ => throw new ArgumentOutOfRangeException(nameof(group), group.Kind, null),
        };

    /// <summary>What a refusal calls readiness's own scope.</summary>
    internal const string ReadinessScope = "readiness";

    /// <summary>
    /// Every statement the group's operation can issue for the model, de-duplicated, in a fixed
    /// order: the host-table statements, then the statements against Cohort's own tables and the
    /// functions it calls. Reads the blocking foreign keys of the Purge scopes through the
    /// database's connection, as a run does.
    /// </summary>
    internal async Task<IReadOnlyList<ReadinessStatement>> EnumerateAsync(
        RetentionStatementGroup group,
        IReadOnlyDictionary<string, RetentionCategoryCapabilities> capabilities,
        CancellationToken ct
    )
    {
        var entries = registry.Scan().Values.ToList();
        var tables = CohortStoreTables.FromModel(db.Model);
        var values = ValuesFor(entries);
        var tenantlessRetained = entries.Any(entry => entry.Tenant is null);
        var anyDependentsStep = entries.Any(entry => dependentsRegistrations.HasStep(entry.EntityType));
        var set = new StatementSet();
        switch (group.Kind)
        {
            case RetentionStatementGroupKind.Sweep:
                await AddScopesAsync(set, entries, capabilities, erasure: false, ct);
                AddGuard(set, tables, entries, anyDependentsStep);
                set.AddCohort(
                    CohortStatements.ForSweep(
                        tables,
                        HandlerBookkeeping(entries),
                        tenantlessRetained && anyDependentsStep,
                        values
                    )
                );
                break;
            case RetentionStatementGroupKind.Erasure:
                await AddScopesAsync(set, entries, capabilities, erasure: true, ct);
                AddGuard(set, tables, entries, anyDependentsStep);
                set.AddCohort(
                    CohortStatements.ForErasure(
                        tables,
                        HandlerBookkeeping(entries),
                        tenantlessRetained && anyDependentsStep,
                        values
                    )
                );
                break;
            case RetentionStatementGroupKind.Preview:
                AddPreviewCounts(set, await SweepScopesAsync(entries, capabilities, ct));
                set.AddCohort(CohortStatements.ForPreview());
                break;
            case RetentionStatementGroupKind.Holds:
                AddHolds(set, tables, entries);
                set.AddCohort(CohortStatements.ForHolds(tables, values));
                break;
            case RetentionStatementGroupKind.Deletion:
                foreach (var entry in entries)
                {
                    set.Add(
                        RetentionHoldSql.ForDeletion(
                            tables.RetentionHolds,
                            entry,
                            new ReadinessTypedNull(typeof(string)),
                            entry.Tenant is null ? null : ReadinessTenantId
                        ),
                        "checking a deletion"
                    );
                }

                AddGuard(set, tables, entries, include: true);
                set.AddCohort(CohortStatements.ForDeletion(tables, tenantlessRetained, values));
                break;
            case RetentionStatementGroupKind.Scrub:
                set.AddCohort(CohortStatements.ForScrub(tables, values));
                break;
            case RetentionStatementGroupKind.Dispatch:
                set.AddCohort(CohortStatements.ForDispatch(tables, values));
                break;
            case RetentionStatementGroupKind.Worker:
                set.AddCohort(CohortStatements.ForWorker(tables, entries, capabilities, values));
                break;
            case RetentionStatementGroupKind.Pruning:
                set.AddCohort(CohortStatements.ForPruning(tables, group.PruningScope!, values));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(group), group.Kind, null);
        }

        return set.Statements;
    }

    /// <summary>
    /// What readiness reads in its own <c>System(Readiness)</c> scope: the stored entity
    /// identities and, when a retained entity is tenantless, the tenantless holds. It belongs to
    /// no caller's group, so it is built apart.
    /// </summary>
    internal IReadOnlyList<ReadinessStatement> EnumerateReadiness()
    {
        var entries = registry.Scan().Values.ToList();
        var set = new StatementSet();
        set.AddCohort(
            CohortStatements.ForReadiness(
                CohortStoreTables.FromModel(db.Model),
                entries.Any(entry => entry.Tenant is null),
                ValuesFor(entries)
            )
        );
        return set.Statements;
    }

    /// <summary>
    /// Whether a run records handler bookkeeping (D2 and T1) for some entity: it has handlers or a
    /// dependents step, the run's own helpers' answer.
    /// </summary>
    internal bool HandlerBookkeeping() => HandlerBookkeeping(registry.Scan().Values);

    private bool HandlerBookkeeping(IEnumerable<RetentionEntry> entries) =>
        entries.Any(entry =>
            RetentionHandlerSupport.HasHandlers(handlerRegistrations, entry.EntityType)
            || dependentsRegistrations.HasStep(entry.EntityType)
        );

    // The values the Cohort-table statements bind: readiness's tenant, and one retained entity's
    // identity (the first by name) for the statements that name an entity.
    private static CohortStatementValues ValuesFor(IReadOnlyList<RetentionEntry> entries)
    {
        var first = entries
            .OrderBy(entry => entry.EntityType.FullName, StringComparer.Ordinal)
            .FirstOrDefault();
        return first is null
            ? new CohortStatementValues(
                ReadinessTenantId,
                typeof(RetentionStatementReadiness),
                new Guid("c0de0000-0000-0000-0000-000000000006"),
                "readiness"
            )
            : new CohortStatementValues(
                ReadinessTenantId,
                first.EntityType,
                first.RetentionEntityId,
                first.Category
            );
    }

    // The sweep scopes (and, with erasure, the erasure scopes) of every entity with a declared
    // category, each strategy it declares. The blocking keys of the Purge scopes are read in one
    // catalog read, as a run reads them.
    private async Task<
        List<(SweepScope Scope, Strategy Strategy, string Operation)>
    > ScopesAsync(
        IReadOnlyList<RetentionEntry> entries,
        IReadOnlyDictionary<string, RetentionCategoryCapabilities> capabilities,
        bool sweeps,
        bool erasures,
        CancellationToken ct
    )
    {
        var validation = validationState.For(db.Model);
        var now = DateTimeOffset.UtcNow;
        var scopes = new List<(SweepScope Scope, Strategy Strategy, string Operation)>();
        foreach (var entry in entries)
        {
            if (!capabilities.TryGetValue(entry.Category, out var declared))
            {
                continue;
            }

            var tenant = entry.Tenant is null ? TenantContext.Tenantless : ReadinessTenant;
            var kinds = validation.ErasureSubjects.TryGetValue(entry.EntityType, out var metadata)
                ? metadata
                : [];
            foreach (var strategy in declared.Strategies.Order())
            {
                if (sweeps && strategy != Strategy.Exempt)
                {
                    var context = new RetentionResolutionContext(entry.Category, tenant, now);
                    scopes.Add(
                        (
                            SweepScope.ForSweep(
                                entry,
                                new RetentionRule(OneYear, strategy),
                                context,
                                settings.TimeZone
                            ),
                            strategy,
                            $"sweeping with {strategy}"
                        )
                    );
                }

                if (!erasures)
                {
                    continue;
                }

                // SoftDelete included: AllowSoftDeleteAsErasure is per request.
                foreach (var kind in kinds)
                {
                    foreach (var legalMinimum in new RetentionPeriod?[] { OneYear, null })
                    {
                        var rule = new RetentionRule(OneYear, strategy, legalMinimum);
                        scopes.Add(
                            (
                                SweepScope.ForErasure(
                                    entry,
                                    rule,
                                    SubjectPredicate(kind),
                                    tenant,
                                    now,
                                    settings.TimeZone
                                ),
                                strategy,
                                $"erasing subjects of kind '{kind.Kind}'"
                            )
                        );
                    }
                }
            }
        }

        var attached = await ReferencingForeignKeySql.AttachAsync(
            db.Database.GetDbConnection(),
            scopes.Select(scope => scope.Scope).ToArray(),
            entry => dependentsRegistrations.HasStep(entry.EntityType),
            ct
        );
        return [.. scopes.Select((scope, index) => (attached[index], scope.Strategy, scope.Operation))];
    }

    private Task<List<(SweepScope Scope, Strategy Strategy, string Operation)>> SweepScopesAsync(
        IReadOnlyList<RetentionEntry> entries,
        IReadOnlyDictionary<string, RetentionCategoryCapabilities> capabilities,
        CancellationToken ct
    ) => ScopesAsync(entries, capabilities, sweeps: true, erasures: false, ct);

    // Every host-table statement a sweep or an erasure issues (`ForScope`).
    private async Task AddScopesAsync(
        StatementSet set,
        IReadOnlyList<RetentionEntry> entries,
        IReadOnlyDictionary<string, RetentionCategoryCapabilities> capabilities,
        bool erasure,
        CancellationToken ct
    )
    {
        foreach (
            var (scope, strategy, operation) in await ScopesAsync(
                entries,
                capabilities,
                sweeps: !erasure,
                erasures: erasure,
                ct
            )
        )
        {
            set.Add(
                SweepStatements.ForScope(
                    scope,
                    strategy,
                    PathOf(scope.Entry, strategy),
                    HeadParameters(scope, strategy),
                    RunValues(scope.Entry)
                ),
                operation
            );
        }
    }

    // What a preview counts (`RetentionPreviewService`): the prediction, and the NULL-anchor
    // count when the scope has a cutoff.
    private static void AddPreviewCounts(
        StatementSet set,
        List<(SweepScope Scope, Strategy Strategy, string Operation)> scopes
    )
    {
        foreach (var (scope, strategy, operation) in scopes)
        {
            set.Add([SweepStatements.CountPredicted(scope, strategy)], operation);
            if (SweepStatements.CountsNullAnchor(scope))
            {
                set.Add(
                    [SweepStatements.CountByAnchor(scope, strategy, SweepCount.NullAnchor)],
                    operation
                );
            }
        }
    }

    // The held-row guard's forms (`ForModel`: the one read over every entity, and the per-entity
    // forms that tell which entity a refusal comes from), for the groups that run it. A sweep or
    // an erasure runs it only for a dependents step; a deletion always does.
    private static void AddGuard(
        StatementSet set,
        CohortStoreTables tables,
        IReadOnlyList<RetentionEntry> entries,
        bool include
    )
    {
        if (!include)
        {
            return;
        }

        set.Add(
            RetentionHoldSql.ForModel(
                tables.RetentionHolds,
                entries,
                Array.Empty<Guid>(),
                new ReadinessTypedNull(typeof(Guid[])),
                new ReadinessTypedNull(typeof(string[]))
            ),
            "guarding held rows"
        );
    }

    // What the holds repository issues against host tables: each entity's target row and
    // covering reads, and the covering read of each erasure-subject kind.
    private void AddHolds(
        StatementSet set,
        CohortStoreTables tables,
        IReadOnlyList<RetentionEntry> entries
    )
    {
        var validation = validationState.For(db.Model);
        foreach (var entry in entries)
        {
            set.Add(
                RetentionHoldSql.ForHolds(
                    tables.RetentionHolds,
                    entry,
                    entries,
                    new ReadinessTypedNull(typeof(string)),
                    entry.Tenant is null ? null : ReadinessTenantId
                ),
                "creating, listing, or checking holds"
            );
        }

        foreach (var kind in validation.ErasureSubjectKinds.Keys.Order(StringComparer.Ordinal))
        {
            var starts = entries
                .Select(entry =>
                    (
                        Entry: entry,
                        Metadata: validation.ErasureSubjects.TryGetValue(
                            entry.EntityType,
                            out var all
                        )
                            ? all.FirstOrDefault(metadata =>
                                string.Equals(metadata.Kind, kind, StringComparison.Ordinal)
                            )
                            : null
                    )
                )
                .Where(start => start.Metadata is not null)
                .Select(start => (start.Entry, Subject: SubjectPredicate(start.Metadata!)))
                .ToList();
            if (starts.Count > 0)
            {
                set.Add(
                    RetentionHoldSql.ForSubjectKind(tables.RetentionHolds, starts, ReadinessTenantId),
                    $"listing the holds that cover a subject of kind '{kind}'"
                );
            }
        }
    }

    /// <summary>
    /// Asks PostgreSQL to explain every statement over <paramref name="database"/>, in its current
    /// transaction or in one opened here and rolled back, and returns one error per entity for
    /// what it refused, then one for the statements that read none of the host's tables
    /// (<see cref="Describe"/>). Over an <see cref="NpgsqlConnection"/> it is one round trip, with
    /// an error barrier and a rollback to a savepoint after each statement so every refusal is
    /// reported and the transaction stays usable; over any other connection (a wrapper can't
    /// batch) the same commands run one at a time. <paramref name="scope"/> names the operation
    /// whose role is checked. With <paramref name="tolerateInsufficientPrivilege"/>, a refusal
    /// for a privilege the role lacks (42501) is not reported: startup's early check of the port
    /// groups, whose roles belong to scopes it can't take on.
    /// </summary>
    internal async Task<IReadOnlyList<string>> ExplainAsync(
        DatabaseFacade database,
        IReadOnlyList<ReadinessStatement> statements,
        string role,
        string scope,
        bool tolerateInsufficientPrivilege,
        CancellationToken ct
    )
    {
        if (statements.Count == 0)
        {
            return [];
        }

        await database.OpenConnectionAsync(ct);
        try
        {
            var connection = database.GetDbConnection();
            var callerTransaction = database.CurrentTransaction?.GetDbTransaction();
            await using var ownTransaction = callerTransaction is null
                ? await connection.BeginTransactionAsync(ct)
                : null;
            var transaction = callerTransaction ?? ownTransaction!;
            var refusals =
                connection is NpgsqlConnection npgsql
                && transaction is NpgsqlTransaction npgsqlTransaction
                    ? await ExplainInBatchAsync(npgsql, npgsqlTransaction, statements, ct)
                    : await ExplainOneAtATimeAsync(connection, transaction, statements, ct);
            if (ownTransaction is not null)
            {
                await ownTransaction.RollbackAsync(ct);
            }

            if (tolerateInsufficientPrivilege)
            {
                refusals.RemoveAll(refusal =>
                    refusal.Failure.SqlState == PostgresErrorCodes.InsufficientPrivilege
                );
            }

            return refusals.Count == 0
                ? []
                : Describe(
                    refusals,
                    role,
                    scope,
                    CohortTables(CohortStoreTables.FromModel(db.Model))
                );
        }
        finally
        {
            await database.CloseConnectionAsync();
        }
    }

    /// <summary>
    /// Refuses a serial <c>Id</c> default whose sequence the role can't use. <c>EXPLAIN</c> never
    /// calls <c>nextval</c>, so a missing sequence privilege shows only when a row is inserted.
    /// The columns are <c>sweep_run_row_detail.Id</c>, always, since the row detail's insert
    /// depends on a rule readiness can't see, and <c>sweep_row_handler_status.Id</c> when a run
    /// records handler bookkeeping. The sequence is found from the column's default expression
    /// (<c>pg_depend</c>); an identity column has none and needs no grant. A role needs
    /// <c>USAGE</c> or <c>UPDATE</c> on a sequence to call <c>nextval</c>.
    /// </summary>
    internal async Task<IReadOnlyList<string>> CheckSequencesAsync(
        DatabaseFacade database,
        string role,
        CancellationToken ct
    )
    {
        const string column = "Id";
        var tables = CohortStoreTables.FromModel(db.Model);
        var checkedTables = new List<RelationalObjectName> { tables.SweepRunRowDetail };
        if (HandlerBookkeeping())
        {
            checkedTables.Add(tables.SweepRowHandlerStatus);
        }

        var errors = new List<string>();
        await database.OpenConnectionAsync(ct);
        try
        {
            await using var command = new SqlParams
            {
                ["schemas"] = checkedTables.Select(table => table.Schema).ToArray(),
                ["tables"] = checkedTables.Select(table => table.Name).ToArray(),
                ["column"] = column,
            }.CreateCommand(
                database.GetDbConnection(),
                database.CurrentTransaction?.GetDbTransaction(),
                """
                /* cohort catalog */
                SELECT requested.ordinal,
                       sequence_namespace.nspname,
                       sequence_class.relname
                FROM ROWS FROM (
                    pg_catalog.unnest(@schemas),
                    pg_catalog.unnest(@tables)
                ) WITH ORDINALITY AS requested(schema_name, table_name, ordinal)
                JOIN pg_catalog.pg_namespace namespace ON namespace.nspname = requested.schema_name
                JOIN pg_catalog.pg_class relation
                  ON relation.relnamespace = namespace.oid AND relation.relname = requested.table_name
                JOIN pg_catalog.pg_attribute attribute
                  ON attribute.attrelid = relation.oid
                 AND attribute.attname = @column
                 AND attribute.attnum > 0
                 AND NOT attribute.attisdropped
                JOIN pg_catalog.pg_attrdef default_expression
                  ON default_expression.adrelid = attribute.attrelid
                 AND default_expression.adnum = attribute.attnum
                JOIN pg_catalog.pg_depend dependency
                  ON dependency.classid = 'pg_catalog.pg_attrdef'::pg_catalog.regclass
                 AND dependency.objid = default_expression.oid
                 AND dependency.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
                JOIN pg_catalog.pg_class sequence_class ON sequence_class.oid = dependency.refobjid
                JOIN pg_catalog.pg_namespace sequence_namespace
                  ON sequence_namespace.oid = sequence_class.relnamespace
                -- The default also depends on its own column (a table, which the privilege
                -- function refuses), so the kind is checked in the CASE, which orders it first.
                WHERE sequence_class.relkind = 'S'
                  AND CASE WHEN sequence_class.relkind = 'S'
                           THEN NOT (
                               pg_catalog.has_sequence_privilege(sequence_class.oid, 'USAGE')
                               OR pg_catalog.has_sequence_privilege(sequence_class.oid, 'UPDATE')
                           )
                           ELSE false
                      END
                ORDER BY requested.ordinal
                """
            );
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var table = checkedTables[(int)reader.GetInt64(0) - 1];
                var sequence = new RelationalObjectName(reader.GetString(1), reader.GetString(2));
                errors.Add(
                    $"Cohort's own tables and functions: {PostgreSqlIdentifier.Format(table)}.{PostgreSqlIdentifier.Quote(column)} takes its default from the sequence {PostgreSqlIdentifier.Format(sequence)}, and the database role '{role}' holds neither USAGE nor UPDATE on it, so every row Cohort inserts there would fail. Grant USAGE on the sequence, or make the column an identity column."
                );
            }
        }
        finally
        {
            await database.CloseConnectionAsync();
        }

        return errors;
    }

    private static async Task<List<Refusal>> ExplainInBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyList<ReadinessStatement> statements,
        CancellationToken ct
    )
    {
        await using var batch = new NpgsqlBatch(connection, transaction)
        {
            EnableErrorBarriers = true,
        };
        batch.BatchCommands.Add(new NpgsqlBatchCommand($"SAVEPOINT {Savepoint}"));
        var explains = new NpgsqlBatchCommand[statements.Count];
        for (var index = 0; index < statements.Count; index++)
        {
            var explain = new NpgsqlBatchCommand($"EXPLAIN {statements[index].Sql}");
            foreach (var parameter in ReadinessParameters.From(statements[index].Parameters))
            {
                explain.Parameters.Add(parameter);
            }

            explains[index] = explain;
            batch.BatchCommands.Add(explain);
            batch.BatchCommands.Add(new NpgsqlBatchCommand($"ROLLBACK TO SAVEPOINT {Savepoint}"));
        }

        batch.BatchCommands.Add(new NpgsqlBatchCommand($"RELEASE SAVEPOINT {Savepoint}"));
        try
        {
            await batch.ExecuteNonQueryAsync(ct);
            return [];
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            var failures = PostgresFailures(exception);
            var refusals = failures
                ?.Select(failure =>
                    (Index: Array.IndexOf(explains, failure.BatchCommand), Failure: failure)
                )
                .ToList();
            if (refusals is null || refusals.Any(refusal => refusal.Index < 0))
            {
                // Not one of the explained statements (the savepoint, a lost connection).
                throw;
            }

            if (
                refusals.Select(refusal => refusal.Failure).FirstOrDefault(f => !IsRefusal(f)) is
                { } operational
            )
            {
                // An operational failure (a lock or statement timeout, a deadlock): not a refusal.
                // Rethrow the first as PostgreSQL raised it, as the one-at-a-time path does, not
                // as the aggregate Npgsql wraps two or more in.
                ExceptionDispatchInfo.Capture(operational).Throw();
            }

            return
            [
                .. refusals.Select(refusal => new Refusal(
                    statements[refusal.Index],
                    refusal.Failure
                )),
            ];
        }
    }

    private static async Task<List<Refusal>> ExplainOneAtATimeAsync(
        DbConnection connection,
        DbTransaction transaction,
        IReadOnlyList<ReadinessStatement> statements,
        CancellationToken ct
    )
    {
        var refusals = new List<Refusal>();
        await RunAsync($"SAVEPOINT {Savepoint}", []);
        foreach (var statement in statements)
        {
            try
            {
                await RunAsync(
                    $"EXPLAIN {statement.Sql}",
                    ReadinessParameters.From(statement.Parameters)
                );
            }
            catch (PostgresException failure) when (IsRefusal(failure))
            {
                refusals.Add(new Refusal(statement, failure));
            }
            catch (PostgresException)
            {
                // An operational failure (a lock or statement timeout, a deadlock): leave the
                // caller's transaction as a refusal does, then let the original propagate.
                try
                {
                    await RunAsync($"ROLLBACK TO SAVEPOINT {Savepoint}", []);
                }
                catch (Exception)
                {
                    // The original is the failure to report; the throw below rethrows it.
                }

                throw;
            }

            await RunAsync($"ROLLBACK TO SAVEPOINT {Savepoint}", []);
        }

        await RunAsync($"RELEASE SAVEPOINT {Savepoint}", []);
        return refusals;

        async Task RunAsync(string sql, NpgsqlParameter[] parameters)
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = sql;
            foreach (var parameter in parameters)
            {
                command.Parameters.Add(parameter);
            }

            await command.ExecuteNonQueryAsync(ct);
        }
    }

    // The PostgreSQL errors inside an exception a batch threw (one, or several in an aggregate),
    // or null when it holds anything else.
    private static List<PostgresException>? PostgresFailures(Exception exception)
    {
        var failures = new List<PostgresException>();
        return Collect(exception) && failures.Count > 0 ? failures : null;

        bool Collect(Exception current)
        {
            switch (current)
            {
                case PostgresException failure:
                    failures.Add(failure);
                    return true;
                case AggregateException aggregate:
                    return aggregate.InnerExceptions.All(Collect);
                default:
                    return current.InnerException is { } inner && Collect(inner);
            }
        }
    }

    // Only a statement PostgreSQL can't plan or start is a configuration refusal: a syntax error or
    // access rule violation (class 42, such as a missing table or column or a lacking privilege),
    // an invalid schema name (3F), an unsupported feature (0A), a data exception from a constant
    // in Cohort's text (22), a limit the statement itself exceeds (class 54, such as too many
    // columns), or a table that can't take the write (55000, such as a retained table in a
    // publication of updates and deletes with no replica identity, which plain EXPLAIN reaches
    // because it starts the executor). A timeout, deadlock, serialization failure, resource
    // exhaustion, or operator intervention says nothing about the host's schema, so it
    // propagates, and so do the rest of class 55 (55P03 lock_not_available, 55006 object_in_use):
    // the code is tested whole, not its class.
    private static bool IsRefusal(PostgresException failure) =>
        failure.SqlState is { } state
        && (
            state is "55000"
            || (state.Length >= 2 && state[..2] is "42" or "3F" or "0A" or "22" or "54")
        );

    private sealed record Refusal(ReadinessStatement Statement, PostgresException Failure);

    private sealed class Cause(string sqlState, string message)
    {
        internal string SqlState { get; } = sqlState;

        internal string Message { get; } = message;

        internal HashSet<string> Operations { get; } = new(StringComparer.Ordinal);

        internal HashSet<string> Excerpts { get; } = new(StringComparer.Ordinal);

        internal HashSet<RelationalObjectName> Relations { get; } = [];
    }

    private static HashSet<RelationalObjectName> CohortTables(CohortStoreTables tables) =>
        [
            tables.RetentionHolds,
            tables.SweepRun,
            tables.SweepRunEntitySummary,
            tables.SweepRunRowDetail,
            tables.SweepRowHandlerStatus,
        ];

    // One error per entity, holding every distinct (SQLSTATE, message) its statements drew. A
    // refusal from a statement that spans entities is reported, as one error, only when no
    // entity's own statement was refused. A refusal from a statement that names no entity and
    // reads no host table (Cohort's own tables and the functions it calls) is reported as one
    // more error, whether or not an entity's was.
    private static List<string> Describe(
        List<Refusal> refusals,
        string role,
        string scope,
        HashSet<RelationalObjectName> cohortTables
    )
    {
        var byEntity = new Dictionary<Type, Dictionary<(string, string), Cause>>();
        var across = new Dictionary<(string, string), Cause>();
        var cohort = new Dictionary<(string, string), Cause>();
        foreach (var refusal in refusals)
        {
            var statement = refusal.Statement;
            var entities = statement.Entities.OfType<Type>().ToList();
            var targets = new List<Dictionary<(string, string), Cause>>();
            if (entities.Count == 0)
            {
                targets.Add(
                    statement.Relations.All(cohortTables.Contains) ? cohort : across
                );
            }

            foreach (var entity in entities)
            {
                if (!byEntity.TryGetValue(entity, out var forEntity))
                {
                    byEntity[entity] = forEntity = [];
                }

                targets.Add(forEntity);
            }

            foreach (var target in targets)
            {
                Record(target, refusal);
            }
        }

        const string Remedy =
            "Apply the host application's pending EF Core migrations, map the entity to what the database has, or grant the database role the privileges Cohort needs.";
        var errors = new List<string>();
        if (byEntity.Count > 0)
        {
            errors.AddRange(
                byEntity.Select(pair =>
                    $"Retained entity {ReadinessStatement.EntityName(pair.Key)}: PostgreSQL refused statements Cohort sends. {string.Join(' ', pair.Value.Values.Select(cause => DescribeCause(cause, role)))} {Remedy}"
                )
            );
        }
        else if (across.Count > 0)
        {
            errors.Add(
                $"Across retained entities: PostgreSQL refused statements Cohort sends. {string.Join(' ', across.Values.Select(cause => DescribeCause(cause, role)))} {Remedy}"
            );
        }

        if (cohort.Count > 0)
        {
            errors.Add(
                $"Cohort's own tables and functions: PostgreSQL refused statements Cohort sends in {scope}. {string.Join(' ', cohort.Values.Select(cause => DescribeCause(cause, role)))} Grant the database role the privileges Cohort needs on its tables, sequences, and functions."
            );
        }

        return errors;

        static void Record(Dictionary<(string, string), Cause> target, Refusal refusal)
        {
            var statement = refusal.Statement;
            var failure = refusal.Failure;
            if (!target.TryGetValue((failure.SqlState, failure.MessageText), out var cause))
            {
                target[(failure.SqlState, failure.MessageText)] = cause = new Cause(
                    failure.SqlState,
                    failure.MessageText
                );
            }

            cause.Operations.UnionWith(statement.Operations);
            cause.Relations.UnionWith(statement.Relations);
            if (
                ReadinessExcerpt.At(statement.Sql, statement.Parameters.Keys, failure.Position) is
                { Length: > 0 } excerpt
            )
            {
                cause.Excerpts.Add(excerpt);
            }
        }
    }

    private static string DescribeCause(Cause cause, string role)
    {
        var parts = new List<string> { $"while {string.Join(", ", cause.Operations)}" };
        if (cause.Excerpts.Count > 0)
        {
            parts.Add(
                $"near {string.Join(" and ", cause.Excerpts.Take(3).Select(excerpt => $"`{excerpt}`"))}"
            );
        }

        if (cause.Relations.Count > 0)
        {
            parts.Add(
                $"reading {string.Join(", ", cause.Relations.Select(PostgreSqlIdentifier.Format))}"
            );
        }

        var privilege =
            cause.SqlState == PostgresErrorCodes.InsufficientPrivilege
                ? $" The database role '{role}' lacks this privilege."
                : "";
        return $"{cause.SqlState}: {cause.Message} ({string.Join("; ", parts)}).{privilege}";
    }

    /// <summary>The run's own path predicate, from the registrations the run reads.</summary>
    private SweepPath PathOf(RetentionEntry entry, Strategy strategy)
    {
        var hasHandlers = RetentionHandlerSupport.HasHandlers(
            handlerRegistrations,
            entry.EntityType
        );
        var hasStep = dependentsRegistrations.HasStep(entry.EntityType);
        var perRow = strategy == Strategy.Anonymise && assignments.RequiresPerRowExecution(entry);
        return new SweepPath(
            SweepStatements.TakesRowByRowPath(strategy, hasHandlers, hasStep, perRow),
            hasStep
        );
    }

    // The values a strategy's mutation head binds, as its CreateMutation does but without
    // running a factory or a converter: the soft-delete time from the strategy's own function, the
    // anonymised-at clock, and each anonymise value as a NULL typed from its property's provider
    // type, or untyped for the Null method, as the run binds it.
    private SqlParams HeadParameters(SweepScope scope, Strategy strategy)
    {
        var entry = scope.Entry;
        var parameters = new SqlParams();
        switch (strategy)
        {
            case Strategy.SoftDelete
                when entry.SoftDelete is { DeletedAtColumn: not null } softDelete:
                parameters["deletedAt"] = SweepStatements.DeletedAtValue(
                    entry,
                    softDelete,
                    scope.Now
                );
                break;
            case Strategy.Anonymise:
                for (var index = 0; index < entry.AnonymiseFields.Count; index++)
                {
                    var field = entry.AnonymiseFields[index];
                    parameters[$"value{index}"] = field
                        is AnonymiseLiteralField { Method: AnonymiseMethod.Null }
                        ? null
                        : new ReadinessTypedNull(assignments.ProviderType(entry, field));
                }

                parameters["anonymisedAt"] = scope.Now;
                break;
        }

        return parameters;
    }

    private SweepRunValues RunValues(RetentionEntry entry)
    {
        var recordId = db
            .Model.FindEntityType(entry.EntityType)!
            .FindProperty(entry.RecordId.RecordIdMember)!;
        return new SweepRunValues(
            new ReadinessTypedNull(
                recordId.GetTypeMapping().Converter?.ProviderClrType ?? recordId.ClrType
            ),
            new ReadinessTypedNull(typeof(string)),
            new ReadinessTypedNull(typeof(string[]))
        );
    }

    // The subject as an erasure binds it, with each member's value as a NULL typed from the
    // member's provider type.
    private static ErasureSubjectPredicate SubjectPredicate(ErasureSubjectMetadata metadata) =>
        new([
            .. metadata.Members.Select(member => new ErasureSubjectMatch(
                member.Name,
                member.Column,
                member.StoreType,
                new ReadinessTypedNull(
                    member.Property.GetTypeMapping().Converter?.ProviderClrType
                        ?? member.SubjectType
                )
            )),
        ]);

    private sealed class StatementSet
    {
        private readonly Dictionary<
            (string Sql, string ParameterTypes),
            ReadinessStatement
        > byText = [];

        internal List<ReadinessStatement> Statements { get; } = [];

        /// <summary>
        /// Adds statements from <see cref="CohortStatements"/>, each under the operation label
        /// its builder belongs to.
        /// </summary>
        internal void AddCohort(IEnumerable<SweepStatement> statements)
        {
            foreach (var statement in statements)
            {
                Add([statement], OperationOf(statement.Name));
            }
        }

        // The operation a refusal of a Cohort-table or function statement names.
        private static string OperationOf(string name) =>
            name switch
            {
                "run-started" or "entity-summary" or "entity-progress" or "row-detail" or "run-settled" =>
                    "recording runs",
                "row-detail-with-payload" or "handler-status" => "queueing row handlers",
                "create-hold" or "remove-hold" or "active-holds" =>
                    "creating, removing, or listing holds",
                "active-tenantless-holds" => "reading tenantless holds",
                "stored-entity-identities" => "checking stored entity identities",
                "run-committed" => "confirming a run committed",
                "entity-lock"
                or "advisory-lock"
                or "advisory-try-lock"
                or "advisory-unlock"
                or "advisory-lock-all"
                or "advisory-unlock-all"
                or "advisory-locks-held" => "taking or releasing Cohort's locks",
                "database-clock" => "reading the database clock",
                "transaction-isolation" => "checking the transaction's isolation",
                "payload-scrub" => "scrubbing captured payloads",
                "remaining-work"
                or "requeue-dead-lettered"
                or "delivered-payload-clear"
                or "expired-claims"
                or "reclaim-lock"
                or "claim-handlers"
                or "requeue-interrupted"
                or "dead-letter-dependents" => "dispatching row handlers",
                _ when name.StartsWith("handler-status-", StringComparison.Ordinal) =>
                    "dispatching row handlers",
                "abandoned-runs" or "abandoned-run-failed" => "recovering abandoned runs",
                "occurrence-ledger" => "reading the worker's ledger",
                "eligible-runs" or "prune-row-details" or "prune-entity-summaries" or "prune-runs" =>
                    "pruning settled runs",
                "prune-holds" => "pruning inactive holds",
                "tenant-coverage" => "scanning for tenants outside the worker's tenant source",
                _ => throw new InvalidOperationException(
                    $"Cohort statement '{name}' has no readiness operation label."
                ),
            };

        internal void Add(IEnumerable<SweepStatement> statements, string operation)
        {
            foreach (var statement in statements)
            {
                var types = DescribeParameters(statement.Parameters);
                if (!byText.TryGetValue((statement.Sql, types), out var known))
                {
                    byText[(statement.Sql, types)] = known = new ReadinessStatement(
                        statement.Name,
                        statement.Sql,
                        statement.Parameters,
                        types
                    );
                    Statements.Add(known);
                }

                if (!known.Entities.Contains(statement.Entity))
                {
                    known.Entities.Add(statement.Entity);
                }

                if (!known.Operations.Contains(operation))
                {
                    known.Operations.Add(operation);
                }

                foreach (
                    var relation in statement.Relations.Where(relation =>
                        !known.Relations.Contains(relation)
                    )
                )
                {
                    known.Relations.Add(relation);
                }
            }
        }

        private static string DescribeParameters(SqlParams parameters) =>
            string.Join(
                ',',
                parameters
                    .OrderBy(pair => pair.Key, StringComparer.Ordinal)
                    .Select(pair => $"{pair.Key}:{DescribeType(pair.Value)}")
            );

        private static string DescribeType(object? value)
        {
            var type = value switch
            {
                null or DBNull => null,
                ReadinessTypedNull typed => typed.Type,
                _ => value.GetType(),
            };
            return type is null ? "null" : type.FullName ?? type.Name;
        }
    }
}
