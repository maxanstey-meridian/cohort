using System.Data.Common;
using System.Text;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;
using Cohort.Infrastructure.Sweep;
using Microsoft.EntityFrameworkCore;
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
/// Readiness for the host-table statements Cohort sends. It builds every statement a sweep,
/// preview, erasure, hold call, and tenant-coverage scan can issue for the host's model and the
/// strategies its categories declare, through the builders the run itself executes, and asks
/// PostgreSQL to analyse and plan each one (<c>EXPLAIN</c>, which runs nothing). What PostgreSQL
/// refuses is what readiness refuses: a missing table or column, a comparison or assignment the
/// types can't make, a column the database generates, an unknown cast type, a privilege the
/// session's role lacks.
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
    /// Every statement the run can issue for the model, de-duplicated, in a fixed order. Reads the
    /// blocking foreign keys of the Purge scopes through the database's connection, as a run does.
    /// </summary>
    internal async Task<IReadOnlyList<ReadinessStatement>> EnumerateAsync(
        IReadOnlyDictionary<string, RetentionCategoryCapabilities> capabilities,
        CancellationToken ct
    )
    {
        var validation = validationState.For(db.Model);
        var entries = registry.Scan().Values.ToList();
        var holdsTable = CohortStoreTables.FromModel(db.Model).RetentionHolds;
        var now = DateTimeOffset.UtcNow;
        var set = new StatementSet();

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
                if (strategy != Strategy.Exempt)
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
        for (var index = 0; index < scopes.Count; index++)
        {
            var (_, strategy, operation) = scopes[index];
            var scope = attached[index];
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

        foreach (var entry in entries)
        {
            set.Add(
                RetentionHoldSql.ForEntity(
                    holdsTable,
                    entry,
                    entries,
                    new ReadinessTypedNull(typeof(string)),
                    entry.Tenant is null ? null : ReadinessTenantId
                ),
                "creating, listing, or checking holds, or checking a deletion"
            );
            if (
                RetentionTenantCoverage.Statement(entry, capabilities, Array.Empty<Guid>()) is
                { } coverage
            )
            {
                set.Add([coverage], "scanning for tenants outside the worker's tenant source");
            }
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
                    RetentionHoldSql.ForSubjectKind(holdsTable, starts, ReadinessTenantId),
                    $"listing the holds that cover a subject of kind '{kind}'"
                );
            }
        }

        set.Add(
            RetentionHoldSql.ForModel(
                holdsTable,
                entries,
                Array.Empty<Guid>(),
                new ReadinessTypedNull(typeof(Guid[])),
                new ReadinessTypedNull(typeof(string[]))
            ),
            "guarding held rows"
        );
        return set.Statements;
    }

    /// <summary>
    /// Asks PostgreSQL to explain every statement, in the caller's transaction or in one opened
    /// here and rolled back, and throws one refusal per entity for what it refused. Over an
    /// <see cref="NpgsqlConnection"/> it is one round trip, with an error barrier and a rollback to
    /// a savepoint after each statement so every refusal is reported and the transaction stays
    /// usable; over any other connection (a wrapper can't batch) the same commands run one at a
    /// time.
    /// </summary>
    internal async Task ExplainAsync(
        IReadOnlyList<ReadinessStatement> statements,
        string role,
        CancellationToken ct
    )
    {
        if (statements.Count == 0)
        {
            return;
        }

        var database = db.Database;
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

            if (refusals.Count > 0)
            {
                throw new RetentionConfigurationException(Describe(refusals, role));
            }
        }
        finally
        {
            await database.CloseConnectionAsync();
        }
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
            if (
                refusals is null
                || refusals.Any(refusal => refusal.Index < 0 || !IsRefusal(refusal.Failure))
            )
            {
                // Not one of the explained statements (the savepoint, a lost connection), or an
                // operational failure (a lock or statement timeout, a deadlock): not a refusal.
                throw;
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

    // Only a statement PostgreSQL can't plan is a configuration refusal: a syntax error or access
    // rule violation (class 42, such as a missing table or column or a lacking privilege), an
    // invalid schema name (3F), an unsupported feature (0A), or a data exception from a constant
    // in Cohort's text (22). A timeout, deadlock, serialization failure, resource exhaustion, or
    // operator intervention says nothing about the host's schema, so it propagates.
    private static bool IsRefusal(PostgresException failure) =>
        failure.SqlState is { Length: >= 2 } state && state[..2] is "42" or "3F" or "0A" or "22";

    private sealed record Refusal(ReadinessStatement Statement, PostgresException Failure);

    private sealed class Cause(string sqlState, string message)
    {
        internal string SqlState { get; } = sqlState;

        internal string Message { get; } = message;

        internal HashSet<string> Operations { get; } = new(StringComparer.Ordinal);

        internal HashSet<string> Excerpts { get; } = new(StringComparer.Ordinal);

        internal HashSet<RelationalObjectName> Relations { get; } = [];
    }

    // One error per entity, holding every distinct (SQLSTATE, message) its statements drew. A
    // refusal from a statement that spans entities is reported, as one error, only when no
    // entity's own statement was refused.
    private static List<string> Describe(List<Refusal> refusals, string role)
    {
        var byEntity = new Dictionary<Type, Dictionary<(string, string), Cause>>();
        var across = new Dictionary<(string, string), Cause>();
        foreach (var refusal in refusals)
        {
            var statement = refusal.Statement;
            var failure = refusal.Failure;
            var entities = statement.Entities.OfType<Type>().ToList();
            var targets = new List<Dictionary<(string, string), Cause>>();
            if (entities.Count == 0)
            {
                targets.Add(across);
            }

            foreach (var entity in entities)
            {
                if (!byEntity.TryGetValue(entity, out var forEntity))
                {
                    byEntity[entity] = forEntity = [];
                }

                targets.Add(forEntity);
            }

            foreach (var perEntity in targets)
            {
                if (!perEntity.TryGetValue((failure.SqlState, failure.MessageText), out var cause))
                {
                    perEntity[(failure.SqlState, failure.MessageText)] = cause = new Cause(
                        failure.SqlState,
                        failure.MessageText
                    );
                }

                cause.Operations.UnionWith(statement.Operations);
                cause.Relations.UnionWith(statement.Relations);
                if (
                    ReadinessExcerpt.At(
                        statement.Sql,
                        statement.Parameters.Keys,
                        failure.Position
                    ) is
                    { Length: > 0 } excerpt
                )
                {
                    cause.Excerpts.Add(excerpt);
                }
            }
        }

        const string Remedy =
            "Apply the host application's pending EF Core migrations, map the entity to what the database has, or grant the database role the privileges Cohort needs.";
        if (byEntity.Count > 0)
        {
            return
            [
                .. byEntity.Select(pair =>
                    $"Retained entity {ReadinessStatement.EntityName(pair.Key)}: PostgreSQL refused statements Cohort sends. {string.Join(' ', pair.Value.Values.Select(cause => DescribeCause(cause, role)))} {Remedy}"
                ),
            ];
        }

        return
        [
            $"Across retained entities: PostgreSQL refused statements Cohort sends. {string.Join(' ', across.Values.Select(cause => DescribeCause(cause, role)))} {Remedy}",
        ];
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
