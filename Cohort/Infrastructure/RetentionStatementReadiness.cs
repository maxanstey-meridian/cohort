using System.Data.Common;
using System.Runtime.ExceptionServices;

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

    internal static string EntityName(Type? entity) =>
        entity is null ? "" : entity.FullName ?? entity.Name;
}

/// <summary>
/// Readiness for the statements Cohort sends against the host's retained tables. It builds every
/// such statement any operation can issue for the host's model and the strategies its categories
/// declare, through the builders the run itself executes, and asks PostgreSQL to analyse and plan
/// each one (<c>EXPLAIN</c>, which runs nothing). What PostgreSQL refuses is what readiness
/// refuses: a missing table or column, a comparison or assignment the types can't make, a column
/// the database generates, an unknown cast type. A privilege the session's role lacks (42501) is
/// not refused here: PostgreSQL refuses it at the statement, under the role that sends it.
/// Statements against Cohort's own tables and the functions it calls are not explained.
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
    /// Every statement against the host's retained tables that any operation can issue for the
    /// model, de-duplicated, each with every operation that sends it: the sweep and erasure
    /// scopes, the held-row guard, the preview counts, the holds repository's reads and locks, a
    /// deletion's hold check, and the worker's tenant-coverage scan (S12). Reads the blocking
    /// foreign keys of the Purge scopes once, through the database's connection, as a run does.
    /// </summary>
    internal async Task<IReadOnlyList<ReadinessStatement>> EnumerateAsync(
        IReadOnlyDictionary<string, RetentionCategoryCapabilities> capabilities,
        CancellationToken ct
    )
    {
        var entries = registry.Scan().Values.ToList();
        var tables = CohortStoreTables.FromModel(db.Model);
        var scopes = await ScopesAsync(entries, capabilities, ct);
        var set = new StatementSet();
        AddScopes(set, scopes);
        AddGuard(set, tables, entries);
        AddPreviewCounts(set, scopes.Where(scope => !scope.Erasure));
        AddHolds(set, tables, entries);
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
            if (RetentionTenantCoverage.Statement(entry, capabilities, Array.Empty<Guid>()) is { } scan)
            {
                set.Add([scan], "the scheduled worker");
            }
        }

        return set.Statements;
    }

    // One sweep or erasure scope readiness explains, and the operation a refusal names for it.
    private sealed record ScopeToExplain(
        SweepScope Scope,
        Strategy Strategy,
        string Operation,
        bool Erasure
    );

    // The sweep scopes and the erasure scopes of every entity with a declared category, each
    // strategy it declares. The blocking keys of the Purge scopes are read in one catalog read
    // over all of them, as a run reads them.
    private async Task<List<ScopeToExplain>> ScopesAsync(
        IReadOnlyList<RetentionEntry> entries,
        IReadOnlyDictionary<string, RetentionCategoryCapabilities> capabilities,
        CancellationToken ct
    )
    {
        var validation = validationState.For(db.Model);
        var now = DateTimeOffset.UtcNow;
        var scopes = new List<ScopeToExplain>();
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
                        new ScopeToExplain(
                            SweepScope.ForSweep(
                                entry,
                                new RetentionRule(OneYear, strategy),
                                context,
                                settings.TimeZone
                            ),
                            strategy,
                            $"sweeping with {strategy}",
                            Erasure: false
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
                            new ScopeToExplain(
                                SweepScope.ForErasure(
                                    entry,
                                    rule,
                                    SubjectPredicate(kind),
                                    tenant,
                                    now,
                                    settings.TimeZone
                                ),
                                strategy,
                                $"erasing subjects of kind '{kind.Kind}'",
                                Erasure: true
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
        return [.. scopes.Select((scope, index) => scope with { Scope = attached[index] })];
    }

    // Every host-table statement a sweep or an erasure issues (`ForScope`).
    private void AddScopes(StatementSet set, IEnumerable<ScopeToExplain> scopes)
    {
        foreach (var (scope, strategy, operation, _) in scopes)
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
    private static void AddPreviewCounts(StatementSet set, IEnumerable<ScopeToExplain> scopes)
    {
        foreach (var (scope, strategy, operation, _) in scopes)
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
    // forms that tell which entity a refusal comes from). A sweep or an erasure runs it only for
    // a dependents step; a deletion always does, so it is always explained.
    private static void AddGuard(
        StatementSet set,
        CohortStoreTables tables,
        IReadOnlyList<RetentionEntry> entries
    )
    {
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
    /// what it refused, or one for the statements that span entities when no entity's own
    /// statement was refused (<see cref="Describe"/>). Over an <see cref="NpgsqlConnection"/> it is one round trip, with
    /// an error barrier and a rollback to a savepoint after each statement so every refusal is
    /// reported and the transaction stays usable; over any other connection (a wrapper can't
    /// batch) the same commands run one at a time. A refusal for a privilege the session's role
    /// lacks (42501) is never reported: PostgreSQL refuses it at the statement, under the role
    /// that sends it, and the permission check runs after the faults readiness is after (a
    /// missing column, a wrong type) but before the executor-start ones, so a role without the
    /// privilege leaves those unchecked.
    /// </summary>
    internal async Task<IReadOnlyList<string>> ExplainAsync(
        DatabaseFacade database,
        IReadOnlyList<ReadinessStatement> statements,
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

            refusals.RemoveAll(refusal =>
                refusal.Failure.SqlState == PostgresErrorCodes.InsufficientPrivilege
            );
            return refusals.Count == 0 ? [] : Describe(refusals);
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

        internal HashSet<RelationalObjectName> Relations { get; } = [];
    }

    // One error per entity, holding every distinct (SQLSTATE, message) its statements drew. A
    // refusal from a statement that spans entities is reported, as one error, only when no
    // entity's own statement was refused.
    private static List<string> Describe(List<Refusal> refusals)
    {
        var byEntity = new Dictionary<Type, Dictionary<(string, string), Cause>>();
        var across = new Dictionary<(string, string), Cause>();
        foreach (var refusal in refusals)
        {
            var statement = refusal.Statement;
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

            foreach (var target in targets)
            {
                Record(target, refusal);
            }
        }

        const string Remedy =
            "Apply the host application's pending EF Core migrations, or map the entity to what the database has.";
        var errors = new List<string>();
        if (byEntity.Count > 0)
        {
            errors.AddRange(
                byEntity.Select(pair =>
                    $"Retained entity {ReadinessStatement.EntityName(pair.Key)}: PostgreSQL refused statements Cohort sends. {string.Join(' ', pair.Value.Values.Select(DescribeCause))} {Remedy}"
                )
            );
        }
        else if (across.Count > 0)
        {
            errors.Add(
                $"Across retained entities: PostgreSQL refused statements Cohort sends. {string.Join(' ', across.Values.Select(DescribeCause))} {Remedy}"
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
        }
    }

    private static string DescribeCause(Cause cause)
    {
        var parts = new List<string> { $"while {string.Join(", ", cause.Operations)}" };
        if (cause.Relations.Count > 0)
        {
            parts.Add(
                $"reading {string.Join(", ", cause.Relations.Select(PostgreSqlIdentifier.Format))}"
            );
        }

        return $"{cause.SqlState}: {cause.Message} ({string.Join("; ", parts)}).";
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
