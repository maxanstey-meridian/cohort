using System.Collections.Concurrent;
using System.Data.Common;
using System.Runtime.CompilerServices;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

using Npgsql;

namespace Cohort.Infrastructure;

internal sealed class RetentionRuntimeReadinessValidator(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionStartupValidator startupValidator,
    CohortSchemaValidator schemaValidator,
    RetentionRegistry registry,
    RetentionRuntimeReadinessState state,
    IServiceScopeFactory scopeFactory,
    AnonymiseAssignmentResolver assignments,
    RetentionValidationState validationState,
    IEnumerable<IRetentionHandlerRegistration> handlerRegistrations,
    IEnumerable<IRetentionDependentsRegistration> dependentsRegistrations,
    IRetentionExecutionSettings settings
)
{
    // pg_input_is_valid, which the anonymise-target check relies on, arrived in PostgreSQL 16.
    private static readonly Version MinimumServerVersion = new(16, 0);

    private readonly RetentionStatementReadiness statementReadiness = new(
        db,
        registry,
        validationState,
        assignments,
        handlerRegistrations,
        dependentsRegistrations,
        settings
    );

    internal IReadOnlyDictionary<string, RetentionCategoryCapabilities>
        ValidatedCapabilities => startupValidator.ValidatedCapabilities;

    /// <summary>
    /// Readiness for one operation: the model pass (once per model and connection string), then
    /// the group pass (once per entry, group, role, and search path), which explains the group's
    /// statements under the role of the scope this call runs in and refuses what PostgreSQL
    /// refuses. Each caller passes the group of statements it is about to send.
    /// </summary>
    public async Task ValidateAsync(RetentionStatementGroup group, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(group);
        var (readiness, session) = await ValidateModelAsync(ct);
        await ValidateGroupAsync(readiness, group, session, ct);
    }

    /// <summary>
    /// Readiness at host startup, in <see cref="RetentionSystemWork.Readiness"/>: the model pass,
    /// then an early check of the port groups' statements under this scope's role. Their roles
    /// belong to scopes startup can't take on, so a privilege the role lacks (42501) is not
    /// reported here; a missing column, a wrong type, or a generated column is. The sequence rule
    /// is about privilege and is skipped too. Nothing is recorded as a pass, so each port's role
    /// still gets its strict group pass when it first reaches readiness. The statements it
    /// enumerates are kept under this scope's role only, so no other role reuses them.
    /// </summary>
    public async Task ValidateAtStartupAsync(CancellationToken ct = default)
    {
        var (readiness, session) = await ValidateModelAsync(ct);
        var facade = db.Database;
        session ??= await ReadServerSessionAsync(
            facade,
            CohortStoreTables.FromModel(db.Model).RetentionHolds,
            ct
        );
        await readiness.Gate.WaitAsync(ct);
        try
        {
            var errors = new List<string>();
            foreach (var group in RetentionStatementReadiness.PortGroups)
            {
                var enumerated = await EnumerateGroupAsync(readiness, group, session.Role, ct);
                errors.AddRange(
                    await statementReadiness.ExplainAsync(
                        facade,
                        enumerated.Statements,
                        session.Role,
                        RetentionStatementReadiness.ScopeOf(group),
                        tolerateInsufficientPrivilege: true,
                        ct
                    )
                );
            }

            if (errors.Count != 0)
            {
                throw new RetentionConfigurationException([.. errors.Distinct(StringComparer.Ordinal)]);
            }
        }
        finally
        {
            readiness.Gate.Release();
        }
    }

    // The model pass, once per model and connection string. Returns the entry and, when the pass
    // ran, the session it read (null when an earlier call's pass stands).
    private async Task<(RetentionRuntimeReadinessEntry Entry, ServerSession? Session)> ValidateModelAsync(
        CancellationToken ct
    )
    {
        ValidateProvider();
        var facade = db.Database;
        var connection = facade.GetDbConnection();
        // Every connection's string is parsed once per call, with no round trip, a wrapped
        // DbConnection's too, since the provider is already confirmed as Npgsql. Only a string
        // Npgsql rejects (a wrapper's own keywords) is left unparsed: it can't be checked for
        // Multiplexing and is the host's to configure.
        var parsed = connection is NpgsqlConnection
            ? new NpgsqlConnectionStringBuilder(connection.ConnectionString)
            : TryParseConnectionString(connection.ConnectionString);
        ValidateSessionLocks(parsed);
        // Two levels of memory. The model instance the context runs has its own entry (its pass
        // and its gate), keyed by the connection string without its password. The password stays
        // out because it is a secret that a rotating token would turn into one entry per token,
        // and because Npgsql drops it once the connection opens. The key still carries everything
        // else a pooler can route by under one host, port, and database name (user, options); two
        // spellings of one database each validate once. A string Npgsql rejects keys by a generic
        // parse without its password keys, or by the raw string if even that parse rejects it.
        // Each database check also records its pass by value, under everything the check reads,
        // so a rebuilt copy of a model that reads the same things skips it. Those shared passes
        // are keyed by the database as the server reports it, read once per entry miss.
        string entryKey;
        if (parsed is null)
        {
            entryKey = KeyWithoutPassword(connection.ConnectionString);
        }
        else
        {
            parsed.Password = null;
            parsed.PersistSecurityInfo = false;
            entryKey = parsed.ConnectionString;
        }

        var readiness = state.For(db.Model, entryKey);
        if (readiness.ModelValidated)
        {
            return (readiness, null);
        }

        await readiness.Gate.WaitAsync(ct);
        try
        {
            if (readiness.ModelValidated)
            {
                return (readiness, null);
            }

            var session = await RunModelPassAsync(ct);
            if (facade.CurrentTransaction is null)
            {
                readiness.ModelValidated = true;
            }

            return (readiness, session);
        }
        finally
        {
            readiness.Gate.Release();
        }
    }

    private async Task<ServerSession> RunModelPassAsync(CancellationToken ct)
    {
        var facade = db.Database;
        await startupValidator.ValidateAsync(ct);
        var storeTables = CohortStoreTables.FromModel(db.Model);
        var holds = PostgreSqlIdentifier.Format(storeTables.RetentionHolds);
        var session = await ReadServerSessionAsync(facade, storeTables.RetentionHolds, ct);
        var callerDatabase = session.Database;

        // Order matters. Startup validation runs first, so a model with a retained entity and no
        // or partial Cohort table mapping gets startup validation's aggregated refusal before
        // any key needs the store tables.
        // Each check then throws before the next runs: version, schema, relations, hold links,
        // columns, anonymise targets, stored identities (which explains the Readiness group
        // under its own scope's role before it reads). The relation and link rules refuse a
        // fault with a message more specific than PostgreSQL's, and deterministically where a
        // plan-dependent EXPLAIN isn't.
        // The statements are not part of this pass. Each port's group pass explains them, under
        // that port's role, after this pass; it owns existence, analysable types, writability,
        // cast types, assignability, and privileges. So the rules here skip what a group pass
        // refuses, a missing table or column, and the port's group pass refuses it before that
        // port runs; a group that sends no host-table statement (Scrub, Dispatch, the Worker's
        // ledger, and Pruning) never touches the table that is missing. When a check is skipped
        // as another copy's recorded pass, the order holds on the same "once, then trusted"
        // footing as the cache itself.
        await RunSharedAsync(
            Key(callerDatabase, RetentionReadinessCheck.ServerVersion, []),
            () => ValidateServerVersionAsync(facade, ct)
        );
        await RunSharedAsync(
            Key(callerDatabase, RetentionReadinessCheck.Schema, [storeTables]),
            () => schemaValidator.ValidateAsync(storeTables, facade, ct)
        );
        var retainedRelations = RetainedRelationRows();
        await RunSharedAsync(
            Key(callerDatabase, RetentionReadinessCheck.RetainedRelations, retainedRelations),
            () => ValidateRetainedRelationsAsync(facade, retainedRelations, ct)
        );
        var links = HoldLinkRows();
        await RunSharedAsync(
            Key(callerDatabase, RetentionReadinessCheck.HoldRelationLinkColumns, links),
            () => ValidateHoldRelationLinkColumnsAsync(facade, links, ct)
        );
        var columns = RetainedColumnRows();
        await RunSharedAsync(
            Key(callerDatabase, RetentionReadinessCheck.RetainedColumns, columns),
            () => ValidateRetainedColumnsAsync(facade, columns, ct)
        );
        // Not shared: what this reads is session state, which no key captures.
        await ValidateAnonymiseTargetsAsync(ct);
        var registered = registry.Scan().Values.Select(entry => entry.RetentionEntityId).Order().ToArray();
        object[] storedIdentityInputs = [holds, .. registered];
        // The hold check runs on its own connection, outside the caller's transaction, so its
        // success stands whatever transaction the caller is in. The checks above read through
        // the caller's connection, and one that passed inside a transaction may have seen
        // DDL that rolls back, so those are recorded only without one.
        await RunSharedAsync(
            Key(callerDatabase, RetentionReadinessCheck.StoredEntityIdentities, storedIdentityInputs),
            () => ValidateStoredEntityIdentitiesAsync(storeTables.RetentionHolds, registered, callerDatabase, ct),
            recordInsideTransaction: true
        );
        return session;
    }

    private static RetentionReadinessCheckKey Key(
        ServerDatabaseIdentity database,
        RetentionReadinessCheck check,
        IEnumerable<object> inputs
    ) => new(database, check, [.. inputs]);

    // Runs a database check unless another copy of the model already passed it against this
    // database on the same inputs; records the pass. A failure is never recorded. A recorded pass
    // is skipped inside a caller's transaction too.
    private async Task RunSharedAsync(
        RetentionReadinessCheckKey key,
        Func<Task> run,
        bool recordInsideTransaction = false
    )
    {
        if (state.HasPassed(key))
        {
            return;
        }

        await run();
        if (recordInsideTransaction || db.Database.CurrentTransaction is null)
        {
            state.RecordPassed(key);
        }
    }

    /// <summary>
    /// The group pass: reads the session (its role and search path), then explains the group's
    /// statements under that role, once per entry, group, role, and search path. A recorded pass
    /// costs the session read. Privileges depend on the role, and an unqualified type name (a
    /// domain, <c>citext</c>) resolves through the path, so a pass under one says nothing about
    /// another. In a caller's transaction nothing is recorded or kept (the transaction's reads
    /// may have seen DDL that rolls back), so the pass runs again until a call outside one
    /// records it.
    /// </summary>
    private async Task ValidateGroupAsync(
        RetentionRuntimeReadinessEntry readiness,
        RetentionStatementGroup group,
        ServerSession? known,
        CancellationToken ct
    )
    {
        var facade = db.Database;
        var session =
            known
            ?? await ReadServerSessionAsync(facade, CohortStoreTables.FromModel(db.Model).RetentionHolds, ct);
        if (
            readiness.Enumerations.TryGetValue((group, session.Role), out var kept)
            && state.HasPassed(GroupKey(session, group, kept))
        )
        {
            return;
        }

        await readiness.Gate.WaitAsync(ct);
        try
        {
            var enumerated = await EnumerateGroupAsync(readiness, group, session.Role, ct);
            await RunSharedAsync(
                GroupKey(session, group, enumerated),
                async () =>
                {
                    var errors = new List<string>(
                        await statementReadiness.ExplainAsync(
                            facade,
                            enumerated.Statements,
                            session.Role,
                            RetentionStatementReadiness.ScopeOf(group),
                            tolerateInsufficientPrivilege: false,
                            ct
                        )
                    );
                    if (group.Kind is RetentionStatementGroupKind.Sweep or RetentionStatementGroupKind.Erasure)
                    {
                        errors.AddRange(await statementReadiness.CheckSequencesAsync(facade, session.Role, ct));
                    }

                    if (errors.Count != 0)
                    {
                        throw new RetentionConfigurationException(errors);
                    }
                }
            );
        }
        finally
        {
            readiness.Gate.Release();
        }
    }

    // The group's statements, enumerated once per entry and role and kept, but only when they
    // were enumerated outside a caller's transaction, the rule a recorded pass follows. The role
    // is the enumerating session's: the blocking-key read keeps only the child tables that role
    // can SELECT, so a statement's text, and with it the set a pass explains, depends on it.
    private async Task<GroupStatements> EnumerateGroupAsync(
        RetentionRuntimeReadinessEntry readiness,
        RetentionStatementGroup group,
        string role,
        CancellationToken ct
    )
    {
        if (readiness.Enumerations.TryGetValue((group, role), out var kept))
        {
            return kept;
        }

        // The statements are built (reading the Purge scopes' blocking keys) before the key,
        // because their text and parameter types are the key.
        var facade = db.Database;
        await facade.OpenConnectionAsync(ct);
        IReadOnlyList<ReadinessStatement> statements;
        try
        {
            statements = await statementReadiness.EnumerateAsync(group, ValidatedCapabilities, ct);
        }
        finally
        {
            await facade.CloseConnectionAsync();
        }

        var enumerated = new GroupStatements(statements, StatementInputs(statements));
        if (facade.CurrentTransaction is null)
        {
            readiness.Enumerations[(group, role)] = enumerated;
        }

        return enumerated;
    }

    // The group first (its kind and, for pruning, its scope), then the role and the search path,
    // then the statements by entity, name, text, and parameter types.
    private static RetentionReadinessCheckKey GroupKey(
        ServerSession session,
        object group,
        GroupStatements statements
    ) =>
        Key(
            session.Database,
            RetentionReadinessCheck.Statements,
            [group, session.Role, session.SearchPath, .. statements.Inputs]
        );

    private static object[] StatementInputs(IReadOnlyList<ReadinessStatement> statements) =>
        [
            .. statements
                .Select(statement => statement.Input)
                .OrderBy(input => input.Entity, StringComparer.Ordinal)
                .ThenBy(input => input.Name, StringComparer.Ordinal)
                .ThenBy(input => input.Sql, StringComparer.Ordinal)
                .ThenBy(input => input.ParameterTypes, StringComparer.Ordinal),
        ];

    /// <summary>
    /// Each retained relation must be an ordinary or partitioned table: Cohort updates, deletes,
    /// and row-locks its rows in place and addresses each by its physical position (ctid), which
    /// a view, a materialized view, or a foreign table can't offer or take. A relation that
    /// doesn't exist is skipped here: each port's group pass refuses it before that port runs.
    /// </summary>
    private static async Task ValidateRetainedRelationsAsync(
        DatabaseFacade database,
        RetainedRelationInput[] entries,
        CancellationToken ct
    )
    {
        if (entries.Length == 0)
        {
            return;
        }

        await database.OpenConnectionAsync(ct);
        try
        {
            await using var command = new SqlParams
            {
                ["schemas"] = entries.Select(entry => entry.Table.Schema).ToArray(),
                ["tables"] = entries.Select(entry => entry.Table.Name).ToArray(),
            }.CreateCommand(
                database.GetDbConnection(),
                database.CurrentTransaction?.GetDbTransaction(),
                """
                /* cohort catalog */
                SELECT requested.ordinal, relation.relkind
                FROM ROWS FROM (
                    pg_catalog.unnest(@schemas),
                    pg_catalog.unnest(@tables)
                ) WITH ORDINALITY AS requested(schema_name, table_name, ordinal)
                JOIN pg_catalog.pg_namespace namespace ON namespace.nspname = requested.schema_name
                JOIN pg_catalog.pg_class relation
                  ON relation.relnamespace = namespace.oid AND relation.relname = requested.table_name
                WHERE relation.relkind NOT IN ('r', 'p')
                """
            );

            var errors = new SortedSet<string>(StringComparer.Ordinal);
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var entry = entries[(int)reader.GetInt64(0) - 1];
                    errors.Add(
                        $"Retained entity {entry.Entity}: {PostgreSqlIdentifier.Format(entry.Table)} is {NotATable(reader.GetChar(1))}, so Cohort can't retain its rows. Map the entity to the table it lives in."
                    );
                }
            }

            if (errors.Count != 0)
            {
                throw new RetentionConfigurationException([.. errors]);
            }
        }
        finally
        {
            await database.CloseConnectionAsync();
        }
    }

    /// <summary>
    /// What the retained-relations check reads, one row per retained entity, ordered by entity name.
    /// </summary>
    private RetainedRelationInput[] RetainedRelationRows() =>
        [
            .. registry
                .Scan()
                .Values.Select(entry => new RetainedRelationInput(
                    entry.EntityType.FullName ?? entry.EntityType.Name,
                    entry.Table
                ))
                .OrderBy(row => row.Entity, StringComparer.Ordinal),
        ];

    private sealed record RetainedRelationInput(string Entity, RelationalObjectName Table);

    /// <summary>
    /// Each <c>[HeldWith]</c> and <c>[HeldWithReferencing]</c> link column's catalog type,
    /// resolved through domains (<c>varchar</c> and <c>text</c> count as one), must be the type of
    /// the record-id column it names, because hold SQL equates the two, and the two columns'
    /// collations must be comparable. PostgreSQL lets <c>citext</c> and <c>text</c>, or <c>int4</c>
    /// and <c>numeric</c>, meet in a comparison, and a collation conflict only shows in some
    /// plans, so a group pass can't see these; the model-level check compares only provider CLR
    /// types. A missing table or column is skipped: each port's group pass refuses it before
    /// that port runs.
    /// </summary>
    private static async Task ValidateHoldRelationLinkColumnsAsync(
        DatabaseFacade database,
        HoldLinkInput[] hops,
        CancellationToken ct
    )
    {
        if (hops.Length == 0)
        {
            return;
        }

        await database.OpenConnectionAsync(ct);
        try
        {
            await using var command = new SqlParams
            {
                ["owner_schemas"] = hops.Select(hop => hop.OwnerTable.Schema).ToArray(),
                ["owner_tables"] = hops.Select(hop => hop.OwnerTable.Name).ToArray(),
                ["link_columns"] = hops.Select(hop => hop.LinkColumn).ToArray(),
                ["target_schemas"] = hops.Select(hop => hop.TargetTable.Schema).ToArray(),
                ["target_tables"] = hops.Select(hop => hop.TargetTable.Name).ToArray(),
                ["target_columns"] = hops.Select(hop => hop.TargetRecordIdColumn).ToArray(),
            }.CreateCommand(
                database.GetDbConnection(),
                database.CurrentTransaction?.GetDbTransaction(),
                """
                /* cohort catalog */
                WITH RECURSIVE requested AS (
                    SELECT requested.ordinal,
                           link.atttypid AS link_type_id,
                           target.atttypid AS target_type_id,
                           link.attcollation AS link_collation_id,
                           target.attcollation AS target_collation_id
                    FROM ROWS FROM (
                        pg_catalog.unnest(@owner_schemas),
                        pg_catalog.unnest(@owner_tables),
                        pg_catalog.unnest(@link_columns),
                        pg_catalog.unnest(@target_schemas),
                        pg_catalog.unnest(@target_tables),
                        pg_catalog.unnest(@target_columns)
                    ) WITH ORDINALITY AS requested(owner_schema, owner_table, link_column, target_schema, target_table, target_column, ordinal)
                    LEFT JOIN pg_catalog.pg_namespace owner_namespace ON owner_namespace.nspname = requested.owner_schema
                    LEFT JOIN pg_catalog.pg_class owner_relation
                      ON owner_relation.relnamespace = owner_namespace.oid AND owner_relation.relname = requested.owner_table
                    LEFT JOIN pg_catalog.pg_attribute link
                      ON link.attrelid = owner_relation.oid
                     AND link.attname = requested.link_column
                     AND link.attnum > 0
                     AND NOT link.attisdropped
                    LEFT JOIN pg_catalog.pg_namespace target_namespace ON target_namespace.nspname = requested.target_schema
                    LEFT JOIN pg_catalog.pg_class target_relation
                      ON target_relation.relnamespace = target_namespace.oid AND target_relation.relname = requested.target_table
                    LEFT JOIN pg_catalog.pg_attribute target
                      ON target.attrelid = target_relation.oid
                     AND target.attname = requested.target_column
                     AND target.attnum > 0
                     AND NOT target.attisdropped
                ),
                -- Each type through its domains to the type it is built on.
                built_on(ordinal, side, type_id) AS (
                    SELECT ordinal, 'link', link_type_id FROM requested WHERE link_type_id IS NOT NULL
                    UNION ALL
                    SELECT ordinal, 'target', target_type_id FROM requested WHERE target_type_id IS NOT NULL
                    UNION ALL
                    SELECT built_on.ordinal, built_on.side, pgtype.typbasetype
                    FROM built_on
                    JOIN pg_catalog.pg_type pgtype ON pgtype.oid = built_on.type_id
                    WHERE pgtype.typtype = 'd'
                ),
                -- varchar and text are one type here: a varchar link equals a text record id
                -- without a cast. Folded after the domain recursion, so a domain over varchar folds too.
                resolved AS (
                    SELECT built_on.ordinal,
                           built_on.side,
                           CASE WHEN built_on.type_id = 'pg_catalog.varchar'::pg_catalog.regtype
                                THEN 'pg_catalog.text'::pg_catalog.regtype::pg_catalog.oid
                                ELSE built_on.type_id
                           END AS type_id
                    FROM built_on
                    JOIN pg_catalog.pg_type pgtype ON pgtype.oid = built_on.type_id
                    WHERE pgtype.typtype <> 'd'
                )
                SELECT requested.ordinal,
                       pg_catalog.format_type(requested.link_type_id, NULL),
                       pg_catalog.format_type(requested.target_type_id, NULL),
                       link_base.type_id IS NOT DISTINCT FROM target_base.type_id,
                       -- Collations matter once the types match, for collatable columns only
                       -- (attcollation is the effective collation, domains included). Two different
                       -- ones with neither the default (OID 100) can't be compared at all, and a
                       -- non-default nondeterministic link collation against the default record id
                       -- would match one link value to several record ids. A default link against
                       -- any record id collation passes: the record id's collation wins.
                       CASE WHEN link_base.type_id IS NOT DISTINCT FROM target_base.type_id
                                 AND requested.link_collation_id <> 0
                                 AND requested.target_collation_id <> 0
                                 AND requested.link_collation_id <> requested.target_collation_id
                                 AND requested.link_collation_id <> 100
                            THEN CASE WHEN requested.target_collation_id <> 100 THEN 1 WHEN NOT link_collation.collisdeterministic THEN 2 ELSE 0 END
                            ELSE 0
                       END,
                       link_collation.collname,
                       target_collation.collname
                FROM requested
                LEFT JOIN resolved link_base ON link_base.ordinal = requested.ordinal AND link_base.side = 'link'
                LEFT JOIN resolved target_base ON target_base.ordinal = requested.ordinal AND target_base.side = 'target'
                LEFT JOIN pg_catalog.pg_collation link_collation ON link_collation.oid = requested.link_collation_id
                LEFT JOIN pg_catalog.pg_collation target_collation ON target_collation.oid = requested.target_collation_id
                -- A missing table or column is refused by each port's group pass, so those rows
                -- are skipped.
                WHERE requested.target_type_id IS NOT NULL AND requested.link_type_id IS NOT NULL
                """
            );

            var errors = new SortedSet<string>(StringComparer.Ordinal);
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var hop = hops[(int)reader.GetInt64(0) - 1];
                    var column = $"{PostgreSqlIdentifier.Format(hop.OwnerTable)}.{PostgreSqlIdentifier.Quote(hop.LinkColumn)}";
                    if (!reader.GetBoolean(3))
                    {
                        errors.Add(
                            $"Hold relation {hop.Declaration}: link column {column} is '{reader.GetString(1)}' in the database, but the record-id column {PostgreSqlIdentifier.Format(hop.TargetTable)}.{PostgreSqlIdentifier.Quote(hop.TargetRecordIdColumn)} it names is '{reader.GetString(2)}', so holds would never match across the relation. Give the link column the record id's type."
                        );
                    }
                    else if (reader.GetInt32(4) is var collationProblem and not 0)
                    {
                        var linkCollation = reader.GetString(5);
                        var targetCollation = reader.GetString(6);
                        var record = $"{PostgreSqlIdentifier.Format(hop.TargetTable)}.{PostgreSqlIdentifier.Quote(hop.TargetRecordIdColumn)}";
                        errors.Add(
                            collationProblem == 1
                                ? $"Hold relation {hop.Declaration}: link column {column} has collation '{linkCollation}' in the database, but the record-id column {record} it names has collation '{targetCollation}', and PostgreSQL can't choose between two different non-default collations, so every hold check across the relation would fail. Give the link column the record id's collation, or the default."
                                : $"Hold relation {hop.Declaration}: link column {column} has the nondeterministic collation '{linkCollation}' in the database, but the record-id column {record} it names has the default collation, so PostgreSQL compares them under '{linkCollation}' and one link value would match several record ids, so a row could be covered by holds on several rows. Give the link column the record id's collation, or a deterministic one."
                        );
                    }
                }
            }

            if (errors.Count != 0)
            {
                throw new RetentionConfigurationException([.. errors]);
            }
        }
        finally
        {
            await database.CloseConnectionAsync();
        }
    }

    /// <summary>
    /// What the link-column check reads, one row per one-hop holder, ordered by the entity that
    /// carries the relation. A relation's declaring entity carries it as a one-hop holder path.
    /// </summary>
    private HoldLinkInput[] HoldLinkRows() =>
        [
            .. registry
                .Scan()
                .Values.OrderBy(entry => entry.EntityType.FullName, StringComparer.Ordinal)
                .SelectMany(entry => entry.HolderPaths.Where(path => path.Hops.Count == 1).Select(path => path.Hops[0]))
                .Select(hop =>
                {
                    var (owner, target) = hop.LinkSide == HoldLinkSide.Near ? (hop.Near, hop.Far) : (hop.Far, hop.Near);
                    return new HoldLinkInput(
                        hop.Declaration,
                        owner.EntityType.FullName ?? owner.EntityType.Name,
                        target.EntityType.FullName ?? target.EntityType.Name,
                        owner.Table,
                        hop.LinkColumn,
                        target.Table,
                        target.RecordId.RecordIdColumn
                    );
                }),
        ];

    private sealed record HoldLinkInput(
        string Declaration,
        string OwnerEntity,
        string TargetEntity,
        RelationalObjectName OwnerTable,
        string LinkColumn,
        RelationalObjectName TargetTable,
        string TargetRecordIdColumn
    );

    private static string NotATable(char kind) =>
        kind switch
        {
            'v' => "a view, which has no physical row positions (ctid) for Cohort to address its rows by",
            'm' => "a materialized view, whose rows can't be updated or deleted",
            'f' => "a foreign table, whose row positions (ctid) and versions (xmin) are whatever its foreign-data wrapper supplies, if anything, so they don't reliably identify its rows",
            _ => $"a relation of kind '{kind}', not a table",
        };

    /// <summary>
    /// A group pass has PostgreSQL analyse the anonymise UPDATE itself, so a missing or
    /// generated column, an unknown cast type, and a store type the column can't be assigned
    /// from are refused there. What it can't see, it can't see because EXPLAIN never runs the
    /// cast on a stored literal or reads a constraint: the catalog is asked, for each target
    /// column (domains, nested ones included, and migration drift), whether each literal is
    /// valid for both types' input functions (the column's typmod included), whether Null meets
    /// a NOT NULL column, whether a single value meets a unique index, whether a literal meets a
    /// foreign key, and whether the column is referenced by one. A missing table, record id, or
    /// column is skipped: each port's group pass refuses it before that port runs.
    /// </summary>
    private async Task ValidateAnonymiseTargetsAsync(CancellationToken ct)
    {
        var targets = registry
            .Scan()
            .Values.Where(entry =>
                ValidatedCapabilities.TryGetValue(entry.Category, out var capabilities)
                && capabilities.Strategies.Contains(Strategy.Anonymise)
            )
            .SelectMany(entry => entry.AnonymiseFields.Select(field => (Entry: entry, Field: field)))
            .ToArray();
        if (targets.Length == 0)
        {
            return;
        }

        var errors = new SortedSet<string>(StringComparer.Ordinal);
        var probes = targets.Select(target => ProbeValue(target.Entry, target.Field, errors)).ToArray();
        await db.Database.OpenConnectionAsync(ct);
        try
        {
            await using var command = new SqlParams
            {
                ["schemas"] = targets.Select(target => target.Entry.Table.Schema).ToArray(),
                ["tables"] = targets.Select(target => target.Entry.Table.Name).ToArray(),
                ["columns"] = targets.Select(target => target.Field.ColumnName).ToArray(),
                ["record_id_columns"] = targets
                    .Select(target => target.Entry.RecordId.RecordIdColumn)
                    .ToArray(),
                ["cast_types"] = targets
                    .Select(target => PostgresStoreTypeSql.CastType(target.Field.StoreType))
                    .ToArray(),
                ["probes"] = probes,
            }.CreateCommand(
                db.Database.GetDbConnection(),
                db.Database.CurrentTransaction?.GetDbTransaction(),
                """
                /* cohort catalog */
                WITH RECURSIVE requested AS (
                    SELECT requested.ordinal,
                           requested.cast_type,
                           requested.probe,
                           relation.oid AS relation_id,
                           record_id.attnum AS record_id_attnum,
                           attribute.attnum,
                           attribute.atttypid,
                           attribute.attnotnull,
                           pg_catalog.format_type(attribute.atttypid, attribute.atttypmod) AS column_type,
                           pg_catalog.to_regtype(requested.cast_type) AS cast_type_id
                    FROM ROWS FROM (
                        pg_catalog.unnest(@schemas),
                        pg_catalog.unnest(@tables),
                        pg_catalog.unnest(@columns),
                        pg_catalog.unnest(@record_id_columns),
                        pg_catalog.unnest(@cast_types),
                        pg_catalog.unnest(@probes)
                    ) WITH ORDINALITY AS requested(schema_name, table_name, column_name, record_id_column, cast_type, probe, ordinal)
                    LEFT JOIN pg_catalog.pg_namespace namespace ON namespace.nspname = requested.schema_name
                    LEFT JOIN pg_catalog.pg_class relation
                      ON relation.relnamespace = namespace.oid AND relation.relname = requested.table_name
                    LEFT JOIN pg_catalog.pg_attribute attribute
                      ON attribute.attrelid = relation.oid
                     AND attribute.attname = requested.column_name
                     AND attribute.attnum > 0
                     AND NOT attribute.attisdropped
                    LEFT JOIN pg_catalog.pg_attribute record_id
                      ON record_id.attrelid = relation.oid
                     AND record_id.attname = requested.record_id_column
                     AND record_id.attnum > 0
                     AND NOT record_id.attisdropped
                ),
                -- Each column type through its domains, for a NOT NULL anywhere in the chain.
                type_chain(type_id, step_id) AS (
                    SELECT pgtype.oid, pgtype.oid
                    FROM pg_catalog.pg_type pgtype
                    WHERE pgtype.oid IN (SELECT atttypid FROM requested)
                    UNION ALL
                    SELECT chain.type_id, step.typbasetype
                    FROM type_chain chain
                    JOIN pg_catalog.pg_type step ON step.oid = chain.step_id
                    WHERE step.typtype = 'd'
                ),
                type_base AS (
                    SELECT chain.type_id,
                           pg_catalog.bool_or(step.typnotnull) AS not_null
                    FROM type_chain chain
                    JOIN pg_catalog.pg_type step ON step.oid = chain.step_id
                    GROUP BY chain.type_id
                )
                SELECT requested.ordinal,
                       requested.relation_id IS NOT NULL AND requested.record_id_attnum IS NOT NULL,
                       requested.attnum IS NOT NULL,
                       requested.column_type,
                       requested.attnotnull OR column_type.not_null,
                       CASE WHEN requested.probe IS NULL THEN NULL
                            ELSE pg_catalog.pg_input_is_valid(requested.probe, requested.column_type)
                       END,
                       CASE WHEN requested.probe IS NULL OR requested.cast_type_id IS NULL THEN NULL
                            ELSE pg_catalog.pg_input_is_valid(requested.probe, requested.cast_type)
                       END,
                       COALESCE(unique_key.unique_without_record_id, false),
                       COALESCE(unique_key.nulls_not_distinct, false),
                       EXISTS (
                           SELECT 1 FROM pg_catalog.pg_constraint foreign_key
                           WHERE foreign_key.contype = 'f'
                             AND foreign_key.conrelid = requested.relation_id
                             AND requested.attnum = ANY(foreign_key.conkey)
                       ),
                       EXISTS (
                           SELECT 1 FROM pg_catalog.pg_constraint foreign_key
                           WHERE foreign_key.contype = 'f'
                             AND foreign_key.confrelid = requested.relation_id
                             AND requested.attnum = ANY(foreign_key.confkey)
                       )
                FROM requested
                LEFT JOIN type_base column_type ON column_type.type_id = requested.atttypid
                -- Unfiltered, non-expression unique indexes whose key (not their INCLUDE columns)
                -- holds the column but not the record id.
                CROSS JOIN LATERAL (
                    SELECT pg_catalog.bool_or(true) AS unique_without_record_id,
                           pg_catalog.bool_or(unique_index.indnullsnotdistinct) AS nulls_not_distinct
                    FROM pg_catalog.pg_index unique_index
                    CROSS JOIN LATERAL (
                        VALUES ((unique_index.indkey::pg_catalog.int2[])[0:unique_index.indnkeyatts - 1])
                    ) AS index_key(columns)
                    WHERE unique_index.indrelid = requested.relation_id
                      AND unique_index.indisunique
                      AND unique_index.indpred IS NULL
                      AND unique_index.indexprs IS NULL
                      AND requested.attnum = ANY(index_key.columns)
                      AND NOT (requested.record_id_attnum = ANY(index_key.columns))
                ) unique_key
                """
            );

            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var (entry, field) = targets[(int)reader.GetInt64(0) - 1];
                    // A missing table, record id, or column is each port's group pass to refuse.
                    if (!reader.GetBoolean(1) || !reader.GetBoolean(2))
                    {
                        continue;
                    }

                    var target = TargetDescription(entry, field);
                    var columnType = reader.GetString(3);
                    var notNull = reader.GetBoolean(4);
                    var probeValidForColumn = reader.IsDBNull(5) || reader.GetBoolean(5);
                    var probeValidForStoreType = reader.IsDBNull(6) || reader.GetBoolean(6);
                    var unfilteredUnique = reader.GetBoolean(7);
                    var nullsNotDistinctUnique = reader.GetBoolean(8);
                    var foreignKeyChild = reader.GetBoolean(9);
                    var referencedKey = reader.GetBoolean(10);

                    var method = field is AnonymiseLiteralField literal ? literal.Method : (AnonymiseMethod?)null;
                    var writesOneValue =
                        method is AnonymiseMethod.FixedLiteral or AnonymiseMethod.EmptyString
                        || field is AnonymiseFactoryField factoryField && assignments.IsStatic(factoryField);
                    var castType = PostgresStoreTypeSql.CastType(field.StoreType);

                    if (!probeValidForColumn || !probeValidForStoreType)
                    {
                        var value = method is AnonymiseMethod.EmptyString
                            ? "the empty string"
                            : $"the literal '{((AnonymiseLiteralField)field).Literal}'";
                        var type = probeValidForColumn
                            ? $"its model store type {castType}"
                            : $"its column type {columnType}";
                        errors.Add(
                            $"{target}, but {value} is not valid for {type} (too long, the wrong shape, or refused by a domain check), so every anonymise of these rows would fail. Change the literal or the column."
                        );
                    }

                    if (method is AnonymiseMethod.Null && notNull)
                    {
                        errors.Add(
                            $"{target}, which uses Null but the column is NOT NULL in the database, so every anonymise of these rows would fail. Make the column nullable or choose another method."
                        );
                    }

                    if (
                        (writesOneValue && unfilteredUnique)
                        || (method is AnonymiseMethod.Null && nullsNotDistinctUnique)
                    )
                    {
                        errors.Add(
                            $"{target}, which a unique index covers without the record id or a filter, but every anonymised row would get the same value, so the second row anonymised would fail. Filter the index on the anonymised-at column (WHERE \"{entry.AnonymisedAt!.AnonymisedAtColumn}\" IS NULL), include the record id in its key, or use a per-row [AnonymiseWith] factory."
                        );
                    }

                    if ((method is AnonymiseMethod.FixedLiteral or AnonymiseMethod.EmptyString) && foreignKeyChild)
                    {
                        errors.Add(
                            $"{target}, which is a foreign key column, and a literal can't satisfy the foreign key. Use Null or a factory that returns a key that exists."
                        );
                    }

                    if (referencedKey)
                    {
                        errors.Add(
                            $"{target}, which is referenced by a foreign key, so anonymising it would be refused while a row references it or would silently rewrite the referencing rows (ON UPDATE CASCADE or SET NULL). Anonymise the referencing rows instead, or stop referencing this column."
                        );
                    }
                }
            }

            if (errors.Count != 0)
            {
                throw new RetentionConfigurationException([.. errors]);
            }
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }
    }

    /// <summary>
    /// The rules for a retained table's columns that a group pass can't see, because
    /// PostgreSQL accepts the statements: the anchor, deleted-at, and anonymised-at markers must
    /// be <c>timestamp with time zone</c> (a naive timestamp compares with a cutoff through a
    /// cross-type operator and silently drifts with the session TimeZone); the tenant column and
    /// the soft-delete flag must be NOT NULL (a NULL tenant never matches, and <c>= FALSE</c>
    /// skips a NULL flag); the anonymised-at marker must be nullable (<c>IS NULL</c> never
    /// matches a NOT NULL column); a column an update writes must not be referenced by a foreign
    /// key (the key would refuse the update while a row references it, or silently rewrite the
    /// referencing rows); and a record id's text form must not depend on session settings (hold
    /// joins compare it). Types and nullability are resolved through any chain of domains. A
    /// missing table or column is skipped: each port's group pass refuses it before that port
    /// runs.
    /// </summary>
    private static async Task ValidateRetainedColumnsAsync(
        DatabaseFacade database,
        RetainedColumnInput[] inputs,
        CancellationToken ct
    )
    {
        if (inputs.Length == 0)
        {
            return;
        }

        await database.OpenConnectionAsync(ct);
        try
        {
            await using var command = new SqlParams
            {
                ["schemas"] = inputs.Select(input => input.Table.Schema).ToArray(),
                ["tables"] = inputs.Select(input => input.Table.Name).ToArray(),
                ["columns"] = inputs.Select(input => input.Column).ToArray(),
                ["base_types"] = inputs.Select(input => input.ExpectedType?.BaseType).ToArray(),
                ["nullability"] = inputs.Select(input => input.Nullability).ToArray(),
                ["written"] = inputs.Select(input => input.Strategy is not null).ToArray(),
                ["session_text"] = inputs.Select(input => input.Kind == RetainedColumnKind.RecordId).ToArray(),
            }.CreateCommand(
                database.GetDbConnection(),
                database.CurrentTransaction?.GetDbTransaction(),
                """
                /* cohort catalog */
                WITH RECURSIVE requested AS (
                    SELECT requested.ordinal,
                           requested.base_type,
                           requested.nullability,
                           requested.written,
                           requested.session_text,
                           relation.oid AS relation_id,
                           attribute.attnum,
                           attribute.atttypid,
                           attribute.attnotnull,
                           pg_catalog.format_type(attribute.atttypid, attribute.atttypmod) AS column_type
                    FROM ROWS FROM (
                        pg_catalog.unnest(@schemas),
                        pg_catalog.unnest(@tables),
                        pg_catalog.unnest(@columns),
                        pg_catalog.unnest(@base_types),
                        pg_catalog.unnest(@nullability),
                        pg_catalog.unnest(@written),
                        pg_catalog.unnest(@session_text)
                    ) WITH ORDINALITY AS requested(schema_name, table_name, column_name, base_type, nullability, written, session_text, ordinal)
                    JOIN pg_catalog.pg_namespace namespace ON namespace.nspname = requested.schema_name
                    JOIN pg_catalog.pg_class relation
                      ON relation.relnamespace = namespace.oid AND relation.relname = requested.table_name
                    JOIN pg_catalog.pg_attribute attribute
                      ON attribute.attrelid = relation.oid
                     AND attribute.attname = requested.column_name
                     AND attribute.attnum > 0
                     AND NOT attribute.attisdropped
                ),
                -- Each column's type through its domains to the type it is built on, with whether
                -- any domain on the way is NOT NULL.
                domain_chain(ordinal, type_id, domain_not_null) AS (
                    SELECT ordinal, atttypid, false FROM requested
                    UNION ALL
                    SELECT chain.ordinal, pgtype.typbasetype, chain.domain_not_null OR pgtype.typnotnull
                    FROM domain_chain chain
                    JOIN pg_catalog.pg_type pgtype ON pgtype.oid = chain.type_id
                    WHERE pgtype.typtype = 'd'
                ),
                base AS (
                    SELECT chain.ordinal, pgtype.typname, namespace.nspname, chain.domain_not_null
                    FROM domain_chain chain
                    JOIN pg_catalog.pg_type pgtype ON pgtype.oid = chain.type_id
                    JOIN pg_catalog.pg_namespace namespace ON namespace.oid = pgtype.typnamespace
                    WHERE pgtype.typtype <> 'd'
                ),
                -- A record id's types, through domains, arrays, ranges, and multiranges too.
                built_on(ordinal, type_id) AS (
                    SELECT ordinal, atttypid FROM requested WHERE session_text
                    UNION
                    SELECT built_on.ordinal,
                           CASE
                               WHEN pgtype.typtype = 'd' THEN pgtype.typbasetype
                               WHEN pgtype.typcategory = 'A' THEN pgtype.typelem
                               WHEN pgtype.typtype = 'r' THEN over_range.rngsubtype
                               ELSE over_multirange.rngtypid
                           END
                    FROM built_on
                    JOIN pg_catalog.pg_type pgtype ON pgtype.oid = built_on.type_id
                    LEFT JOIN pg_catalog.pg_range over_range ON over_range.rngtypid = pgtype.oid
                    LEFT JOIN pg_catalog.pg_range over_multirange ON over_multirange.rngmultitypid = pgtype.oid
                    WHERE pgtype.typtype IN ('d', 'r', 'm') OR pgtype.typcategory = 'A'
                )
                SELECT checked.ordinal, checked.is_wrong_type, checked.column_type, checked.is_referenced,
                       checked.is_null_problem, NULL::pg_catalog.text
                FROM (
                    SELECT requested.ordinal,
                           COALESCE(
                               requested.base_type IS NOT NULL
                               AND (base.typname <> requested.base_type OR base.nspname <> 'pg_catalog'),
                               false
                           ) AS is_wrong_type,
                           requested.column_type,
                           COALESCE(
                               requested.written
                               AND EXISTS (
                                   SELECT 1 FROM pg_catalog.pg_constraint foreign_key
                                   WHERE foreign_key.contype = 'f'
                                     AND foreign_key.confrelid = requested.relation_id
                                     AND requested.attnum = ANY(foreign_key.confkey)
                               ),
                               false
                           ) AS is_referenced,
                           -- The column's own NOT NULL, or a NOT NULL domain anywhere in its chain.
                           COALESCE(
                               CASE requested.nullability
                                   WHEN 'not_null' THEN NOT (requested.attnotnull OR base.domain_not_null)
                                   WHEN 'nullable' THEN requested.attnotnull OR base.domain_not_null
                                   ELSE false
                               END,
                               false
                           ) AS is_null_problem
                    FROM requested
                    LEFT JOIN base ON base.ordinal = requested.ordinal
                ) AS checked
                WHERE checked.is_wrong_type OR checked.is_referenced OR checked.is_null_problem
                UNION ALL
                SELECT built_on.ordinal, false, NULL, false, false, pgtype.typname::pg_catalog.text
                FROM built_on
                JOIN pg_catalog.pg_type pgtype ON pgtype.oid = built_on.type_id
                JOIN pg_catalog.pg_namespace namespace ON namespace.oid = pgtype.typnamespace
                WHERE namespace.nspname = 'pg_catalog'
                """
            );

            var errors = new SortedSet<string>(StringComparer.Ordinal);
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var input = inputs[(int)reader.GetInt64(0) - 1];
                    var column = $"{PostgreSqlIdentifier.Format(input.Table)}.{PostgreSqlIdentifier.Quote(input.Column)}";
                    var head = $"Retained entity {input.Entity}:";
                    if (!reader.IsDBNull(5))
                    {
                        var typeName = reader.GetString(5);
                        if (RetentionStartupValidator.HasSessionDependentText(typeName))
                        {
                            errors.Add(
                                $"Record-id convention on {input.Entity}: record-id column {column} is built on '{typeName}', whose text form depends on session settings (TimeZone, DateStyle, IntervalStyle, extra_float_digits), so holds and audit rows could silently stop matching. Use a uuid, integer, numeric, or text record id, or mark a stable unique column with [RetentionRecordId]."
                            );
                        }

                        continue;
                    }

                    if (reader.GetBoolean(1))
                    {
                        var expected = input.ExpectedType!;
                        errors.Add(
                            $"{head} {input.KindText} member {input.Member} names column {column}, which is {reader.GetString(2)} in the database, not {expected.Display}, so {expected.Consequence}. Migrate the column to {expected.Display}."
                        );
                    }

                    if (reader.GetBoolean(4))
                    {
                        errors.Add(
                            input.Kind == RetainedColumnKind.AnonymisedAt
                                ? $"{head} {input.KindText} member {input.Member} names column {column}, which is NOT NULL in the database, so no row could be found as not yet anonymised (NULL marks those rows) and anonymising would never find a row. Make the column nullable."
                                : $"{head} {input.KindText} member {input.Member} names column {column}, which is nullable in the database, so a row with a NULL there is never matched ({(input.Kind == RetainedColumnKind.Tenant ? "no tenant's run, erasure, or hold selects it" : "soft-deleting compares the flag with FALSE, which skips NULL")}) and stays forever. Make the column NOT NULL."
                        );
                    }

                    if (reader.GetBoolean(3))
                    {
                        errors.Add(
                            $"{head} member {input.Member} writes column {column}, which is referenced by a foreign key, so {input.Strategy} a row would be refused while a row references it or would silently rewrite the referencing rows (ON UPDATE CASCADE or SET NULL). Stop referencing this column."
                        );
                    }
                }
            }

            if (errors.Count != 0)
            {
                throw new RetentionConfigurationException([.. errors]);
            }
        }
        finally
        {
            await database.CloseConnectionAsync();
        }
    }

    /// <summary>
    /// What the retained-columns check reads, as the validated capabilities declare them, ordered
    /// by entity name, column, member, and kind: per retained entity its record id and its anchor
    /// and, when it has one, its tenant column (all only read), and the columns the update
    /// strategies write.
    /// </summary>
    private RetainedColumnInput[] RetainedColumnRows() =>
        [
            .. registry
                .Scan()
                .Values.Where(entry => ValidatedCapabilities.ContainsKey(entry.Category))
                .SelectMany(entry =>
                {
                    var strategies = ValidatedCapabilities[entry.Category].Strategies;
                    var entity = entry.EntityType.FullName ?? entry.EntityType.Name;
                    var columns = new List<RetainedColumnInput>
                    {
                        new(
                            entity,
                            RetainedColumnKind.RecordId,
                            null,
                            entry.RecordId.RecordIdMember,
                            entry.Table,
                            entry.RecordId.RecordIdColumn
                        ),
                        new(entity, RetainedColumnKind.Anchor, null, entry.AnchorMember, entry.Table, entry.AnchorColumn),
                    };
                    if (entry.Tenant is { } tenant)
                    {
                        columns.Add(
                            new(
                                entity,
                                RetainedColumnKind.Tenant,
                                null,
                                tenant.TenantMember,
                                entry.Table,
                                tenant.TenantColumn
                            )
                        );
                    }

                    if (strategies.Contains(Strategy.SoftDelete) && entry.SoftDelete is { } softDelete)
                    {
                        columns.Add(
                            new(
                                entity,
                                RetainedColumnKind.SoftDeleteFlag,
                                "soft-deleting",
                                softDelete.IsDeletedMember,
                                entry.Table,
                                softDelete.IsDeletedColumn
                            )
                        );
                        if (softDelete is { DeletedAtMember: { } member, DeletedAtColumn: { } column })
                        {
                            columns.Add(
                                new(entity, RetainedColumnKind.DeletedAt, "soft-deleting", member, entry.Table, column)
                            );
                        }
                    }

                    if (strategies.Contains(Strategy.Anonymise) && entry.AnonymisedAt is { } anonymisedAt)
                    {
                        columns.Add(
                            new(
                                entity,
                                RetainedColumnKind.AnonymisedAt,
                                "anonymising",
                                anonymisedAt.AnonymisedAtMember,
                                entry.Table,
                                anonymisedAt.AnonymisedAtColumn
                            )
                        );
                    }

                    return columns;
                })
                .OrderBy(input => input.Entity, StringComparer.Ordinal)
                .ThenBy(input => input.Column, StringComparer.Ordinal)
                .ThenBy(input => input.Member, StringComparer.Ordinal)
                .ThenBy(input => input.KindText, StringComparer.Ordinal),
        ];

    // Which column of a retained entity a row names; it decides the rules the column must meet and
    // the words a message calls it by. Strategy is "soft-deleting" or "anonymising" for a column
    // that strategy writes, and null for a column Cohort only reads.
    private enum RetainedColumnKind
    {
        RecordId,
        Anchor,
        Tenant,
        SoftDeleteFlag,
        DeletedAt,
        AnonymisedAt,
    }

    private sealed record RetainedColumnInput(
        string Entity,
        RetainedColumnKind Kind,
        string? Strategy,
        string Member,
        RelationalObjectName Table,
        string Column
    )
    {
        internal string KindText =>
            Kind switch
            {
                RetainedColumnKind.RecordId => "record-id",
                RetainedColumnKind.Anchor => "anchor",
                RetainedColumnKind.Tenant => "tenant",
                RetainedColumnKind.SoftDeleteFlag => "soft-delete flag",
                RetainedColumnKind.DeletedAt => "deleted-at",
                RetainedColumnKind.AnonymisedAt => "anonymised-at",
                _ => throw new ArgumentOutOfRangeException(nameof(Kind), Kind, null),
            };

        // Only the timestamp columns have a type rule left: PostgreSQL compares or assigns a
        // timestamp without time zone through a cross-type operator, so a group pass passes it.
        // The other columns' types are the group passes'.
        internal ExpectedColumnType? ExpectedType =>
            Kind is RetainedColumnKind.Anchor or RetainedColumnKind.DeletedAt or RetainedColumnKind.AnonymisedAt
                ? new(
                    "timestamptz",
                    "timestamp with time zone",
                    "every run would fail or, for timestamp without time zone, silently drift with the session TimeZone"
                )
                : null;

        // A tenant column or a soft-delete flag must be NOT NULL (a NULL one is never matched);
        // the anonymised-at marker must be nullable (IS NULL never matches a NOT NULL column).
        internal string Nullability =>
            Kind switch
            {
                RetainedColumnKind.Tenant or RetainedColumnKind.SoftDeleteFlag => "not_null",
                RetainedColumnKind.AnonymisedAt => "nullable",
                _ => "",
            };
    }

    // BaseType is the pg_catalog type name (typname) the column must be built on.
    private sealed record ExpectedColumnType(string BaseType, string Display, string Consequence);

    private static string TargetDescription(RetentionEntry entry, AnonymiseField field) =>
        $"Anonymise convention on {entry.EntityType.FullName}: {(field is AnonymiseFactoryField ? "[AnonymiseWith]" : "[Anonymise]")} member {field.MemberName} writes column {PostgreSqlIdentifier.Format(entry.Table)}.{PostgreSqlIdentifier.Quote(field.ColumnName)}";

    /// <summary>
    /// The text the input functions must accept: the literal exactly as the UPDATE sends it.
    /// Null, factories and a literal whose provider value isn't text have no probe; their
    /// structural checks still run. A converter that refuses the literal would throw on every
    /// run, so it is refused here.
    /// </summary>
    private string? ProbeValue(RetentionEntry entry, AnonymiseField field, ISet<string> errors)
    {
        if (field is not AnonymiseLiteralField literal)
        {
            return null;
        }

        try
        {
            return assignments.ProviderLiteral(entry, literal) as string;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            errors.Add(
                $"{TargetDescription(entry, field)}, but its value converter threw {exception.GetType().Name} converting the {(literal.Method is AnonymiseMethod.EmptyString ? "empty string" : $"literal '{literal.Literal}'")}, so every anonymise of these rows would fail. Change the literal or the converter."
            );
            return null;
        }
    }

    /// <summary>
    /// Cohort's catalog checks call <c>pg_input_is_valid</c>, which PostgreSQL 16 added, so
    /// refuse an older server before any of them runs.
    /// </summary>
    private static async Task ValidateServerVersionAsync(DatabaseFacade database, CancellationToken ct)
    {
        await database.OpenConnectionAsync(ct);
        try
        {
            // Asked of the server rather than read off NpgsqlConnection: a host may hand EF a
            // wrapped DbConnection (a profiler, say).
            var (version, versionText) = await ReadServerVersionAsync(database, ct);
            if (version < MinimumServerVersion)
            {
                throw new RetentionConfigurationException([
                    $"Cohort requires PostgreSQL 16 or later; the database server is PostgreSQL {versionText}.",
                ]);
            }
        }
        finally
        {
            await database.CloseConnectionAsync();
        }
    }

    /// <summary>The database a session reaches, the role it runs as, and its search path.</summary>
    private sealed record ServerSession(ServerDatabaseIdentity Database, string Role, string SearchPath);

    private static async Task<(Version Version, string Text)> ReadServerVersionAsync(
        DatabaseFacade database,
        CancellationToken ct
    )
    {
        await using var command = new SqlParams().CreateCommand(
            database.GetDbConnection(),
            database.CurrentTransaction?.GetDbTransaction(),
            "/* cohort catalog */ SELECT pg_catalog.current_setting('server_version_num')::integer, pg_catalog.current_setting('server_version')"
        );
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        // server_version_num is major * 10000 + minor from PostgreSQL 10 on.
        var number = reader.GetInt32(0);
        return (new Version(number / 10000, number % 10000), reader.GetString(1));
    }

    /// <summary>
    /// A hold names its entity by <c>[RetentionEntityId]</c>, which lives only in code. Once no
    /// retained entity declares that identity, no sweep, erasure or deletion matches the hold
    /// again and its row loses protection silently, so refuse while an active hold names one.
    /// Holds span tenants, so this runs in its own <see cref="RetentionSystemWork.Readiness"/>
    /// scope whatever scope readiness was called from: a tenant's scope would see only that
    /// tenant's holds under row-level security. It also runs when no entity is retained, since
    /// removing the last <c>[Retain]</c> orphans holds the same way. The scope's context is used
    /// only through its connection, so its model doesn't matter, but it must reach the caller's
    /// database. Its success is recorded under the holds relation and the registered ids, and
    /// stands whatever transaction the caller is in, since it never reads through the caller's
    /// connection; a recorded pass skips the scope altogether.
    /// <para>
    /// The scope's own statements (the Readiness group: this read, and the tenantless holds read
    /// when a retained entity is tenantless) are explained first, under the role of this scope's
    /// session, keyed by it, and that scope's database is compared with the caller's
    /// (<see cref="ValidateReadinessScopeAsync"/>). This step runs once per entry, so the hold
    /// guard checks its own <c>System(Readiness)</c> scope the same way before it reads
    /// tenantless holds (<see cref="ReadCallerDatabaseAsync"/>): a role or database that scope
    /// reaches only later is checked when it first does.
    /// </para>
    /// </summary>
    private Task ValidateStoredEntityIdentitiesAsync(
        RelationalObjectName holdsTable,
        Guid[] registered,
        ServerDatabaseIdentity callerDatabase,
        CancellationToken ct
    ) =>
        scopeFactory.RunInScopeAsync(
            new RetentionExecutionContext.System(RetentionSystemWork.Readiness),
            services => ValidateStoredEntityIdentitiesAsync(
                services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext).Database,
                registered,
                holdsTable,
                callerDatabase,
                ct
            ),
            ct
        );

    private async Task ValidateStoredEntityIdentitiesAsync(
        DatabaseFacade systemDatabase,
        Guid[] registered,
        RelationalObjectName holdsTable,
        ServerDatabaseIdentity callerDatabase,
        CancellationToken ct
    )
    {
        var holds = PostgreSqlIdentifier.Format(holdsTable);
        await systemDatabase.OpenConnectionAsync(ct);
        try
        {
            await ValidateReadinessScopeAsync(systemDatabase, callerDatabase, ct);

            var statement = RetentionHoldSql.StoredEntityIdentities(CohortStoreTables.FromModel(db.Model), registered);
            await using var command = statement.Parameters.CreateCommand(
                systemDatabase.GetDbConnection(),
                null,
                statement.Sql
            );

            var errors = new List<string>();
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var id = reader.GetGuid(0);
                    var count = reader.GetInt64(1);
                    errors.Add(
                        $"Retention entity identity '{id}' is not registered by any retained entity in the EF model, but {count} active hold(s) name it. Sweeps, erasures and deletions would no longer honour those holds. Restore [RetentionEntityId(\"{id}\")] on the entity it belonged to. If the change is deliberate, re-point the holds first: UPDATE {holds} SET \"RetentionEntityId\" = '<new id>' WHERE \"RetentionEntityId\" = '{id}'; or, only if their protection is no longer wanted, end them: UPDATE {holds} SET \"RemovedAt\" = pg_catalog.statement_timestamp() WHERE \"RetentionEntityId\" = '{id}' AND (\"RemovedAt\" IS NULL OR \"RemovedAt\" > pg_catalog.statement_timestamp())."
                    );
                }
            }

            if (errors.Count != 0)
            {
                throw new RetentionConfigurationException(errors);
            }
        }
        finally
        {
            await systemDatabase.CloseConnectionAsync();
        }
    }

    /// <summary>
    /// The database the caller's own connection reaches, read fresh through this validator's
    /// context: inside the caller's or the run's transaction when it has one, read-only. The hold
    /// guard reads it before it opens its <c>System(Readiness)</c> scope, so no statement of the
    /// caller's connection runs inside that scope's hook, and passes it to
    /// <see cref="ValidateReadinessScopeAsync"/>. Nothing is remembered: a server restart, a
    /// failover, or a recreated holds table changes what <see cref="ServerDatabaseIdentity"/> says.
    /// </summary>
    internal async Task<ServerDatabaseIdentity> ReadCallerDatabaseAsync(CancellationToken ct) =>
        (await ReadServerSessionAsync(db.Database, CohortStoreTables.FromModel(db.Model).RetentionHolds, ct)).Database;

    /// <summary>
    /// The hold guard's check of its own <c>System(Readiness)</c> scope before it reads tenantless
    /// holds: <paramref name="systemDatabase"/> (the scope's context, connection open) must reach
    /// <paramref name="callerDatabase"/>, and the Readiness group's statements are explained under
    /// that session's role, a pass kept per role. Runs no model pass and keeps no state on the
    /// entry; a pass already recorded costs the session read. Takes no Gate: the stored-identities
    /// step runs it while the model pass holds the entry's, which isn't re-entrant.
    /// </summary>
    internal async Task ValidateReadinessScopeAsync(
        DatabaseFacade systemDatabase,
        ServerDatabaseIdentity callerDatabase,
        CancellationToken ct
    )
    {
        // A host that gives every context one connection puts this scope's context on the
        // caller's physical connection, which carries the caller's transaction. The context
        // doesn't know that transaction, and the explain would begin one of its own, so it joins
        // the caller's for the session read and the explain, as a handler scope's context does.
        var callerTransaction = db.Database.CurrentTransaction;
        var joinsCallerTransaction =
            callerTransaction is not null
            && systemDatabase.CurrentTransaction is null
            && ReferenceEquals(systemDatabase.GetDbConnection(), db.Database.GetDbConnection());
        if (joinsCallerTransaction)
        {
            await systemDatabase.UseTransactionAsync(callerTransaction!.GetDbTransaction(), ct);
        }

        try
        {
            // Only the database is compared: the System scope may run under another role.
            var session = await ReadServerSessionAsync(
                systemDatabase,
                CohortStoreTables.FromModel(db.Model).RetentionHolds,
                ct
            );
            if (session.Database != callerDatabase)
            {
                throw new RetentionConfigurationException([
                    "Cohort's readiness scope resolved a DbContext that reaches a different database from the calling scope's. Cohort checks holds across tenants in a System(Readiness) scope, so every scope must reach the same database.",
                ]);
            }

            var readinessStatements = statementReadiness.EnumerateReadiness();
            await RunSharedAsync(
                GroupKey(session, "Readiness", new GroupStatements(readinessStatements, StatementInputs(readinessStatements))),
                async () =>
                {
                    var refused = await statementReadiness.ExplainAsync(
                        systemDatabase,
                        readinessStatements,
                        session.Role,
                        RetentionStatementReadiness.ReadinessScope,
                        tolerateInsufficientPrivilege: false,
                        ct
                    );
                    if (refused.Count != 0)
                    {
                        throw new RetentionConfigurationException(refused);
                    }
                },
                recordInsideTransaction: true
            );
        }
        finally
        {
            if (joinsCallerTransaction)
            {
                await systemDatabase.UseTransactionAsync(null, CancellationToken.None);
            }
        }
    }

    /// <summary>
    /// Which database a connection reaches, as the server sees it. Connection strings
    /// spell one database many ways (<c>localhost</c> or <c>127.0.0.1</c>, a pooler or the direct
    /// port), so the database name and OID, the server's start time, and the holds table's OID
    /// decide sameness instead. Also reads the session's role and search path (<c>$user</c>
    /// expanded), which the group passes are keyed by. Reads in the database's current
    /// transaction when it has one.
    /// </summary>
    private static async Task<ServerSession> ReadServerSessionAsync(
        DatabaseFacade database,
        RelationalObjectName holdsTable,
        CancellationToken ct
    )
    {
        await database.OpenConnectionAsync(ct);
        try
        {
            // The holds table's OID comes from the catalog by schema and name, not to_regclass:
            // resolving a name needs USAGE on its schema, and a role without it must reach the
            // group pass, which refuses it by name.
            await using var command = new SqlParams
            {
                ["holds_schema"] = holdsTable.Schema,
                ["holds_name"] = holdsTable.Name,
            }.CreateCommand(
                database.GetDbConnection(),
                database.CurrentTransaction?.GetDbTransaction(),
                """
                /* cohort catalog */
                SELECT pg_catalog.current_database()::text,
                       (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database())::bigint,
                       pg_catalog.pg_postmaster_start_time(),
                       (SELECT relation.oid
                        FROM pg_catalog.pg_class relation
                        JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
                        WHERE namespace.nspname = @holds_schema AND relation.relname = @holds_name)::bigint,
                       current_user::pg_catalog.text,
                       pg_catalog.array_to_string(pg_catalog.current_schemas(true)::pg_catalog.text[], ',')
                """
            );
            await using var reader = await command.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            return new ServerSession(
                new ServerDatabaseIdentity(
                    reader.GetString(0),
                    reader.GetInt64(1),
                    reader.GetFieldValue<DateTime>(2),
                    reader.IsDBNull(3) ? null : reader.GetInt64(3)
                ),
                reader.GetString(4),
                reader.GetString(5)
            );
        }
        finally
        {
            await database.CloseConnectionAsync();
        }
    }

    /// <summary>
    /// The worker, each run and each row-handler delivery hold PostgreSQL session advisory
    /// locks. Under Npgsql multiplexing a connection is bound to a server session only inside a
    /// transaction, so a lock taken outside one stays on whichever session ran it while the
    /// unlock runs on another and finds nothing to release. Checked on every call, before the
    /// entry is looked up; it reads the builder the entry key is built from, so it costs nothing
    /// extra. A wrapped DbConnection (a profiler, say) is checked by its connection string like
    /// any other. Only a string Npgsql rejects can't be inspected, so it is the host's to
    /// configure; <paramref name="settings"/> is null for one.
    /// </summary>
    private static void ValidateSessionLocks(NpgsqlConnectionStringBuilder? settings)
    {
        if (settings is { Multiplexing: true })
        {
            throw new RetentionConfigurationException([
                "Cohort can't run on an Npgsql connection with Multiplexing enabled: its worker, runs and row-handler deliveries hold PostgreSQL session advisory locks, and a multiplexed connection only keeps one server session inside a transaction, so those locks would be taken on one session and released on another. Give Cohort's DbContext a connection string without Multiplexing=true.",
            ]);
        }
    }

    // Null when Npgsql rejects the string (an unknown keyword, a malformed value), which a
    // wrapper can pass through for its own settings.
    private static NpgsqlConnectionStringBuilder? TryParseConnectionString(string connectionString)
    {
        try
        {
            return new NpgsqlConnectionStringBuilder(connectionString);
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    // The entry key for a string Npgsql rejects: any keyword is accepted, and the password
    // (Npgsql's synonyms; the builder's keys are case-insensitive) stays out of the key. Raw only
    // when even this parse throws ArgumentException (an unterminated quote, say).
    private static string KeyWithoutPassword(string connectionString)
    {
        try
        {
            var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
            builder.Remove("Password");
            builder.Remove("PWD");
            builder.Remove("PSW");
            return builder.ConnectionString;
        }
        catch (ArgumentException)
        {
            return connectionString;
        }
    }

    private void ValidateProvider()
    {
        string? providerName;
        try
        {
            providerName = db.Database.ProviderName;
        }
        catch (InvalidOperationException)
        {
            providerName = null;
        }

        if (
            !string.Equals(
                providerName,
                "Npgsql.EntityFrameworkCore.PostgreSQL",
                StringComparison.Ordinal
            )
        )
        {
            throw new RetentionConfigurationException([
                $"Cohort requires the Npgsql Entity Framework Core provider; configured provider is '{providerName ?? "<unknown>"}'.",
            ]);
        }
    }
}

internal sealed class RetentionRuntimeReadinessState
{
    // Per model instance, held weakly (like RetentionValidationState): the entry is exact for
    // whichever model the context runs, and a host whose EF model cache can't hold its models gets
    // a new instance per context that dies with its context instead of being kept alive here.
    // Within a model, the entry is keyed (ordinal) by the connection string without its
    // password: parsed once per call, no round trip, for every connection's string (a wrapped
    // DbConnection's too); a string Npgsql rejects keys by its raw string. It carries everything a
    // pooler can route by under one host, port, and database name (user, options). Two spellings
    // of one database each validate once.
    private readonly ConditionalWeakTable<
        IModel,
        ConcurrentDictionary<string, RetentionRuntimeReadinessEntry>
    > entries = new();

    // The database checks that passed, by value: one small key per distinct input per database
    // per check, never evicted. A rebuilt copy of a model that reads the same things skips them.
    private readonly ConcurrentDictionary<RetentionReadinessCheckKey, byte> passed = new();

    internal RetentionRuntimeReadinessEntry For(IModel model, string connectionString) =>
        entries
            .GetValue(
                model,
                static _ =>
                    new ConcurrentDictionary<string, RetentionRuntimeReadinessEntry>(StringComparer.Ordinal)
            )
            .GetOrAdd(connectionString, static _ => new RetentionRuntimeReadinessEntry());

    internal bool HasPassed(RetentionReadinessCheckKey key) => passed.ContainsKey(key);

    internal void RecordPassed(RetentionReadinessCheckKey key) => passed.TryAdd(key, 0);
}

/// <summary>
/// One group's statements as enumerated, and the inputs a pass over them is keyed by (the
/// statements by entity, name, text, and parameter types).
/// </summary>
internal sealed record GroupStatements(IReadOnlyList<ReadinessStatement> Statements, object[] Inputs);

internal sealed class RetentionRuntimeReadinessEntry
{
    internal SemaphoreSlim Gate { get; } = new(1, 1);

    /// <summary>The model pass passed outside a caller's transaction.</summary>
    internal volatile bool ModelValidated;

    /// <summary>
    /// Each group's statements, enumerated once per entry and role and kept only when they were
    /// enumerated outside a caller's transaction. Keyed by the group record, so a pruning group's
    /// scope is part of the key, and by the enumerating session's role, since the blocking-key
    /// read drops the child tables that role can't SELECT and so changes the statements' text.
    /// </summary>
    internal ConcurrentDictionary<(RetentionStatementGroup Group, string Role), GroupStatements> Enumerations { get; } =
        new();
}
