using System.Collections.Concurrent;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

internal sealed class RetentionRuntimeReadinessValidator(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionStartupValidator startupValidator,
    CohortSchemaValidator schemaValidator,
    RetentionRegistry registry,
    RetentionRuntimeReadinessState state,
    IServiceScopeFactory scopeFactory,
    IEnumerable<IAnonymiseValueFactory> anonymiseValueFactories
)
{
    // pg_input_is_valid, which the anonymise-target check relies on, arrived in PostgreSQL 16.
    private const int MinimumServerVersionNum = 160000;

    internal IReadOnlyDictionary<string, RetentionCategoryCapabilities>
        ValidatedCapabilities => startupValidator.ValidatedCapabilities;

    public async Task ValidateAsync(CancellationToken ct = default)
    {
        ValidateProvider();
        var readiness = state.For(CreateKey());
        if (readiness.Validated)
        {
            return;
        }

        await readiness.Gate.WaitAsync(ct);
        try
        {
            if (readiness.Validated)
            {
                return;
            }

            await ValidateServerVersionAsync(ct);
            await startupValidator.ValidateAsync(ct);
            await schemaValidator.ValidateAsync(ct);
            await ValidateRecordIdColumnTypesAsync(ct);
            await ValidateAnonymiseTargetsAsync(ct);
            // The hold check runs on its own connection, outside the caller's transaction, so its
            // success stands whatever transaction the caller is in; cache it apart from the checks
            // that read through the caller's connection.
            if (!readiness.StoredEntityIdentitiesValidated)
            {
                await ValidateStoredEntityIdentitiesAsync(ct);
                readiness.StoredEntityIdentitiesValidated = true;
            }

            if (db.Database.CurrentTransaction is null)
            {
                readiness.Validated = true;
            }
        }
        finally
        {
            readiness.Gate.Release();
        }
    }

    /// <summary>
    /// Cohort stores a record id as its column's text form. The model's store type name cannot
    /// show a domain, and the startup check only knows type names, so resolve each record-id
    /// column's catalog type through domains, arrays and ranges to the types it is built on.
    /// </summary>
    private async Task ValidateRecordIdColumnTypesAsync(CancellationToken ct)
    {
        var entries = registry.Scan().Values.ToArray();
        if (entries.Length == 0)
        {
            return;
        }

        await db.Database.OpenConnectionAsync(ct);
        try
        {
            await using var command = new SqlParams
            {
                ["schemas"] = entries.Select(entry => entry.Table.Schema).ToArray(),
                ["tables"] = entries.Select(entry => entry.Table.Name).ToArray(),
                ["columns"] = entries.Select(entry => entry.RecordId.RecordIdColumn).ToArray(),
            }.CreateCommand(
                db.Database.GetDbConnection(),
                db.Database.CurrentTransaction?.GetDbTransaction(),
                """
                WITH RECURSIVE record_id_column AS (
                    SELECT requested.ordinal, attribute.atttypid AS type_id
                    FROM ROWS FROM (
                        pg_catalog.unnest(@schemas),
                        pg_catalog.unnest(@tables),
                        pg_catalog.unnest(@columns)
                    ) WITH ORDINALITY AS requested(schema_name, table_name, column_name, ordinal)
                    JOIN pg_catalog.pg_namespace namespace ON namespace.nspname = requested.schema_name
                    JOIN pg_catalog.pg_class relation
                      ON relation.relnamespace = namespace.oid AND relation.relname = requested.table_name
                    JOIN pg_catalog.pg_attribute attribute
                      ON attribute.attrelid = relation.oid
                     AND attribute.attname = requested.column_name
                     AND attribute.attnum > 0
                     AND NOT attribute.attisdropped
                ),
                built_on(ordinal, type_id) AS (
                    SELECT ordinal, type_id FROM record_id_column
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
                SELECT built_on.ordinal, pgtype.typname
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
                    var typeName = reader.GetString(1);
                    if (!RetentionStartupValidator.HasSessionDependentText(typeName))
                    {
                        continue;
                    }

                    var entry = entries[(int)reader.GetInt64(0) - 1];
                    errors.Add(
                        $"Record-id convention on {entry.EntityType.FullName}: record-id column {PostgreSqlIdentifier.Format(entry.Table)}.{PostgreSqlIdentifier.Quote(entry.RecordId.RecordIdColumn)} is built on '{typeName}', whose text form depends on session settings (TimeZone, DateStyle, IntervalStyle, extra_float_digits), so holds and audit rows could silently stop matching. Use a uuid, integer, numeric, or text record id, or mark a stable unique column with [RetentionRecordId]."
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
            await db.Database.CloseConnectionAsync();
        }
    }

    /// <summary>
    /// The startup check knows only an anonymise target's CLR type; the column decides what the
    /// UPDATE can write. Ask the catalog, which sees domains, column types declared without a
    /// length and migration drift, for each target column's facts, and test each literal with
    /// the column type's own input function (typmod included) so that a literal the column can't
    /// take fails here instead of on every run.
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
                ["probes"] = targets.Select(target => ProbeValue(target.Entry, target.Field)).ToArray(),
            }.CreateCommand(
                db.Database.GetDbConnection(),
                db.Database.CurrentTransaction?.GetDbTransaction(),
                """
                SELECT requested.ordinal,
                       pg_catalog.format_type(attribute.atttypid, attribute.atttypmod),
                       attribute.attnotnull OR pgtype.typnotnull,
                       CASE WHEN requested.probe IS NULL THEN NULL
                            ELSE pg_catalog.pg_input_is_valid(
                                requested.probe,
                                pg_catalog.format_type(attribute.atttypid, attribute.atttypmod)
                            )
                       END,
                       EXISTS (
                           SELECT 1 FROM pg_catalog.pg_index unique_index
                           WHERE unique_index.indrelid = relation.oid
                             AND unique_index.indisunique
                             AND unique_index.indpred IS NULL
                             AND unique_index.indexprs IS NULL
                             AND attribute.attnum = ANY(unique_index.indkey)
                             AND NOT (record_id.attnum = ANY(unique_index.indkey))
                       ),
                       EXISTS (
                           SELECT 1 FROM pg_catalog.pg_index unique_index
                           WHERE unique_index.indrelid = relation.oid
                             AND unique_index.indisunique
                             AND unique_index.indnullsnotdistinct
                             AND unique_index.indpred IS NULL
                             AND unique_index.indexprs IS NULL
                             AND attribute.attnum = ANY(unique_index.indkey)
                             AND NOT (record_id.attnum = ANY(unique_index.indkey))
                       ),
                       EXISTS (
                           SELECT 1 FROM pg_catalog.pg_constraint foreign_key
                           WHERE foreign_key.contype = 'f'
                             AND foreign_key.conrelid = relation.oid
                             AND attribute.attnum = ANY(foreign_key.conkey)
                       ),
                       EXISTS (
                           SELECT 1 FROM pg_catalog.pg_constraint foreign_key
                           WHERE foreign_key.contype = 'f'
                             AND foreign_key.confrelid = relation.oid
                             AND attribute.attnum = ANY(foreign_key.confkey)
                       )
                FROM ROWS FROM (
                    pg_catalog.unnest(@schemas),
                    pg_catalog.unnest(@tables),
                    pg_catalog.unnest(@columns),
                    pg_catalog.unnest(@record_id_columns),
                    pg_catalog.unnest(@probes)
                ) WITH ORDINALITY AS requested(schema_name, table_name, column_name, record_id_column, probe, ordinal)
                JOIN pg_catalog.pg_namespace namespace ON namespace.nspname = requested.schema_name
                JOIN pg_catalog.pg_class relation
                  ON relation.relnamespace = namespace.oid AND relation.relname = requested.table_name
                JOIN pg_catalog.pg_attribute attribute
                  ON attribute.attrelid = relation.oid
                 AND attribute.attname = requested.column_name
                 AND attribute.attnum > 0
                 AND NOT attribute.attisdropped
                JOIN pg_catalog.pg_type pgtype ON pgtype.oid = attribute.atttypid
                JOIN pg_catalog.pg_attribute record_id
                  ON record_id.attrelid = relation.oid
                 AND record_id.attname = requested.record_id_column
                 AND record_id.attnum > 0
                 AND NOT record_id.attisdropped
                """
            );

            var errors = new SortedSet<string>(StringComparer.Ordinal);
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var (entry, field) = targets[(int)reader.GetInt64(0) - 1];
                    var columnType = reader.GetString(1);
                    var notNull = reader.GetBoolean(2);
                    var probeValid = reader.IsDBNull(3) || reader.GetBoolean(3);
                    var unfilteredUnique = reader.GetBoolean(4);
                    var nullsNotDistinctUnique = reader.GetBoolean(5);
                    var foreignKeyChild = reader.GetBoolean(6);
                    var referencedKey = reader.GetBoolean(7);

                    var method = field is AnonymiseLiteralField literal ? literal.Method : (AnonymiseMethod?)null;
                    var writesOneValue =
                        method is AnonymiseMethod.FixedLiteral or AnonymiseMethod.EmptyString
                        || field is AnonymiseFactoryField factoryField && IsStaticFactory(factoryField);
                    var target =
                        $"Anonymise convention on {entry.EntityType.FullName}: {(field is AnonymiseFactoryField ? "[AnonymiseWith]" : "[Anonymise]")} member {field.MemberName} writes column {PostgreSqlIdentifier.Format(entry.Table)}.{PostgreSqlIdentifier.Quote(field.ColumnName)}";

                    if (!probeValid)
                    {
                        var value = method is AnonymiseMethod.EmptyString
                            ? "the empty string"
                            : $"the literal '{((AnonymiseLiteralField)field).Literal}'";
                        errors.Add(
                            $"{target}, but {value} is not valid for its column type {columnType} (too long, the wrong shape, or refused by a domain check), so every anonymise of these rows would fail. Change the literal or the column."
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
                            $"{target}, which a unique index covers without the record id or a filter, but every anonymised row would get the same value, so the second row anonymised would fail. Filter the index on the anonymised-at column (WHERE \"{entry.AnonymisedAt?.AnonymisedAtColumn ?? "AnonymisedAt"}\" IS NULL), include the record id in it, or use a per-row [AnonymiseWith] factory."
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
    /// The text the column's input function must accept: the literal as the UPDATE sends it,
    /// after the property's value converter. Null, factories and a literal whose provider value
    /// isn't text have no probe; their structural checks still run.
    /// </summary>
    private string? ProbeValue(RetentionEntry entry, AnonymiseField field)
    {
        if (field is not AnonymiseLiteralField literal)
        {
            return null;
        }

        string? value = literal.Method switch
        {
            AnonymiseMethod.EmptyString => string.Empty,
            AnonymiseMethod.FixedLiteral => literal.Literal,
            _ => null,
        };
        if (value is null)
        {
            return null;
        }

        var converter = db
            .Model.FindEntityType(entry.EntityType)
            ?.FindProperty(field.MemberName)
            ?.GetTypeMapping()
            .Converter;
        return (converter?.ConvertToProvider(value) ?? value) as string;
    }

    private bool IsStaticFactory(AnonymiseFactoryField field) =>
        anonymiseValueFactories.FirstOrDefault(factory => factory.GetType() == field.FactoryType)
            ?.ExecutionMode
        is AnonymiseFactoryExecutionMode.Static;

    /// <summary>
    /// Cohort's catalog checks call <c>pg_input_is_valid</c>, which PostgreSQL 16 added, so
    /// refuse an older server before any of them runs.
    /// </summary>
    private async Task ValidateServerVersionAsync(CancellationToken ct)
    {
        await db.Database.OpenConnectionAsync(ct);
        try
        {
            await using var command = new SqlParams().CreateCommand(
                db.Database.GetDbConnection(),
                db.Database.CurrentTransaction?.GetDbTransaction(),
                "SELECT pg_catalog.current_setting('server_version_num')::integer, pg_catalog.current_setting('server_version')"
            );
            await using var reader = await command.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            if (reader.GetInt32(0) < MinimumServerVersionNum)
            {
                throw new RetentionConfigurationException([
                    $"Cohort requires PostgreSQL 16 or later; the database server is PostgreSQL {reader.GetString(1)}.",
                ]);
            }
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }
    }

    /// <summary>
    /// A hold names its entity by <c>[RetentionEntityId]</c>, which lives only in code. Once no
    /// retained entity declares that identity, no sweep, erasure or deletion matches the hold
    /// again and its row loses protection silently, so refuse while an active hold names one.
    /// Holds span tenants, so this runs in its own <see cref="RetentionSystemWork.Readiness"/>
    /// scope whatever scope readiness was called from: a tenant's scope would see only that
    /// tenant's holds under row-level security. It also runs when no entity is retained, since
    /// removing the last <c>[Retain]</c> orphans holds the same way.
    /// </summary>
    private async Task ValidateStoredEntityIdentitiesAsync(CancellationToken ct)
    {
        var registered = registry.Scan().Values.Select(entry => entry.RetentionEntityId).ToArray();
        var holds = PostgreSqlIdentifier.Format(CohortStoreTables.FromModel(db.Model).RetentionHolds);

        var callerDatabase = await ReadServerDatabaseIdentityAsync(db, holds, ct);
        await scopeFactory.RunInScopeAsync(
            new RetentionExecutionContext.System(RetentionSystemWork.Readiness),
            services => ValidateStoredEntityIdentitiesAsync(
                services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext),
                registered,
                holds,
                callerDatabase,
                ct
            ),
            ct
        );
    }

    private async Task ValidateStoredEntityIdentitiesAsync(
        DbContext systemDb,
        Guid[] registered,
        string holds,
        ServerDatabaseIdentity callerDatabase,
        CancellationToken ct
    )
    {
        await systemDb.Database.OpenConnectionAsync(ct);
        try
        {
            if (
                !ReferenceEquals(systemDb.Model, db.Model)
                || await ReadServerDatabaseIdentityAsync(systemDb, holds, ct) != callerDatabase
            )
            {
                throw new RetentionConfigurationException([
                    "Cohort's readiness scope resolved a DbContext with a different model or database from the calling scope's. Cohort checks holds across tenants in a System(Readiness) scope, so every scope must reach the same database and model.",
                ]);
            }

            await using var command = new SqlParams { ["registered"] = registered }.CreateCommand(
                systemDb.Database.GetDbConnection(),
                null,
                $"""
                SELECT hold."RetentionEntityId", pg_catalog.count(*)
                FROM {holds} AS hold
                WHERE NOT (hold."RetentionEntityId" = ANY(@registered))
                  AND {RetentionHoldSql.ActivePredicate("hold")}
                GROUP BY hold."RetentionEntityId"
                ORDER BY hold."RetentionEntityId"
                """
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
            await systemDb.Database.CloseConnectionAsync();
        }
    }

    /// <summary>
    /// Which database a context's connection reaches, as the server sees it. Connection strings
    /// spell one database many ways (<c>localhost</c> or <c>127.0.0.1</c>, a pooler or the direct
    /// port), so the database name, the server's start time and the hold table's OID decide
    /// sameness instead. Reads in the context's current transaction when it has one.
    /// </summary>
    private static async Task<ServerDatabaseIdentity> ReadServerDatabaseIdentityAsync(
        DbContext context,
        string holds,
        CancellationToken ct
    )
    {
        await context.Database.OpenConnectionAsync(ct);
        try
        {
            await using var command = new SqlParams { ["holds"] = holds }.CreateCommand(
                context.Database.GetDbConnection(),
                context.Database.CurrentTransaction?.GetDbTransaction(),
                """
                SELECT pg_catalog.current_database()::text,
                       pg_catalog.pg_postmaster_start_time(),
                       pg_catalog.to_regclass(@holds)::oid::bigint
                """
            );
            await using var reader = await command.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            return new ServerDatabaseIdentity(
                reader.GetString(0),
                reader.GetFieldValue<DateTime>(1),
                reader.IsDBNull(2) ? null : reader.GetInt64(2)
            );
        }
        finally
        {
            await context.Database.CloseConnectionAsync();
        }
    }

    private sealed record ServerDatabaseIdentity(
        string Database,
        DateTime ServerStartedAt,
        long? HoldsTableOid
    );

    // Readiness depends on the model (and so Cohort's table mapping) and on the database it
    // runs against; Npgsql's DataSource already carries host and port. This only keys the
    // cache, so two spellings of one database each validate once; whether two contexts reach
    // the same database is decided on the server.
    private RetentionRuntimeReadinessKey CreateKey()
    {
        var connection = db.Database.GetDbConnection();
        return new RetentionRuntimeReadinessKey(db.Model, connection.DataSource, connection.Database);
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
    private readonly ConcurrentDictionary<
        RetentionRuntimeReadinessKey,
        RetentionRuntimeReadinessEntry
    > entries = new();

    internal RetentionRuntimeReadinessEntry For(RetentionRuntimeReadinessKey key) =>
        entries.GetOrAdd(key, static _ => new RetentionRuntimeReadinessEntry());
}

internal sealed record RetentionRuntimeReadinessKey(
    Microsoft.EntityFrameworkCore.Metadata.IModel Model,
    string DataSource,
    string Database
);

internal sealed class RetentionRuntimeReadinessEntry
{
    internal SemaphoreSlim Gate { get; } = new(1, 1);

    internal volatile bool Validated;

    /// <summary>
    /// The cross-tenant hold check passed. Set even when the caller was in a transaction, because
    /// that check never reads through the caller's connection.
    /// </summary>
    internal volatile bool StoredEntityIdentitiesValidated;
}
