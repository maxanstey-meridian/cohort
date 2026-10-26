using System.Collections.Concurrent;
using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
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
    AnonymiseAssignmentResolver assignments
)
{
    // pg_input_is_valid, which the anonymise-target check relies on, arrived in PostgreSQL 16.
    private static readonly Version MinimumServerVersion = new(16, 0);

    internal IReadOnlyDictionary<string, RetentionCategoryCapabilities>
        ValidatedCapabilities => startupValidator.ValidatedCapabilities;

    public async Task ValidateAsync(CancellationToken ct = default)
    {
        ValidateProvider();
        ValidateSessionLocks();
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
            await ValidateRetainedTablesAsync(ct);
            await ValidateAnonymiseTargetsAsync(ct);
            await ValidateReferencedMarkerColumnsAsync(ct);
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
    /// Each retained table must exist as an ordinary or partitioned table, since Cohort changes
    /// its rows in place and addresses each by its physical position (ctid), and its record-id
    /// column must exist. Cohort stores a record id as its column's text form; the model's store
    /// type name cannot show a domain, and the startup check only knows type names, so each
    /// record-id column's catalog type is resolved through domains, arrays and ranges to the
    /// types it is built on, which must not depend on session settings.
    /// </summary>
    private async Task ValidateRetainedTablesAsync(CancellationToken ct)
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
                    SELECT requested.ordinal,
                           relation.oid AS relation_id,
                           relation.relkind AS relation_kind,
                           attribute.atttypid AS type_id
                    FROM ROWS FROM (
                        pg_catalog.unnest(@schemas),
                        pg_catalog.unnest(@tables),
                        pg_catalog.unnest(@columns)
                    ) WITH ORDINALITY AS requested(schema_name, table_name, column_name, ordinal)
                    LEFT JOIN pg_catalog.pg_namespace namespace ON namespace.nspname = requested.schema_name
                    LEFT JOIN pg_catalog.pg_class relation
                      ON relation.relnamespace = namespace.oid AND relation.relname = requested.table_name
                    LEFT JOIN pg_catalog.pg_attribute attribute
                      ON attribute.attrelid = relation.oid
                     AND attribute.attname = requested.column_name
                     AND attribute.attnum > 0
                     AND NOT attribute.attisdropped
                ),
                built_on(ordinal, type_id) AS (
                    SELECT ordinal, type_id FROM record_id_column WHERE type_id IS NOT NULL
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
                SELECT ordinal, NULL::pg_catalog.name, relation_id IS NULL, NULL::pg_catalog.char
                FROM record_id_column
                WHERE type_id IS NULL
                UNION ALL
                SELECT ordinal, NULL, NULL, relation_kind
                FROM record_id_column
                WHERE relation_kind NOT IN ('r', 'p')
                UNION ALL
                SELECT built_on.ordinal, pgtype.typname, NULL, NULL
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
                    var entry = entries[(int)reader.GetInt64(0) - 1];
                    if (!reader.IsDBNull(3))
                    {
                        errors.Add(
                            $"Retained entity {entry.EntityType.FullName}: {PostgreSqlIdentifier.Format(entry.Table)} is {NotATable(reader.GetChar(3))}, so Cohort can't retain its rows. Map the entity to the table it lives in."
                        );
                        continue;
                    }

                    if (reader.IsDBNull(1))
                    {
                        errors.Add(
                            reader.GetBoolean(2)
                                ? $"Retained entity {entry.EntityType.FullName}: table {PostgreSqlIdentifier.Format(entry.Table)} was not found in the database, so every run would fail. Apply the host application's pending EF Core migrations, or map the entity to the table it lives in."
                                : $"Record-id convention on {entry.EntityType.FullName}: record-id column {PostgreSqlIdentifier.Format(entry.Table)}.{PostgreSqlIdentifier.Quote(entry.RecordId.RecordIdColumn)} was not found in the database, so every run would fail. Apply the host application's pending EF Core migrations, or map the record id to the column it lives in."
                        );
                        continue;
                    }

                    var typeName = reader.GetString(1);
                    if (!RetentionStartupValidator.HasSessionDependentText(typeName))
                    {
                        continue;
                    }

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

    private static string NotATable(char kind) =>
        kind switch
        {
            'v' => "a view, which has no physical row positions (ctid) for Cohort to address its rows by",
            'm' => "a materialized view, whose rows can't be updated or deleted",
            'f' => "a foreign table, whose row positions (ctid) and versions (xmin) are whatever its foreign-data wrapper supplies, if anything, so they don't reliably identify its rows",
            _ => $"a relation of kind '{kind}', not a table",
        };

    /// <summary>
    /// The startup check knows only an anonymise target's CLR type; the column decides what the
    /// UPDATE can write. Ask the catalog, which sees domains (nested ones included), column types
    /// declared without a length and migration drift, for each target column's facts. The UPDATE
    /// casts each value to the model's store type and PostgreSQL then assigns it to the column,
    /// so the model's type must reach the column's without an explicit cast, and each literal is
    /// tested with both types' input functions (the column's typmod included), so that a value
    /// the column can't take fails here instead of on every run.
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
                -- Each type through its domains to its base type (level 0) and, for an array, its
                -- element type through the element's domains (level 1), as PostgreSQL coerces.
                type_chain(type_id, level, step_id) AS (
                    SELECT pgtype.oid, 0, pgtype.oid
                    FROM pg_catalog.pg_type pgtype
                    WHERE pgtype.oid IN (SELECT atttypid FROM requested UNION SELECT cast_type_id FROM requested)
                    UNION ALL
                    SELECT chain.type_id,
                           CASE WHEN step.typtype = 'd' THEN chain.level ELSE 1 END,
                           CASE WHEN step.typtype = 'd' THEN step.typbasetype ELSE step.typelem END
                    FROM type_chain chain
                    JOIN pg_catalog.pg_type step ON step.oid = chain.step_id
                    WHERE step.typtype = 'd' OR (chain.level = 0 AND step.typcategory = 'A')
                ),
                type_base AS (
                    SELECT chain.type_id,
                           pg_catalog.bool_or(step.typnotnull) FILTER (WHERE chain.level = 0) AS not_null,
                           pg_catalog.max(step.oid) FILTER (WHERE chain.level = 0 AND step.typtype <> 'd') AS base_id,
                           pg_catalog.max(step.typcategory::text) FILTER (WHERE chain.level = 0 AND step.typtype <> 'd') AS base_category,
                           pg_catalog.max(step.oid) FILTER (WHERE chain.level = 1 AND step.typtype <> 'd') AS element_id,
                           pg_catalog.max(step.typcategory::text) FILTER (WHERE chain.level = 1 AND step.typtype <> 'd') AS element_category
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
                       -- PostgreSQL's assignment coercion (find_coercion_pathway): domains reduce
                       -- to their base types; a pg_cast row decides alone; arrays coerce through
                       -- their elements; any type reaches a string type through text I/O.
                       CASE WHEN requested.cast_type IS NULL THEN true
                            WHEN requested.cast_type_id IS NULL THEN NULL
                            WHEN cast_type.base_id = column_type.base_id THEN true
                            WHEN base_cast.oid IS NOT NULL THEN base_cast.castcontext IN ('a', 'i')
                            WHEN cast_type.base_category = 'A' AND column_type.base_category = 'A' THEN
                                 cast_type.element_id = column_type.element_id
                                 OR CASE WHEN element_cast.oid IS NOT NULL THEN element_cast.castcontext IN ('a', 'i')
                                         ELSE column_type.element_category = 'S'
                                    END
                            ELSE column_type.base_category = 'S'
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
                LEFT JOIN type_base cast_type ON cast_type.type_id = requested.cast_type_id
                LEFT JOIN pg_catalog.pg_cast base_cast
                  ON base_cast.castsource = cast_type.base_id AND base_cast.casttarget = column_type.base_id
                LEFT JOIN pg_catalog.pg_cast element_cast
                  ON element_cast.castsource = cast_type.element_id
                 AND element_cast.casttarget = column_type.element_id
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
                    // The record-id check, which runs first, refuses a missing table or record id.
                    if (!reader.GetBoolean(1))
                    {
                        continue;
                    }

                    var target = TargetDescription(entry, field);
                    if (!reader.GetBoolean(2))
                    {
                        errors.Add(
                            $"{target}, which was not found in the database, so every anonymise of these rows would fail. Apply the host application's pending EF Core migrations, or map the member to the column it lives in."
                        );
                        continue;
                    }

                    var columnType = reader.GetString(3);
                    var notNull = reader.GetBoolean(4);
                    var probeValidForColumn = reader.IsDBNull(5) || reader.GetBoolean(5);
                    var probeValidForStoreType = reader.IsDBNull(6) || reader.GetBoolean(6);
                    bool? storeTypeAssignable = reader.IsDBNull(7) ? null : reader.GetBoolean(7);
                    var unfilteredUnique = reader.GetBoolean(8);
                    var nullsNotDistinctUnique = reader.GetBoolean(9);
                    var foreignKeyChild = reader.GetBoolean(10);
                    var referencedKey = reader.GetBoolean(11);

                    var method = field is AnonymiseLiteralField literal ? literal.Method : (AnonymiseMethod?)null;
                    var writesOneValue =
                        method is AnonymiseMethod.FixedLiteral or AnonymiseMethod.EmptyString
                        || field is AnonymiseFactoryField factoryField && assignments.IsStatic(factoryField);
                    var castType = PostgresStoreTypeSql.CastType(field.StoreType);

                    if (storeTypeAssignable is null)
                    {
                        errors.Add(
                            $"{target}, whose model store type {castType} is not a type the database knows, so every anonymise of these rows would fail. Bring the model and the column back in line."
                        );
                    }
                    else if (storeTypeAssignable is false)
                    {
                        errors.Add(
                            $"{target}, whose model store type {castType} can't be assigned to its column type {columnType} (Cohort's UPDATE casts the value to the model's type, and PostgreSQL has no assignment cast from it), so every anonymise of these rows would fail. Bring the model and the column back in line."
                        );
                    }

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
    /// An update must never change a key another table references: the foreign key would refuse
    /// it while a row references it, or silently rewrite the referencing rows. The anonymise
    /// check covers the anonymised columns; this covers the columns the update strategies always
    /// write, the soft-delete flag and deleted-at, and the anonymised-at marker.
    /// </summary>
    private async Task ValidateReferencedMarkerColumnsAsync(CancellationToken ct)
    {
        var markers = registry
            .Scan()
            .Values.Where(entry => ValidatedCapabilities.ContainsKey(entry.Category))
            .SelectMany(entry =>
            {
                var strategies = ValidatedCapabilities[entry.Category].Strategies;
                var columns = new List<(RetentionEntry Entry, string Strategy, string Member, string Column)>();
                if (strategies.Contains(Strategy.SoftDelete) && entry.SoftDelete is { } softDelete)
                {
                    columns.Add((entry, "soft-deleting", softDelete.IsDeletedMember, softDelete.IsDeletedColumn));
                    if (softDelete is { DeletedAtMember: { } member, DeletedAtColumn: { } column })
                    {
                        columns.Add((entry, "soft-deleting", member, column));
                    }
                }

                if (strategies.Contains(Strategy.Anonymise) && entry.AnonymisedAt is { } anonymisedAt)
                {
                    columns.Add((entry, "anonymising", anonymisedAt.AnonymisedAtMember, anonymisedAt.AnonymisedAtColumn));
                }

                return columns;
            })
            .ToArray();
        if (markers.Length == 0)
        {
            return;
        }

        await db.Database.OpenConnectionAsync(ct);
        try
        {
            await using var command = new SqlParams
            {
                ["schemas"] = markers.Select(marker => marker.Entry.Table.Schema).ToArray(),
                ["tables"] = markers.Select(marker => marker.Entry.Table.Name).ToArray(),
                ["columns"] = markers.Select(marker => marker.Column).ToArray(),
            }.CreateCommand(
                db.Database.GetDbConnection(),
                db.Database.CurrentTransaction?.GetDbTransaction(),
                """
                SELECT requested.ordinal
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
                WHERE EXISTS (
                    SELECT 1 FROM pg_catalog.pg_constraint foreign_key
                    WHERE foreign_key.contype = 'f'
                      AND foreign_key.confrelid = relation.oid
                      AND attribute.attnum = ANY(foreign_key.confkey)
                )
                """
            );

            var errors = new SortedSet<string>(StringComparer.Ordinal);
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var (entry, strategy, member, column) = markers[(int)reader.GetInt64(0) - 1];
                    errors.Add(
                        $"Retained entity {entry.EntityType.FullName}: member {member} writes column {PostgreSqlIdentifier.Format(entry.Table)}.{PostgreSqlIdentifier.Quote(column)}, which is referenced by a foreign key, so {strategy} a row would be refused while a row references it or would silently rewrite the referencing rows (ON UPDATE CASCADE or SET NULL). Stop referencing this column."
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
    private async Task ValidateServerVersionAsync(CancellationToken ct)
    {
        await db.Database.OpenConnectionAsync(ct);
        try
        {
            // Asked of the server rather than read off NpgsqlConnection: a host may hand EF a
            // wrapped DbConnection (a profiler, say).
            var (version, versionText) = await ReadServerVersionAsync(db.Database.GetDbConnection(), ct);
            if (version < MinimumServerVersion)
            {
                throw new RetentionConfigurationException([
                    $"Cohort requires PostgreSQL 16 or later; the database server is PostgreSQL {versionText}.",
                ]);
            }
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }
    }

    private async Task<(Version Version, string Text)> ReadServerVersionAsync(
        DbConnection connection,
        CancellationToken ct
    )
    {
        await using var command = new SqlParams().CreateCommand(
            connection,
            db.Database.CurrentTransaction?.GetDbTransaction(),
            "SELECT pg_catalog.current_setting('server_version_num')::integer, pg_catalog.current_setting('server_version')"
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

    /// <summary>
    /// The worker, each run and each row-handler delivery hold PostgreSQL session advisory
    /// locks. Under Npgsql multiplexing a connection is bound to a server session only inside a
    /// transaction, so a lock taken outside one stays on whichever session ran it while the
    /// unlock runs on another and finds nothing to release. Checked on every call, not cached:
    /// the readiness key doesn't carry connection settings. A wrapped DbConnection (a profiler,
    /// say) can't be inspected, so it is the host's to configure.
    /// </summary>
    private void ValidateSessionLocks()
    {
        if (db.Database.GetDbConnection() is NpgsqlConnection connection
            && new NpgsqlConnectionStringBuilder(connection.ConnectionString).Multiplexing)
        {
            throw new RetentionConfigurationException([
                "Cohort can't run on an Npgsql connection with Multiplexing enabled: its worker, runs and row-handler deliveries hold PostgreSQL session advisory locks, and a multiplexed connection only keeps one server session inside a transaction, so those locks would be taken on one session and released on another. Give Cohort's DbContext a connection string without Multiplexing=true.",
            ]);
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
