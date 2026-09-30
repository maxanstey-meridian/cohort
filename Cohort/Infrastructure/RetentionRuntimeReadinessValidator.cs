using System.Collections.Concurrent;
using Cohort.Application;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

internal sealed class RetentionRuntimeReadinessValidator(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionStartupValidator startupValidator,
    CohortSchemaValidator schemaValidator,
    RetentionRegistry registry,
    RetentionRuntimeReadinessState state
)
{
    internal IReadOnlyDictionary<string, Cohort.Domain.RetentionCategoryCapabilities>
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

            await startupValidator.ValidateAsync(ct);
            await schemaValidator.ValidateAsync(ct);
            await ValidateRecordIdColumnTypesAsync(ct);
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

    // Readiness depends on the model (and so Cohort's table mapping) and on the database it
    // runs against; Npgsql's DataSource already carries host and port.
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
}
