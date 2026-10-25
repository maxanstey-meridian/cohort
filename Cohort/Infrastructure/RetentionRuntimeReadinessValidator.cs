using System.Collections.Concurrent;

using Cohort.Application;
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
    IServiceScopeFactory scopeFactory
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

        await using var scope = await scopeFactory.CreateEnteredScopeAsync(
            new RetentionExecutionContext.System(RetentionSystemWork.Readiness),
            ct
        );
        var systemDb = scope.ServiceProvider.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext);
        var callerDatabase = await ReadServerDatabaseIdentityAsync(db, holds, ct);
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
