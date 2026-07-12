using System.Data;
using System.Data.Common;

using Cohort.Application;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

internal sealed class CohortSchemaValidator(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db
)
{
    public async Task ValidateAsync(CancellationToken ct)
    {
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;
        Exception? primaryException = null;

        try
        {
            if (shouldCloseConnection)
            {
                await db.Database.OpenConnectionAsync(ct);
            }

            var storeTables = CohortStoreTables.FromModel(db.Model);
            var existingTransaction = db.Database.CurrentTransaction;
            await using var ownedTransaction = existingTransaction is null
                ? await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, ct)
                : null;
            var transaction = existingTransaction?.GetDbTransaction() ?? ownedTransaction!;
            var tables = await ResolveTablesAsync(connection, transaction, storeTables, ct);
            var columns = await ReadColumnsAsync(connection, transaction, tables.Values, ct);
            var indexes = await ReadIndexesAsync(connection, transaction, tables.Values, ct);
            var checkConstraints = await ReadCheckConstraintsAsync(
                connection,
                transaction,
                tables.Values,
                ct
            );
            var foreignKeys = await ReadForeignKeysAsync(connection, transaction, tables.Values, ct);
            if (ownedTransaction is not null)
            {
                await ownedTransaction.CommitAsync(ct);
            }
            var missing = new List<string>();

            foreach (var table in CohortSchemaContract.Tables)
            {
                var mappedTable = table.ResolveStoreTable(storeTables);
                if (!tables.TryGetValue(table.Role, out var tableId))
                {
                    missing.Add($"table '{PostgreSqlIdentifier.Format(mappedTable)}'");
                    continue;
                }

                foreach (var column in table.Columns)
                {
                    if (!columns.TryGetValue((tableId, column.Name), out var actual))
                    {
                        missing.Add(
                            $"column '{PostgreSqlIdentifier.Format(mappedTable)}.{PostgreSqlIdentifier.Quote(column.Name)}'"
                        );
                        continue;
                    }

                    if (!IsCompatibleType(column.CatalogType, actual)
                        || actual.Nullable != column.Nullable
                        || column.Generated && !actual.Generated)
                    {
                        missing.Add(
                            $"column capability '{table.Role}.{PostgreSqlIdentifier.Quote(column.Name)} {column.CatalogType} {(column.Nullable ? "NULL" : "NOT NULL")}{(column.Generated ? " GENERATED" : "")}' on table '{PostgreSqlIdentifier.Format(mappedTable)}'"
                        );
                    }
                }

                if (!indexes.Any(index => index.TableId == tableId
                    && index.Unique
                    && index.Primary
                    && index.Columns.SequenceEqual(table.PrimaryKey)))
                {
                    missing.Add(
                        $"primary key capability '{table.Role}({string.Join(", ", table.PrimaryKey)})' on table '{PostgreSqlIdentifier.Format(mappedTable)}'"
                    );
                }

                foreach (var indexRequirement in table.RequiredIndexes)
                {
                    if (!indexes.Any(index => index.TableId == tableId
                        && index.Unique == indexRequirement.Unique
                        && !index.Primary
                        && index.Columns.SequenceEqual(indexRequirement.Columns)
                        && index.Predicate == NormalizePredicate(indexRequirement.Predicate)))
                    {
                        missing.Add(
                            $"index capability '{table.Role}({string.Join(", ", indexRequirement.Columns)})' on table '{PostgreSqlIdentifier.Format(mappedTable)}'"
                        );
                    }
                }

                foreach (var checkRequirement in table.RequiredChecks)
                {
                    if (!checkConstraints.Any(constraint => constraint.TableId == tableId
                        && constraint.Name == checkRequirement.Name
                        && constraint.Validated
                        && constraint.Expression == NormalizeSql(checkRequirement.NormalizedSql)))
                    {
                        missing.Add(
                            $"check constraint capability '{table.Role}.{checkRequirement.Name}' on table '{PostgreSqlIdentifier.Format(mappedTable)}'"
                        );
                    }
                }

                foreach (var foreignKeyRequirement in table.RequiredForeignKeys)
                {
                    if (!tables.TryGetValue(
                            foreignKeyRequirement.PrincipalTable,
                            out var principalTableId
                        ))
                    {
                        continue;
                    }

                    if (!foreignKeys.Any(foreignKey => foreignKey.TableId == tableId
                        && foreignKey.Columns.SequenceEqual(foreignKeyRequirement.Columns)
                        && foreignKey.ReferencedTableId == principalTableId
                        && foreignKey.ReferencedColumns.SequenceEqual(
                            foreignKeyRequirement.PrincipalColumns
                        )
                        && foreignKey.DeleteAction == foreignKeyRequirement.CatalogDeleteAction
                        && foreignKey.Validated))
                    {
                        var principalTable = CohortSchemaContract
                            .GetTable(foreignKeyRequirement.PrincipalTable)
                            .ResolveStoreTable(storeTables);
                        missing.Add(
                            $"foreign key capability '{table.Role}({string.Join(", ", foreignKeyRequirement.Columns)}) -> {foreignKeyRequirement.PrincipalTable}({string.Join(", ", foreignKeyRequirement.PrincipalColumns)}) ON DELETE {DescribeDeleteAction(foreignKeyRequirement.CatalogDeleteAction)}' on tables '{PostgreSqlIdentifier.Format(mappedTable)}' -> '{PostgreSqlIdentifier.Format(principalTable)}'"
                        );
                    }
                }
            }

            if (missing.Count != 0)
            {
                throw new RetentionConfigurationException([
                    $"The configured PostgreSQL schema is missing Cohort runtime capabilities: {string.Join(", ", missing)}. Apply the host application's pending EF Core migrations before starting Cohort.",
                ]);
            }
        }
        catch (Exception ex)
        {
            primaryException = ex;
            throw;
        }
        finally
        {
            await OperationalConnectionCleanup.RunAsync(
                null,
                shouldCloseConnection
                    ? cleanupToken => db.Database.CloseConnectionAsync().WaitAsync(cleanupToken)
                    : null,
                primaryException,
                null
            );
        }
    }

    private static string DescribeDeleteAction(char deleteAction)
    {
        return deleteAction switch
        {
            'a' => "NO ACTION",
            'r' => "RESTRICT",
            'c' => "CASCADE",
            'n' => "SET NULL",
            'd' => "SET DEFAULT",
            _ => throw new ArgumentOutOfRangeException(nameof(deleteAction)),
        };
    }

    private static async Task<Dictionary<string, uint>> ResolveTablesAsync(
        DbConnection connection,
        DbTransaction transaction,
        CohortStoreTables tables,
        CancellationToken ct
    )
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT requested.role, relation.oid
            FROM ROWS FROM (
                pg_catalog.unnest(@roles),
                pg_catalog.unnest(@schemas),
                pg_catalog.unnest(@tables)
            ) AS requested(role, schema_name, table_name)
            JOIN pg_catalog.pg_namespace namespace
              ON namespace.nspname = requested.schema_name
            JOIN pg_catalog.pg_class relation
              ON relation.relnamespace = namespace.oid
             AND relation.relname = requested.table_name
             AND relation.relkind IN ('r', 'p')
            """;
        AddTablesParameters(command, tables);

        var result = new Dictionary<string, uint>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (!reader.IsDBNull(1))
            {
                result.Add(reader.GetString(0), reader.GetFieldValue<uint>(1));
            }
        }

        return result;
    }

    private static async Task<Dictionary<(uint TableId, string Column), ColumnStructure>> ReadColumnsAsync(
        DbConnection connection,
        DbTransaction transaction,
        IEnumerable<uint> tableIds,
        CancellationToken ct
    )
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT attrelid, attname, typname, atttypmod, NOT attnotnull,
                   attidentity <> '' OR COALESCE(
                        pg_catalog.pg_get_expr(attrdef.adbin, attrdef.adrelid) LIKE 'nextval(%',
                       FALSE
                   )
            FROM pg_catalog.pg_attribute attribute
            JOIN pg_catalog.pg_type ON pg_type.oid = atttypid
            LEFT JOIN pg_catalog.pg_attrdef attrdef
              ON attrdef.adrelid = attrelid AND attrdef.adnum = attnum
            WHERE attrelid = ANY (ARRAY(
                SELECT value::pg_catalog.oid
                FROM pg_catalog.unnest(@tableIds) AS value
            ))
              AND attnum > 0 AND NOT attisdropped
            """;
        AddTableIdsParameter(command, tableIds);

        var result = new Dictionary<(uint, string), ColumnStructure>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(
                (reader.GetFieldValue<uint>(0), reader.GetString(1)),
                new ColumnStructure(
                    reader.GetString(2),
                    reader.GetInt32(3),
                    reader.GetBoolean(4),
                    reader.GetBoolean(5)
                )
            );
        }

        return result;
    }

    private static async Task<List<IndexStructure>> ReadIndexesAsync(
        DbConnection connection,
        DbTransaction transaction,
        IEnumerable<uint> tableIds,
        CancellationToken ct
    )
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT i.indrelid, i.indisunique, i.indisprimary,
                   ARRAY(SELECT a.attname
                          FROM pg_catalog.unnest(i.indkey) WITH ORDINALITY AS key(attnum, position)
                           JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = key.attnum
                          WHERE key.position <= i.indnkeyatts
                          ORDER BY key.position),
                    pg_catalog.pg_get_expr(i.indpred, i.indrelid)
            FROM pg_catalog.pg_index i
            WHERE i.indrelid = ANY (ARRAY(
                SELECT value::pg_catalog.oid
                FROM pg_catalog.unnest(@tableIds) AS value
            ))
              AND i.indisvalid AND i.indisready
            """;
        AddTableIdsParameter(command, tableIds);

        var result = new List<IndexStructure>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new IndexStructure(
                reader.GetFieldValue<uint>(0),
                reader.GetBoolean(1),
                reader.GetBoolean(2),
                reader.GetFieldValue<string[]>(3),
                NormalizePredicate(reader.IsDBNull(4) ? null : reader.GetString(4))
            ));
        }

        return result;
    }

    private static async Task<List<CheckConstraintStructure>> ReadCheckConstraintsAsync(
        DbConnection connection,
        DbTransaction transaction,
        IEnumerable<uint> tableIds,
        CancellationToken ct
    )
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT conrelid, conname, convalidated, pg_catalog.pg_get_expr(conbin, conrelid)
            FROM pg_catalog.pg_constraint
            WHERE contype = 'c'
              AND conrelid = ANY (ARRAY(
                  SELECT value::pg_catalog.oid
                  FROM pg_catalog.unnest(@tableIds) AS value
              ))
            """;
        AddTableIdsParameter(command, tableIds);

        var result = new List<CheckConstraintStructure>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new CheckConstraintStructure(
                reader.GetFieldValue<uint>(0),
                reader.GetString(1),
                reader.GetBoolean(2),
                NormalizeSql(reader.GetString(3))
            ));
        }

        return result;
    }

    private static async Task<List<ForeignKeyStructure>> ReadForeignKeysAsync(
        DbConnection connection,
        DbTransaction transaction,
        IEnumerable<uint> tableIds,
        CancellationToken ct
    )
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT c.conrelid,
                   ARRAY(SELECT a.attname FROM pg_catalog.unnest(c.conkey) WITH ORDINALITY AS key(attnum, position)
                         JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = key.attnum
                         ORDER BY key.position),
                   c.confrelid,
                   ARRAY(SELECT a.attname FROM pg_catalog.unnest(c.confkey) WITH ORDINALITY AS key(attnum, position)
                         JOIN pg_catalog.pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = key.attnum
                         ORDER BY key.position),
                   c.confdeltype, c.convalidated
            FROM pg_catalog.pg_constraint c
            WHERE c.contype = 'f'
              AND c.conrelid = ANY (ARRAY(
                  SELECT value::pg_catalog.oid
                  FROM pg_catalog.unnest(@tableIds) AS value
              ))
            """;
        AddTableIdsParameter(command, tableIds);

        var result = new List<ForeignKeyStructure>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new ForeignKeyStructure(
                reader.GetFieldValue<uint>(0),
                reader.GetFieldValue<string[]>(1),
                reader.GetFieldValue<uint>(2),
                reader.GetFieldValue<string[]>(3),
                reader.GetChar(4),
                reader.GetBoolean(5)
            ));
        }

        return result;
    }

    private static void AddTablesParameters(DbCommand command, CohortStoreTables tables)
    {
        var mapped = CohortSchemaContract.Tables
            .Select(table => (table.Role, Table: table.ResolveStoreTable(tables)))
            .ToArray();
        AddArrayParameter(command, "roles", mapped.Select(table => table.Role).ToArray());
        AddArrayParameter(command, "schemas", mapped.Select(table => table.Table.Schema).ToArray());
        AddArrayParameter(command, "tables", mapped.Select(table => table.Table.Name).ToArray());
    }

    private static void AddArrayParameter(DbCommand command, string name, string[] values)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = values;
        command.Parameters.Add(parameter);
    }

    private static void AddTableIdsParameter(DbCommand command, IEnumerable<uint> tableIds)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = "tableIds";
        parameter.Value = tableIds.Select(id => (long)id).ToArray();
        command.Parameters.Add(parameter);
    }

    private static string? NormalizePredicate(string? predicate)
    {
        return predicate is null
            ? null
            : new string(predicate.Where(char.IsLetterOrDigit).ToArray()).ToUpperInvariant();
    }

    internal static string NormalizeSql(string sql)
    {
        var compact = new string(sql
            .Where(character => !char.IsWhiteSpace(character)
                && character is not '"' and not '\'' and not ':')
            .Select(char.ToUpperInvariant)
            .ToArray());
        return NormalizeParentheses(compact);
    }

    private static string NormalizeParentheses(string sql)
    {
        var result = new System.Text.StringBuilder(sql.Length);
        for (var index = 0; index < sql.Length; index++)
        {
            if (sql[index] != '(')
            {
                result.Append(sql[index]);
                continue;
            }

            var depth = 1;
            var end = index + 1;
            for (; end < sql.Length && depth != 0; end++)
            {
                depth += sql[end] switch
                {
                    '(' => 1,
                    ')' => -1,
                    _ => 0,
                };
            }

            if (depth != 0)
            {
                return sql;
            }

            var inner = NormalizeParentheses(sql[(index + 1)..(end - 1)]);
            var isWholeExpression = index == 0 && end == sql.Length;
            var groupsBooleanExpression = inner.Contains("AND", StringComparison.Ordinal)
                || inner.Contains("OR", StringComparison.Ordinal);
            if (groupsBooleanExpression && !isWholeExpression)
            {
                result.Append('(').Append(inner).Append(')');
            }
            else
            {
                result.Append(inner);
            }

            index = end - 1;
        }

        return result.ToString();
    }

    private static bool IsCompatibleType(string required, ColumnStructure actual)
    {
        return actual.Type == required
            || required == "text" && actual.Type == "varchar" && actual.TypeModifier == -1;
    }

    private sealed record ColumnStructure(
        string Type,
        int TypeModifier,
        bool Nullable,
        bool Generated
    );
    private sealed record IndexStructure(
        uint TableId,
        bool Unique,
        bool Primary,
        string[] Columns,
        string? Predicate
    );
    private sealed record CheckConstraintStructure(uint TableId, string Name, bool Validated, string Expression);
    private sealed record ForeignKeyStructure(
        uint TableId,
        string[] Columns,
        uint ReferencedTableId,
        string[] ReferencedColumns,
        char DeleteAction,
        bool Validated
    );
}
