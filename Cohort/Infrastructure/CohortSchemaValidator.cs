using System.Data;
using System.Data.Common;

using Cohort.Application;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

internal sealed class CohortSchemaValidator(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db
)
{
    private static readonly IReadOnlyDictionary<string, string[]> RequiredColumns =
        new Dictionary<string, string[]>(StringComparer.Ordinal)
        {
            [CohortTableNames.RetentionHolds] =
            [
                "HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt",
                "ExpiresAt", "RemovedAt",
            ],
            [CohortTableNames.SweepRun] =
            [
                "SweepId", "StartedAt", "Status", "SettledAt", "Duration", "TriggerKind",
                "DryRun", "TenantId", "TotalAffected", "Error",
            ],
            [CohortTableNames.SweepRunEntitySummary] =
            [
                "SweepId", "At", "EntityType", "RetentionEntityId", "Category", "TenantId",
                "Strategy", "ResolvedPeriod", "Affected", "HeldCount", "SkippedCount",
                "NullAnchorCount", "RuleSource", "RuleReason",
            ],
            [CohortTableNames.SweepRunRowDetail] =
            [
                "Id", "SweepId", "At", "EntityType", "RetentionEntityId", "EntityId",
                "Category", "Strategy", "TenantId", "CapturedPayload",
            ],
            [CohortTableNames.SweepRowHandlerStatus] =
            [
                "Id", "SweepRunRowDetailId", "HandlerType", "DispatchPhase", "State", "Attempt",
                "QueuedAt", "NextAttemptAt", "ClaimedAt", "ClaimToken", "CompletedAt", "LastError",
            ],
        };

    private static readonly ColumnRequirement[] RequiredColumnStructures =
    [
        new(CohortTableNames.RetentionHolds, "HoldId", "uuid", false),
        new(CohortTableNames.RetentionHolds, "RetentionEntityId", "uuid", false),
        new(CohortTableNames.RetentionHolds, "RecordId", "text", false),
        new(CohortTableNames.RetentionHolds, "TenantId", "uuid", true),
        new(CohortTableNames.SweepRun, "SweepId", "uuid", false),
        new(CohortTableNames.SweepRun, "Status", "int4", false),
        new(CohortTableNames.SweepRunEntitySummary, "SweepId", "uuid", false),
        new(CohortTableNames.SweepRunEntitySummary, "EntityType", "text", false),
        new(CohortTableNames.SweepRunEntitySummary, "RetentionEntityId", "uuid", false),
        new(CohortTableNames.SweepRunEntitySummary, "Category", "text", false),
        new(CohortTableNames.SweepRunEntitySummary, "TenantId", "uuid", false),
        new(CohortTableNames.SweepRunEntitySummary, "Strategy", "int4", false),
        new(CohortTableNames.SweepRunRowDetail, "Id", "int8", false),
        new(CohortTableNames.SweepRunRowDetail, "SweepId", "uuid", false),
        new(CohortTableNames.SweepRunRowDetail, "EntityType", "text", false),
        new(CohortTableNames.SweepRunRowDetail, "RetentionEntityId", "uuid", false),
        new(CohortTableNames.SweepRunRowDetail, "EntityId", "text", false),
        new(CohortTableNames.SweepRunRowDetail, "Category", "text", false),
        new(CohortTableNames.SweepRunRowDetail, "Strategy", "int4", false),
        new(CohortTableNames.SweepRunRowDetail, "TenantId", "uuid", false),
        new(CohortTableNames.SweepRowHandlerStatus, "Id", "int8", false),
        new(CohortTableNames.SweepRowHandlerStatus, "SweepRunRowDetailId", "int8", false),
        new(CohortTableNames.SweepRowHandlerStatus, "HandlerType", "text", false),
        new(CohortTableNames.SweepRowHandlerStatus, "State", "int4", false),
        new(CohortTableNames.SweepRowHandlerStatus, "NextAttemptAt", "timestamptz", false),
        new(CohortTableNames.SweepRowHandlerStatus, "ClaimToken", "uuid", true),
    ];

    private static readonly IndexRequirement[] RequiredIndexes =
    [
        new(CohortTableNames.RetentionHolds, ["HoldId"], true, true),
        new(CohortTableNames.RetentionHolds, ["RetentionEntityId", "TenantId", "RecordId"]),
        new(CohortTableNames.RetentionHolds, ["RetentionEntityId", "RecordId"]),
        new(CohortTableNames.SweepRun, ["SweepId"], true, true),
        new(CohortTableNames.SweepRunEntitySummary,
            ["SweepId", "RetentionEntityId", "Category", "TenantId", "Strategy"], true, true),
        new(CohortTableNames.SweepRunEntitySummary, ["SweepId"]),
        new(CohortTableNames.SweepRunRowDetail, ["Id"], true, true),
        new(CohortTableNames.SweepRunRowDetail, ["SweepId"]),
        new(CohortTableNames.SweepRunRowDetail,
            ["SweepId", "RetentionEntityId", "EntityId", "Category", "Strategy", "TenantId"],
            true),
        new(CohortTableNames.SweepRowHandlerStatus, ["Id"], true, true),
        new(CohortTableNames.SweepRowHandlerStatus, ["SweepRunRowDetailId", "HandlerType"], true),
        new(CohortTableNames.SweepRowHandlerStatus, ["State", "NextAttemptAt", "Id"]),
    ];

    private static readonly CheckConstraintRequirement[] RequiredCheckConstraints =
    [
        new(CohortTableNames.SweepRun, "CK_sweep_run_Status_Range", "Status>=0ANDStatus<=4"),
        new(CohortTableNames.SweepRun, "CK_sweep_run_Started_Unsettled", "Status<>0ORSettledAtISNULL"),
        new(CohortTableNames.SweepRun, "CK_sweep_run_Terminal_Settled", "Status=0ORSettledAtISNOTNULL"),
        new(CohortTableNames.SweepRun, "CK_sweep_run_TotalAffected_Nonnegative", "TotalAffectedISNULLORTotalAffected>=0"),
        new(CohortTableNames.SweepRun, "CK_sweep_run_Duration_Nonnegative", "DurationISNULLORDuration>=000000INTERVAL"),
        new(CohortTableNames.SweepRowHandlerStatus, "CK_sweep_row_handler_status_Claim", "State=1ANDClaimedAtISNOTNULLANDClaimTokenISNOTNULLORState<>1ANDClaimedAtISNULLANDClaimTokenISNULL"),
        new(CohortTableNames.SweepRowHandlerStatus, "CK_sweep_row_handler_status_Completion", "State=ANYARRAY[2,3]ANDCompletedAtISNOTNULLORState=ANYARRAY[0,1]ANDCompletedAtISNULL"),
    ];

    private static readonly ForeignKeyRequirement[] RequiredForeignKeys =
    [
        new(
            CohortTableNames.SweepRunEntitySummary,
            ["SweepId"],
            CohortTableNames.SweepRun,
            ["SweepId"],
            'r'
        ),
        new(
            CohortTableNames.SweepRunRowDetail,
            ["SweepId"],
            CohortTableNames.SweepRun,
            ["SweepId"],
            'r'
        ),
        new(
            CohortTableNames.SweepRowHandlerStatus,
            ["SweepRunRowDetailId"],
            CohortTableNames.SweepRunRowDetail,
            ["Id"],
            'c'
        ),
    ];

    public async Task ValidateAsync(CancellationToken ct)
    {
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;
        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            await using var transaction = await connection.BeginTransactionAsync(
                IsolationLevel.RepeatableRead,
                ct
            );
            var tables = await ResolveTablesAsync(connection, transaction, ct);
            var columns = await ReadColumnsAsync(connection, transaction, tables.Values, ct);
            var indexes = await ReadIndexesAsync(connection, transaction, tables.Values, ct);
            var checkConstraints = await ReadCheckConstraintsAsync(
                connection,
                transaction,
                tables.Values,
                ct
            );
            var foreignKeys = await ReadForeignKeysAsync(connection, transaction, tables.Values, ct);
            await transaction.CommitAsync(ct);
            var missing = new List<string>();

            foreach (var (table, requiredColumns) in RequiredColumns)
            {
                if (!tables.TryGetValue(table, out var tableId))
                {
                    missing.Add($"table '{table}'");
                    continue;
                }

                missing.AddRange(requiredColumns
                    .Where(column => !columns.ContainsKey((tableId, column)))
                    .Select(column => $"column '{table}.\"{column}\"'"));
            }

            foreach (var requirement in RequiredColumnStructures)
            {
                if (!tables.TryGetValue(requirement.Table, out var tableId)
                    || !columns.TryGetValue((tableId, requirement.Column), out var actual))
                {
                    continue;
                }

                if (!IsCompatibleType(requirement.Type, actual.Type)
                    || actual.Nullable != requirement.Nullable)
                {
                    missing.Add(
                        $"column capability '{requirement.Table}.\"{requirement.Column}\" {requirement.Type} {(requirement.Nullable ? "NULL" : "NOT NULL")}'"
                    );
                }
            }

            foreach (var requirement in RequiredIndexes)
            {
                if (!tables.TryGetValue(requirement.Table, out var tableId))
                {
                    continue;
                }

                if (!indexes.Any(index => index.TableId == tableId
                    && index.Unique == requirement.Unique
                    && index.Primary == requirement.Primary
                    && index.Columns.SequenceEqual(requirement.Columns)
                    && index.Predicate == NormalizePredicate(requirement.Predicate)))
                {
                    missing.Add($"index capability '{requirement.Table}({string.Join(", ", requirement.Columns)})'");
                }
            }

            foreach (var requirement in RequiredCheckConstraints)
            {
                if (!tables.TryGetValue(requirement.Table, out var tableId))
                {
                    continue;
                }

                if (!checkConstraints.Any(constraint => constraint.TableId == tableId
                    && constraint.Name == requirement.Name
                    && constraint.Validated
                    && constraint.Expression == NormalizeSql(requirement.Expression)))
                {
                    missing.Add($"check constraint capability '{requirement.Table}.{requirement.Name}'");
                }
            }

            foreach (var requirement in RequiredForeignKeys)
            {
                if (!tables.TryGetValue(requirement.Table, out var tableId)
                    || !tables.TryGetValue(requirement.ReferencedTable, out var referencedTableId))
                {
                    continue;
                }

                if (!foreignKeys.Any(foreignKey => foreignKey.TableId == tableId
                    && foreignKey.Columns.SequenceEqual(requirement.Columns)
                    && foreignKey.ReferencedTableId == referencedTableId
                    && foreignKey.ReferencedColumns.SequenceEqual(requirement.ReferencedColumns)
                    && foreignKey.DeleteAction == requirement.DeleteAction
                    && foreignKey.Validated))
                {
                    missing.Add(
                        $"foreign key capability '{requirement.Table}({string.Join(", ", requirement.Columns)}) -> {requirement.ReferencedTable}({string.Join(", ", requirement.ReferencedColumns)}) ON DELETE {DescribeDeleteAction(requirement.DeleteAction)}'"
                    );
                }
            }

            if (missing.Count != 0)
            {
                throw new RetentionConfigurationException([
                    $"The configured PostgreSQL schema is missing Cohort runtime capabilities: {string.Join(", ", missing)}. Apply the host application's pending EF Core migrations before starting Cohort.",
                ]);
            }
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
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
        CancellationToken ct
    )
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT name, to_regclass(name)::oid FROM unnest(@tables) AS name";
        AddTablesParameter(command);

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
            SELECT attrelid, attname, typname, NOT attnotnull
            FROM pg_attribute
            JOIN pg_type ON pg_type.oid = atttypid
            WHERE attrelid = ANY (ARRAY(SELECT value::oid FROM unnest(@tableIds) AS value))
              AND attnum > 0 AND NOT attisdropped
            """;
        AddTableIdsParameter(command, tableIds);

        var result = new Dictionary<(uint, string), ColumnStructure>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(
                (reader.GetFieldValue<uint>(0), reader.GetString(1)),
                new ColumnStructure(reader.GetString(2), reader.GetBoolean(3))
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
                         FROM unnest(i.indkey) WITH ORDINALITY AS key(attnum, position)
                          JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = key.attnum
                          WHERE key.position <= i.indnkeyatts
                          ORDER BY key.position),
                   pg_get_expr(i.indpred, i.indrelid)
            FROM pg_index i
            WHERE i.indrelid = ANY (ARRAY(SELECT value::oid FROM unnest(@tableIds) AS value))
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
            SELECT conrelid, conname, convalidated, pg_get_expr(conbin, conrelid)
            FROM pg_constraint
            WHERE contype = 'c'
              AND conrelid = ANY (ARRAY(SELECT value::oid FROM unnest(@tableIds) AS value))
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
                   ARRAY(SELECT a.attname FROM unnest(c.conkey) WITH ORDINALITY AS key(attnum, position)
                         JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = key.attnum
                         ORDER BY key.position),
                   c.confrelid,
                   ARRAY(SELECT a.attname FROM unnest(c.confkey) WITH ORDINALITY AS key(attnum, position)
                         JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = key.attnum
                         ORDER BY key.position),
                   c.confdeltype, c.convalidated
            FROM pg_constraint c
            WHERE c.contype = 'f'
              AND c.conrelid = ANY (ARRAY(SELECT value::oid FROM unnest(@tableIds) AS value))
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

    private static void AddTablesParameter(DbCommand command)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = "tables";
        parameter.Value = RequiredColumns.Keys.ToArray();
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

    private static string NormalizeSql(string sql) =>
        new(sql
            .Where(character => !char.IsWhiteSpace(character)
                && character is not '"' and not '(' and not ')' and not '\'' and not ':')
            .Select(char.ToUpperInvariant)
            .ToArray());

    private static bool IsCompatibleType(string required, string actual)
    {
        return actual == required || required == "text" && actual == "varchar";
    }

    private sealed record ColumnRequirement(string Table, string Column, string Type, bool Nullable);
    private sealed record ColumnStructure(string Type, bool Nullable);
    private sealed record IndexRequirement(
        string Table,
        string[] Columns,
        bool Unique = false,
        bool Primary = false,
        string? Predicate = null
    );
    private sealed record IndexStructure(
        uint TableId,
        bool Unique,
        bool Primary,
        string[] Columns,
        string? Predicate
    );
    private sealed record CheckConstraintRequirement(string Table, string Name, string Expression);
    private sealed record CheckConstraintStructure(uint TableId, string Name, bool Validated, string Expression);
    private sealed record ForeignKeyRequirement(
        string Table,
        string[] Columns,
        string ReferencedTable,
        string[] ReferencedColumns,
        char DeleteAction
    );
    private sealed record ForeignKeyStructure(
        uint TableId,
        string[] Columns,
        uint ReferencedTableId,
        string[] ReferencedColumns,
        char DeleteAction,
        bool Validated
    );
}
