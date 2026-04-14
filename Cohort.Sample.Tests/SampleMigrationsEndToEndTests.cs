using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class SampleMigrationsEndToEndTests(PostgresFixture fixture) : IAsyncLifetime
{
    private readonly string databaseName = $"cohort_migration_{Guid.NewGuid():N}";
    private string connectionString = "";

    public async Task InitializeAsync()
    {
        var adminConnectionString = CreateAdminConnectionString(fixture.ConnectionString);

        await using var connection = new NpgsqlConnection(adminConnectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = $"CREATE DATABASE \"{databaseName}\"";
        await command.ExecuteNonQueryAsync();

        var builder = new NpgsqlConnectionStringBuilder(fixture.ConnectionString)
        {
            Database = databaseName,
        };
        connectionString = builder.ConnectionString;
    }

    public async Task DisposeAsync()
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var adminConnectionString = CreateAdminConnectionString(fixture.ConnectionString);

        await using var connection = new NpgsqlConnection(adminConnectionString);
        await connection.OpenAsync();

        await using (var terminate = connection.CreateCommand())
        {
            terminate.CommandText =
                $"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{databaseName}'
                  AND pid <> pg_backend_pid()
                """;
            await terminate.ExecuteNonQueryAsync();
        }

        await using var drop = connection.CreateCommand();
        drop.CommandText = $"DROP DATABASE IF EXISTS \"{databaseName}\"";
        await drop.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Add_Note_Tenant_Migration_Preserves_Legacy_Notes_And_Leaves_Tenant_Null()
    {
        var options = CreateOptions();
        var noteId = Guid.NewGuid();

        await using (var db = new SampleDbContext(options))
        {
            var migrator = db.Database.GetService<IMigrator>();
            await migrator.MigrateAsync("20260411230000_AddExemptDocument");

            await db.Database.ExecuteSqlRawAsync(
                """
                INSERT INTO "notes" ("Id", "CreatedAt", "Body")
                VALUES ({0}, {1}, {2})
                """,
                noteId,
                new DateTimeOffset(2026, 4, 10, 12, 0, 0, TimeSpan.Zero),
                "legacy-note"
            );

            await migrator.MigrateAsync();
        }

        await using var verify = new SampleDbContext(options);
        var legacyNote = await verify.Notes.SingleAsync(note => note.Id == noteId);

        legacyNote.Body.Should().Be("legacy-note");
        legacyNote.TenantId.Should().BeNull();
    }

    [Fact]
    public async Task Legacy_Cohort_Schema_Upgrades_To_Current_ConfigureCohortTables_Shape()
    {
        var options = CreateOptions();

        await using (var db = new SampleDbContext(options))
        {
            await LegacyCohortSchema.BootstrapPreRowDispatchAsync(connectionString);

            var migrator = db.Database.GetService<IMigrator>();
            await migrator.MigrateAsync();
        }

        var sweepRunColumns = await GetColumnsAsync("sweep_run");
        sweepRunColumns.Should().ContainKey("TriggerKind");
        sweepRunColumns["TriggerKind"].DataType.Should().Be("integer");
        sweepRunColumns["TriggerKind"].IsNullable.Should().BeFalse();

        var entitySummaryColumns = await GetColumnsAsync("sweep_run_entity_summary");
        entitySummaryColumns.Should().ContainKey("SkippedCount");
        entitySummaryColumns["SkippedCount"].DataType.Should().Be("integer");
        entitySummaryColumns["SkippedCount"].IsNullable.Should().BeFalse();
        entitySummaryColumns.Should().ContainKey("RuleSource");
        entitySummaryColumns["RuleSource"].DataType.Should().Be("text");
        entitySummaryColumns["RuleSource"].IsNullable.Should().BeTrue();
        entitySummaryColumns.Should().ContainKey("RuleReason");
        entitySummaryColumns["RuleReason"].DataType.Should().Be("text");
        entitySummaryColumns["RuleReason"].IsNullable.Should().BeTrue();

        var rowDetailColumns = await GetColumnsAsync("sweep_run_row_detail");
        rowDetailColumns.Should().ContainKey("Id");
        rowDetailColumns["Id"].DataType.Should().Be("bigint");
        rowDetailColumns["Id"].IsNullable.Should().BeFalse();
        rowDetailColumns["Id"].IdentityGeneration.Should().Be("BY DEFAULT");
        rowDetailColumns.Should().ContainKey("CapturedPayload");
        rowDetailColumns["CapturedPayload"].DataType.Should().Be("text");
        rowDetailColumns["CapturedPayload"].IsNullable.Should().BeTrue();
        (await GetPrimaryKeyColumnsAsync("sweep_run_row_detail")).Should().Equal("Id");

        var handlerStatusColumns = await GetColumnsAsync("sweep_row_handler_status");
        handlerStatusColumns.Should().ContainKeys(
            "Id",
            "SweepRunRowDetailId",
            "HandlerType",
            "State",
            "Attempt",
            "QueuedAt",
            "NextAttemptAt",
            "ClaimedAt",
            "CompletedAt",
            "LastError"
        );
        handlerStatusColumns["Id"].DataType.Should().Be("bigint");
        handlerStatusColumns["Id"].IsNullable.Should().BeFalse();
        handlerStatusColumns["Id"].IdentityGeneration.Should().Be("BY DEFAULT");
        handlerStatusColumns["SweepRunRowDetailId"].DataType.Should().Be("bigint");
        handlerStatusColumns["SweepRunRowDetailId"].IsNullable.Should().BeFalse();
        handlerStatusColumns["HandlerType"].DataType.Should().Be("text");
        handlerStatusColumns["HandlerType"].IsNullable.Should().BeFalse();
        handlerStatusColumns["State"].DataType.Should().Be("integer");
        handlerStatusColumns["State"].IsNullable.Should().BeFalse();
        handlerStatusColumns["Attempt"].DataType.Should().Be("integer");
        handlerStatusColumns["Attempt"].IsNullable.Should().BeFalse();
        handlerStatusColumns["QueuedAt"].DataType.Should().Be("timestamp with time zone");
        handlerStatusColumns["QueuedAt"].IsNullable.Should().BeFalse();
        handlerStatusColumns["NextAttemptAt"].DataType.Should().Be("timestamp with time zone");
        handlerStatusColumns["NextAttemptAt"].IsNullable.Should().BeFalse();
        handlerStatusColumns["ClaimedAt"].DataType.Should().Be("timestamp with time zone");
        handlerStatusColumns["ClaimedAt"].IsNullable.Should().BeTrue();
        handlerStatusColumns["CompletedAt"].DataType.Should().Be("timestamp with time zone");
        handlerStatusColumns["CompletedAt"].IsNullable.Should().BeTrue();
        handlerStatusColumns["LastError"].DataType.Should().Be("text");
        handlerStatusColumns["LastError"].IsNullable.Should().BeTrue();
        (await GetPrimaryKeyColumnsAsync("sweep_row_handler_status")).Should().Equal("Id");

        (
            await HasForeignKeyAsync(
                "sweep_row_handler_status",
                "SweepRunRowDetailId",
                "sweep_run_row_detail",
                "Id"
            )
        ).Should().BeTrue();

        var rowDetailIndexes = await GetIndexDefinitionsAsync("sweep_run_row_detail");
        rowDetailIndexes.Should().Contain(index =>
            index.Contains("CREATE UNIQUE INDEX", StringComparison.Ordinal)
            && index.Contains(
                "(\"SweepId\", \"EntityType\", \"EntityId\", \"Category\", \"Strategy\", \"TenantId\")",
                StringComparison.Ordinal
            )
        );

        var handlerStatusIndexes = await GetIndexDefinitionsAsync("sweep_row_handler_status");
        handlerStatusIndexes.Should().Contain(index =>
            index.Contains("CREATE UNIQUE INDEX", StringComparison.Ordinal)
            && index.Contains(
                "(\"SweepRunRowDetailId\", \"HandlerType\")",
                StringComparison.Ordinal
            )
        );
        handlerStatusIndexes.Should().Contain(index =>
            index.Contains("CREATE INDEX", StringComparison.Ordinal)
            && index.Contains(
                "(\"State\", \"NextAttemptAt\", \"Id\")",
                StringComparison.Ordinal
            )
        );
    }

    [Fact]
    public async Task Add_Handler_Fixtures_Migration_Adds_The_Blob_Backed_File_Table()
    {
        var options = CreateOptions();

        await using (var db = new SampleDbContext(options))
        {
            var migrator = db.Database.GetService<IMigrator>();
            await migrator.MigrateAsync("20260413150144_AddRowHandlerDispatch");
            await migrator.MigrateAsync();
        }

        var columns = await GetColumnsAsync("blob_backed_files");
        columns.Should().ContainKeys(
            "Id",
            "TenantId",
            "CreatedAt",
            "StoragePath",
            "OriginalFileName",
            "ContentType"
        );
        columns["Id"].DataType.Should().Be("uuid");
        columns["Id"].IsNullable.Should().BeFalse();
        columns["TenantId"].DataType.Should().Be("uuid");
        columns["TenantId"].IsNullable.Should().BeFalse();
        columns["CreatedAt"].DataType.Should().Be("timestamp with time zone");
        columns["CreatedAt"].IsNullable.Should().BeFalse();
        columns["StoragePath"].DataType.Should().Be("text");
        columns["StoragePath"].IsNullable.Should().BeFalse();
        columns["OriginalFileName"].DataType.Should().Be("text");
        columns["OriginalFileName"].IsNullable.Should().BeFalse();
        columns["ContentType"].DataType.Should().Be("text");
        columns["ContentType"].IsNullable.Should().BeFalse();
        (await GetPrimaryKeyColumnsAsync("blob_backed_files")).Should().Equal("Id");
    }

    private DbContextOptions<SampleDbContext> CreateOptions()
    {
        return new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(connectionString)
            .Options;
    }

    private async Task<Dictionary<string, ColumnSchema>> GetColumnsAsync(string tableName)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT column_name, is_nullable, data_type, identity_generation
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = @tableName
            ORDER BY ordinal_position
            """;
        command.Parameters.AddWithValue("tableName", tableName);

        var columns = new Dictionary<string, ColumnSchema>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            columns[reader.GetString(0)] = new ColumnSchema(
                reader.GetString(1) == "YES",
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3)
            );
        }

        return columns;
    }

    private async Task<string[]> GetPrimaryKeyColumnsAsync(string tableName)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = 'public'
              AND tc.table_name = @tableName
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """;
        command.Parameters.AddWithValue("tableName", tableName);

        var columns = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            columns.Add(reader.GetString(0));
        }

        return columns.ToArray();
    }

    private async Task<bool> HasForeignKeyAsync(
        string tableName,
        string columnName,
        string referencedTable,
        string referencedColumn
    )
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT COUNT(*)
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON tc.constraint_name = ccu.constraint_name
             AND tc.table_schema = ccu.table_schema
            WHERE tc.table_schema = 'public'
              AND tc.table_name = @tableName
              AND tc.constraint_type = 'FOREIGN KEY'
              AND kcu.column_name = @columnName
              AND ccu.table_name = @referencedTable
              AND ccu.column_name = @referencedColumn
            """;
        command.Parameters.AddWithValue("tableName", tableName);
        command.Parameters.AddWithValue("columnName", columnName);
        command.Parameters.AddWithValue("referencedTable", referencedTable);
        command.Parameters.AddWithValue("referencedColumn", referencedColumn);

        return (long)(await command.ExecuteScalarAsync())! == 1;
    }

    private async Task<string[]> GetIndexDefinitionsAsync(string tableName)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = @tableName
            ORDER BY indexname
            """;
        command.Parameters.AddWithValue("tableName", tableName);

        var indexes = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            indexes.Add(reader.GetString(0));
        }

        return indexes.ToArray();
    }

    private static string CreateAdminConnectionString(string originalConnectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(originalConnectionString)
        {
            Database = "postgres",
        };

        return builder.ConnectionString;
    }

    private sealed record ColumnSchema(bool IsNullable, string DataType, string? IdentityGeneration);
}
