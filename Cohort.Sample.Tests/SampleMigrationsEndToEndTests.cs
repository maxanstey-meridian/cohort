using System.Reflection;
using Cohort.Domain;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class SampleMigrationsEndToEndTests(PostgresFixture fixture) : IAsyncLifetime
{
    private static readonly (string EntityType, Guid RetentionEntityId)[] StableEntityMappings =
    [
        ("Cohort.Sample.Entities.Note", Guid.Parse("a3f467fe-c5d0-4f17-9897-83c373cc1dc8")),
        (
            "Cohort.Sample.Entities.BlobBackedFile",
            Guid.Parse("2fb1804d-9ad8-4543-a177-5d4cd14d62ee")
        ),
        (
            "Cohort.Sample.Entities.PerRowAuditedLog",
            Guid.Parse("42670ee7-c26a-4a2a-a2ab-d9571db7d4f6")
        ),
        (
            "Cohort.Sample.Entities.ExternalNumberedLog",
            Guid.Parse("d0991164-8823-4f4e-aac1-f9d8d1753764")
        ),
        (
            "Cohort.Sample.Entities.TenantlessLog",
            Guid.Parse("992a65db-d658-4b76-aaf5-b11ca52c4a8f")
        ),
        (
            "Cohort.Sample.Entities.AnonymisedContact",
            Guid.Parse("fd4a533e-e6a9-44ea-948e-cbf881f35e57")
        ),
        (
            "Cohort.Sample.Entities.TombstoneRecord",
            Guid.Parse("6ebbc096-d3b8-4077-8f21-bf9b4d53c869")
        ),
        (
            "Cohort.Sample.Entities.SoftDeleteRecord",
            Guid.Parse("6107ff39-bf33-413c-889e-6347c909ba15")
        ),
        (
            "Cohort.Sample.Entities.TenantlessSoftDelete",
            Guid.Parse("36d4a1a6-f2d8-40a8-84ea-5a062fc82889")
        ),
        (
            "Cohort.Sample.Entities.NullableAnchorEvent",
            Guid.Parse("314fd4f7-f771-4b94-ab6e-7fc0a09a6ef5")
        ),
    ];

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
            terminate.CommandText = $"""
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
    public async Task Require_Retained_Tenant_Ids_Migration_Requires_Null_Tenants_To_Be_Backfilled()
    {
        var options = CreateOptions();
        var noteId = Guid.NewGuid();
        var nullableAnchorEventId = Guid.NewGuid();

        await using (var db = new SampleDbContext(options))
        {
            var migrator = db.Database.GetService<IMigrator>();
            await migrator.MigrateAsync("20260612111420_AddNullAnchorReporting");

            await db.Database.ExecuteSqlRawAsync(
                """
                INSERT INTO "notes" ("Id", "CreatedAt", "Body")
                VALUES ({0}, {1}, {2})
                """,
                noteId,
                new DateTimeOffset(2026, 4, 10, 12, 0, 0, TimeSpan.Zero),
                "legacy-note"
            );
            await db.Database.ExecuteSqlRawAsync(
                """
                INSERT INTO "nullable_anchor_events" ("Id", "TenantId", "OccurredAt", "Payload")
                VALUES ({0}, NULL, NULL, {1})
                """,
                nullableAnchorEventId,
                "legacy-event"
            );

            Func<Task> migrate = () =>
                migrator.MigrateAsync("20260711130000_RequireRetainedTenantIds");

            var exception = await migrate.Should().ThrowAsync<PostgresException>();
            exception.Which.SqlState.Should().Be(PostgresErrorCodes.RaiseException);
            exception
                .Which.MessageText.Should()
                .Be(
                    "Cannot require retained tenant IDs: notes and/or nullable_anchor_events contain NULL TenantId values. Backfill TenantId before applying migration 20260711130000_RequireRetainedTenantIds."
                );
        }

        await using var verify = new NpgsqlConnection(connectionString);
        await verify.OpenAsync();
        await using var command = verify.CreateCommand();
        command.CommandText = """
            SELECT
                EXISTS (SELECT 1 FROM notes WHERE "Id" = @noteId),
                EXISTS (SELECT 1 FROM nullable_anchor_events WHERE "Id" = @eventId)
            """;
        command.Parameters.AddWithValue("noteId", noteId);
        command.Parameters.AddWithValue("eventId", nullableAnchorEventId);
        await using var reader = await command.ExecuteReaderAsync();
        await reader.ReadAsync();
        reader.GetBoolean(0).Should().BeTrue();
        reader.GetBoolean(1).Should().BeTrue();

        var noteColumns = await GetColumnsAsync("notes");
        noteColumns["TenantId"].IsNullable.Should().BeTrue();
        var nullableAnchorEventColumns = await GetColumnsAsync("nullable_anchor_events");
        nullableAnchorEventColumns["TenantId"].IsNullable.Should().BeTrue();
    }

    [Fact]
    public async Task Stable_Entity_Identity_Migration_Backfills_All_Retained_Entity_History()
    {
        var options = CreateOptions();
        var sweepId = Guid.NewGuid();
        var holdId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();

        await using (var db = new SampleDbContext(options))
        {
            var migrator = db.Database.GetService<IMigrator>();
            await migrator.MigrateAsync("20260711150000_AddExplicitSweepRunStatus");
            await SeedAllHistoricalAuditRowsAsync(sweepId, tenantId);
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"""
                INSERT INTO "retention_holds"
                    ("HoldId", "TableName", "RecordId", "TenantId", "Reason", "CreatedAt")
                VALUES
                    ({holdId}, {"notes"}, {"held-record"}, {tenantId}, {"migration-test"}, {DateTimeOffset.UtcNow})
                """
            );
            await migrator.MigrateAsync("20260711160000_AddStableRetentionEntityIdentity");
        }

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT summary."EntityType", summary."RetentionEntityId", detail."RetentionEntityId", detail."EntityId"
            FROM "sweep_run_entity_summary" AS summary
            INNER JOIN "sweep_run_row_detail" AS detail
                ON detail."SweepId" = summary."SweepId"
               AND detail."EntityType" = summary."EntityType"
               AND detail."Category" = summary."Category"
               AND detail."TenantId" = summary."TenantId"
               AND detail."Strategy" = summary."Strategy"
            WHERE summary."SweepId" = @sweepId
            ORDER BY summary."EntityType"
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<(string EntityType, Guid SummaryId, Guid DetailId, string EntityId)>();
        while (await reader.ReadAsync())
        {
            rows.Add(
                (reader.GetString(0), reader.GetGuid(1), reader.GetGuid(2), reader.GetString(3))
            );
        }

        foreach (var mapping in StableEntityMappings)
        {
            rows.Should()
                .ContainSingle(row =>
                    row.EntityType == mapping.EntityType
                    && row.SummaryId == mapping.RetentionEntityId
                    && row.DetailId == mapping.RetentionEntityId
                    && row.EntityId
                        == (
                            mapping.EntityType == "Cohort.Sample.Entities.Note"
                                ? "known-record"
                                : mapping.EntityType
                        )
                );
        }
        rows.Should().HaveCount(StableEntityMappings.Length);
        await reader.CloseAsync();

        await using var holdCommand = connection.CreateCommand();
        holdCommand.CommandText =
            "SELECT \"RetentionEntityId\", \"RecordId\", \"TenantId\" FROM \"retention_holds\" WHERE \"HoldId\" = @holdId";
        holdCommand.Parameters.AddWithValue("holdId", holdId);
        await using var holdReader = await holdCommand.ExecuteReaderAsync();
        (await holdReader.ReadAsync()).Should().BeTrue();
        holdReader.GetGuid(0).Should().Be(RetentionEntityIdentity.For<Note>());
        holdReader.GetString(1).Should().Be("held-record");
        holdReader.GetGuid(2).Should().Be(tenantId);
    }

    [Fact]
    public async Task Stable_Entity_Identity_Migration_Rejects_Unmapped_Retention_Holds()
    {
        var options = CreateOptions();
        var holdId = Guid.NewGuid();

        await using var db = new SampleDbContext(options);
        var migrator = db.Database.GetService<IMigrator>();
        await migrator.MigrateAsync("20260711150000_AddExplicitSweepRunStatus");
        await db.Database.ExecuteSqlInterpolatedAsync(
            $"""
            INSERT INTO "retention_holds"
                ("HoldId", "TableName", "RecordId", "TenantId", "Reason", "CreatedAt")
            VALUES
                ({holdId}, {"legacy_records"}, {"held-record"}, {Guid.NewGuid()}, {"migration-test"}, {DateTimeOffset.UtcNow})
            """
        );

        Func<Task> migrate = () =>
            migrator.MigrateAsync("20260711160000_AddStableRetentionEntityIdentity");

        var exception = await migrate.Should().ThrowAsync<PostgresException>();
        exception.Which.SqlState.Should().Be(PostgresErrorCodes.RaiseException);
        exception
            .Which.MessageText.Should()
            .Be(
                "Cannot assign stable retention entity identities: retention_holds contains unmapped TableName values: legacy_records. Add an explicit mapping before applying migration 20260711160000_AddStableRetentionEntityIdentity."
            );

        await using var verify = new NpgsqlConnection(connectionString);
        await verify.OpenAsync();
        await using var command = verify.CreateCommand();
        command.CommandText =
            "SELECT \"TableName\" FROM \"retention_holds\" WHERE \"HoldId\" = @holdId";
        command.Parameters.AddWithValue("holdId", holdId);
        (await command.ExecuteScalarAsync()).Should().Be("legacy_records");
    }

    [Fact]
    public async Task Handler_State_Constraint_Migration_Requeues_Legacy_InFlight_Work()
    {
        var options = CreateOptions();
        var sweepId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();

        await using (var db = new SampleDbContext(options))
        {
            var migrator = db.Database.GetService<IMigrator>();
            await migrator.MigrateAsync("20260711150000_AddExplicitSweepRunStatus");
            await SeedHistoricalAuditRowsAsync(sweepId, tenantId);
            await db.Database.ExecuteSqlRawAsync(
                """
                INSERT INTO "sweep_row_handler_status"
                    ("SweepRunRowDetailId", "HandlerType", "DispatchPhase", "State", "Attempt", "QueuedAt", "NextAttemptAt", "ClaimedAt")
                SELECT "Id", {0}, 1, 1, 1, {1}, {2}, {3}
                FROM "sweep_run_row_detail"
                WHERE "SweepId" = {4}
                """,
                "Legacy.Handler",
                new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2026, 7, 11, 12, 1, 0, TimeSpan.Zero),
                sweepId
            );

            await migrator.MigrateAsync("20260711170000_AddRowHandlerStateConstraints");
        }

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT \"State\", \"ClaimedAt\", \"ClaimToken\", \"NextAttemptAt\" FROM \"sweep_row_handler_status\" WHERE \"HandlerType\" = 'Legacy.Handler'";
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt32(0).Should().Be(0);
        reader.IsDBNull(1).Should().BeTrue();
        reader.IsDBNull(2).Should().BeTrue();
        reader.GetFieldValue<DateTimeOffset>(3).Should().BeOnOrBefore(DateTimeOffset.UtcNow);
    }

    [Fact]
    public async Task Handler_State_Constraint_Migration_Clears_Claims_From_Completed_Work()
    {
        var options = CreateOptions();
        var sweepId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();

        await using (var db = new SampleDbContext(options))
        {
            var migrator = db.Database.GetService<IMigrator>();
            await migrator.MigrateAsync("20260711150000_AddExplicitSweepRunStatus");
            await SeedHistoricalAuditRowsAsync(sweepId, tenantId);
            await db.Database.ExecuteSqlRawAsync(
                """
                INSERT INTO "sweep_row_handler_status"
                    ("SweepRunRowDetailId", "HandlerType", "DispatchPhase", "State", "Attempt", "QueuedAt", "NextAttemptAt", "ClaimedAt", "ClaimToken", "CompletedAt")
                SELECT "Id", {0}, 1, 2, 1, {1}, {1}, {2}, {3}, {4}
                FROM "sweep_run_row_detail"
                WHERE "SweepId" = {5}
                """,
                "Legacy.CompletedHandler",
                new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2026, 7, 11, 12, 1, 0, TimeSpan.Zero),
                Guid.NewGuid(),
                new DateTimeOffset(2026, 7, 11, 12, 2, 0, TimeSpan.Zero),
                sweepId
            );

            await migrator.MigrateAsync("20260711170000_AddRowHandlerStateConstraints");
        }

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT \"State\", \"ClaimedAt\", \"ClaimToken\", \"CompletedAt\" FROM \"sweep_row_handler_status\" WHERE \"HandlerType\" = 'Legacy.CompletedHandler'";
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt32(0).Should().Be(2);
        reader.IsDBNull(1).Should().BeTrue();
        reader.IsDBNull(2).Should().BeTrue();
        reader.IsDBNull(3).Should().BeFalse();
    }

    [Fact]
    public async Task Stable_Entity_Identity_Migration_Rejects_Unmapped_Audit_Entity_Types()
    {
        var options = CreateOptions();
        var sweepId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();

        await using var db = new SampleDbContext(options);
        var migrator = db.Database.GetService<IMigrator>();
        await migrator.MigrateAsync("20260711150000_AddExplicitSweepRunStatus");
        await SeedHistoricalAuditRowsAsync(sweepId, tenantId);
        await db.Database.ExecuteSqlRawAsync(
            """
            UPDATE "sweep_run_entity_summary"
            SET "EntityType" = {0}
            WHERE "SweepId" = {1};

            UPDATE "sweep_run_row_detail"
            SET "EntityType" = {0}
            WHERE "SweepId" = {1};
            """,
            "Legacy.Namespace.Note",
            sweepId
        );

        Func<Task> migrate = () =>
            migrator.MigrateAsync("20260711160000_AddStableRetentionEntityIdentity");

        var exception = await migrate.Should().ThrowAsync<PostgresException>();
        exception.Which.SqlState.Should().Be(PostgresErrorCodes.RaiseException);
        exception
            .Which.MessageText.Should()
            .Be(
                "Cannot assign stable retention entity identities: sweep audit history contains unmapped EntityType values: Legacy.Namespace.Note. Add an explicit mapping before applying migration 20260711160000_AddStableRetentionEntityIdentity."
            );
    }

    [Fact]
    public async Task Stable_Entity_Identity_Migration_Rejects_Row_Details_Without_A_Matching_Summary()
    {
        var options = CreateOptions();
        var sweepId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();

        await using var db = new SampleDbContext(options);
        var migrator = db.Database.GetService<IMigrator>();
        await migrator.MigrateAsync("20260711150000_AddExplicitSweepRunStatus");
        await SeedHistoricalAuditRowsAsync(sweepId, tenantId);
        await db.Database.ExecuteSqlRawAsync(
            "DELETE FROM \"sweep_run_entity_summary\" WHERE \"SweepId\" = {0}",
            sweepId
        );

        Func<Task> migrate = () =>
            migrator.MigrateAsync("20260711160000_AddStableRetentionEntityIdentity");

        var exception = await migrate.Should().ThrowAsync<PostgresException>();
        exception.Which.SqlState.Should().Be(PostgresErrorCodes.RaiseException);
        exception
            .Which.MessageText.Should()
            .Be(
                "Cannot assign stable retention entity identities: sweep_run_row_detail contains rows without matching entity summaries for: Cohort.Sample.Entities.Note. Repair the audit history before applying migration 20260711160000_AddStableRetentionEntityIdentity."
            );
    }

    [Fact]
    public async Task Stable_Entity_Identity_Migration_Downgrade_Preserves_Representable_Holds()
    {
        var options = CreateOptions();
        var holdId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();

        await using (var db = new SampleDbContext(options))
        {
            var migrator = db.Database.GetService<IMigrator>();
            await migrator.MigrateAsync("20260711150000_AddExplicitSweepRunStatus");
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"""
                INSERT INTO "retention_holds"
                    ("HoldId", "TableName", "RecordId", "TenantId", "Reason", "CreatedAt")
                VALUES
                    ({holdId}, {"notes"}, {"held-record"}, {tenantId}, {"migration-test"}, {DateTimeOffset.UtcNow})
                """
            );
            await migrator.MigrateAsync("20260711160000_AddStableRetentionEntityIdentity");
            await migrator.MigrateAsync("20260711150000_AddExplicitSweepRunStatus");
        }

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT \"TableName\", \"RecordId\", \"TenantId\" FROM \"retention_holds\" WHERE \"HoldId\" = @holdId";
        command.Parameters.AddWithValue("holdId", holdId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetString(0).Should().Be("notes");
        reader.GetString(1).Should().Be("held-record");
        reader.GetGuid(2).Should().Be(tenantId);
    }

    [Fact]
    public void Sample_Retention_Entity_Identity_Attributes_Are_Unique()
    {
        var identities = typeof(SampleDbContext)
            .Assembly.GetTypes()
            .Select(type =>
                (Type: type, Attribute: type.GetCustomAttribute<RetentionEntityIdAttribute>())
            )
            .Where(value => value.Attribute is not null)
            .Select(value => (value.Type, Id: value.Attribute!.Id))
            .ToArray();

        identities.Should().NotBeEmpty();
        identities.Select(value => value.Id).Should().OnlyHaveUniqueItems();
    }

    [Theory]
    [InlineData("Cohort.Sample.Entities.Note", "known-record")]
    public async Task Stable_Entity_Identity_Migration_Rejects_Duplicate_Historical_Row_Details(
        string entityType,
        string entityId
    )
    {
        var options = CreateOptions();
        var sweepId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();

        await using (var db = new SampleDbContext(options))
        {
            var migrator = db.Database.GetService<IMigrator>();
            await migrator.MigrateAsync("20260711150000_AddExplicitSweepRunStatus");
            await SeedHistoricalAuditRowsAsync(sweepId, tenantId);
            await migrator.MigrateAsync("20260711160000_AddStableRetentionEntityIdentity");
        }

        Func<Task> insertDuplicate = async () =>
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = """
                INSERT INTO "sweep_run_row_detail"
                    ("SweepId", "EntityType", "EntityId", "Category", "Strategy", "TenantId", "At", "RetentionEntityId")
                SELECT "SweepId", "EntityType", "EntityId", "Category", "Strategy", "TenantId", "At", "RetentionEntityId"
                FROM "sweep_run_row_detail"
                WHERE "SweepId" = @sweepId AND "EntityType" = @entityType AND "EntityId" = @entityId
                """;
            command.Parameters.AddWithValue("sweepId", sweepId);
            command.Parameters.AddWithValue("entityType", entityType);
            command.Parameters.AddWithValue("entityId", entityId);
            await command.ExecuteNonQueryAsync();
        };

        var exception = await insertDuplicate.Should().ThrowAsync<PostgresException>();
        exception.Which.SqlState.Should().Be(PostgresErrorCodes.UniqueViolation);
    }

    [Fact]
    public async Task Legacy_Cohort_Schema_Upgrades_To_Current_ConfigureCohortTables_Shape()
    {
        var options = CreateOptions();

        await using (var db = new SampleDbContext(options))
        {
            await LegacyCohortSchema.BootstrapPreRowDispatchAsync(connectionString);

            var migrator = db.Database.GetService<IMigrator>();
            await migrator.MigrateAsync("20260612111420_AddNullAnchorReporting");
            await db.Database.ExecuteSqlRawAsync(
                """
                INSERT INTO "sweep_run"
                    ("SweepId", "StartedAt", "CompletedAt", "FailedAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected", "Error")
                VALUES
                    ({0}, {1}, {2}, NULL, NULL, 2, FALSE, {3}, 0, NULL),
                    ({4}, {1}, NULL, {2}, NULL, 2, FALSE, {3}, 0, 'failed'),
                    ({5}, {1}, {2}, {2}, NULL, 2, FALSE, {3}, 0, 'partial'),
                    ({6}, {1}, NULL, NULL, NULL, 2, FALSE, {3}, 0, NULL)
                """,
                Guid.Parse("00000000-0000-0000-0000-000000000001"),
                new DateTimeOffset(2026, 7, 10, 12, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2026, 7, 10, 12, 1, 0, TimeSpan.Zero),
                Guid.Empty,
                Guid.Parse("00000000-0000-0000-0000-000000000002"),
                Guid.Parse("00000000-0000-0000-0000-000000000003"),
                Guid.Parse("00000000-0000-0000-0000-000000000004")
            );
            await migrator.MigrateAsync();
        }

        var sweepRunColumns = await GetColumnsAsync("sweep_run");
        sweepRunColumns.Should().ContainKey("TriggerKind");
        sweepRunColumns["TriggerKind"].DataType.Should().Be("integer");
        sweepRunColumns["TriggerKind"].IsNullable.Should().BeFalse();
        sweepRunColumns["Status"].DataType.Should().Be("integer");
        sweepRunColumns["Status"].IsNullable.Should().BeFalse();
        sweepRunColumns["SettledAt"].DataType.Should().Be("timestamp with time zone");
        sweepRunColumns["SettledAt"].IsNullable.Should().BeTrue();
        sweepRunColumns.Should().NotContainKey("CompletedAt");
        sweepRunColumns.Should().NotContainKey("FailedAt");

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT "Status", "SettledAt"
                FROM "sweep_run"
                WHERE "SweepId" IN (
                    '00000000-0000-0000-0000-000000000001',
                    '00000000-0000-0000-0000-000000000002',
                    '00000000-0000-0000-0000-000000000003',
                    '00000000-0000-0000-0000-000000000004'
                )
                ORDER BY "SweepId"
                """;
            await using var reader = await command.ExecuteReaderAsync();
            var rows = new List<(int Status, bool HasSettledAt)>();
            while (await reader.ReadAsync())
            {
                rows.Add((reader.GetInt32(0), !reader.IsDBNull(1)));
            }

            rows.Should().Equal((1, true), (3, true), (2, true), (0, false));
        }

        var entitySummaryColumns = await GetColumnsAsync("sweep_run_entity_summary");
        entitySummaryColumns.Should().ContainKey("SkippedCount");
        entitySummaryColumns["SkippedCount"].DataType.Should().Be("bigint");
        entitySummaryColumns["SkippedCount"].IsNullable.Should().BeFalse();
        entitySummaryColumns.Should().ContainKey("RuleSource");
        entitySummaryColumns["RuleSource"].DataType.Should().Be("text");
        entitySummaryColumns["RuleSource"].IsNullable.Should().BeTrue();
        entitySummaryColumns.Should().ContainKey("RuleReason");
        entitySummaryColumns["RuleReason"].DataType.Should().Be("text");
        entitySummaryColumns["RuleReason"].IsNullable.Should().BeTrue();
        (await GetPrimaryKeyColumnsAsync("sweep_run_entity_summary"))
            .Should()
            .Equal("SweepId", "RetentionEntityId", "Category", "TenantId", "Strategy");

        var rowDetailColumns = await GetColumnsAsync("sweep_run_row_detail");
        rowDetailColumns.Should().ContainKey("Id");
        rowDetailColumns["Id"].DataType.Should().Be("bigint");
        rowDetailColumns["Id"].IsNullable.Should().BeFalse();
        rowDetailColumns["Id"].IdentityGeneration.Should().Be("BY DEFAULT");
        rowDetailColumns.Should().ContainKey("CapturedPayload");
        rowDetailColumns["CapturedPayload"].DataType.Should().Be("text");
        rowDetailColumns["CapturedPayload"].IsNullable.Should().BeTrue();
        rowDetailColumns["RetentionEntityId"].DataType.Should().Be("uuid");
        rowDetailColumns["RetentionEntityId"].IsNullable.Should().BeFalse();
        rowDetailColumns.Should().NotContainKey("RuleSource");
        rowDetailColumns.Should().NotContainKey("RuleReason");
        (await GetPrimaryKeyColumnsAsync("sweep_run_row_detail")).Should().Equal("Id");

        var handlerStatusColumns = await GetColumnsAsync("sweep_row_handler_status");
        handlerStatusColumns
            .Should()
            .ContainKeys(
                "Id",
                "SweepRunRowDetailId",
                "HandlerType",
                "DispatchPhase",
                "State",
                "Attempt",
                "QueuedAt",
                "NextAttemptAt",
                "ClaimedAt",
                "ClaimToken",
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
        handlerStatusColumns["DispatchPhase"].DataType.Should().Be("integer");
        handlerStatusColumns["DispatchPhase"].IsNullable.Should().BeFalse();
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
        handlerStatusColumns["ClaimToken"].DataType.Should().Be("uuid");
        handlerStatusColumns["ClaimToken"].IsNullable.Should().BeTrue();
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
        )
            .Should()
            .BeTrue();

        var rowDetailIndexes = await GetIndexDefinitionsAsync("sweep_run_row_detail");
        rowDetailIndexes
            .Should()
            .Contain(index =>
                index.Contains("CREATE UNIQUE INDEX", StringComparison.Ordinal)
                && index.Contains(
                    "(\"SweepId\", \"RetentionEntityId\", \"EntityId\", \"Category\", \"Strategy\", \"TenantId\")",
                    StringComparison.Ordinal
                )
                && !index.Contains(" WHERE ", StringComparison.Ordinal)
            );

        var handlerStatusIndexes = await GetIndexDefinitionsAsync("sweep_row_handler_status");
        handlerStatusIndexes
            .Should()
            .Contain(index =>
                index.Contains("CREATE UNIQUE INDEX", StringComparison.Ordinal)
                && index.Contains(
                    "(\"SweepRunRowDetailId\", \"HandlerType\")",
                    StringComparison.Ordinal
                )
            );
        handlerStatusIndexes
            .Should()
            .Contain(index =>
                index.Contains("CREATE INDEX", StringComparison.Ordinal)
                && index.Contains(
                    "(\"State\", \"NextAttemptAt\", \"Id\")",
                    StringComparison.Ordinal
                )
            );

        var holdIndexes = await GetIndexDefinitionsAsync("retention_holds");
        holdIndexes
            .Should()
            .Contain(index =>
                index.Contains("CREATE INDEX", StringComparison.Ordinal)
                && index.Contains(
                    "(\"RetentionEntityId\", \"TenantId\", \"RecordId\")",
                    StringComparison.Ordinal
                )
            );
        holdIndexes
            .Should()
            .Contain(index =>
                index.Contains("CREATE INDEX", StringComparison.Ordinal)
                && index.Contains("(\"RetentionEntityId\", \"RecordId\")", StringComparison.Ordinal)
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
        columns
            .Should()
            .ContainKeys(
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
        return new DbContextOptionsBuilder<SampleDbContext>().UseNpgsql(connectionString).Options;
    }

    private async Task SeedHistoricalAuditRowsAsync(Guid sweepId, Guid tenantId)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO "sweep_run"
                ("SweepId", "StartedAt", "TriggerKind", "DryRun", "TenantId", "TotalAffected", "Error", "Status", "SettledAt")
            VALUES
                (@sweepId, '2026-07-11T12:00:00Z', 2, FALSE, @tenantId, 2, NULL, 1, '2026-07-11T12:01:00Z');

            INSERT INTO "sweep_run_entity_summary"
                ("SweepId", "EntityType", "Category", "TenantId", "Strategy", "Affected", "At", "HeldCount", "ResolvedPeriod", "SkippedCount", "NullAnchorCount", "RuleSource", "RuleReason")
            VALUES
                (@sweepId, 'Cohort.Sample.Entities.Note', 'known', @tenantId, 0, 1, '2026-07-11T12:00:00Z', 0, INTERVAL '30 days', 0, 0, 'historical', 'known preserved');

            INSERT INTO "sweep_run_row_detail"
                ("SweepId", "EntityType", "EntityId", "Category", "Strategy", "TenantId", "At", "CapturedPayload")
            VALUES
                (@sweepId, 'Cohort.Sample.Entities.Note', 'known-record', 'known', 0, @tenantId, '2026-07-11T12:00:00Z', 'known payload');
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("tenantId", tenantId);
        await command.ExecuteNonQueryAsync();
    }

    private async Task SeedAllHistoricalAuditRowsAsync(Guid sweepId, Guid tenantId)
    {
        await SeedHistoricalAuditRowsAsync(sweepId, tenantId);

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        foreach (
            var mapping in StableEntityMappings.Where(mapping =>
                mapping.EntityType != "Cohort.Sample.Entities.Note"
            )
        )
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                INSERT INTO "sweep_run_entity_summary"
                    ("SweepId", "EntityType", "Category", "TenantId", "Strategy", "Affected", "At", "HeldCount", "ResolvedPeriod", "SkippedCount", "NullAnchorCount", "RuleSource", "RuleReason")
                VALUES
                    (@sweepId, @entityType, @entityType, @tenantId, 0, 1, '2026-07-11T12:00:00Z', 0, INTERVAL '30 days', 0, 0, 'historical', 'known preserved');

                INSERT INTO "sweep_run_row_detail"
                    ("SweepId", "EntityType", "EntityId", "Category", "Strategy", "TenantId", "At", "CapturedPayload")
                VALUES
                    (@sweepId, @entityType, @entityType, @entityType, 0, @tenantId, '2026-07-11T12:00:00Z', 'known payload');
                """;
            command.Parameters.AddWithValue("sweepId", sweepId);
            command.Parameters.AddWithValue("entityType", mapping.EntityType);
            command.Parameters.AddWithValue("tenantId", tenantId);
            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task<Dictionary<string, ColumnSchema>> GetColumnsAsync(string tableName)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = """
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
        command.CommandText = """
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
        command.CommandText = """
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
        command.CommandText = """
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

    private sealed record ColumnSchema(
        bool IsNullable,
        string DataType,
        string? IdentityGeneration
    );
}
