using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class SampleMigrationsEndToEndTests(PostgresFixture fixture) : IAsyncLifetime
{
    // The last migration whose schema still has sweep_row_handler_status; the next drops it.
    private const string LastMigrationWithHandlerStatus = "20261107150342_AddSampleOutbox";

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
            SELECT summary."EntityType", summary."RetentionEntityId", detail."RetentionEntityId", detail."EntityId" AS "RecordId"
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
        var rows = new List<(string EntityType, Guid SummaryId, Guid DetailId, string RecordId)>();
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
                    && row.RecordId
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
    public async Task Later_Migrations_Preserve_Historical_Error_And_LastError_Bytes()
    {
        const string historicalError = "legacy Error: quote=' slash=\\ tab=\t line1\nline2 trailing  ";
        const string historicalLastError =
            "legacy LastError: code=host-owned; details=[A|B]\r\nsecond line  ";
        var options = CreateOptions();
        var sweepId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();

        await using (var db = new SampleDbContext(options))
        {
            var migrator = db.Database.GetService<IMigrator>();
            await migrator.MigrateAsync("20260711150000_AddExplicitSweepRunStatus");
            await SeedHistoricalAuditRowsAsync(sweepId, tenantId);
            await db.Database.ExecuteSqlRawAsync(
                "UPDATE \"sweep_run\" SET \"Error\" = {0} WHERE \"SweepId\" = {1}",
                historicalError,
                sweepId
            );
            await db.Database.ExecuteSqlRawAsync(
                """
                INSERT INTO "sweep_row_handler_status"
                    ("SweepRunRowDetailId", "HandlerType", "DispatchPhase", "State", "Attempt", "QueuedAt", "NextAttemptAt", "CompletedAt", "LastError")
                SELECT "Id", {0}, 1, 3, 1, {1}, {1}, {1}, {2}
                FROM "sweep_run_row_detail"
                WHERE "SweepId" = {3}
                """,
                "Legacy.DiagnosticHandler",
                new DateTimeOffset(2026, 7, 11, 12, 2, 0, TimeSpan.Zero),
                historicalLastError,
                sweepId
            );

            await migrator.MigrateAsync(LastMigrationWithHandlerStatus);
        }

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                run."Error",
                status."LastError",
                pg_catalog.convert_to(run."Error", 'UTF8'),
                pg_catalog.convert_to(status."LastError", 'UTF8')
            FROM "sweep_run" AS run
            INNER JOIN "sweep_run_row_detail" AS detail ON detail."SweepId" = run."SweepId"
            INNER JOIN "sweep_row_handler_status" AS status
                ON status."SweepRunRowDetailId" = detail."Id"
            WHERE run."SweepId" = @sweepId
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetString(0).Should().Be(historicalError);
        reader.GetString(1).Should().Be(historicalLastError);
        reader.GetFieldValue<byte[]>(2).Should().Equal(System.Text.Encoding.UTF8.GetBytes(historicalError));
        reader
            .GetFieldValue<byte[]>(3)
            .Should()
            .Equal(System.Text.Encoding.UTF8.GetBytes(historicalLastError));
    }

    // A 0.11.0 database holds a row a failing dependents step skipped as a row detail plus a
    // dead-lettered status, written by PersistBeforeFailureAsync: State 3, Attempt 1, never
    // claimed, and QueuedAt, NextAttemptAt, and CompletedAt from one database-clock instant. An
    // after-commit handler dead-lettered on its first attempt, its payload scrubbed, differs only
    // in its CompletedAt, the host clock at dead-letter time. The migration that drops the table
    // moves the first's LastError into Failure and leaves the second's row detail, a mutated
    // row's, NULL. Two more statuses share the skipped row's instants and differ only in State
    // (a handler that succeeded) or Attempt (a dead-letter on a later attempt), so each of those
    // terms is needed too. A dead-letter can't carry a claim (CK_sweep_row_handler_status_Claim),
    // so no seed can differ only in ClaimedAt.
    [Fact]
    public async Task Remove_Row_Handler_Queue_Migration_Moves_Only_A_Skipped_Rows_Error_Into_Failure()
    {
        const string skippedError = "type=System.InvalidOperationException;code=;diagnosticId=0123456789abcdef";
        const string afterCommitError = "type=System.TimeoutException;code=;diagnosticId=fedcba9876543210";
        const string succeededError = "type=System.TimeoutException;code=;diagnosticId=00000000000000aa";
        const string retriedError = "type=System.TimeoutException;code=;diagnosticId=00000000000000bb";
        var options = CreateOptions();
        var sweepId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var queuedAt = new DateTimeOffset(2026, 10, 30, 3, 0, 0, TimeSpan.Zero);

        await using (var db = new SampleDbContext(options))
        {
            var migrator = db.Database.GetService<IMigrator>();
            await migrator.MigrateAsync(LastMigrationWithHandlerStatus);
            await using (var connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = """
                    INSERT INTO "sweep_run"
                        ("SweepId", "StartedAt", "Status", "SettledAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected", "Error")
                    VALUES (@sweepId, @queuedAt, 2, @queuedAt + INTERVAL '1 minute', INTERVAL '1 minute', 2, FALSE, @tenantId, 1, 'reason=rows-skipped');

                    WITH details AS (
                        INSERT INTO "sweep_run_row_detail"
                            ("SweepId", "At", "EntityType", "RetentionEntityId", "RecordId", "Category", "Strategy", "TenantId", "CapturedPayload")
                        VALUES
                            (@sweepId, @queuedAt, 'Cohort.Sample.Entities.Note', @entityId, 'skipped-record', 'short-lived', 0, @tenantId, NULL),
                            (@sweepId, @queuedAt, 'Cohort.Sample.Entities.Note', @entityId, 'mutated-record', 'short-lived', 0, @tenantId, NULL),
                            (@sweepId, @queuedAt, 'Cohort.Sample.Entities.Note', @entityId, 'succeeded-record', 'short-lived', 0, @tenantId, NULL),
                            (@sweepId, @queuedAt, 'Cohort.Sample.Entities.Note', @entityId, 'retried-record', 'short-lived', 0, @tenantId, NULL)
                        RETURNING "Id", "RecordId"
                    )
                    INSERT INTO "sweep_row_handler_status"
                        ("SweepRunRowDetailId", "HandlerType", "DispatchPhase", "State", "Attempt", "QueuedAt", "NextAttemptAt", "ClaimedAt", "ClaimToken", "CompletedAt", "LastError")
                    SELECT details."Id", seed.handler_type, 0, seed.state, seed.attempt, @queuedAt, @queuedAt, NULL, NULL,
                           @queuedAt + seed.completed_after, seed.last_error
                    FROM details
                    JOIN (VALUES
                        ('skipped-record', 'Legacy.Step', 3, 1, INTERVAL '0 seconds', @skippedError),
                        ('mutated-record', 'Legacy.Handler', 3, 1, INTERVAL '2 seconds', @afterCommitError),
                        ('succeeded-record', 'Legacy.Handler', 2, 1, INTERVAL '0 seconds', @succeededError),
                        ('retried-record', 'Legacy.Handler', 3, 2, INTERVAL '0 seconds', @retriedError)
                    ) AS seed (record_id, handler_type, state, attempt, completed_after, last_error)
                        ON seed.record_id = details."RecordId";
                    """;
                command.Parameters.AddWithValue("sweepId", sweepId);
                command.Parameters.AddWithValue("tenantId", tenantId);
                command.Parameters.AddWithValue("queuedAt", queuedAt);
                command.Parameters.AddWithValue("entityId", Note.RetentionIdentity);
                command.Parameters.AddWithValue("skippedError", skippedError);
                command.Parameters.AddWithValue("afterCommitError", afterCommitError);
                command.Parameters.AddWithValue("succeededError", succeededError);
                command.Parameters.AddWithValue("retriedError", retriedError);
                await command.ExecuteNonQueryAsync();
            }

            await migrator.MigrateAsync();
        }

        await using var verify = new NpgsqlConnection(connectionString);
        await verify.OpenAsync();
        await using var read = verify.CreateCommand();
        read.CommandText = """
            SELECT "RecordId", "Failure"
            FROM "sweep_run_row_detail"
            WHERE "SweepId" = @sweepId
            ORDER BY "RecordId"
            """;
        read.Parameters.AddWithValue("sweepId", sweepId);
        var failures = new List<(string RecordId, string? Failure)>();
        await using (var reader = await read.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                failures.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));
            }
        }

        failures
            .Should()
            .Equal(
                ("mutated-record", null),
                ("retried-record", null),
                ("skipped-record", skippedError),
                ("succeeded-record", null)
            );
        (await GetColumnsAsync("sweep_row_handler_status")).Should().BeEmpty("the migration drops the table");
    }

    [Theory]
    // What 0.9 actually wrote: a TimeSpan is days plus a time of day, never months. The day
    // count is kept as recorded, not re-expressed as years; the part of a day is dropped.
    [InlineData("2555 days 12:00:00", 0, 2555)]
    // A hand-written interval with years folds them into months.
    [InlineData("7 years 3 days 12 hours", 84, 3)]
    public async Task Resolved_Period_Migration_Keeps_Each_Summary_Period_As_Months_And_Days(
        string historicalInterval,
        int expectedMonths,
        int expectedDays
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
            await db.Database.ExecuteSqlRawAsync(
                "UPDATE \"sweep_run_entity_summary\" SET \"ResolvedPeriod\" = CAST({0} AS interval) WHERE \"SweepId\" = {1}",
                historicalInterval,
                sweepId
            );

            await migrator.MigrateAsync();
        }

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT \"ResolvedMonths\", \"ResolvedDays\" FROM \"sweep_run_entity_summary\" WHERE \"SweepId\" = @sweepId";
        command.Parameters.AddWithValue("sweepId", sweepId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt32(0).Should().Be(expectedMonths);
        reader.GetInt32(1).Should().Be(expectedDays);
        (await GetColumnsAsync("sweep_run_entity_summary")).Should().NotContainKey("ResolvedPeriod");
    }

    [Fact]
    public async Task Result_Counts_Migration_Adds_Retained_Deferred_And_Blocked_Counts_As_Zero_To_Existing_Summaries()
    {
        var options = CreateOptions();
        var sweepId = Guid.NewGuid();

        await using (var db = new SampleDbContext(options))
        {
            var migrator = db.Database.GetService<IMigrator>();
            await migrator.MigrateAsync("20261022095727_StoreResolvedPeriodAsMonthsAndDays");
            await db.Database.ExecuteSqlRawAsync(
                """
                INSERT INTO "sweep_run"
                    ("SweepId", "StartedAt", "Status", "SettledAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected")
                VALUES ({0}, {1}, 1, {1}, interval '1 second', 2, FALSE, {2}, 1);
                INSERT INTO "sweep_run_entity_summary"
                    ("SweepId", "At", "EntityType", "RetentionEntityId", "Category", "TenantId", "Strategy",
                     "ResolvedMonths", "ResolvedDays", "Affected", "HeldCount", "SkippedCount", "NullAnchorCount")
                VALUES ({0}, {1}, 'Cohort.Sample.Entities.Note', {3}, 'short-lived', {2}, 0, 0, 30, 1, 0, 0, 0);
                """,
                sweepId,
                new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero),
                Guid.NewGuid(),
                RetentionEntityIdentity.For<Note>()
            );

            await migrator.MigrateAsync();
        }

        var columns = await GetColumnsAsync("sweep_run_entity_summary");
        foreach (var column in new[] { "RetainedCount", "DeferredCount", "BlockedCount" })
        {
            columns.Should().ContainKey(column);
            columns[column].DataType.Should().Be("bigint");
            columns[column].IsNullable.Should().BeFalse();
        }

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT \"RetainedCount\", \"DeferredCount\", \"BlockedCount\" FROM \"sweep_run_entity_summary\" WHERE \"SweepId\" = @sweepId";
        command.Parameters.AddWithValue("sweepId", sweepId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt64(0).Should().Be(0);
        reader.GetInt64(1).Should().Be(0);
        reader.GetInt64(2).Should().Be(0);
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
        rowDetailColumns.Should().ContainKey("Failure");
        rowDetailColumns["Failure"].DataType.Should().Be("text");
        rowDetailColumns["Failure"].IsNullable.Should().BeTrue();
        rowDetailColumns.Should().NotContainKey("CapturedPayload");
        rowDetailColumns["RetentionEntityId"].DataType.Should().Be("uuid");
        rowDetailColumns["RetentionEntityId"].IsNullable.Should().BeFalse();
        rowDetailColumns["RecordId"].DataType.Should().Be("text");
        rowDetailColumns["RecordId"].IsNullable.Should().BeFalse();
        rowDetailColumns.Should().NotContainKey("EntityId");
        rowDetailColumns.Should().NotContainKey("RuleSource");
        rowDetailColumns.Should().NotContainKey("RuleReason");
        (await GetPrimaryKeyColumnsAsync("sweep_run_row_detail")).Should().Equal("Id");

        (await GetColumnsAsync("sweep_row_handler_status")).Should().BeEmpty("the row-handler queue's table is gone");

        (
            await HasForeignKeyAsync(
                "sweep_run_row_detail",
                "SweepId",
                "sweep_run",
                "SweepId"
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
                    "(\"SweepId\", \"RetentionEntityId\", \"RecordId\", \"Category\", \"Strategy\", \"TenantId\")",
                    StringComparison.Ordinal
                )
                && !index.Contains(" WHERE ", StringComparison.Ordinal)
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

    private DbContextOptions<SampleDbContext> CreateOptions(
        bool schemaQualifyHistoryTable = false
    )
    {
        return new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(
                connectionString,
                options =>
                {
                    if (schemaQualifyHistoryTable)
                    {
                        options.MigrationsHistoryTable("__EFMigrationsHistory", "public");
                    }
                }
            )
            .Options;
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
