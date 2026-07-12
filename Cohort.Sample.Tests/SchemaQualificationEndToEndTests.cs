using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Migrations;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using System.Text.RegularExpressions;

namespace Cohort.Sample.Tests;

// End-to-end test: schema identity crosses EF metadata, every SQL adapter, and PostgreSQL.
public sealed class SchemaQualificationEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    private const string DataSchema = "Tenant Data \"EU\"";
    private const string CohortSchema = "Cohort Store \"EU\"";
    private const string HostileSchema = "Hostile Path \"EU\"";
    private const string RecordsTable = "Mixed Case \"Records\"";

    [Fact]
    public void Raw_PostgreSql_SQL_Does_Not_Contain_Unqualified_Shadowable_Builtins()
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (root.GetFiles("Cohort.slnx").Length == 0)
        {
            root = root.Parent
                ?? throw new InvalidOperationException("Could not locate the Cohort solution root.");
        }

        string[] files =
        [
            "Cohort/Infrastructure/CohortSchemaValidator.cs",
            "Cohort/Infrastructure/Holds/RetentionHoldSql.cs",
            "Cohort/Infrastructure/Holds/RetentionEntityLockSql.cs",
            "Cohort/Infrastructure/RetentionRunAdvisoryLock.cs",
            "Cohort/Infrastructure/Sweep/RelationalSweepStrategyCore.cs",
            "Cohort/Infrastructure/Sweep/AnonymiseSqlBuilder.cs",
            "Cohort/Infrastructure/Handlers/RetentionRowDispatcher.cs",
        ];
        var unqualified = new Regex(
            @"(?<![\w.])(count|statement_timestamp|hashtextextended|unnest|pg_(?:advisory|try_advisory|get_expr)[a-z_]*)\s*\(",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant
        );

        files.SelectMany(file =>
                File.ReadLines(Path.Combine(root.FullName, file))
                    .Select((line, index) => (File: file, Line: index + 1, Text: line))
            )
            .Where(line => unqualified.IsMatch(line.Text))
            .Should().BeEmpty();
    }

    [Fact]
    public async Task Custom_schemas_are_isolated_from_search_path_and_public_decoys()
    {
        var connectionString = new NpgsqlConnectionStringBuilder(ConnectionString)
        {
            SearchPath = $"\"{Escape(HostileSchema)}\", pg_catalog",
        }.ConnectionString;
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var heldId = Guid.NewGuid();
        var sweptId = Guid.NewGuid();
        var erasedId = Guid.NewGuid();
        var softDeleteId = Guid.NewGuid();
        var anonymiseId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);

        await CreateSchemaAsync(connectionString);
        try
        {
            await using var services = BuildServices(connectionString);
            await StartValidationAsync(services);

            await using (var scope = services.CreateAsyncScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<QualifiedDbContext>();
                db.Records.AddRange(
                    new QualifiedRecord(heldId, tenantId, subjectId, now.AddDays(-60), "held"),
                    new QualifiedRecord(sweptId, tenantId, Guid.NewGuid(), now.AddDays(-60), "sweep")
                );
                db.SoftDeleteRecords.Add(
                    new QualifiedSoftDeleteRecord(
                        softDeleteId,
                        tenantId,
                        Guid.NewGuid(),
                        now.AddDays(-60),
                        "soft delete"
                    )
                );
                db.AnonymiseRecords.Add(
                    new QualifiedAnonymiseRecord(
                        anonymiseId,
                        tenantId,
                        Guid.NewGuid(),
                        now.AddDays(-60),
                        "personal@example.test"
                    )
                );
                await db.SaveChangesAsync();

                var holds = scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>();
                await holds.CreateAsync(
                    new RetentionHoldRequest(
                        Guid.NewGuid(),
                        QualifiedRecord.RetentionId,
                        heldId.ToString(),
                        tenantId,
                        "litigation",
                        now
                    ),
                    default
                );
                (await holds.ListActiveAsync(now, default)).Should().ContainSingle();
            }

            var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());
            var preview = await services.GetRequiredService<IRetentionPreview>().PreviewAsync(tenant, now);
            preview.Counts.Where(count => count.Strategy != Strategy.Exempt)
                .Should().HaveCount(3).And.OnlyContain(count => count.Affected == 1);

            var sweep = await services.GetRequiredService<IRetentionSweep>().SweepAsync(tenant, now);
            sweep.EntityFailures.Should().BeEmpty();
            sweep.Counts.Where(count => count.Strategy != Strategy.Exempt)
                .Should().HaveCount(3).And.OnlyContain(count => count.Affected == 1);

            await using (var scope = services.CreateAsyncScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<QualifiedDbContext>();
                (await db.Records.AnyAsync(record => record.Id == heldId)).Should().BeTrue();
                (await db.Records.AnyAsync(record => record.Id == sweptId)).Should().BeFalse();
                (await db.SoftDeleteRecords.SingleAsync(record => record.Id == softDeleteId))
                    .IsDeleted.Should().BeTrue();
                (await db.AnonymiseRecords.SingleAsync(record => record.Id == anonymiseId))
                    .Email.Should().BeEmpty();
                db.Records.Add(
                    new QualifiedRecord(erasedId, tenantId, subjectId, now.AddDays(-60), "erase")
                );
                db.SoftDeleteRecords.Add(
                    new QualifiedSoftDeleteRecord(
                        Guid.NewGuid(),
                        tenantId,
                        subjectId,
                        now.AddDays(-60),
                        "erase soft"
                    )
                );
                db.AnonymiseRecords.Add(
                    new QualifiedAnonymiseRecord(
                        Guid.NewGuid(),
                        tenantId,
                        subjectId,
                        now.AddDays(-60),
                        "erase@example.test"
                    )
                );
                await db.SaveChangesAsync();
            }

            var erasure = await services
                .GetRequiredService<IRetentionErasureService>()
                .EraseAsync(
                    tenant,
                    new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
                    now
                );
            erasure.Counts.Where(count => count.Strategy != Strategy.Exempt)
                .Should().HaveCount(3).And.OnlyContain(count => count.Affected == 1);

            await AssertScheduledLockUsesPgCatalogAsync(
                connectionString,
                services,
                sweep.SweepId,
                now
            );

            (await services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()).Settled
                .Should().BeTrue();

            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $$"""
                SELECT
                    (SELECT pg_catalog.count(*) FROM "{{Escape(CohortSchema)}}"."sweep_run"),
                    (SELECT pg_catalog.count(*) FROM "{{Escape(CohortSchema)}}"."sweep_run_row_detail"),
                    (SELECT pg_catalog.count(*) FROM "{{Escape(CohortSchema)}}"."sweep_row_handler_status"),
                    (SELECT pg_catalog.count(*) FROM public."sweep_run"),
                    (SELECT pg_catalog.count(*) FROM public."{{Escape(RecordsTable)}}")
                """;
            await using var reader = await command.ExecuteReaderAsync();
            await reader.ReadAsync();
            reader.GetInt64(0).Should().Be(2);
            reader.GetInt64(1).Should().Be(6);
            reader.GetInt64(2).Should().Be(6);
            reader.GetInt64(3).Should().Be(0);
            reader.GetInt64(4).Should().Be(1);
        }
        finally
        {
            await DropSchemaAsync(connectionString);
        }
    }

    [Fact]
    public async Task Host_owned_move_preserves_populated_Cohort_tables_and_runtime_behavior()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var connectionString = new NpgsqlConnectionStringBuilder(database.ConnectionString)
        {
            SearchPath = $"\"{Escape(HostileSchema)}\", pg_catalog",
        }.ConnectionString;
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var heldId = Guid.NewGuid();
        var sweptId = Guid.NewGuid();
        var erasedId = Guid.NewGuid();
        var holdId = Guid.NewGuid();
        var historicalSweepId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var schemas = connection.CreateCommand();
            schemas.CommandText = $$"""
                CREATE SCHEMA "{{Escape(HostileSchema)}}";
                CREATE SCHEMA "{{Escape(CohortSchema)}}";
                CREATE FUNCTION "{{Escape(HostileSchema)}}".statement_timestamp()
                RETURNS timestamp with time zone LANGUAGE sql IMMUTABLE
                AS $body$ SELECT '-infinity'::pg_catalog.timestamptz $body$;
                CREATE FUNCTION "{{Escape(HostileSchema)}}".pg_advisory_xact_lock(bigint)
                RETURNS void LANGUAGE plpgsql AS $body$ BEGIN RAISE EXCEPTION 'hostile lock shadow'; END $body$;
                CREATE FUNCTION "{{Escape(HostileSchema)}}".hashtextextended(text, bigint)
                RETURNS bigint LANGUAGE plpgsql AS $body$ BEGIN RAISE EXCEPTION 'hostile hash shadow'; END $body$;
                CREATE FUNCTION "{{Escape(HostileSchema)}}".unnest(text[])
                RETURNS SETOF text LANGUAGE plpgsql AS $body$ BEGIN RAISE EXCEPTION 'hostile unnest shadow'; END $body$;
                """;
            await schemas.ExecuteNonQueryAsync();
        }

        var sourceOptions = new DbContextOptionsBuilder<MoveSourceDbContext>()
            .UseNpgsql(connectionString)
            .Options;
        await using (var source = new MoveSourceDbContext(sourceOptions))
        {
            await source.Database.ExecuteSqlRawAsync(source.Database.GenerateCreateScript());
            source.Records.AddRange(
                new QualifiedRecord(heldId, tenantId, Guid.NewGuid(), now.AddDays(-60), "held before move"),
                new QualifiedRecord(sweptId, tenantId, Guid.NewGuid(), now.AddDays(-60), "swept after move")
            );
            await source.SaveChangesAsync();
        }

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $$"""
                INSERT INTO public.retention_holds
                    ("HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt")
                VALUES (@holdId, @retentionEntityId, @recordId, @tenantId, 'pre-move hold', @createdAt);

                INSERT INTO public.sweep_run
                    ("SweepId", "StartedAt", "Status", "SettledAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected", "Error")
                VALUES (@sweepId, @createdAt, 1, @settledAt, interval '1 second', 2, false, @tenantId, 1, @historicalError);
                INSERT INTO public.sweep_run_entity_summary
                    ("SweepId", "At", "EntityType", "RetentionEntityId", "Category", "TenantId", "Strategy", "ResolvedPeriod", "Affected", "HeldCount", "SkippedCount", "NullAnchorCount", "RuleSource", "RuleReason")
                VALUES (@sweepId, @settledAt, @entityType, @retentionEntityId, 'qualified', @tenantId, 0, interval '30 days', 1, 0, 0, 0, 'historical', 'before move');
                INSERT INTO public.sweep_run_row_detail
                    ("Id", "SweepId", "At", "EntityType", "RetentionEntityId", "RecordId", "Category", "Strategy", "TenantId", "CapturedPayload")
                VALUES (900001, @sweepId, @settledAt, @entityType, @retentionEntityId, @recordId, 'qualified', 0, @tenantId, NULL);
                INSERT INTO public.sweep_row_handler_status
                    ("Id", "SweepRunRowDetailId", "HandlerType", "DispatchPhase", "State", "Attempt", "QueuedAt", "NextAttemptAt", "ClaimedAt", "ClaimToken", "CompletedAt", "LastError")
                VALUES (900001, 900001, 'historical-handler', 0, 2, 1, @createdAt, @createdAt, NULL, NULL, @settledAt, @historicalLastError);

                ALTER TABLE public.retention_holds SET SCHEMA "{{Escape(CohortSchema)}}";
                ALTER TABLE public.sweep_run SET SCHEMA "{{Escape(CohortSchema)}}";
                ALTER TABLE public.sweep_run_entity_summary SET SCHEMA "{{Escape(CohortSchema)}}";
                ALTER TABLE public.sweep_run_row_detail SET SCHEMA "{{Escape(CohortSchema)}}";
                ALTER TABLE public.sweep_row_handler_status SET SCHEMA "{{Escape(CohortSchema)}}";
                CREATE TABLE public.sweep_run ("SweepId" uuid PRIMARY KEY);
                """;
            command.Parameters.AddWithValue("holdId", holdId);
            command.Parameters.AddWithValue("retentionEntityId", QualifiedRecord.RetentionId);
            command.Parameters.AddWithValue("recordId", heldId.ToString());
            command.Parameters.AddWithValue("tenantId", tenantId);
            command.Parameters.AddWithValue("createdAt", now.AddDays(-2));
            command.Parameters.AddWithValue("settledAt", now.AddDays(-2).AddSeconds(1));
            command.Parameters.AddWithValue("sweepId", historicalSweepId);
            command.Parameters.AddWithValue("entityType", typeof(QualifiedRecord).FullName!);
            command.Parameters.AddWithValue("historicalError", "historical error before move");
            command.Parameters.AddWithValue("historicalLastError", "historical handler error before move");
            await command.ExecuteNonQueryAsync();
        }

        await using var services = BuildServices(connectionString);
        await StartValidationAsync(services);
        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());
        await using (var scope = services.CreateAsyncScope())
        {
            var holds = scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>();
            (await holds.ListActiveAsync(now, default))
                .Should().ContainSingle(hold => hold.HoldId == holdId);
        }

        var sweep = await services.GetRequiredService<IRetentionSweep>().SweepAsync(tenant, now);
        sweep.Counts.Should().ContainSingle(count =>
            count.EntityType == typeof(QualifiedRecord)
            && count.Affected == 1
            && count.HeldCount == 1
        );

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<QualifiedDbContext>();
            (await db.Records.AnyAsync(record => record.Id == heldId)).Should().BeTrue();
            (await db.Records.AnyAsync(record => record.Id == sweptId)).Should().BeFalse();
            db.Records.Add(new QualifiedRecord(
                erasedId,
                tenantId,
                subjectId,
                now.AddDays(-1),
                "erased after move"
            ));
            await db.SaveChangesAsync();
        }

        var erasure = await services.GetRequiredService<IRetentionErasureService>().EraseAsync(
            tenant,
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            now
        );
        erasure.Counts.Should().ContainSingle(count =>
            count.EntityType == typeof(QualifiedRecord) && count.Affected == 1
        );
        (await services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()).Settled
            .Should().BeTrue();

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var verify = connection.CreateCommand();
            verify.CommandText = $$"""
                SELECT hold."HoldId", hold."RetentionEntityId", hold."RecordId", hold."TenantId",
                       hold."Reason", hold."CreatedAt", hold."ExpiresAt", hold."RemovedAt",
                       run."SweepId", run."StartedAt", run."Status", run."SettledAt", run."Duration",
                       run."TriggerKind", run."DryRun", run."TenantId", run."TotalAffected", run."Error",
                       summary."SweepId", summary."At", summary."EntityType", summary."RetentionEntityId",
                       summary."Category", summary."TenantId", summary."Strategy", summary."ResolvedPeriod",
                       summary."Affected", summary."HeldCount", summary."SkippedCount", summary."NullAnchorCount",
                       summary."RuleSource", summary."RuleReason",
                       detail."Id", detail."SweepId", detail."At", detail."EntityType",
                       detail."RetentionEntityId", detail."RecordId", detail."Category", detail."Strategy",
                       detail."TenantId", detail."CapturedPayload",
                       status."Id", status."SweepRunRowDetailId", status."HandlerType", status."DispatchPhase",
                       status."State", status."Attempt", status."QueuedAt", status."NextAttemptAt",
                       status."ClaimedAt", status."ClaimToken", status."CompletedAt", status."LastError",
                       (SELECT pg_catalog.count(*) FROM public.sweep_run)
                FROM "{{Escape(CohortSchema)}}".sweep_run run
                JOIN "{{Escape(CohortSchema)}}".retention_holds hold ON hold."HoldId" = @holdId
                JOIN "{{Escape(CohortSchema)}}".sweep_run_entity_summary summary ON summary."SweepId" = run."SweepId"
                JOIN "{{Escape(CohortSchema)}}".sweep_run_row_detail detail ON detail."SweepId" = run."SweepId"
                JOIN "{{Escape(CohortSchema)}}".sweep_row_handler_status status ON status."SweepRunRowDetailId" = detail."Id"
                WHERE run."SweepId" = @sweepId
                """;
            verify.Parameters.AddWithValue("holdId", holdId);
            verify.Parameters.AddWithValue("sweepId", historicalSweepId);
            await using var reader = await verify.ExecuteReaderAsync();
            (await reader.ReadAsync()).Should().BeTrue();
            var createdAt = now.AddDays(-2);
            var settledAt = createdAt.AddSeconds(1);
            reader.GetGuid(0).Should().Be(holdId);
            reader.GetGuid(1).Should().Be(QualifiedRecord.RetentionId);
            reader.GetString(2).Should().Be(heldId.ToString());
            reader.GetGuid(3).Should().Be(tenantId);
            reader.GetString(4).Should().Be("pre-move hold");
            reader.GetFieldValue<DateTimeOffset>(5).Should().Be(createdAt);
            reader.IsDBNull(6).Should().BeTrue();
            reader.IsDBNull(7).Should().BeTrue();
            reader.GetGuid(8).Should().Be(historicalSweepId);
            reader.GetFieldValue<DateTimeOffset>(9).Should().Be(createdAt);
            reader.GetInt32(10).Should().Be(1);
            reader.GetFieldValue<DateTimeOffset>(11).Should().Be(settledAt);
            reader.GetFieldValue<TimeSpan>(12).Should().Be(TimeSpan.FromSeconds(1));
            reader.GetInt32(13).Should().Be(2);
            reader.GetBoolean(14).Should().BeFalse();
            reader.GetGuid(15).Should().Be(tenantId);
            reader.GetInt64(16).Should().Be(1);
            reader.GetString(17).Should().Be("historical error before move");
            reader.GetGuid(18).Should().Be(historicalSweepId);
            reader.GetFieldValue<DateTimeOffset>(19).Should().Be(settledAt);
            reader.GetString(20).Should().Be(typeof(QualifiedRecord).FullName);
            reader.GetGuid(21).Should().Be(QualifiedRecord.RetentionId);
            reader.GetString(22).Should().Be("qualified");
            reader.GetGuid(23).Should().Be(tenantId);
            reader.GetInt32(24).Should().Be(0);
            reader.GetFieldValue<TimeSpan>(25).Should().Be(TimeSpan.FromDays(30));
            reader.GetInt64(26).Should().Be(1);
            reader.GetInt64(27).Should().Be(0);
            reader.GetInt64(28).Should().Be(0);
            reader.GetInt64(29).Should().Be(0);
            reader.GetString(30).Should().Be("historical");
            reader.GetString(31).Should().Be("before move");
            reader.GetInt64(32).Should().Be(900001);
            reader.GetGuid(33).Should().Be(historicalSweepId);
            reader.GetFieldValue<DateTimeOffset>(34).Should().Be(settledAt);
            reader.GetString(35).Should().Be(typeof(QualifiedRecord).FullName);
            reader.GetGuid(36).Should().Be(QualifiedRecord.RetentionId);
            reader.GetString(37).Should().Be(heldId.ToString());
            reader.GetString(38).Should().Be("qualified");
            reader.GetInt32(39).Should().Be(0);
            reader.GetGuid(40).Should().Be(tenantId);
            reader.IsDBNull(41).Should().BeTrue();
            reader.GetInt64(42).Should().Be(900001);
            reader.GetInt64(43).Should().Be(900001);
            reader.GetString(44).Should().Be("historical-handler");
            reader.GetInt32(45).Should().Be(0);
            reader.GetInt32(46).Should().Be(2);
            reader.GetInt32(47).Should().Be(1);
            reader.GetFieldValue<DateTimeOffset>(48).Should().Be(createdAt);
            reader.GetFieldValue<DateTimeOffset>(49).Should().Be(createdAt);
            reader.IsDBNull(50).Should().BeTrue();
            reader.IsDBNull(51).Should().BeTrue();
            reader.GetFieldValue<DateTimeOffset>(52).Should().Be(settledAt);
            reader.GetString(53).Should().Be("historical handler error before move");
            reader.GetInt64(54).Should().Be(0);
        }

        await AssertExactMovedSchemaContractAsync(connectionString);
    }

    private static async Task AssertExactMovedSchemaContractAsync(string connectionString)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        var expectedIndexes = CohortSchemaContract.Tables.SelectMany(table =>
                new[] { $"{table.Role}|p|{string.Join(",", table.PrimaryKey)}" }
                    .Concat(table.RequiredIndexes.Select(index =>
                        $"{table.Role}|{(index.Unique ? "u" : "n")}|{string.Join(",", index.Columns)}|{NormalizeIndexPredicate(index.Predicate)}"
                    ))
            )
            .Order(StringComparer.Ordinal)
            .ToArray();
        var actualIndexes = await ReadCatalogSignaturesAsync(connection, """
            SELECT relation.relname || '|' ||
                   CASE WHEN idx.indisprimary THEN 'p' WHEN idx.indisunique THEN 'u' ELSE 'n' END || '|' ||
                   pg_catalog.array_to_string(ARRAY(
                       SELECT attribute.attname
                        FROM pg_catalog.unnest(idx.indkey) WITH ORDINALITY key(attnum, position)
                       JOIN pg_catalog.pg_attribute attribute
                          ON attribute.attrelid = idx.indrelid AND attribute.attnum = key.attnum
                        WHERE key.position <= idx.indnkeyatts ORDER BY key.position
                   ), ',') || CASE WHEN idx.indisprimary THEN '' ELSE '|' ||
                   COALESCE(pg_catalog.upper(pg_catalog.regexp_replace(
                       pg_catalog.pg_get_expr(idx.indpred, idx.indrelid), '[^[:alnum:]]', '', 'g'
                   )), '') END
            FROM pg_catalog.pg_index idx
            JOIN pg_catalog.pg_class relation ON relation.oid = idx.indrelid
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = @schema AND idx.indisvalid AND idx.indisready
            ORDER BY 1
            """);
        actualIndexes.Should().Equal(expectedIndexes);

        var expectedChecks = CohortSchemaContract.Tables.SelectMany(table =>
                table.RequiredChecks.Select(check =>
                    $"{table.Role}|{check.Name}|{CohortSchemaValidator.NormalizeSql(check.NormalizedSql)}"
                )
            ).Order(StringComparer.Ordinal).ToArray();
        var actualChecks = (await ReadCatalogSignaturesAsync(connection, """
            SELECT relation.relname || '|' || con.conname || '|' ||
                   pg_catalog.pg_get_expr(con.conbin, con.conrelid)
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class relation ON relation.oid = con.conrelid
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = @schema AND con.contype = 'c' AND con.convalidated
            ORDER BY 1
            """))
            .Select(value =>
            {
                var parts = value.Split('|', 3);
                return $"{parts[0]}|{parts[1]}|{CohortSchemaValidator.NormalizeSql(parts[2])}";
            })
            .ToArray();
        actualChecks.Should().Equal(expectedChecks);

        var expectedForeignKeys = CohortSchemaContract.Tables.SelectMany(table =>
                table.RequiredForeignKeys.Select(foreignKey =>
                    $"{table.Role}|{string.Join(",", foreignKey.Columns)}|{foreignKey.PrincipalTable}|{string.Join(",", foreignKey.PrincipalColumns)}|{foreignKey.CatalogDeleteAction}"
                )
            ).Order(StringComparer.Ordinal).ToArray();
        var actualForeignKeys = await ReadCatalogSignaturesAsync(connection, """
            SELECT dependent.relname || '|' ||
                   pg_catalog.array_to_string(ARRAY(
                       SELECT attribute.attname FROM pg_catalog.unnest(con.conkey) WITH ORDINALITY key(attnum, position)
                       JOIN pg_catalog.pg_attribute attribute ON attribute.attrelid = con.conrelid AND attribute.attnum = key.attnum
                       ORDER BY key.position
                   ), ',') || '|' || principal.relname || '|' ||
                   pg_catalog.array_to_string(ARRAY(
                       SELECT attribute.attname FROM pg_catalog.unnest(con.confkey) WITH ORDINALITY key(attnum, position)
                       JOIN pg_catalog.pg_attribute attribute ON attribute.attrelid = con.confrelid AND attribute.attnum = key.attnum
                       ORDER BY key.position
                    ), ',') || '|' || con.confdeltype::pg_catalog.text
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class dependent ON dependent.oid = con.conrelid
            JOIN pg_catalog.pg_class principal ON principal.oid = con.confrelid
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid = dependent.relnamespace
            WHERE namespace.nspname = @schema AND con.contype = 'f' AND con.convalidated
            ORDER BY 1
            """);
        actualForeignKeys.Should().Equal(expectedForeignKeys);
    }

    private static string NormalizeIndexPredicate(string? predicate) => predicate is null
        ? ""
        : new string(predicate.Where(char.IsLetterOrDigit).ToArray()).ToUpperInvariant();

    private static async Task<string[]> ReadCatalogSignaturesAsync(
        NpgsqlConnection connection,
        string sql
    )
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("schema", CohortSchema);
        await using var reader = await command.ExecuteReaderAsync();
        var values = new List<string>();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetString(0));
        }
        return values.ToArray();
    }

    private static ServiceProvider BuildServices(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(
            new ConfigurationBuilder().AddInMemoryCollection().Build()
        );
        services.AddDbContext<QualifiedDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider, QualifiedRuleProvider>();
        services.AddCohort<QualifiedDbContext>();
        services.AddRowHandler<QualifiedRecord, QualifiedRecordHandler>();
        services.AddRowHandler<QualifiedSoftDeleteRecord, QualifiedSoftDeleteRecordHandler>();
        services.AddRowHandler<QualifiedAnonymiseRecord, QualifiedAnonymiseRecordHandler>();
        return services.BuildServiceProvider(validateScopes: true);
    }

    private static async Task StartValidationAsync(ServiceProvider services)
    {
        var validation = services
            .GetServices<IHostedService>()
            .Single(service => service.GetType().Name == "RetentionValidationHostedService");
        await validation.StartAsync(default);
    }

    private static async Task CreateSchemaAsync(string connectionString)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = $$"""
                CREATE SCHEMA "{{Escape(HostileSchema)}}";
                CREATE TABLE public."{{Escape(RecordsTable)}}" ("Id" uuid PRIMARY KEY);
                INSERT INTO public."{{Escape(RecordsTable)}}" ("Id") VALUES ('00000000-0000-0000-0000-000000000001');

                CREATE FUNCTION "{{Escape(HostileSchema)}}".statement_timestamp()
                RETURNS timestamp with time zone LANGUAGE sql IMMUTABLE
                AS $body$ SELECT '-infinity'::pg_catalog.timestamptz $body$;

                CREATE FUNCTION "{{Escape(HostileSchema)}}".fail_shadow()
                RETURNS void LANGUAGE plpgsql
                AS $body$ BEGIN RAISE EXCEPTION 'hostile search_path shadow was invoked'; END $body$;
                CREATE FUNCTION "{{Escape(HostileSchema)}}".pg_advisory_lock(bigint)
                RETURNS void LANGUAGE sql AS $body$ SELECT "{{Escape(HostileSchema)}}".fail_shadow() $body$;
                CREATE FUNCTION "{{Escape(HostileSchema)}}".pg_try_advisory_lock(bigint)
                RETURNS boolean LANGUAGE plpgsql AS $body$ BEGIN RAISE EXCEPTION 'hostile lock shadow'; END $body$;
                CREATE FUNCTION "{{Escape(HostileSchema)}}".pg_advisory_unlock(bigint)
                RETURNS boolean LANGUAGE plpgsql AS $body$ BEGIN RAISE EXCEPTION 'hostile unlock shadow'; END $body$;
                CREATE FUNCTION "{{Escape(HostileSchema)}}".pg_advisory_xact_lock(bigint)
                RETURNS void LANGUAGE sql AS $body$ SELECT "{{Escape(HostileSchema)}}".fail_shadow() $body$;
                CREATE FUNCTION "{{Escape(HostileSchema)}}".hashtextextended(text, bigint)
                RETURNS bigint LANGUAGE plpgsql AS $body$ BEGIN RAISE EXCEPTION 'hostile hash shadow'; END $body$;

                CREATE FUNCTION "{{Escape(HostileSchema)}}".unnest(text[])
                RETURNS SETOF text LANGUAGE plpgsql AS $body$ BEGIN RAISE EXCEPTION 'hostile unnest shadow'; END $body$;
                CREATE FUNCTION "{{Escape(HostileSchema)}}".unnest(bigint[])
                RETURNS SETOF bigint LANGUAGE plpgsql AS $body$ BEGIN RAISE EXCEPTION 'hostile unnest shadow'; END $body$;
                CREATE FUNCTION "{{Escape(HostileSchema)}}".unnest(text[], text[], text[])
                RETURNS TABLE(role text, schema_name text, table_name text) LANGUAGE plpgsql
                AS $body$ BEGIN RAISE EXCEPTION 'hostile unnest shadow'; END $body$;
                CREATE FUNCTION "{{Escape(HostileSchema)}}".pg_get_expr(pg_catalog.pg_node_tree, pg_catalog.oid)
                RETURNS text LANGUAGE plpgsql AS $body$ BEGIN RAISE EXCEPTION 'hostile pg_get_expr shadow'; END $body$;

                CREATE FUNCTION "{{Escape(HostileSchema)}}".decoy_count(bigint)
                RETURNS bigint LANGUAGE sql IMMUTABLE AS $body$ SELECT 777::bigint $body$;
                CREATE AGGREGATE "{{Escape(HostileSchema)}}".count(*) (
                    SFUNC = "{{Escape(HostileSchema)}}".decoy_count,
                    STYPE = bigint,
                    INITCOND = '777'
                );

                CREATE VIEW "{{Escape(HostileSchema)}}".pg_namespace AS
                    SELECT 0::pg_catalog.oid AS oid, 'decoy'::text AS nspname WHERE false;
                CREATE VIEW "{{Escape(HostileSchema)}}".pg_class AS
                    SELECT 0::pg_catalog.oid AS oid,
                           0::pg_catalog.oid AS relnamespace,
                           'decoy'::text AS relname,
                           NULL::pg_catalog."char" AS relkind
                    WHERE false;
                """;
            await command.ExecuteNonQueryAsync();
        }

        var options = new DbContextOptionsBuilder<QualifiedDbContext>()
            .UseNpgsql(connectionString)
            .Options;
        await using var db = new QualifiedDbContext(options);
        await db.Database.ExecuteSqlRawAsync(db.Database.GenerateCreateScript());
    }

    private static async Task DropSchemaAsync(string connectionString)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $$"""
            DROP SCHEMA IF EXISTS "{{Escape(DataSchema)}}" CASCADE;
            DROP SCHEMA IF EXISTS "{{Escape(CohortSchema)}}" CASCADE;
            DROP SCHEMA IF EXISTS "{{Escape(HostileSchema)}}" CASCADE;
            DROP TABLE IF EXISTS public."{{Escape(RecordsTable)}}";
            """;
        await command.ExecuteNonQueryAsync();
    }

    private static string Escape(string identifier) => identifier.Replace("\"", "\"\"");

    private static async Task AssertScheduledLockUsesPgCatalogAsync(
        string connectionString,
        ServiceProvider services,
        Guid sweepId,
        DateTimeOffset now
    )
    {
        await using var owner = new NpgsqlConnection(connectionString);
        await owner.OpenAsync();
        await using (var prepare = owner.CreateCommand())
        {
            prepare.CommandText = $$"""
                UPDATE "{{Escape(CohortSchema)}}"."sweep_run"
                SET "Status" = @started,
                    "StartedAt" = @startedAt,
                    "SettledAt" = NULL,
                    "Duration" = NULL
                WHERE "SweepId" = @sweepId;
                SELECT pg_catalog.pg_advisory_lock(@lockKey);
                """;
            prepare.Parameters.AddWithValue("started", (int)SweepRunStatus.Started);
            prepare.Parameters.AddWithValue("startedAt", now.AddDays(-1));
            prepare.Parameters.AddWithValue("sweepId", sweepId);
            prepare.Parameters.AddWithValue("lockKey", RetentionRunAdvisoryLock.GetKey(sweepId));
            await prepare.ExecuteNonQueryAsync();
        }

        var dispatcher = services.GetRequiredService<IRetentionRowDispatcher>();
        await dispatcher.FlushAsync();
        (await ReadRunStatusAsync(connectionString, sweepId)).Should().Be(SweepRunStatus.Started);

        await using (var release = owner.CreateCommand())
        {
            release.CommandText = "SELECT pg_catalog.pg_advisory_unlock(@lockKey)";
            release.Parameters.AddWithValue("lockKey", RetentionRunAdvisoryLock.GetKey(sweepId));
            (await release.ExecuteScalarAsync()).Should().Be(true);
        }

        await dispatcher.FlushAsync();
        (await ReadRunStatusAsync(connectionString, sweepId)).Should().Be(SweepRunStatus.Failed);
    }

    private static async Task<SweepRunStatus> ReadRunStatusAsync(
        string connectionString,
        Guid sweepId
    )
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $$"""
            SELECT "Status"
            FROM "{{Escape(CohortSchema)}}"."sweep_run"
            WHERE "SweepId" = @sweepId
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        return (SweepRunStatus)(int)(await command.ExecuteScalarAsync())!;
    }

    private sealed class QualifiedDbContext(DbContextOptions<QualifiedDbContext> options)
        : DbContext(options)
    {
        public DbSet<QualifiedRecord> Records => Set<QualifiedRecord>();
        public DbSet<QualifiedSoftDeleteRecord> SoftDeleteRecords => Set<QualifiedSoftDeleteRecord>();
        public DbSet<QualifiedAnonymiseRecord> AnonymiseRecords => Set<QualifiedAnonymiseRecord>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<QualifiedRecord>(builder =>
            {
                builder.ToTable(RecordsTable, DataSchema);
                builder.HasKey(record => record.Id);
            });
            modelBuilder.Entity<QualifiedSoftDeleteRecord>(builder =>
            {
                builder.ToTable("Soft Delete \"Records\"", DataSchema);
                builder.HasKey(record => record.Id);
            });
            modelBuilder.Entity<QualifiedAnonymiseRecord>(builder =>
            {
                builder.ToTable("Anonymise \"Records\"", DataSchema);
                builder.HasKey(record => record.Id);
            });
            modelBuilder.ConfigureCohortTables(CohortSchema);
        }
    }

    private sealed class MoveSourceDbContext(DbContextOptions<MoveSourceDbContext> options)
        : DbContext(options)
    {
        public DbSet<QualifiedRecord> Records => Set<QualifiedRecord>();
        public DbSet<QualifiedSoftDeleteRecord> SoftDeleteRecords => Set<QualifiedSoftDeleteRecord>();
        public DbSet<QualifiedAnonymiseRecord> AnonymiseRecords => Set<QualifiedAnonymiseRecord>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<QualifiedRecord>(builder =>
            {
                builder.ToTable(RecordsTable, DataSchema);
                builder.HasKey(record => record.Id);
            });
            modelBuilder.Entity<QualifiedSoftDeleteRecord>(builder =>
            {
                builder.ToTable("Soft Delete \"Records\"", DataSchema);
                builder.HasKey(record => record.Id);
            });
            modelBuilder.Entity<QualifiedAnonymiseRecord>(builder =>
            {
                builder.ToTable("Anonymise \"Records\"", DataSchema);
                builder.HasKey(record => record.Id);
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("qualified", nameof(CreatedAt), AuditRowDetail = AuditRowDetail.PerRow)]
    [RetentionEntityId("6e0196b1-cb7b-4e36-98de-4fa00559bb8d")]
    private sealed class QualifiedRecord(
        Guid id,
        Guid tenantId,
        Guid subjectId,
        DateTimeOffset createdAt,
        string payload
    )
    {
        internal static readonly Guid RetentionId =
            Guid.Parse("6e0196b1-cb7b-4e36-98de-4fa00559bb8d");

        public Guid Id { get; set; } = id;
        public Guid TenantId { get; set; } = tenantId;
        [ErasureSubject]
        public Guid SubjectId { get; set; } = subjectId;
        public DateTimeOffset CreatedAt { get; set; } = createdAt;
        public string Payload { get; set; } = payload;
    }

    private sealed class QualifiedRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category switch
            {
                "qualified" => new([Strategy.Purge]),
                "qualified-soft-delete" => new([Strategy.SoftDelete]),
                "qualified-anonymise" => new([Strategy.Anonymise]),
                _ => null,
            };

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        ) => Task.FromResult<RetentionRule?>(
            context.Category switch
            {
                "qualified" => new RetentionRule(
                    TimeSpan.FromDays(30),
                    Strategy.Purge,
                    AuditRowDetail: AuditRowDetail.PerRow
                ),
                "qualified-soft-delete" => new RetentionRule(
                    TimeSpan.FromDays(30),
                    Strategy.SoftDelete,
                    AuditRowDetail: AuditRowDetail.PerRow
                ),
                "qualified-anonymise" => new RetentionRule(
                    TimeSpan.FromDays(30),
                    Strategy.Anonymise,
                    AuditRowDetail: AuditRowDetail.PerRow
                ),
                _ => null,
            }
        );
    }

    private sealed class QualifiedRecordHandler : IRetentionHandler<QualifiedRecord>;
    private sealed class QualifiedSoftDeleteRecordHandler
        : IRetentionHandler<QualifiedSoftDeleteRecord>;
    private sealed class QualifiedAnonymiseRecordHandler
        : IRetentionHandler<QualifiedAnonymiseRecord>;

    [Retain("qualified-soft-delete", nameof(CreatedAt), AuditRowDetail = AuditRowDetail.PerRow)]
    [RetentionEntityId("58eafbe2-d98b-474b-aa51-47c514216480")]
    private sealed class QualifiedSoftDeleteRecord(
        Guid id,
        Guid tenantId,
        Guid subjectId,
        DateTimeOffset createdAt,
        string payload
    )
    {
        public Guid Id { get; set; } = id;
        public Guid TenantId { get; set; } = tenantId;
        [ErasureSubject]
        public Guid SubjectId { get; set; } = subjectId;
        public DateTimeOffset CreatedAt { get; set; } = createdAt;
        public string Payload { get; set; } = payload;
        public bool IsDeleted { get; set; }
        public DateTimeOffset? DeletedAt { get; set; }
    }

    [Retain("qualified-anonymise", nameof(CreatedAt), AuditRowDetail = AuditRowDetail.PerRow)]
    [RetentionEntityId("e7c31f21-23a4-4b9c-ab0f-b5eb232ac7ee")]
    private sealed class QualifiedAnonymiseRecord(
        Guid id,
        Guid tenantId,
        Guid subjectId,
        DateTimeOffset createdAt,
        string email
    )
    {
        public Guid Id { get; set; } = id;
        public Guid TenantId { get; set; } = tenantId;
        [ErasureSubject]
        public Guid SubjectId { get; set; } = subjectId;
        public DateTimeOffset CreatedAt { get; set; } = createdAt;
        [Anonymise(AnonymiseMethod.EmptyString)]
        public string Email { get; set; } = email;
        public DateTimeOffset? AnonymisedAt { get; set; }
    }
}
