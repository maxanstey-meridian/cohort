using System.Collections.Concurrent;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class RestrictiveForeignKeySweepEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Sweep_Deletes_Retained_Child_Before_Parent_Under_Restrictive_Foreign_Key()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<RestrictiveForeignKeyDbContext>();
            await db.Database.EnsureCreatedAsync();

            var parent = new ARestrictiveParent
            {
                Id = Guid.NewGuid(),
                TenantId = tenantId,
                CreatedAt = asOf.AddDays(-90),
            };
            db.Parents.Add(parent);
            db.Children.Add(
                new ZRestrictiveChild
                {
                    Id = Guid.NewGuid(),
                    ParentId = parent.Id,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-90),
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await services
            .GetRequiredService<IRetentionSweep>()
            .ExecuteAsync(RetentionSweepRequest.Tenanted(new TenantContext(tenantId, "uk", new Dictionary<string, string>()), asOf));

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ARestrictiveParent));
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ZRestrictiveChild));
        result.Counts.Where(count =>
                count.EntityType == typeof(ARestrictiveParent)
                || count.EntityType == typeof(ZRestrictiveChild)
            )
            .Should()
            .OnlyContain(count => count.Affected == 1);

        await using var verifyScope = services.CreateAsyncScope();
        var verify = verifyScope.ServiceProvider.GetRequiredService<RestrictiveForeignKeyDbContext>();
        (await verify.Parents.CountAsync()).Should().Be(0);
        (await verify.Children.CountAsync()).Should().Be(0);

        await verify.Database.OpenConnectionAsync();
        await using (var runCommand = verify.Database.GetDbConnection().CreateCommand())
        {
            runCommand.CommandText =
                "SELECT \"Status\", \"TotalAffected\" FROM \"sweep_run\" WHERE \"SweepId\" = @sweepId";
            var sweepId = runCommand.CreateParameter();
            sweepId.ParameterName = "sweepId";
            sweepId.Value = result.SweepId;
            runCommand.Parameters.Add(sweepId);

            await using var reader = await runCommand.ExecuteReaderAsync();
            (await reader.ReadAsync()).Should().BeTrue();
            ((SweepRunStatus)reader.GetInt32(0)).Should().Be(SweepRunStatus.Succeeded);
            reader.GetInt64(1).Should().Be(2);
        }

        await using (var summaryCommand = verify.Database.GetDbConnection().CreateCommand())
        {
            summaryCommand.CommandText = """
                SELECT "EntityType", "Category", "Strategy", "ResolvedMonths", "ResolvedDays", "Affected", "HeldCount", "SkippedCount"
                FROM "sweep_run_entity_summary"
                WHERE "SweepId" = @sweepId
                ORDER BY "EntityType"
                """;
            var sweepId = summaryCommand.CreateParameter();
            sweepId.ParameterName = "sweepId";
            sweepId.Value = result.SweepId;
            summaryCommand.Parameters.Add(sweepId);

            var summaries = new List<(string, string, Strategy, int, int, long, long, long)>();
            await using var reader = await summaryCommand.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                summaries.Add(
                    (
                        reader.GetString(0),
                        reader.GetString(1),
                        (Strategy)reader.GetInt32(2),
                        reader.GetInt32(3),
                        reader.GetInt32(4),
                        reader.GetInt64(5),
                        reader.GetInt64(6),
                        reader.GetInt64(7)
                    )
                );
            }

            summaries
                .Should()
                .Equal(
                    (
                        typeof(ARestrictiveParent).FullName!,
                        "restrictive-fk",
                        Strategy.Purge,
                        0,
                        30,
                        1L,
                        0L,
                        0L
                    ),
                    (
                        typeof(ZLongLivedChild).FullName!,
                        "restrictive-fk-long",
                        Strategy.Purge,
                        0,
                        365,
                        0L,
                        0L,
                        0L
                    ),
                    (
                        typeof(ZRestrictiveChild).FullName!,
                        "restrictive-fk",
                        Strategy.Purge,
                        0,
                        30,
                        1L,
                        0L,
                        0L
                    )
                );
        }
    }

    [Fact]
    public async Task A_Parent_Referenced_By_An_Unretained_Child_Is_Blocked_And_The_Rest_Are_Swept()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var logs = new RecordingLogProvider();
        // Batch size 1: the referenced parent is the oldest, so it would head every batch.
        await using var services = BuildServiceProvider(
            database.ConnectionString,
            batchSize: 1,
            configure: services => services.AddSingleton<ILoggerProvider>(logs)
        );
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var referencedId = Guid.NewGuid();
        var sweptIds = new[] { Guid.NewGuid(), Guid.NewGuid() };

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<RestrictiveForeignKeyDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Parents.AddRange(
                Parent(referencedId, tenantId, asOf.AddDays(-300)),
                Parent(sweptIds[0], tenantId, asOf.AddDays(-200)),
                Parent(sweptIds[1], tenantId, asOf.AddDays(-100))
            );
            db.Invoices.Add(new HostInvoice { Id = Guid.NewGuid(), ParentId = referencedId });
            await db.SaveChangesAsync();
        }

        var result = await SweepAsync(services, tenantId, asOf);

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ARestrictiveParent))
            .Which.Should().Match<EntitySweepCount>(count =>
                count.Affected == 2 && count.BlockedCount == 1 && count.DeferredCount == 0);
        (await LoadRunStatusAsync(database.ConnectionString, result.SweepId)).Should().Be(SweepRunStatus.Succeeded);
        (await LoadSummaryAsync(database.ConnectionString, result.SweepId, typeof(ARestrictiveParent)))
            .Should().Be(new SummaryCounts(Affected: 2, Deferred: 0, Blocked: 1));
        (await LoadParentIdsAsync(services)).Should().Equal(referencedId);
        // The warning names the keys that can hold the rows, never the rows.
        logs.Entries.Should().ContainSingle(entry =>
                entry.Level == LogLevel.Warning
                && entry.Message.Contains(typeof(ARestrictiveParent).FullName!, StringComparison.Ordinal)
                && entry.Message.Contains("FK_restrictive_fk_host_invoices_restrictive_fk_parents_ParentId", StringComparison.Ordinal))
            .Which.Message.Should().Contain("1 row(s)");
        logs.Entries.Should().NotContain(entry => entry.Message.Contains(referencedId.ToString(), StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_Row_That_References_Itself_Is_Purged_And_One_Its_Own_Table_Still_References_Is_Blocked()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var logs = new RecordingLogProvider();
        await using var services = BuildServiceProvider(
            database.ConnectionString,
            configure: services => services.AddSingleton<ILoggerProvider>(logs)
        );
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var selfReferencingId = Guid.NewGuid();
        var referencedId = Guid.NewGuid();
        var youngReferrerId = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<RestrictiveForeignKeyDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Parents.AddRange(
                Parent(selfReferencingId, tenantId, asOf.AddDays(-300)),
                Parent(referencedId, tenantId, asOf.AddDays(-200)),
                // Not due yet, and still pointing at its due parent.
                Parent(youngReferrerId, tenantId, asOf.AddDays(-1))
            );
            await db.SaveChangesAsync();
        }
        // A database-only self-reference (NO ACTION) the EF model doesn't know about.
        await ExecuteAsync(
            database.ConnectionString,
            $"""
            ALTER TABLE restrictive_fk_parents
                ADD COLUMN "ReferencesId" uuid CONSTRAINT restrictive_fk_parent_self REFERENCES restrictive_fk_parents ("Id");
            UPDATE restrictive_fk_parents SET "ReferencesId" = "Id" WHERE "Id" = '{selfReferencingId}';
            UPDATE restrictive_fk_parents SET "ReferencesId" = '{referencedId}' WHERE "Id" = '{youngReferrerId}';
            """
        );

        var result = await SweepAsync(services, tenantId, asOf);

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ARestrictiveParent))
            .Which.Should().Match<EntitySweepCount>(count =>
                count.Affected == 1 && count.BlockedCount == 1 && count.DeferredCount == 0);
        (await LoadParentIdsAsync(services)).Should().Equal(referencedId, youngReferrerId);
        logs.Entries.Should().Contain(entry =>
            entry.Level == LogLevel.Warning && entry.Message.Contains("restrictive_fk_parent_self", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Preview_And_Dry_Run_Count_Referenced_Parents_As_Blocked_Not_Eligible()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var referencedId = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<RestrictiveForeignKeyDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Parents.AddRange(
                Parent(referencedId, tenantId, asOf.AddDays(-300)),
                Parent(Guid.NewGuid(), tenantId, asOf.AddDays(-200))
            );
            db.Invoices.Add(new HostInvoice { Id = Guid.NewGuid(), ParentId = referencedId });
            await db.SaveChangesAsync();
        }

        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());
        RetentionSweepResult preview;
        RetentionSweepResult dryRun;
        await using (var scope = services.CreateAsyncScope())
        {
            preview = await scope.ServiceProvider.GetRequiredService<IRetentionPreview>().PreviewAsync(tenant, asOf);
            dryRun = await scope.ServiceProvider.GetRequiredService<IRetentionSweep>()
                .ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, asOf, dryRun: true));
        }

        foreach (var result in new[] { preview, dryRun })
        {
            result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ARestrictiveParent))
                .Which.Should().Match<EntitySweepCount>(count => count.Affected == 1 && count.BlockedCount == 1);
        }
        (await LoadSummaryAsync(database.ConnectionString, dryRun.SweepId, typeof(ARestrictiveParent)))
            .Should().Be(new SummaryCounts(Affected: 1, Deferred: 0, Blocked: 1));
        (await LoadParentIdsAsync(services)).Should().HaveCount(2);
    }

    [Fact]
    public async Task A_Retained_Child_With_A_Longer_Period_Blocks_Its_Parent_Until_It_Expires()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var firstSweep = new DateTimeOffset(2025, 6, 1, 12, 0, 0, TimeSpan.Zero);
        var secondSweep = new DateTimeOffset(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);
        var parentId = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<RestrictiveForeignKeyDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Parents.Add(Parent(parentId, tenantId, firstSweep.AddDays(-120)));
            // 31 days old at the first sweep: inside its 365-day period, so it keeps the parent.
            db.LongLivedChildren.Add(
                new ZLongLivedChild
                {
                    Id = Guid.NewGuid(),
                    ParentId = parentId,
                    TenantId = tenantId,
                    CreatedAt = firstSweep.AddDays(-31),
                }
            );
            await db.SaveChangesAsync();
        }

        var first = await SweepAsync(services, tenantId, firstSweep);

        first.EntityFailures.Should().BeEmpty();
        first.Counts.Should().ContainSingle(count => count.EntityType == typeof(ARestrictiveParent))
            .Which.Should().Match<EntitySweepCount>(count => count.Affected == 0 && count.BlockedCount == 1);
        (await LoadParentIdsAsync(services)).Should().Equal(parentId);

        var second = await SweepAsync(services, tenantId, secondSweep);

        second.EntityFailures.Should().BeEmpty();
        second.Counts.Where(count =>
                count.EntityType == typeof(ARestrictiveParent) || count.EntityType == typeof(ZLongLivedChild))
            .Should().HaveCount(2)
            .And.OnlyContain(count => count.Affected == 1 && count.BlockedCount == 0);
        (await LoadParentIdsAsync(services)).Should().BeEmpty();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_Row_Rejected_By_A_Constraint_Trigger_Is_Blocked_And_Its_Batch_Commits(bool rowByRow)
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var logs = new RecordingLogProvider();
        await using var services = BuildServiceProvider(
            database.ConnectionString,
            configure: services =>
            {
                services.AddSingleton<ILoggerProvider>(logs);
                if (rowByRow)
                {
                    services.AddRowHandler<ARestrictiveParent, ParentCaptureHandler>();
                }
            }
        );
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        var blockedId = ids[1];

        await SeedParentsAsync(services, tenantId, asOf, ids);
        await ExecuteAsync(
            database.ConnectionString,
            $"""
            CREATE FUNCTION guard_parent() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF OLD."Id" = '{blockedId}' THEN
                    RAISE EXCEPTION 'parent is guarded' USING ERRCODE = '23514', CONSTRAINT = 'restrictive_fk_parent_guard';
                END IF;
                RETURN OLD;
            END $$;
            CREATE TRIGGER guard_parent BEFORE DELETE ON restrictive_fk_parents
                FOR EACH ROW EXECUTE FUNCTION guard_parent();
            """
        );

        var result = await SweepAsync(services, tenantId, asOf);

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ARestrictiveParent))
            .Which.Should().Match<EntitySweepCount>(count =>
                count.Affected == 2 && count.BlockedCount == 1 && count.DeferredCount == 0);
        (await LoadRunStatusAsync(database.ConnectionString, result.SweepId)).Should().Be(SweepRunStatus.Succeeded);
        (await LoadSummaryAsync(database.ConnectionString, result.SweepId, typeof(ARestrictiveParent)))
            .Should().Be(new SummaryCounts(Affected: 2, Deferred: 0, Blocked: 1));
        (await LoadParentIdsAsync(services)).Should().Equal(blockedId);
        // The row the constraint refused was never mutated, so no OnAfter work is queued for it.
        (await LoadHandlerRecordIdsAsync(database.ConnectionString, result.SweepId))
            .Should().BeEquivalentTo(rowByRow ? [ids[0].ToString(), ids[2].ToString()] : Array.Empty<string>());
        // The warning names the constraint, never the row.
        logs.Entries.Should().Contain(entry =>
            entry.Level == LogLevel.Warning
            && entry.Message.Contains("restrictive_fk_parent_guard", StringComparison.Ordinal)
            && entry.Message.Contains("23514", StringComparison.Ordinal));
        logs.Entries.Should().NotContain(entry => entry.Message.Contains(blockedId.ToString(), StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_Deferred_Constraint_Violation_Blocks_One_Row_Not_The_Batch()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        var blockedId = ids[1];

        await SeedParentsAsync(services, tenantId, asOf, ids);
        // Checked at COMMIT unless the batch asks for its constraints immediately.
        await ExecuteAsync(
            database.ConnectionString,
            $"""
            CREATE FUNCTION guard_parent_deferred() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF OLD."Id" = '{blockedId}' THEN
                    RAISE EXCEPTION 'parent is guarded' USING ERRCODE = '23514', CONSTRAINT = 'restrictive_fk_parent_deferred_guard';
                END IF;
                RETURN NULL;
            END $$;
            CREATE CONSTRAINT TRIGGER guard_parent_deferred AFTER DELETE ON restrictive_fk_parents
                DEFERRABLE INITIALLY DEFERRED
                FOR EACH ROW EXECUTE FUNCTION guard_parent_deferred();
            """
        );

        var result = await SweepAsync(services, tenantId, asOf);

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ARestrictiveParent))
            .Which.Should().Match<EntitySweepCount>(count => count.Affected == 2 && count.BlockedCount == 1);
        (await LoadParentIdsAsync(services)).Should().Equal(blockedId);
    }

    [Fact]
    public async Task A_Fully_Blocked_Batch_Fails_The_Entity_Once()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString, batchSize: 2);
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var ids = Enumerable.Range(0, 5).Select(_ => Guid.NewGuid()).ToArray();

        await SeedParentsAsync(services, tenantId, asOf, ids);
        // A deterministic misconfiguration: every row is refused. The sequence counts
        // attempts, since nextval survives the rollback of the statement that called it.
        await ExecuteAsync(
            database.ConnectionString,
            """
            CREATE SEQUENCE guard_attempts;
            CREATE FUNCTION guard_every_parent() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                PERFORM nextval('guard_attempts');
                RAISE EXCEPTION 'every parent is guarded' USING ERRCODE = '23514';
            END $$;
            CREATE TRIGGER guard_every_parent BEFORE DELETE ON restrictive_fk_parents
                FOR EACH ROW EXECUTE FUNCTION guard_every_parent();
            """
        );

        var result = await SweepAsync(services, tenantId, asOf);

        result.EntityFailures.Should().ContainSingle()
            .Which.Should().MatchRegex("^type=Npgsql\\.PostgresException;code=sqlstate:23514;diagnosticId=[0-9a-f]{32}$");
        (await LoadRunStatusAsync(database.ConnectionString, result.SweepId)).Should().Be(SweepRunStatus.PartiallyFailed);
        (await LoadParentIdsAsync(services)).Should().HaveCount(5);
        // One batch: the whole batch, then each half. No second batch is bisected.
        (await ScalarAsync<long>(database.ConnectionString, "SELECT last_value FROM guard_attempts")).Should().Be(3);
    }

    [Theory]
    [InlineData("restrict")]
    [InlineData("no-action")]
    [InlineData("cycle")]
    public async Task A_Full_Batch_Refused_By_One_Foreign_Key_The_Reference_Check_Cannot_See_Is_Blocked_Not_Failed(string reference)
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString, batchSize: 2);
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var ids = Enumerable.Range(0, 5).Select(_ => Guid.NewGuid()).ToArray();

        await SeedParentsAsync(services, tenantId, asOf, ids);
        // Every row of every batch is refused by the same foreign key (SQLSTATE 23503, which
        // PostgreSQL raises for RESTRICT and NO ACTION alike), but each row because of what
        // references it, not because of the schema. The model doesn't declare the key, so the
        // reference check can't defer the rows.
        if (reference == "cycle")
        {
            // The CASCADE child is referenced back from a newer row of the target table, not
            // yet due: the refusal names the target table itself, though Purge wrote nothing.
            await ExecuteAsync(
                database.ConnectionString,
                $"""
                CREATE TABLE hidden_fk_children (
                    "Id" uuid PRIMARY KEY,
                    "ParentId" uuid NOT NULL REFERENCES restrictive_fk_parents ("Id") ON DELETE CASCADE
                );
                INSERT INTO hidden_fk_children ("Id", "ParentId")
                    SELECT gen_random_uuid(), "Id" FROM restrictive_fk_parents;
                ALTER TABLE restrictive_fk_parents ADD COLUMN "ChildId" uuid REFERENCES hidden_fk_children ("Id");
                INSERT INTO restrictive_fk_parents ("Id", "TenantId", "CreatedAt", "ChildId")
                    SELECT gen_random_uuid(), '{tenantId}', '{asOf:O}', "Id" FROM hidden_fk_children;
                """
            );
        }
        else
        {
            await AddHiddenGrandchildReferenceAsync(database.ConnectionString, ids, restrict: reference == "restrict");
        }

        for (var run = 0; run < 2; run++)
        {
            // The second run starts from the same oldest rows: it must get past them again.
            var result = await SweepAsync(services, tenantId, asOf);

            result.EntityFailures.Should().BeEmpty();
            result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ARestrictiveParent))
                .Which.Should().Match<EntitySweepCount>(count =>
                    count.Affected == 0 && count.BlockedCount == 5 && count.DeferredCount == 0);
            (await LoadRunStatusAsync(database.ConnectionString, result.SweepId)).Should().Be(SweepRunStatus.Succeeded);
            (await LoadSummaryAsync(database.ConnectionString, result.SweepId, typeof(ARestrictiveParent)))
                .Should().Be(new SummaryCounts(Affected: 0, Deferred: 0, Blocked: 5));
        }

        (await LoadParentIdsAsync(services)).Take(ids.Length).Should().Equal(ids);
    }

    [Fact]
    public async Task Discovery_Sends_At_Most_A_Batch_Of_Record_Ids_However_Many_Rows_The_Run_Has_Blocked()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var logs = new RecordingLogProvider();
        await using var services = BuildServiceProvider(
            database.ConnectionString,
            batchSize: 2,
            configure: services => services.AddSingleton<ILoggerProvider>(logs),
            logCommands: true
        );
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var anchor = asOf.AddDays(-300);
        var ids = Enumerable.Range(0, 10).Select(_ => Guid.NewGuid()).ToArray();
        var blockedIds = ids[..8];

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<RestrictiveForeignKeyDbContext>();
            await db.Database.EnsureCreatedAsync();
            // One shared anchor: discovery's order falls entirely to the record id.
            db.Parents.AddRange(ids.Select(id => Parent(id, tenantId, anchor)));
            await db.SaveChangesAsync();
        }
        // Eight of the ten rows are held by a NO ACTION grandchild behind a CASCADE child the
        // reference check can't see, so each is refused when tried, in whatever order discovery
        // reaches them.
        await AddHiddenGrandchildReferenceAsync(database.ConnectionString, blockedIds);

        var result = await SweepAsync(services, tenantId, asOf);

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ARestrictiveParent))
            .Which.Should().Match<EntitySweepCount>(count =>
                count.Affected == 2 && count.BlockedCount == 8 && count.DeferredCount == 0);
        (await LoadSummaryAsync(database.ConnectionString, result.SweepId, typeof(ARestrictiveParent)))
            .Should().Be(new SummaryCounts(Affected: 2, Deferred: 0, Blocked: 8));
        (await LoadParentIdsAsync(services)).Should().BeEquivalentTo(blockedIds);
        var commands = LoggedCommands(logs)
            .Where(command => command.Text.Contains("FROM \"public\".\"restrictive_fk_parents\"", StringComparison.Ordinal))
            .ToList();
        // Each batch's discovery carries at most a batch's worth of record ids, not every row
        // the run has blocked so far.
        var discoveries = commands.Where(command => command.Text.Contains("LIMIT $", StringComparison.Ordinal)).ToList();
        discoveries.Should().HaveCountGreaterThanOrEqualTo(5);
        discoveries.Should().OnlyContain(command => command.ArrayElements <= 2);
        // The positive control: the counts measured after the loop send every blocked id, so
        // the log does show array parameters, and the check above can't pass by seeing none.
        commands.Should().Contain(command =>
            command.Text.Contains("pg_catalog.count(*)", StringComparison.Ordinal) && command.ArrayElements == blockedIds.Length);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_Full_Batch_Refused_By_Different_Constraints_Is_Blocked_Not_Failed(bool sameSqlState)
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString, batchSize: 2);
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };

        await SeedParentsAsync(services, tenantId, asOf, ids);
        // Two refusals by two constraints: a trigger (23514) on one row, and on the other
        // either a second trigger under another name (also 23514) or a NO ACTION foreign key
        // behind a CASCADE child, which the reference check can't see (23503).
        await ExecuteAsync(
            database.ConnectionString,
            $"""
            CREATE FUNCTION guard_one_parent() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF OLD."Id" = '{ids[0]}' THEN
                    RAISE EXCEPTION 'parent is guarded' USING ERRCODE = '23514', CONSTRAINT = 'restrictive_fk_parent_guard';
                END IF;
                IF {(sameSqlState ? "TRUE" : "FALSE")} AND OLD."Id" = '{ids[1]}' THEN
                    RAISE EXCEPTION 'parent is guarded' USING ERRCODE = '23514', CONSTRAINT = 'restrictive_fk_parent_other_guard';
                END IF;
                RETURN OLD;
            END $$;
            CREATE TRIGGER guard_one_parent BEFORE DELETE ON restrictive_fk_parents
                FOR EACH ROW EXECUTE FUNCTION guard_one_parent();
            """
        );
        await AddHiddenGrandchildReferenceAsync(database.ConnectionString, [ids[1]]);

        var result = await SweepAsync(services, tenantId, asOf);

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ARestrictiveParent))
            .Which.Should().Match<EntitySweepCount>(count =>
                count.Affected == 0 && count.BlockedCount == 2 && count.DeferredCount == 0);
        (await LoadRunStatusAsync(database.ConnectionString, result.SweepId)).Should().Be(SweepRunStatus.Succeeded);
        (await LoadParentIdsAsync(services)).Should().Equal(ids);
    }

    [Fact]
    public async Task A_Full_Batch_Refused_By_The_Key_An_Anonymise_Writes_Fails_The_Entity_Once()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildOutboundServiceProvider(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var ownerId = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<OutboundForeignKeyDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Owners.Add(new OutboundOwner { Id = ownerId });
            for (var index = 0; index < 5; index++)
            {
                db.Rows.Add(new OutboundAnonymisedRow
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-300 + index),
                    OwnerId = ownerId,
                });
            }
            await db.SaveChangesAsync();
        }
        // The factory's key names no owner, so the table's own foreign key refuses the value
        // Cohort writes, for every row alike: the rule is wrong, not the rows. The sequence
        // counts attempts, since nextval survives the rollback of the statement that called it.
        await ExecuteAsync(
            database.ConnectionString,
            """
            CREATE SEQUENCE anonymise_attempts;
            CREATE FUNCTION count_anonymise() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                PERFORM nextval('anonymise_attempts');
                RETURN NEW;
            END $$;
            CREATE TRIGGER count_anonymise BEFORE UPDATE ON outbound_fk_rows
                FOR EACH STATEMENT EXECUTE FUNCTION count_anonymise();
            """
        );

        RetentionSweepResult result;
        await using (var scope = services.CreateAsyncScope())
        {
            result = await scope.ServiceProvider
                .GetRequiredService<IRetentionSweep>()
                .ExecuteAsync(RetentionSweepRequest.Tenanted(new TenantContext(tenantId, "uk", new Dictionary<string, string>()), asOf));
        }

        result.EntityFailures.Should().ContainSingle()
            .Which.Should().MatchRegex("^type=Npgsql\\.PostgresException;code=sqlstate:23503;diagnosticId=[0-9a-f]{32}$");
        (await LoadRunStatusAsync(database.ConnectionString, result.SweepId)).Should().Be(SweepRunStatus.PartiallyFailed);
        (await ScalarAsync<long>(database.ConnectionString, "SELECT count(*) FROM outbound_fk_rows WHERE \"AnonymisedAt\" IS NULL"))
            .Should().Be(5);
        // One batch: the whole batch, then each half. No second batch is bisected.
        (await ScalarAsync<long>(database.ConnectionString, "SELECT last_value FROM anonymise_attempts")).Should().Be(3);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Erasure_Reports_A_Referenced_Row_As_Blocked(bool dryRun)
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var referencedId = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<RestrictiveForeignKeyDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Parents.AddRange(
                Parent(referencedId, tenantId, asOf.AddDays(-2), subjectId),
                Parent(Guid.NewGuid(), tenantId, asOf.AddDays(-1), subjectId)
            );
            db.Invoices.Add(new HostInvoice { Id = Guid.NewGuid(), ParentId = referencedId });
            await db.SaveChangesAsync();
        }

        ErasureResult result;
        await using (var scope = services.CreateAsyncScope())
        {
            result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                RetentionErasureRequest.Tenanted(
                    new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                    new ErasureScope("person", subjectId, dryRun: dryRun),
                    asOf
                )
            );
        }

        result.Complete.Should().BeFalse();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ARestrictiveParent))
            .Which.Should().Match<EntitySweepCount>(count =>
                count.Affected == 1 && count.BlockedCount == 1 && count.DeferredCount == 0);
        if (dryRun)
        {
            // A prediction: Complete says the real run would leave the row, but nothing failed.
            result.EntityFailures.Should().BeEmpty();
            (await LoadRunStatusAsync(database.ConnectionString, result.SweepId)).Should().Be(SweepRunStatus.Succeeded);
            (await LoadParentIdsAsync(services)).Should().HaveCount(2);
            return;
        }

        const string reason = "reason=rows-blocked;entityType=Cohort.Sample.Tests.RestrictiveForeignKeySweepEndToEndTests+ARestrictiveParent;count=1";
        result.EntityFailures.Should().ContainSingle().Which.Should().Be(reason);
        (await LoadRunStatusAsync(database.ConnectionString, result.SweepId)).Should().Be(SweepRunStatus.PartiallyFailed);
        (await LoadParentIdsAsync(services)).Should().Equal(referencedId);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Erasure_In_Single_Row_Batches_Continues_Past_A_Blocked_Row_Among_Null_Anchors(bool callerTransaction)
    {
        // Without a legal minimum an erasure has no cutoff, so rows with a NULL anchor are in
        // scope; discovery's order puts them last. Each one-row batch continues after the row
        // the last one reached, past the blocked one, through the NULLs.
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString, batchSize: 1);
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var datedId = Guid.NewGuid();
        var nullIds = Enumerable.Range(0, 4).Select(_ => Guid.NewGuid()).OrderBy(id => id.ToString(), StringComparer.Ordinal).ToArray();
        var blockedId = nullIds[1];

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<RestrictiveForeignKeyDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Parents.Add(Parent(datedId, tenant.Id, asOf.AddDays(-1), subjectId));
            db.Parents.AddRange(nullIds.Select(id => Parent(id, tenant.Id, null, subjectId)));
            await db.SaveChangesAsync();
        }
        await AddHiddenGrandchildReferenceAsync(database.ConnectionString, [blockedId]);

        ErasureResult result;
        await using (var scope = services.CreateAsyncScope())
        {
            var erasure = scope.ServiceProvider.GetRequiredService<IRetentionErasureService>();
            if (callerTransaction)
            {
                var db = scope.ServiceProvider.GetRequiredService<RestrictiveForeignKeyDbContext>();
                await using var transaction = await db.Database.BeginTransactionAsync();
                result = await erasure.ExecuteAsync(
                    RetentionErasureRequest.Tenanted(tenant, new ErasureScope("person", subjectId), asOf, ErasureTransaction.Caller)
                );
                await transaction.CommitAsync();
            }
            else
            {
                result = await erasure.ExecuteAsync(
                    RetentionErasureRequest.Tenanted(tenant, new ErasureScope("person", subjectId), asOf)
                );
            }
        }

        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ARestrictiveParent))
            .Which.Should().Match<EntitySweepCount>(count =>
                count.Affected == 4 && count.BlockedCount == 1 && count.DeferredCount == 0 && count.NullAnchorCount == 0);
        result.EntityFailures.Should().ContainSingle().Which.Should().Be(
            "reason=rows-blocked;entityType=Cohort.Sample.Tests.RestrictiveForeignKeySweepEndToEndTests+ARestrictiveParent;count=1"
        );
        (await LoadSummaryAsync(database.ConnectionString, result.SweepId, typeof(ARestrictiveParent)))
            .Should().Be(new SummaryCounts(Affected: 4, Deferred: 0, Blocked: 1));
        (await LoadParentIdsAsync(services)).Should().Equal(blockedId);
    }

    private static ARestrictiveParent Parent(Guid id, Guid tenantId, DateTimeOffset? createdAt, Guid? subjectId = null) =>
        new()
        {
            Id = id,
            TenantId = tenantId,
            CreatedAt = createdAt,
            SubjectId = subjectId,
        };

    /// <summary>Seeds expired parents, oldest first, in the order given.</summary>
    private static async Task SeedParentsAsync(ServiceProvider services, Guid tenantId, DateTimeOffset asOf, IReadOnlyList<Guid> ids)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<RestrictiveForeignKeyDbContext>();
        await db.Database.EnsureCreatedAsync();
        for (var index = 0; index < ids.Count; index++)
        {
            db.Parents.Add(Parent(ids[index], tenantId, asOf.AddDays(-300 + index)));
        }
        await db.SaveChangesAsync();
    }

    private static async Task<RetentionSweepResult> SweepAsync(ServiceProvider services, Guid tenantId, DateTimeOffset asOf)
    {
        await using var scope = services.CreateAsyncScope();
        return await scope.ServiceProvider
            .GetRequiredService<IRetentionSweep>()
            .ExecuteAsync(RetentionSweepRequest.Tenanted(new TenantContext(tenantId, "uk", new Dictionary<string, string>()), asOf));
    }

    private static async Task<List<Guid>> LoadParentIdsAsync(ServiceProvider services)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<RestrictiveForeignKeyDbContext>();
        return await db.Parents.OrderBy(parent => parent.CreatedAt).Select(parent => parent.Id).ToListAsync();
    }

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Holds <paramref name="parentIds"/> through a foreign key the model doesn't declare, so
    /// the reference check can't see it: each parent gets a CASCADE child, which a NO ACTION
    /// (or RESTRICT) grandchild references. Deleting the parent fails with SQLSTATE 23503.
    /// </summary>
    private static async Task AddHiddenGrandchildReferenceAsync(
        string connectionString,
        IReadOnlyCollection<Guid> parentIds,
        bool restrict = false
    )
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"""
            CREATE TABLE hidden_fk_children (
                "Id" uuid PRIMARY KEY,
                "ParentId" uuid NOT NULL REFERENCES restrictive_fk_parents ("Id") ON DELETE CASCADE
            );
            CREATE TABLE hidden_fk_grandchildren (
                "Id" uuid PRIMARY KEY,
                "ChildId" uuid NOT NULL REFERENCES hidden_fk_children ("Id"){(restrict ? " ON DELETE RESTRICT" : "")}
            );
            INSERT INTO hidden_fk_children ("Id", "ParentId")
                SELECT gen_random_uuid(), parent_id FROM unnest(@parentIds) AS parent_id;
            INSERT INTO hidden_fk_grandchildren ("Id", "ChildId")
                SELECT gen_random_uuid(), "Id" FROM hidden_fk_children;
            """,
            connection
        );
        command.Parameters.AddWithValue("parentIds", parentIds.ToArray());
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<T> ScalarAsync<T>(string connectionString, string sql, params (string Name, object Value)[] parameters)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }
        return (T)(await command.ExecuteScalarAsync())!;
    }

    private static async Task<SweepRunStatus> LoadRunStatusAsync(string connectionString, Guid sweepId) =>
        (SweepRunStatus)await ScalarAsync<int>(
            connectionString,
            "SELECT \"Status\" FROM \"sweep_run\" WHERE \"SweepId\" = @sweepId",
            ("sweepId", sweepId)
        );

    private static async Task<SummaryCounts> LoadSummaryAsync(string connectionString, Guid sweepId, Type entityType)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT "Affected", "DeferredCount", "BlockedCount"
            FROM "sweep_run_entity_summary"
            WHERE "SweepId" = @sweepId AND "EntityType" = @entityType
            """,
            connection
        );
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("entityType", entityType.FullName!);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        return new SummaryCounts(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
    }

    private static async Task<List<string>> LoadHandlerRecordIdsAsync(string connectionString, Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT detail."RecordId"
            FROM "sweep_row_handler_status" AS status
            INNER JOIN "sweep_run_row_detail" AS detail ON detail."Id" = status."SweepRunRowDetailId"
            WHERE detail."SweepId" = @sweepId
            """,
            connection
        );
        command.Parameters.AddWithValue("sweepId", sweepId);
        var recordIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            recordIds.Add(reader.GetString(0));
        }
        return recordIds;
    }

    private sealed record SummaryCounts(long Affected, long Deferred, long Blocked);

    /// <summary>A log entry; <c>Fields</c> holds its structured state, the message template's named values.</summary>
    private sealed record LogEntry(LogLevel Level, string Message, IReadOnlyDictionary<string, object?> Fields);

    private sealed class RecordingLogProvider : ILoggerProvider
    {
        private readonly ConcurrentQueue<LogEntry> entries = new();

        public IReadOnlyList<LogEntry> Entries => entries.ToArray();

        public ILogger CreateLogger(string categoryName) => new RecordingLogger(entries);

        public void Dispose() { }

        private sealed class RecordingLogger(ConcurrentQueue<LogEntry> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter
            )
            {
                var fields = new Dictionary<string, object?>(StringComparer.Ordinal);
                if (state is IEnumerable<KeyValuePair<string, object?>> values)
                {
                    foreach (var (key, value) in values)
                    {
                        fields[key] = value;
                    }
                }
                entries.Enqueue(new LogEntry(logLevel, formatter(state, exception), fields));
            }
        }
    }

    [RowHandlerId("64c313a9-a25a-4f7d-bb7c-aea7112c1edc")]
    private sealed class ParentCaptureHandler : IRetentionHandler<ARestrictiveParent>
    {
        public Task OnBeforeAsync(ARestrictiveParent row, RetentionBeforeContext ctx, CancellationToken ct)
        {
            ctx.Snapshot["id"] = row.Id.ToString();
            return Task.CompletedTask;
        }
    }

    private sealed record LoggedCommand(string Text, int ArrayElements);

    /// <summary>
    /// Every command Npgsql logged, with how many elements its array parameters carry between
    /// them. Cohort runs its sweep SQL on the raw connection, outside EF's command pipeline, so
    /// an EF interceptor wouldn't see it; Npgsql's own command log does, parameters included
    /// when parameter logging is on. Npgsql logs an array parameter as "[a, b, c]", cut off
    /// after 10 elements with ", ...", so a count is exact only up to 10.
    /// </summary>
    private static IEnumerable<LoggedCommand> LoggedCommands(RecordingLogProvider logs) =>
        logs.Entries
            .Where(entry => entry.Fields.GetValueOrDefault("CommandText") is string
                && entry.Fields.GetValueOrDefault("Parameters") is object[])
            .Select(entry => new LoggedCommand(
                (string)entry.Fields["CommandText"]!,
                ((object[])entry.Fields["Parameters"]!).Sum(CountArrayElements)
            ));

    private static int CountArrayElements(object? value) => value switch
    {
        "[]" => 0,
        string array when array.StartsWith('[') && array.EndsWith(']') => array.Split(", ").Length,
        _ => 0,
    };

    private static ServiceProvider BuildServiceProvider(
        string connectionString,
        int? batchSize = null,
        Action<IServiceCollection>? configure = null,
        bool logCommands = false
    )
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(
            new ConfigurationBuilder()
                .AddInMemoryCollection(
                    batchSize is null
                        ? []
                        : new Dictionary<string, string?> { [$"{CohortOptions.SectionName}:SweepBatchSize"] = batchSize.ToString() }
                )
                .Build()
        );
        services.AddLogging();
        if (logCommands)
        {
            // Npgsql logs its commands to the provider's own logger factory. Built by the
            // provider, so the provider disposes the data source with everything else.
            services.AddSingleton(provider => new NpgsqlDataSourceBuilder(connectionString)
                .UseLoggerFactory(provider.GetRequiredService<ILoggerFactory>())
                .EnableParameterLogging()
                .Build());
            services.AddDbContext<RestrictiveForeignKeyDbContext>((provider, options) =>
                options.UseNpgsql(provider.GetRequiredService<NpgsqlDataSource>()));
        }
        else
        {
            services.AddDbContext<RestrictiveForeignKeyDbContext>(options =>
                options.UseNpgsql(connectionString)
            );
        }
        services.AddSingleton<IRetentionRuleProvider>(new PurgeCategoryRepository());
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<RestrictiveForeignKeyDbContext>();
        configure?.Invoke(services);
        return services.BuildServiceProvider(validateScopes: true);
    }

    private static ServiceProvider BuildOutboundServiceProvider(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(
            new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?> { [$"{CohortOptions.SectionName}:SweepBatchSize"] = "2" })
                .Build()
        );
        services.AddLogging();
        services.AddDbContext<OutboundForeignKeyDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(new AnonymiseCategoryRepository());
        services.AddSingleton<IAnonymiseValueFactory, MissingOwnerFactory>();
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<OutboundForeignKeyDbContext>();
        return services.BuildServiceProvider(validateScopes: true);
    }

    private sealed class AnonymiseCategoryRepository : ITestRetentionRuleProvider
    {
        private static readonly ITestRetentionRule Resolver = new StaticTestRetentionRule(
            new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Anonymise)
        );

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct) =>
            Task.FromResult(category == "outbound-fk" ? Resolver : null);
    }

    /// <summary>A static factory whose key names no owner.</summary>
    private sealed class MissingOwnerFactory : IAnonymiseValueFactory
    {
        public object? Create(AnonymiseValueContext context) => Guid.Parse("22222222-2222-2222-2222-222222222222");
    }

    private sealed class OutboundForeignKeyDbContext(DbContextOptions<OutboundForeignKeyDbContext> options)
        : DbContext(options)
    {
        public DbSet<OutboundOwner> Owners => Set<OutboundOwner>();
        public DbSet<OutboundAnonymisedRow> Rows => Set<OutboundAnonymisedRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<OutboundOwner>(entity =>
            {
                entity.ToTable("outbound_fk_owners");
                entity.HasKey(owner => owner.Id);
            });
            modelBuilder.Entity<OutboundAnonymisedRow>(entity =>
            {
                entity.ToTable("outbound_fk_rows");
                entity.HasKey(row => row.Id);
                entity
                    .HasOne<OutboundOwner>()
                    .WithMany()
                    .HasForeignKey(row => row.OwnerId)
                    .OnDelete(DeleteBehavior.Restrict);
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    private sealed class OutboundOwner
    {
        public Guid Id { get; init; }
    }

    [Retain("outbound-fk", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0002-000000000004")]
    private sealed class OutboundAnonymisedRow
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [AnonymiseWith(typeof(MissingOwnerFactory))]
        public Guid OwnerId { get; init; }

        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    private sealed class PurgeCategoryRepository : ITestRetentionRuleProvider
    {
        private static readonly ITestRetentionRule Resolver = new StaticTestRetentionRule(
            new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge)
        );
        private static readonly ITestRetentionRule LongLivedResolver = new StaticTestRetentionRule(
            new RetentionRule(new RetentionPeriod(Days: 365), Strategy.Purge)
        );

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct) =>
            Task.FromResult(
                category switch
                {
                    "restrictive-fk" => Resolver,
                    "restrictive-fk-long" => LongLivedResolver,
                    _ => null,
                }
            );
    }

    private sealed class RestrictiveForeignKeyDbContext(
        DbContextOptions<RestrictiveForeignKeyDbContext> options
    ) : DbContext(options)
    {
        public DbSet<ARestrictiveParent> Parents => Set<ARestrictiveParent>();
        public DbSet<ZRestrictiveChild> Children => Set<ZRestrictiveChild>();
        public DbSet<ZLongLivedChild> LongLivedChildren => Set<ZLongLivedChild>();
        public DbSet<HostInvoice> Invoices => Set<HostInvoice>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ARestrictiveParent>(entity =>
            {
                entity.ToTable("restrictive_fk_parents");
                entity.HasKey(parent => parent.Id);
            });
            modelBuilder.Entity<ZRestrictiveChild>(entity =>
            {
                entity.ToTable("restrictive_fk_children");
                entity.HasKey(child => child.Id);
                entity
                    .HasOne<ARestrictiveParent>()
                    .WithMany()
                    .HasForeignKey(child => child.ParentId)
                    .OnDelete(DeleteBehavior.Restrict);
            });
            modelBuilder.Entity<ZLongLivedChild>(entity =>
            {
                entity.ToTable("restrictive_fk_long_lived_children");
                entity.HasKey(child => child.Id);
                entity
                    .HasOne<ARestrictiveParent>()
                    .WithMany()
                    .HasForeignKey(child => child.ParentId)
                    .OnDelete(DeleteBehavior.Restrict);
            });
            // A host table Cohort doesn't retain, still holding its parent.
            modelBuilder.Entity<HostInvoice>(entity =>
            {
                entity.ToTable("restrictive_fk_host_invoices");
                entity.HasKey(invoice => invoice.Id);
                entity
                    .HasOne<ARestrictiveParent>()
                    .WithMany()
                    .HasForeignKey(invoice => invoice.ParentId)
                    .OnDelete(DeleteBehavior.Restrict);
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("restrictive-fk", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0002-000000000001")]
    private sealed class ARestrictiveParent
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset? CreatedAt { get; init; }

        [ErasureSubject("person")]
        public Guid? SubjectId { get; init; }
    }

    [Retain("restrictive-fk", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0002-000000000002")]
    private sealed class ZRestrictiveChild
    {
        public Guid Id { get; init; }
        public Guid ParentId { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("restrictive-fk-long", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0002-000000000003")]
    private sealed class ZLongLivedChild
    {
        public Guid Id { get; init; }
        public Guid ParentId { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class HostInvoice
    {
        public Guid Id { get; init; }
        public Guid ParentId { get; init; }
    }
}
