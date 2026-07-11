using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class SweepConcurrencyAndVolumeEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Two_Concurrent_Engines_Sweep_Every_Row_Exactly_Once()
    {
        // Direct IRetentionSweep calls race by design: the advisory lock guards only the
        // hosted worker. The trigger below blocks each DELETE after its SKIP LOCKED
        // selection, proving both engines hold distinct batches at the same time.
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        const int expiredRows = 400;
        const long overlapGateKey = 7_310_042_120;
        var sweepApplicationName = $"cohort-concurrency-{Guid.NewGuid():N}";
        var expiredIds = Enumerable.Range(0, expiredRows).Select(_ => Guid.NewGuid()).ToArray();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                expiredIds.Select((id, index) => new Note
                    {
                        Id = id,
                        TenantId = tenantId,
                        CreatedAt = asOf.AddDays(index == 0 ? -121 : -120),
                        Body = $"concurrent-expired-{index}",
                    })
            );
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-5),
                    Body = "concurrent-fresh",
                }
            );
            await db.SaveChangesAsync();
        }

        // Small batches force the two engines to interleave instead of one grabbing
        // everything in a single pass.
        var sweepConnectionString = new NpgsqlConnectionStringBuilder(GetConnectionString())
        {
            ApplicationName = sweepApplicationName,
        }.ConnectionString;
        using var host = new CohortTestHost(
            sweepConnectionString,
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:SweepBatchSize"] = "50",
            }
        );
        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());

        await using var gateConnection = new NpgsqlConnection(GetConnectionString());
        await gateConnection.OpenAsync();
        await using var gateTransaction = await gateConnection.BeginTransactionAsync();
        await using (var gateCommand = gateConnection.CreateCommand())
        {
            gateCommand.Transaction = gateTransaction;
            gateCommand.CommandText = $"SELECT pg_advisory_xact_lock({overlapGateKey})";
            await gateCommand.ExecuteNonQueryAsync();
        }

        await InstallSweepOverlapGateAsync(tenantId, overlapGateKey);
        var owningSweep = host.RunSweepAsync(tenant, asOf);

        RetentionSweepResult[] results;
        var gateReleased = false;
        var sweepsCompleted = false;
        Task<RetentionSweepResult>? competingSweep = null;
        try
        {
            await WaitForConcurrentSweepBackendsAsync(
                sweepApplicationName,
                expectedCount: 1
            );
            competingSweep = host.RunSweepAsync(tenant, asOf);
            var sweepBackendIds = await WaitForConcurrentSweepBackendsAsync(
                sweepApplicationName,
                expectedCount: 2
            );
            sweepBackendIds.Should().OnlyHaveUniqueItems().And.HaveCount(2);
            owningSweep.IsCompleted.Should().BeFalse();
            competingSweep.IsCompleted.Should().BeFalse();

            await gateTransaction.CommitAsync();
            gateReleased = true;
            var owningResult = await owningSweep.WaitAsync(TimeSpan.FromSeconds(15));
            var competingResult = await competingSweep.WaitAsync(TimeSpan.FromSeconds(15));
            results = [owningResult, competingResult];
            sweepsCompleted = true;
        }
        finally
        {
            if (!gateReleased)
            {
                await gateTransaction.RollbackAsync();
                try
                {
                    await owningSweep.WaitAsync(TimeSpan.FromSeconds(15));
                    if (competingSweep is not null)
                    {
                        await competingSweep.WaitAsync(TimeSpan.FromSeconds(15));
                    }
                }
                catch
                {
                    // Preserve the gate/assertion failure while still removing the trigger.
                }
            }

            await RemoveSweepOverlapGateAsync();
            if (!sweepsCompleted)
            {
                await RemoveSweepLedgerAsync();
            }
        }

        var ledgerEntries = await ReadSweepLedgerAsync();
        await RemoveSweepLedgerAsync();

        var totalNotesAffected = results
            .SelectMany(result => result.Counts)
            .Where(count => count.EntityType == typeof(Note))
            .Sum(count => count.Affected);
        totalNotesAffected.Should().Be(expiredRows);

        results.Should().AllSatisfy(result => result.EntityFailures.Should().BeEmpty());

        ledgerEntries.Should().HaveCount(expiredRows);
        ledgerEntries.Select(entry => entry.RecordId).Should().BeEquivalentTo(expiredIds);
        ledgerEntries
            .GroupBy(entry => entry.RecordId)
            .Should()
            .OnlyContain(group => group.Count() == 1);
        await using var verify = Host.CreateDbContext();
        var remaining = await verify.Notes.Where(note => note.TenantId == tenantId).ToListAsync();
        remaining.Should().ContainSingle(note => note.Body == "concurrent-fresh");
    }

    [Fact]
    public async Task Sweep_Retires_A_Large_Backlog_Across_Many_Batches()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        const int backlogRows = 25_000;

        await SeedNotesInBulkAsync(tenantId, asOf.AddDays(-120), backlogRows);

        using var host = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:SweepBatchSize"] = "1000",
            }
        );

        var result = await host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.EntityFailures.Should().BeEmpty();
        result
            .Counts.Should()
            .Contain(count => count.EntityType == typeof(Note) && count.Affected == backlogRows);

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.CountAsync(note => note.TenantId == tenantId)).Should().Be(0);
    }

    [Fact]
    public async Task Sweep_Fills_A_Batch_Past_Oldest_Rows_Locked_By_Another_Transaction()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var ids = Enumerable.Range(0, 4).Select(_ => Guid.NewGuid()).ToArray();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                ids.Select((id, index) => new Note
                {
                    Id = id,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120).AddMinutes(index),
                    Body = $"locked-prefix-{index}",
                })
            );
            await db.SaveChangesAsync();
        }

        await using var blocker = new NpgsqlConnection(GetConnectionString());
        await blocker.OpenAsync();
        await using var blockerTransaction = await blocker.BeginTransactionAsync();
        await using (var lockCommand = blocker.CreateCommand())
        {
            lockCommand.Transaction = blockerTransaction;
            lockCommand.CommandText = """
                SELECT "Id"
                FROM "notes"
                WHERE "Id" = ANY(@ids)
                FOR UPDATE
                """;
            lockCommand.Parameters.AddWithValue("ids", ids[..2]);
            await lockCommand.ExecuteNonQueryAsync();
        }

        using var host = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:SweepBatchSize"] = "2",
            }
        );
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var result = await host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf,
            timeout.Token
        );

        result
            .Counts.Should()
            .Contain(count => count.EntityType == typeof(Note) && count.Affected == 2);
        await blockerTransaction.CommitAsync();

        await using var verify = Host.CreateDbContext();
        var remainingIds = await verify.Notes
            .Where(note => note.TenantId == tenantId)
            .Select(note => note.Id)
            .ToListAsync();
        remainingIds.Should().BeEquivalentTo(ids[..2]);
    }

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }

    private async Task InstallSweepOverlapGateAsync(Guid tenantId, long gateKey)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            CREATE OR REPLACE FUNCTION cohort_test_sweep_overlap_gate()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $function$
            BEGIN
                IF OLD."TenantId" = '{tenantId}'::uuid THEN
                    PERFORM pg_advisory_xact_lock_shared({gateKey});
                    INSERT INTO cohort_test_sweep_ledger (record_id, backend_id)
                    VALUES (OLD."Id", pg_backend_pid());
                END IF;
                RETURN OLD;
            END;
            $function$;

            CREATE TABLE cohort_test_sweep_ledger (
                record_id uuid NOT NULL,
                backend_id integer NOT NULL
            );

            CREATE TRIGGER cohort_test_sweep_overlap_gate
            BEFORE DELETE ON "notes"
            FOR EACH ROW
            EXECUTE FUNCTION cohort_test_sweep_overlap_gate();
            """;
        await command.ExecuteNonQueryAsync();
    }

    private async Task<int[]> WaitForConcurrentSweepBackendsAsync(
        string applicationName,
        int expectedCount
    )
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync(timeout.Token);

        while (true)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT array_agg(pid ORDER BY pid)
                FROM pg_stat_activity
                WHERE application_name = @applicationName
                  AND state = 'active'
                  AND query NOT LIKE '%pg_stat_activity%'
            """;
            command.Parameters.AddWithValue("applicationName", applicationName);
            var value = await command.ExecuteScalarAsync(timeout.Token);
            var backendIds = value is int[] ids ? ids : [];
            if (backendIds.Length == expectedCount)
            {
                return backendIds;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(20), timeout.Token);
        }
    }

    private async Task RemoveSweepOverlapGateAsync()
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            DROP TRIGGER IF EXISTS cohort_test_sweep_overlap_gate ON "notes";
            DROP FUNCTION IF EXISTS cohort_test_sweep_overlap_gate();
            """;
        await command.ExecuteNonQueryAsync();
    }

    private async Task RemoveSweepLedgerAsync()
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "DROP TABLE IF EXISTS cohort_test_sweep_ledger";
        await command.ExecuteNonQueryAsync();
    }

    private async Task<(Guid RecordId, int BackendId)[]> ReadSweepLedgerAsync()
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT record_id, backend_id
            FROM cohort_test_sweep_ledger
            ORDER BY record_id
            """;
        await using var reader = await command.ExecuteReaderAsync();
        var entries = new List<(Guid RecordId, int BackendId)>();
        while (await reader.ReadAsync())
        {
            entries.Add((reader.GetGuid(0), reader.GetInt32(1)));
        }

        return entries.ToArray();
    }

    private async Task SeedNotesInBulkAsync(Guid tenantId, DateTimeOffset createdAt, int count)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO "notes" ("Id", "TenantId", "SubjectId", "CreatedAt", "Body")
            SELECT gen_random_uuid(), @tenantId, NULL, @createdAt, 'volume-' || g
            FROM generate_series(1, @count) AS g
            """;
        command.Parameters.AddWithValue("tenantId", tenantId);
        command.Parameters.AddWithValue("createdAt", createdAt);
        command.Parameters.AddWithValue("count", count);
        await command.ExecuteNonQueryAsync();
    }
}
