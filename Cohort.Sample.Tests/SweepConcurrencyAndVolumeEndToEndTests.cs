using Cohort.Application;
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
        const int expiredRows = 80;
        const long overlapGateKey = 7_310_042_120;
        var objectSuffix = Guid.NewGuid().ToString("N");
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
                [$"{CohortOptions.SectionName}:SweepBatchSize"] = "1",
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

        await InstallSweepOverlapGateAsync(tenantId, overlapGateKey, objectSuffix);
        await using var candidateGateConnection = new NpgsqlConnection(GetConnectionString());
        await candidateGateConnection.OpenAsync();
        await using var candidateGateTransaction =
            await candidateGateConnection.BeginTransactionAsync();
        await using (var candidateGateCommand = candidateGateConnection.CreateCommand())
        {
            candidateGateCommand.Transaction = candidateGateTransaction;
            candidateGateCommand.CommandText =
                "SELECT 1 FROM \"notes\" WHERE \"Id\" = @recordId FOR UPDATE";
            candidateGateCommand.Parameters.AddWithValue("recordId", expiredIds[0]);
            await candidateGateCommand.ExecuteNonQueryAsync();
        }
        var owningSweep = host.RunSweepAsync(tenant, asOf);

        RetentionSweepResult[] results;
        var gateReleased = false;
        var sweepsCompleted = false;
        Task<RetentionSweepResult>? competingSweep = null;
        try
        {
            await WaitForActiveMutationBackendsAsync(
                sweepApplicationName,
                expectedCount: 1
            );
            await candidateGateTransaction.CommitAsync();
            competingSweep = host.RunSweepAsync(tenant, asOf);
            var sweepBackendIds = await WaitForActiveMutationBackendsAsync(
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

            await RemoveSweepOverlapGateAsync(objectSuffix);
            if (!sweepsCompleted)
            {
                await RemoveSweepLedgerAsync(objectSuffix);
            }
        }

        var ledgerEntries = await ReadSweepLedgerAsync(objectSuffix);
        await RemoveSweepLedgerAsync(objectSuffix);

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
    public async Task Two_Concurrent_Handler_Aware_Engines_Report_Each_Row_Exactly_Once()
    {
        const long gateKey = 7_310_042_121;
        const int expiredRows = 80;
        var tenantId = Guid.NewGuid();
        var objectSuffix = Guid.NewGuid().ToString("N");
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var ids = Enumerable.Range(0, expiredRows).Select(_ => Guid.NewGuid()).ToArray();
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(ids.Select((id, index) => new Note
            {
                Id = id,
                TenantId = tenantId,
                CreatedAt = asOf.AddDays(-120).AddSeconds(index),
                Body = $"handler-concurrent-{index}",
            }));
            await db.SaveChangesAsync();
        }

        var applicationName = $"cohort-handler-concurrency-{Guid.NewGuid():N}";
        var connectionString = new NpgsqlConnectionStringBuilder(GetConnectionString())
        {
            ApplicationName = applicationName,
        }.ConnectionString;
        using var host = new CohortTestHost(
            connectionString,
            new SampleRetentionRuleProvider(),
            new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:SweepBatchSize"] = "1",
            },
            services => services.AddRowHandler<Note, ConcurrentNoteHandler>()
        );
        await using var gate = await HoldGateAsync(gateKey);
        await InstallMutationOverlapGateAsync("notes", "DELETE", tenantId, gateKey, objectSuffix);
        await using var oldestLock = await LockRowAsync("notes", ids[0]);
        RetentionSweepResult[] results;
        var gateReleased = false;
        try
        {
            var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());
            var first = host.RunSweepAsync(tenant, asOf);
            await WaitForConcurrentSweepBackendsAsync(applicationName, 1, gateKey);
            await oldestLock.CommitAsync();
            var second = host.RunSweepAsync(tenant, asOf);
            var backends = await WaitForActiveMutationBackendsAsync(applicationName, 2);
            backends.Should().OnlyHaveUniqueItems().And.HaveCount(2);
            first.IsCompleted.Should().BeFalse();
            second.IsCompleted.Should().BeFalse();
            await gate.CommitAsync();
            gateReleased = true;
            results = await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(20));
        }
        finally
        {
            if (!gateReleased)
            {
                await gate.RollbackAsync();
            }
            await RemoveMutationOverlapGateAsync("notes", objectSuffix);
        }

        results.SelectMany(result => result.Counts)
            .Where(count => count.EntityType == typeof(Note))
            .Sum(count => count.Affected).Should().Be(expiredRows);
        results.Should().AllSatisfy(result => result.EntityFailures.Should().BeEmpty());
        var evidence = await ReadSweepLedgerAsync(objectSuffix);
        evidence.Should().HaveCount(expiredRows);
        evidence.Select(row => row.BackendId).Distinct().Should().HaveCount(2);
        evidence.GroupBy(row => row.RecordId).Should().OnlyContain(group => group.Count() == 1);
        await RemoveSweepLedgerAsync(objectSuffix);
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.CountAsync(note => ids.Contains(note.Id))).Should().Be(0);
    }

    [Fact]
    public async Task Two_Concurrent_Anonymisation_Engines_Report_Each_Row_Exactly_Once()
    {
        const long gateKey = 7_310_042_122;
        const int expiredRows = 80;
        var tenantId = Guid.NewGuid();
        var objectSuffix = Guid.NewGuid().ToString("N");
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var ids = Enumerable.Range(0, expiredRows).Select(_ => Guid.NewGuid()).ToArray();
        await using (var db = Host.CreateDbContext())
        {
            db.AnonymisedContacts.AddRange(ids.Select((id, index) => new AnonymisedContact
            {
                Id = id,
                TenantId = tenantId,
                CreatedAt = asOf.AddDays(-120).AddSeconds(index),
                EmailAddress = $"person-{index}@example.test",
                GivenName = "Concurrent",
                Surname = index.ToString(),
                Notes = "preserve",
            }));
            await db.SaveChangesAsync();
        }

        var applicationName = $"cohort-anonymise-concurrency-{Guid.NewGuid():N}";
        var connectionString = new NpgsqlConnectionStringBuilder(GetConnectionString())
        {
            ApplicationName = applicationName,
        }.ConnectionString;
        using var host = new CohortTestHost(
            connectionString,
            new SampleRetentionRuleProvider(),
            new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:SweepBatchSize"] = "1",
            }
        );
        await using var gate = await HoldGateAsync(gateKey);
        await InstallMutationOverlapGateAsync("anonymised_contacts", "UPDATE", tenantId, gateKey, objectSuffix);
        await using var oldestLock = await LockRowAsync("anonymised_contacts", ids[0]);
        RetentionSweepResult[] results;
        var gateReleased = false;
        try
        {
            var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());
            var first = host.RunSweepAsync(tenant, asOf);
            await WaitForConcurrentSweepBackendsAsync(applicationName, 1, gateKey);
            await oldestLock.CommitAsync();
            var second = host.RunSweepAsync(tenant, asOf);
            var backends = await WaitForActiveMutationBackendsAsync(applicationName, 2);
            backends.Should().OnlyHaveUniqueItems().And.HaveCount(2);
            first.IsCompleted.Should().BeFalse();
            second.IsCompleted.Should().BeFalse();
            await gate.CommitAsync();
            gateReleased = true;
            results = await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(20));
        }
        finally
        {
            if (!gateReleased)
            {
                await gate.RollbackAsync();
            }
            await RemoveMutationOverlapGateAsync("anonymised_contacts", objectSuffix);
        }

        results.SelectMany(result => result.Counts)
            .Where(count => count.EntityType == typeof(AnonymisedContact))
            .Sum(count => count.Affected).Should().Be(expiredRows);
        results.Should().AllSatisfy(result => result.EntityFailures.Should().BeEmpty());
        var evidence = await ReadSweepLedgerAsync(objectSuffix);
        evidence.Should().HaveCount(expiredRows);
        evidence.Select(row => row.BackendId).Distinct().Should().HaveCount(2);
        evidence.GroupBy(row => row.RecordId).Should().OnlyContain(group => group.Count() == 1);
        await RemoveSweepLedgerAsync(objectSuffix);
        await using var verify = Host.CreateDbContext();
        var records = await verify.AnonymisedContacts.Where(contact => ids.Contains(contact.Id)).ToListAsync();
        records.Should().HaveCount(expiredRows)
            .And.OnlyContain(contact =>
                contact.AnonymisedAt == asOf
                && contact.EmailAddress == null
                && contact.Notes == "preserve"
            );
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

    private async Task InstallSweepOverlapGateAsync(Guid tenantId, long gateKey, string suffix)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            CREATE FUNCTION cohort_test_sweep_overlap_gate_{suffix}()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $function$
            BEGIN
                IF OLD."TenantId" = '{tenantId}'::uuid THEN
                    PERFORM pg_advisory_xact_lock_shared({gateKey});
                    INSERT INTO cohort_test_sweep_ledger_{suffix} (record_id, backend_id)
                    VALUES (OLD."Id", pg_backend_pid());
                END IF;
                RETURN OLD;
            END;
            $function$;

            CREATE TABLE cohort_test_sweep_ledger_{suffix} (
                record_id uuid NOT NULL,
                backend_id integer NOT NULL
            );

            CREATE TRIGGER cohort_test_sweep_overlap_gate_{suffix}
            BEFORE DELETE ON "notes"
            FOR EACH ROW
            EXECUTE FUNCTION cohort_test_sweep_overlap_gate_{suffix}();
            """;
        await command.ExecuteNonQueryAsync();
    }

    private async Task<int[]> WaitForConcurrentSweepBackendsAsync(
        string applicationName,
        int expectedCount,
        long gateKey
    )
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync(timeout.Token);

        while (true)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT pg_catalog.array_agg(activity.pid ORDER BY activity.pid)
                FROM pg_catalog.pg_stat_activity activity
                JOIN pg_catalog.pg_locks waiting
                  ON waiting.pid = activity.pid
                 AND waiting.locktype = 'advisory'
                 AND NOT waiting.granted
                 AND waiting.objsubid = 1
                 AND ((waiting.classid::bigint << 32) + waiting.objid::bigint) = @gateKey
                WHERE activity.application_name = @applicationName
            """;
            command.Parameters.AddWithValue("applicationName", applicationName);
            command.Parameters.AddWithValue("gateKey", gateKey);
            var value = await command.ExecuteScalarAsync(timeout.Token);
            var backendIds = value is int[] ids ? ids : [];
            if (backendIds.Length == expectedCount)
            {
                return backendIds;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(20), timeout.Token);
        }
    }

    private async Task<int[]> WaitForActiveMutationBackendsAsync(
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
                SELECT pg_catalog.array_agg(activity.pid ORDER BY activity.pid)
                FROM pg_catalog.pg_stat_activity activity
                WHERE activity.application_name = @applicationName
                  AND activity.state = 'active'
                  AND activity.query NOT ILIKE '%pg_stat_activity%'
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

    private async Task<NpgsqlTransaction> HoldGateAsync(long gateKey)
    {
        var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        var transaction = await connection.BeginTransactionAsync();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT pg_catalog.pg_advisory_xact_lock(@gateKey)";
        command.Parameters.AddWithValue("gateKey", gateKey);
        await command.ExecuteNonQueryAsync();
        return transaction;
    }

    private async Task<NpgsqlTransaction> LockRowAsync(string table, Guid recordId)
    {
        var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        var transaction = await connection.BeginTransactionAsync();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT 1 FROM \"{table}\" WHERE \"Id\" = @recordId FOR UPDATE";
        command.Parameters.AddWithValue("recordId", recordId);
        await command.ExecuteNonQueryAsync();
        return transaction;
    }

    private async Task InstallMutationOverlapGateAsync(
        string table,
        string operation,
        Guid tenantId,
        long gateKey,
        string suffix
    )
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            CREATE TABLE cohort_test_sweep_ledger_{suffix} (record_id uuid NOT NULL, backend_id integer NOT NULL);
            CREATE FUNCTION cohort_test_mutation_overlap_gate_{suffix}() RETURNS trigger LANGUAGE plpgsql AS $function$
            BEGIN
                IF OLD."TenantId" = '{tenantId}'::uuid THEN
                    PERFORM pg_catalog.pg_advisory_xact_lock_shared({gateKey});
                    INSERT INTO cohort_test_sweep_ledger_{suffix} VALUES (OLD."Id", pg_catalog.pg_backend_pid());
                END IF;
                RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
            END $function$;
            CREATE TRIGGER cohort_test_mutation_overlap_gate_{suffix} BEFORE {operation} ON "{table}"
            FOR EACH ROW EXECUTE FUNCTION cohort_test_mutation_overlap_gate_{suffix}();
            """;
        await command.ExecuteNonQueryAsync();
    }

    private async Task RemoveMutationOverlapGateAsync(string table, string suffix)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            DROP TRIGGER IF EXISTS cohort_test_mutation_overlap_gate_{suffix} ON "{table}";
            DROP FUNCTION IF EXISTS cohort_test_mutation_overlap_gate_{suffix}();
            """;
        await command.ExecuteNonQueryAsync();
    }

    private async Task RemoveSweepOverlapGateAsync(string suffix)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            DROP TRIGGER IF EXISTS cohort_test_sweep_overlap_gate_{suffix} ON "notes";
            DROP FUNCTION IF EXISTS cohort_test_sweep_overlap_gate_{suffix}();
            """;
        await command.ExecuteNonQueryAsync();
    }

    private async Task RemoveSweepLedgerAsync(string suffix)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"DROP TABLE IF EXISTS cohort_test_sweep_ledger_{suffix}";
        await command.ExecuteNonQueryAsync();
    }

    private async Task<(Guid RecordId, int BackendId)[]> ReadSweepLedgerAsync(string suffix)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT record_id, backend_id
            FROM cohort_test_sweep_ledger_{suffix}
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

    private sealed class ConcurrentRuleProvider(string blockedCategory) : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider _inner = new();
        private readonly TaskCompletionSource _bothCallsEntered = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private readonly TaskCompletionSource _release = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private int _callCount;

        internal Task BothCallsEntered => _bothCallsEntered.Task;

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            _inner.GetCapabilities(category);

        public async Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            if (context.Category == blockedCategory)
            {
                if (Interlocked.Increment(ref _callCount) == 2)
                {
                    _bothCallsEntered.TrySetResult();
                }
                await _release.Task.WaitAsync(ct);
            }
            return await _inner.ResolveAsync(context, ct);
        }

        internal void Release() => _release.TrySetResult();
    }

    private sealed class ConcurrentNoteHandler : IRetentionHandler<Note>;
}
