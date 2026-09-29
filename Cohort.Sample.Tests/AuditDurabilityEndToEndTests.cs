using System.Collections.Concurrent;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class AuditDurabilityEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Committed_Mutation_Remains_In_Authoritative_Totals_When_Entity_Settlement_Fails()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        var functionName = $"fail_summary_{noteId:N}";
        var triggerName = $"fail_summary_{noteId:N}";

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "durable-progress",
                }
            );
            await db.SaveChangesAsync();
        }

        await ExecuteAsync(
            $"""
            CREATE FUNCTION "{functionName}"() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW."TenantId" = '{tenantId}'::uuid THEN
                    RAISE EXCEPTION 'entity settlement exploded';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER "{triggerName}"
            BEFORE UPDATE ON "sweep_run_entity_summary"
            FOR EACH ROW EXECUTE FUNCTION "{functionName}"();
            """
        );

        try
        {
            var emittedEvents = new ConcurrentQueue<SweepEvent>();
            using var host = new CohortTestHost(
                ConnectionString,
                configureServices: services =>
                    services.AddSingleton<IRetentionAuditObserver>(
                        new RecordingAuditObserver(emittedEvents)
                    )
            );

            var result = await host.RunSweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                asOf
            );

            result
                .EntityFailures.Should()
                .AllSatisfy(failure =>
                {
                    failure.Should().MatchRegex(
                        "^type=Npgsql\\.PostgresException;code=sqlstate:P0001;diagnosticId=[0-9a-f]{32}$"
                    );
                    failure.Should().NotContain("entity settlement exploded");
                });
            result.Counts.Single(count => count.EntityType == typeof(Note)).Affected.Should().Be(1);
            emittedEvents
                .OfType<SweepEvent.PartiallyFailed>()
                .Should()
                .ContainSingle(evt => evt.TotalAffected == 1);
            await AssertTotalsAsync(result.SweepId, tenantId, expectedAffected: 1);
        }
        finally
        {
            await ExecuteAsync(
                $"DROP TRIGGER IF EXISTS \"{triggerName}\" ON \"sweep_run_entity_summary\"; DROP FUNCTION IF EXISTS \"{functionName}\"();"
            );
        }
    }

    [Theory]
    [InlineData(Strategy.Purge)]
    [InlineData(Strategy.Anonymise)]
    public async Task Cancellation_During_Mutation_Rolls_Back_Entity_And_Audit_Progress(
        Strategy strategy
    )
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        var functionName = $"delay_mutation_{noteId:N}";
        var triggerName = $"delay_mutation_{noteId:N}";
        var (lockKey1, lockKey2) = AdvisoryLockKeys(noteId);
        var (tableName, operation) = strategy switch
        {
            Strategy.Purge => ("notes", "DELETE"),
            Strategy.Anonymise => ("anonymised_contacts", "UPDATE"),
            _ => throw new ArgumentOutOfRangeException(nameof(strategy)),
        };

        await using (var db = Host.CreateDbContext())
        {
            switch (strategy)
            {
                case Strategy.Purge:
                    db.Notes.Add(
                        new Note
                        {
                            Id = noteId,
                            TenantId = tenantId,
                            SubjectId = subjectId,
                            CreatedAt = asOf.AddDays(-120),
                            Body = "cancel-after-mutation",
                        }
                    );
                    break;
                case Strategy.Anonymise:
                    db.AnonymisedContacts.Add(
                        new AnonymisedContact
                        {
                            Id = noteId,
                            TenantId = tenantId,
                            SubjectId = subjectId,
                            CreatedAt = asOf.AddDays(-120),
                            EmailAddress = "cancelled@example.org",
                            GivenName = "Cancel",
                            Surname = "Mutation",
                            Notes = "must remain unchanged",
                        }
                    );
                    break;
            }
            await db.SaveChangesAsync();
        }

        await ExecuteAsync(
            $"""
            CREATE FUNCTION "{functionName}"() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF OLD."TenantId" = '{tenantId}'::uuid THEN
                    PERFORM pg_advisory_xact_lock({lockKey1}, {lockKey2});
                END IF;
                RETURN OLD;
            END $$;
            CREATE TRIGGER "{triggerName}"
            AFTER {operation} ON "{tableName}"
            FOR EACH ROW EXECUTE FUNCTION "{functionName}"();
            """
        );

        try
        {
            var emittedEvents = new ConcurrentQueue<SweepEvent>();
            using var host = new CohortTestHost(
                ConnectionString,
                configureServices: services =>
                    services.AddSingleton<IRetentionAuditObserver>(
                        new RecordingAuditObserver(emittedEvents)
                    )
            );
            using var cancellation = new CancellationTokenSource();
            await using var lockConnection = await HoldAdvisoryLockAsync(lockKey1, lockKey2);

            var runTask = host.RunSweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                asOf,
                cancellation.Token
            );

            await WaitForAdvisoryLockWaiterAsync(lockKey1, lockKey2);
            cancellation.Cancel();
            await lockConnection.CloseAsync();

            Func<Task> act = async () => await runTask;
            await act.Should().ThrowAsync<OperationCanceledException>();

            await using (var verify = Host.CreateDbContext())
            {
                switch (strategy)
                {
                    case Strategy.Purge:
                        (await verify.Notes.AnyAsync(row => row.Id == noteId)).Should().BeTrue();
                        break;
                    case Strategy.Anonymise:
                        var contact = await verify.AnonymisedContacts.SingleAsync(row =>
                            row.Id == noteId
                        );
                        contact.EmailAddress.Should().Be("cancelled@example.org");
                        contact.GivenName.Should().Be("Cancel");
                        contact.Surname.Should().Be("Mutation");
                        contact.AnonymisedAt.Should().BeNull();
                        break;
                }
            }

            emittedEvents
                .OfType<SweepEvent.EntityProgress>()
                .Should()
                .NotContain(progress => progress.Affected > 0);
            emittedEvents.OfType<SweepEvent.RowDetail>().Should().BeEmpty();
            await AssertCancelledWithoutProgressAsync(tenantId);
        }
        finally
        {
            await ExecuteAsync(
                $"DROP TRIGGER IF EXISTS \"{triggerName}\" ON \"{tableName}\"; DROP FUNCTION IF EXISTS \"{functionName}\"();"
            );
        }
    }

    private async Task AssertCancelledWithoutProgressAsync(Guid tenantId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT run."Status", run."SettledAt", run."Error", run."TotalAffected",
                   COUNT(detail."SweepId"), COALESCE(SUM(summary."Affected"), 0)
            FROM "sweep_run" run
            LEFT JOIN "sweep_run_row_detail" detail ON detail."SweepId" = run."SweepId"
            LEFT JOIN "sweep_run_entity_summary" summary ON summary."SweepId" = run."SweepId"
            WHERE run."SweepId" = (
                SELECT "SweepId"
                FROM "sweep_run"
                WHERE "TenantId" = @tenantId
                ORDER BY "StartedAt" DESC
                LIMIT 1
            )
            GROUP BY run."SweepId"
            """;
        command.Parameters.AddWithValue("tenantId", tenantId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt32(0).Should().Be((int)SweepRunStatus.Cancelled);
        reader.IsDBNull(1).Should().BeFalse();
        reader.GetString(2).Should().NotBeNullOrWhiteSpace();
        reader.GetInt64(3).Should().Be(0);
        reader.GetInt64(4).Should().Be(0);
        reader.GetInt64(5).Should().Be(0);
    }

    private static (int Key1, int Key2) AdvisoryLockKeys(Guid marker)
    {
        var bytes = marker.ToByteArray();
        return (
            BitConverter.ToInt32(bytes, 0) & int.MaxValue,
            BitConverter.ToInt32(bytes, 4) & int.MaxValue
        );
    }

    private async Task<NpgsqlConnection> HoldAdvisoryLockAsync(int key1, int key2)
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT pg_advisory_lock(@key1, @key2)";
        command.Parameters.AddWithValue("key1", key1);
        command.Parameters.AddWithValue("key2", key2);
        await command.ExecuteNonQueryAsync();
        return connection;
    }

    private async Task WaitForAdvisoryLockWaiterAsync(int key1, int key2)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync(timeout.Token);

        while (true)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks
                    WHERE locktype = 'advisory'
                      AND classid = @key1::oid
                      AND objid = @key2::oid
                      AND objsubid = 2
                      AND NOT granted
                )
                """;
            command.Parameters.AddWithValue("key1", key1);
            command.Parameters.AddWithValue("key2", key2);

            if ((bool)(await command.ExecuteScalarAsync(timeout.Token))!)
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(20), timeout.Token);
        }
    }

    private async Task AssertTotalsAsync(Guid sweepId, Guid tenantId, long expectedAffected)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT run."TotalAffected", summary."Affected"
            FROM "sweep_run" run
            JOIN "sweep_run_entity_summary" summary ON summary."SweepId" = run."SweepId"
            WHERE run."SweepId" = @sweepId
              AND summary."TenantId" = @tenantId
              AND summary."EntityType" LIKE '%Note'
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("tenantId", tenantId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt64(0).Should().Be(expectedAffected);
        reader.GetInt64(1).Should().Be(expectedAffected);
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private sealed class RecordingAuditObserver(ConcurrentQueue<SweepEvent> events)
        : IRetentionAuditObserver
    {
        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            events.Enqueue(evt);
            return Task.CompletedTask;
        }
    }
}
