using System.Collections.Concurrent;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class AuditObserverEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Row_Detail_Observer_Runs_Only_After_Mutation_And_Audit_Commit()
    {
        var tenantId = Guid.NewGuid();
        var recordId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var observer = new CommitVisibilityObserver(ConnectionString, recordId);

        await using (var db = Host.CreateDbContext())
        {
            db.PerRowAuditedLogs.Add(
                new PerRowAuditedLog
                {
                    Id = recordId,
                    TenantId = tenantId,
                    CreatedAt = now.AddDays(-60),
                    Payload = "post-commit-observer",
                }
            );
            await db.SaveChangesAsync();
        }

        using var host = new CohortTestHost(
            ConnectionString,
            configureServices: services => services.AddSingleton<IRetentionAuditObserver>(observer)
        );

        await host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now
        );

        var visibility = await observer.Visibility.WaitAsync(TimeSpan.FromSeconds(5));
        visibility.SourceRowExists.Should().BeFalse();
        visibility.AuditRowExists.Should().BeTrue();
    }

    [Fact]
    public async Task Rolled_Back_Row_Detail_And_Progress_Are_Not_Observed()
    {
        var tenantId = Guid.NewGuid();
        var recordId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var functionName = $"fail_observed_progress_{recordId:N}";
        var triggerName = $"fail_observed_progress_{recordId:N}";
        var observer = new RecordingObserver();

        await using (var db = Host.CreateDbContext())
        {
            db.PerRowAuditedLogs.Add(
                new PerRowAuditedLog
                {
                    Id = recordId,
                    TenantId = tenantId,
                    CreatedAt = now.AddDays(-60),
                    Payload = "rolled-back-observer",
                }
            );
            await db.SaveChangesAsync();
        }

        await ExecuteAsync(
            $"""
            CREATE FUNCTION "{functionName}"() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF OLD."TenantId" = '{tenantId}'::uuid
                   AND NEW."TotalAffected" > OLD."TotalAffected" THEN
                    RAISE EXCEPTION 'progress write failed';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER "{triggerName}"
            BEFORE UPDATE ON "sweep_run"
            FOR EACH ROW EXECUTE FUNCTION "{functionName}"();
            """
        );

        try
        {
            using var host = new CohortTestHost(
                ConnectionString,
                configureServices: services =>
                    services.AddSingleton<IRetentionAuditObserver>(observer)
            );

            var result = await host.RunSweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                now
            );

            result
                .EntityFailures.Should()
                .ContainSingle()
                .Which.Should()
                .MatchRegex(
                    "^type=Npgsql\\.PostgresException;code=sqlstate:P0001;diagnosticId=[0-9a-f]{32}$"
                );
            observer
                .Events.OfType<SweepEvent.RowDetail>()
                .Should()
                .NotContain(detail => detail.RecordId == recordId.ToString());
            observer
                .Events.OfType<SweepEvent.EntityProgress>()
                .Should()
                .NotContain(progress =>
                    progress.EntityType == typeof(PerRowAuditedLog) && progress.Affected > 0
                );

            await using var verify = Host.CreateDbContext();
            (await verify.PerRowAuditedLogs.AnyAsync(row => row.Id == recordId)).Should().BeTrue();

            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT COUNT(*) FROM \"sweep_run_row_detail\" WHERE \"SweepId\" = @sweepId AND \"RecordId\" = @recordId";
            command.Parameters.AddWithValue("sweepId", result.SweepId);
            command.Parameters.AddWithValue("recordId", recordId.ToString());
            ((long)(await command.ExecuteScalarAsync())!).Should().Be(0);
        }
        finally
        {
            await ExecuteAsync(
                $"DROP TRIGGER IF EXISTS \"{triggerName}\" ON \"sweep_run\"; DROP FUNCTION IF EXISTS \"{functionName}\"();"
            );
        }
    }

    [Fact]
    public async Task Observer_Failures_And_Timeouts_Do_Not_Fail_The_Run_Or_Block_Later_Observers()
    {
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var blocking = new BlockingObserver();
        var healthy = new RecordingObserver();
        var logs = new RecordingLogProvider();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = now.AddDays(-60),
                    Body = "observer-isolation",
                }
            );
            await db.SaveChangesAsync();
        }

        using var host = new CohortTestHost(
            ConnectionString,
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:AuditObservers:Timeout"] = "00:00:00.050",
            },
            configureServices: services =>
            {
                services.AddSingleton<IRetentionAuditObserver>(new ThrowingObserver());
                services.AddSingleton<IRetentionAuditObserver>(blocking);
                services.AddSingleton<IRetentionAuditObserver>(healthy);
                services.AddSingleton<ILoggerProvider>(logs);
            }
        );

        var result = await host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now
        );

        await blocking.CancellationObserved.WaitAsync(TimeSpan.FromSeconds(5));
        result.EntityFailures.Should().BeEmpty();
        healthy.Events[0].Should().BeOfType<SweepEvent.Started>();
        healthy.Events[^1].Should().BeOfType<SweepEvent.Completed>();
        logs.Entries.Should().Contain(entry =>
            entry.Level == LogLevel.Error && entry.Message.Contains("failed processing committed event")
        );
        logs.Entries.Should().Contain(entry =>
            entry.Level == LogLevel.Warning && entry.Message.Contains("timed out processing committed event")
        );

        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT \"Status\", \"TotalAffected\" FROM \"sweep_run\" WHERE \"SweepId\" = @sweepId";
        command.Parameters.AddWithValue("sweepId", result.SweepId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt32(0).Should().Be((int)SweepRunStatus.Succeeded);
        reader.GetInt64(1).Should().Be(1);
    }

    [Fact]
    public async Task Observer_That_Blocks_Synchronously_Is_Bounded_By_Timeout_And_Quarantined()
    {
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var observer = new SynchronouslyBlockingObserver(TimeSpan.FromSeconds(3));

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = now.AddDays(-60),
                    Body = "synchronous-observer",
                }
            );
            await db.SaveChangesAsync();
        }

        using var host = new CohortTestHost(
            ConnectionString,
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:AuditObservers:Timeout"] = "00:00:00.100",
            },
            configureServices: services => services.AddSingleton<IRetentionAuditObserver>(observer)
        );

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var result = await host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now
        );
        stopwatch.Stop();

        result.EntityFailures.Should().BeEmpty();
        stopwatch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(2));
        observer.Calls.Should().Be(1);
    }

    private sealed class CommitVisibilityObserver(string connectionString, Guid recordId)
        : IRetentionAuditObserver
    {
        private readonly TaskCompletionSource<CommitVisibility> visibility =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<CommitVisibility> Visibility => visibility.Task;

        public async Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            if (evt is not SweepEvent.RowDetail detail || detail.RecordId != recordId.ToString())
            {
                return;
            }

            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT
                    EXISTS (SELECT 1 FROM "per_row_audited_logs" WHERE "Id" = @recordId),
                    EXISTS (SELECT 1 FROM "sweep_run_row_detail" WHERE "SweepId" = @sweepId AND "RecordId" = @recordIdText)
                """;
            command.Parameters.AddWithValue("recordId", recordId);
            command.Parameters.AddWithValue("sweepId", detail.SweepId);
            command.Parameters.AddWithValue("recordIdText", recordId.ToString());
            await using var reader = await command.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            visibility.TrySetResult(new CommitVisibility(reader.GetBoolean(0), reader.GetBoolean(1)));
        }
    }

    private sealed class RecordingObserver : IRetentionAuditObserver
    {
        private readonly ConcurrentQueue<SweepEvent> events = new();

        public IReadOnlyList<SweepEvent> Events => events.ToArray();

        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            events.Enqueue(evt);
            return Task.CompletedTask;
        }
    }

    private sealed class SynchronouslyBlockingObserver(TimeSpan block) : IRetentionAuditObserver
    {
        private int calls;

        public int Calls => Volatile.Read(ref calls);

        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                Thread.Sleep(block);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingObserver : IRetentionAuditObserver
    {
        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct) =>
            throw new InvalidOperationException("observer failure must be isolated");
    }

    private sealed class BlockingObserver : IRetentionAuditObserver
    {
        private readonly TaskCompletionSource cancellationObserved =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task CancellationObserved => cancellationObserved.Task;

        public async Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            if (evt is not SweepEvent.Started)
            {
                return;
            }

            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                cancellationObserved.TrySetResult();
                throw;
            }
        }
    }

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
                entries.Enqueue(new LogEntry(logLevel, formatter(state, exception)));
            }
        }
    }

    private sealed record CommitVisibility(bool SourceRowExists, bool AuditRowExists);

    public sealed record LogEntry(LogLevel Level, string Message);

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }
}
