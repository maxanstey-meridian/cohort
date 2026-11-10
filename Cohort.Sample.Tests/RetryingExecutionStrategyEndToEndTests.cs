using System.Collections.Concurrent;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Npgsql;

namespace Cohort.Sample.Tests;

// A host whose DbContext retries transient failures (EnableRetryOnFailure) refuses EF work in a
// transaction opened outside its execution strategy. Every transaction Cohort begins therefore
// runs inside the strategy, which re-runs it from the start on a transient failure before the
// commit. Each test runs on such a host; a statement is made to fail with a deadlock (40P01) by
// rewriting its text on the connection, and a commit by a hook on the connection's transaction.
[Collection("Integration")]
public sealed class RetryingExecutionStrategyEndToEndTests(PostgresFixture fixture)
{
    private static readonly DateTimeOffset AsOf = new(2026, 11, 4, 12, 0, 0, TimeSpan.Zero);

    // The statement that records a batch's progress: the last thing a batch does before it commits.
    private const string ProgressStatement = "WITH upserted_summary";

    // The statement the probing step sends after its save, which a test makes fail.
    private const string StepProbeStatement = "SELECT 1 /* cohort step probe */";

    [Fact]
    public async Task A_step_that_saves_retires_the_rows_of_a_retrying_context()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        var faults = new Faults();
        using var host = RetryingHost(database, faults, services => services.AddRetentionDependents<Note, NoteOutboxStep>());
        var tenant = Tenant();
        var noteId = Guid.NewGuid();
        await SeedAsync(host, new Note { Id = noteId, TenantId = tenant.Id, CreatedAt = AsOf.AddDays(-45), Body = "step" });

        var result = await host.RunSweepAsync(tenant, AsOf);

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
            .Which.Should().BeEquivalentTo(new { Affected = 1L, SkippedCount = 0L });
        await using var verify = host.CreateDbContext();
        (await verify.Notes.CountAsync()).Should().Be(0);
        (await verify.OutboxMessages.Select(message => message.Payload).ToListAsync()).Should().Equal(noteId.ToString());
    }

    [Fact]
    public async Task A_per_row_anonymise_factory_without_a_step_anonymises_the_rows_of_a_retrying_context()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        var faults = new Faults();
        using var host = RetryingHost(database, faults);
        var tenant = Tenant();
        var recordId = Guid.NewGuid();
        await using (var seed = host.CreateDbContext())
        {
            seed.TombstoneRecords.Add(
                new TombstoneRecord
                {
                    Id = recordId,
                    TenantId = tenant.Id,
                    CreatedAt = AsOf.AddDays(-60),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "Original",
                    ContactEmail = "person@example.org",
                    Notes = "must survive",
                }
            );
            await seed.SaveChangesAsync();
        }

        var result = await host.RunSweepAsync(tenant, AsOf);

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(TombstoneRecord))
            .Which.Affected.Should().Be(1);
        await using var verify = host.CreateDbContext();
        var record = await verify.TombstoneRecords.SingleAsync(row => row.Id == recordId);
        record.ExternalId.Should().Be(GuidTombstoneFactory.TombstoneValue);
        record.DisplayName.Should().Be("Original-tombstone");
        record.ContactEmail.Should().BeNull();
        record.Notes.Should().Be("must survive");
        record.AnonymisedAt.Should().Be(AsOf);
    }

    [Fact]
    public async Task A_transient_failure_before_the_commit_runs_the_batch_again_and_keeps_one_result()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        var faults = new Faults();
        var observer = new RecordingObserver();
        using var host = RetryingHost(
            database,
            faults,
            services =>
            {
                services.AddRetentionDependents<PerRowAuditedLog, CountingOutboxStep>();
                services.AddSingleton<IRetentionAuditObserver>(observer);
            }
        );
        var tenant = Tenant();
        var logId = Guid.NewGuid();
        await SeedAsync(host, new PerRowAuditedLog { Id = logId, TenantId = tenant.Id, CreatedAt = AsOf.AddDays(-45), Payload = "retry" });
        // After the step saved, the delete ran and the row detail was written: all of it rolls back.
        faults.FailStatementAfterTheNextStepRun(ProgressStatement, times: 1);

        var result = await host.RunSweepAsync(tenant, AsOf);

        faults.StatementFailuresInjected.Should().Be(1);
        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(PerRowAuditedLog))
            .Which.Affected.Should().Be(1);
        faults.StepRuns.Should().Be(2);
        await using var verify = host.CreateDbContext();
        (await verify.PerRowAuditedLogs.CountAsync()).Should().Be(0);
        (await verify.OutboxMessages.Select(message => message.Payload).ToListAsync()).Should().Equal(logId.ToString());
        (await CountAsync(database, "sweep_run_row_detail")).Should().Be(1);
        observer.Events.OfType<SweepEvent.RowDetail>().Should().ContainSingle().Which.RecordId.Should().Be(logId.ToString());
        // Every entity of the run reports progress, so only this entity's counts.
        observer.Events.OfType<SweepEvent.EntityProgress>()
            .Where(progress => progress.EntityType == typeof(PerRowAuditedLog))
            .Should().ContainSingle().Which.Affected.Should().Be(1);
    }

    [Fact]
    public async Task A_batch_that_fails_every_attempt_fails_the_entity_with_the_databases_error()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        var faults = new Faults();
        var logs = new CapturingLoggerProvider();
        using var host = RetryingHost(
            database,
            faults,
            services =>
            {
                services.AddRetentionDependents<PerRowAuditedLog, CountingOutboxStep>();
                services.AddLogging(logging => logging.AddProvider(logs));
            }
        );
        var tenant = Tenant();
        await SeedAsync(host, new PerRowAuditedLog { Id = Guid.NewGuid(), TenantId = tenant.Id, CreatedAt = AsOf.AddDays(-45), Payload = "always" });
        // The strategy's first attempt and its two retries: every attempt fails.
        faults.FailStatementAfterTheNextStepRun(ProgressStatement, times: 3);

        var result = await host.RunSweepAsync(tenant, AsOf);

        faults.StatementFailuresInjected.Should().Be(3);
        faults.StepRuns.Should().Be(3);
        result.EntityFailures.Should().ContainSingle()
            .Which.Should().MatchRegex("^type=Npgsql\\.PostgresException;code=sqlstate:40P01;diagnosticId=[0-9a-f]{32}$");
        // The exception the run caught, not EF's RetryLimitExceededException around it.
        logs.Exceptions.Should().Contain(exception => exception is PostgresException && ((PostgresException)exception).SqlState == "40P01");
        logs.Exceptions.Should().NotContain(exception => exception is RetryLimitExceededException);
        await using var verify = host.CreateDbContext();
        (await verify.PerRowAuditedLogs.CountAsync()).Should().Be(1);
        (await verify.OutboxMessages.CountAsync()).Should().Be(0);
    }

    [Fact]
    public async Task A_commit_that_fails_is_not_retried_and_the_next_run_retires_the_row()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        var faults = new Faults();
        using var host = RetryingHost(database, faults, services => services.AddRetentionDependents<PerRowAuditedLog, CountingOutboxStep>());
        var tenant = Tenant();
        var logId = Guid.NewGuid();
        await SeedAsync(host, new PerRowAuditedLog { Id = logId, TenantId = tenant.Id, CreatedAt = AsOf.AddDays(-45), Payload = "commit" });
        // The step arms it, so the next commit is the batch's own.
        faults.FailTheCommitAfterTheNextStepRun();

        var failed = await host.RunSweepAsync(tenant, AsOf);

        faults.CommitFailuresInjected.Should().Be(1);
        failed.EntityFailures.Should().ContainSingle()
            .Which.Should().MatchRegex("^type=System\\.IO\\.IOException;code=hresult:0x[0-9A-F]{8};diagnosticId=[0-9a-f]{32}$");
        faults.StepRuns.Should().Be(1);
        await using (var verify = host.CreateDbContext())
        {
            (await verify.PerRowAuditedLogs.CountAsync()).Should().Be(1);
            (await verify.OutboxMessages.CountAsync()).Should().Be(0);
        }

        var next = await host.RunSweepAsync(tenant, AsOf);

        next.EntityFailures.Should().BeEmpty();
        next.Counts.Should().ContainSingle(count => count.EntityType == typeof(PerRowAuditedLog))
            .Which.Affected.Should().Be(1);
        await using var after = host.CreateDbContext();
        (await after.PerRowAuditedLogs.CountAsync()).Should().Be(0);
        (await after.OutboxMessages.Select(message => message.Payload).ToListAsync()).Should().Equal(logId.ToString());
    }

    [Fact]
    public async Task A_purge_with_no_step_still_succeeds_on_a_retrying_context()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        var faults = new Faults();
        using var host = RetryingHost(database, faults);
        var tenant = Tenant();
        await SeedAsync(host, new Note { Id = Guid.NewGuid(), TenantId = tenant.Id, CreatedAt = AsOf.AddDays(-45), Body = "purge" });

        var result = await host.RunSweepAsync(tenant, AsOf);

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
            .Which.Affected.Should().Be(1);
        await using var verify = host.CreateDbContext();
        (await verify.Notes.CountAsync()).Should().Be(0);
    }

    // A context that doesn't retry gets no execution strategy from Cohort: Npgsql's default one
    // rethrows a transient failure as an InvalidOperationException around it, which no host that
    // doesn't retry saw before.
    [Fact]
    public async Task A_transient_failure_of_the_run_started_write_surfaces_the_postgres_exception_on_a_context_that_does_not_retry()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        var faults = new Faults();
        using var host = RetryingHost(database, faults, retries: false);
        var tenant = Tenant();
        await SeedAsync(host, new Note { Id = Guid.NewGuid(), TenantId = tenant.Id, CreatedAt = AsOf.AddDays(-45), Body = "no retry" });
        faults.FailStatement("INSERT INTO \"public\".\"sweep_run\"", times: 1);

        var run = () => host.RunSweepAsync(tenant, AsOf);

        var thrown = await run.Should().ThrowAsync<PostgresException>();
        thrown.Which.SqlState.Should().Be("40P01");
        faults.StatementFailuresInjected.Should().Be(1);
    }

    // A lost session is a transient failure to the strategy, which runs the batch again on the
    // connection EF reopens: a session that holds none of the run's locks, so another owner may
    // already be running the same work. The lock check before the commit refuses it.
    [Fact]
    public async Task A_run_whose_session_is_lost_mid_batch_commits_nothing_from_the_retried_batch()
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        var faults = new Faults();
        var observer = new RecordingObserver();
        using var host = RetryingHost(
            database,
            faults,
            services =>
            {
                services.AddRetentionDependents<PerRowAuditedLog, CountingOutboxStep>();
                services.AddSingleton<IRetentionAuditObserver>(observer);
            }
        );
        var tenant = Tenant();
        await SeedAsync(host, new PerRowAuditedLog { Id = Guid.NewGuid(), TenantId = tenant.Id, CreatedAt = AsOf.AddDays(-45), Payload = "lost" });
        // After the step ran and the row was retired, as the batch records its progress. Not
        // sooner: a failure while the step's own savepoint is open is taken for the step's.
        faults.TerminateTheRunSessionBeforeTheProgressStatementAfterTheStepRan(database.ConnectionString);

        var sweep = () => host.RunSweepAsync(tenant, AsOf);

        await sweep.Should().ThrowAsync<InvalidOperationException>().WithMessage("*lost the session holding its lock*");
        faults.SessionsTerminated.Should().Be(1);
        // The strategy ran the batch again, so the commit the lock check refused was the retry's.
        faults.StepRuns.Should().Be(2);
        await using var verify = host.CreateDbContext();
        (await verify.PerRowAuditedLogs.CountAsync()).Should().Be(1);
        (await verify.OutboxMessages.CountAsync()).Should().Be(0);
        (await CountAsync(database, "sweep_run_row_detail")).Should().Be(0);
        var sweepId = observer.Events.OfType<SweepEvent.Started>().Should().ContainSingle().Subject.SweepId;
        (await RunStatusAsync(database, sweepId)).Should().Be(SweepRunStatus.Failed);
        observer.Events.Should().NotContain(evt => evt is SweepEvent.Completed || evt is SweepEvent.PartiallyFailed);
        observer.Events.OfType<SweepEvent.EntityProgress>()
            .Where(progress => progress.EntityType == typeof(PerRowAuditedLog))
            .Should().BeEmpty();
    }

    // A failure while the step runs is the step's, deadlock or not, and whether or not the
    // context retries: the row is skipped, the step isn't run again, and the batch carries on.
    // The strategy never sees it, so a retrying context doesn't re-run the batch for it.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_transient_failure_inside_the_step_skips_the_row_and_does_not_rerun_the_batch(bool retries)
    {
        await using var database = await MigratedTemporaryDatabaseAsync();
        var faults = new Faults();
        using var host = RetryingHost(
            database,
            faults,
            services => services.AddRetentionDependents<PerRowAuditedLog, ProbingOutboxStep>(),
            retries
        );
        var tenant = Tenant();
        await SeedAsync(host, new PerRowAuditedLog { Id = Guid.NewGuid(), TenantId = tenant.Id, CreatedAt = AsOf.AddDays(-45), Payload = "no retry" });
        faults.FailStatement(StepProbeStatement, times: 1);

        var result = await host.RunSweepAsync(tenant, AsOf);

        faults.StatementFailuresInjected.Should().Be(1);
        faults.StepRuns.Should().Be(1);
        result.EntityFailures.Should().ContainSingle()
            .Which.Should().Be($"reason=rows-skipped;entityType={typeof(PerRowAuditedLog).FullName};count=1");
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(PerRowAuditedLog))
            .Which.Should().BeEquivalentTo(new { Affected = 0L, SkippedCount = 1L });
        await using var verify = host.CreateDbContext();
        (await verify.PerRowAuditedLogs.CountAsync()).Should().Be(1);
        (await verify.OutboxMessages.CountAsync()).Should().Be(0);
    }

    // The host's context retries transient failures, or with retries: false is an ordinary host's
    // that doesn't. Each scope gets its own connection, which rewrites the statement a test chose
    // to fail and fails the commit a test armed.
    private static CohortTestHost RetryingHost(
        TemporaryDatabase database,
        Faults faults,
        Action<IServiceCollection>? configureServices = null,
        bool retries = true
    ) =>
        new(
            database.ConnectionString,
            configureServices: services =>
            {
                services.AddSingleton(faults);
                services.ConfigureDbContext<SampleDbContext>((_, options) =>
                    options.UseNpgsql(
                        new PassThroughConnection(
                            new NpgsqlConnection(database.ConnectionString),
                            rewriteCommandText: faults.Rewrite,
                            beforeCommit: faults.BeforeCommit
                        ),
                        contextOwnsConnection: true,
                        npgsql =>
                        {
                            if (retries)
                            {
                                npgsql.EnableRetryOnFailure(
                                    maxRetryCount: 2,
                                    maxRetryDelay: TimeSpan.FromMilliseconds(50),
                                    errorCodesToAdd: null
                                );
                            }
                        }
                    )
                );
                configureServices?.Invoke(services);
            }
        );

    private static TenantContext Tenant() => new(Guid.NewGuid(), "uk", new Dictionary<string, string>());

    private static async Task SeedAsync(CohortTestHost host, params object[] rows)
    {
        await using var db = host.CreateDbContext();
        db.AddRange(rows);
        await db.SaveChangesAsync();
    }

    private static async Task<long> CountAsync(TemporaryDatabase database, string table)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"""SELECT count(*) FROM public."{table}" """, connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static async Task<SweepRunStatus> RunStatusAsync(TemporaryDatabase database, Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("""SELECT "Status" FROM public."sweep_run" WHERE "SweepId" = @sweepId""", connection);
        command.Parameters.AddWithValue("sweepId", sweepId);
        return (SweepRunStatus)(int)(await command.ExecuteScalarAsync())!;
    }

    private async Task<TemporaryDatabase> MigratedTemporaryDatabaseAsync()
    {
        var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var db = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>().UseNpgsql(database.ConnectionString).Options
        );
        await db.Database.MigrateAsync();
        return database;
    }

    // Makes a chosen statement, or the next commit after the step ran, fail as a transient
    // database failure would. Every other statement and commit passes through untouched.
    private sealed class Faults
    {
        private const string Deadlock =
            "DO $$ BEGIN RAISE EXCEPTION 'injected deadlock' USING ERRCODE = '40P01'; END $$";

        private string? pendingStatementPrefix;
        private int pendingStatementFailures;
        private string? statementPrefix;
        private int statementFailuresLeft;
        private int statementFailuresInjected;
        private bool commitArmedByStep;
        private int commitArmed;
        private int commitFailuresInjected;
        private int stepRuns;
        private string? terminateConnectionString;
        private int sessionsTerminated;
        private Guid runSweepId;

        public int SessionsTerminated => Volatile.Read(ref sessionsTerminated);

        public int StatementFailuresInjected => Volatile.Read(ref statementFailuresInjected);

        public int CommitFailuresInjected => Volatile.Read(ref commitFailuresInjected);

        public int StepRuns => Volatile.Read(ref stepRuns);

        // Every entity of the run writes the same kind of statement, so the failure waits for the
        // step: the first such statement after it ran is the step's batch's.
        public void FailStatementAfterTheNextStepRun(string textPrefix, int times)
        {
            pendingStatementPrefix = textPrefix;
            pendingStatementFailures = times;
        }

        // Fails the statements that start with the text from now on, for a test with no step.
        public void FailStatement(string textPrefix, int times)
        {
            statementPrefix = textPrefix;
            Volatile.Write(ref statementFailuresLeft, times);
        }

        public void FailTheCommitAfterTheNextStepRun() => commitArmedByStep = true;

        // Ends the session that holds the run's lock, as a failover or a timeout does, as the
        // batch whose step ran writes its progress: that statement then fails on the dead
        // session. Once only, so the strategy's retry runs on.
        public void TerminateTheRunSessionBeforeTheProgressStatementAfterTheStepRan(string connectionString) =>
            terminateConnectionString = connectionString;

        public string Rewrite(string text)
        {
            if (
                terminateConnectionString is not null
                && Volatile.Read(ref stepRuns) > 0
                && text.StartsWith(ProgressStatement, StringComparison.Ordinal)
                && Interlocked.Exchange(ref terminateConnectionString, null) is { } connectionString
            )
            {
                TerminateTheRunSession(connectionString);
            }

            if (
                statementPrefix is not null
                && text.StartsWith(statementPrefix, StringComparison.Ordinal)
                && Interlocked.Decrement(ref statementFailuresLeft) >= 0
            )
            {
                Interlocked.Increment(ref statementFailuresInjected);
                return Deadlock;
            }

            return text;
        }

        private void TerminateTheRunSession(string connectionString)
        {
            var key = RetentionRunAdvisoryLock.KeyFor(runSweepId);
            using var connection = new NpgsqlConnection($"{connectionString};Pooling=false");
            connection.Open();
            using var terminate = connection.CreateCommand();
            // A bigint advisory key is held as classid (high 32 bits) and objid (low 32 bits).
            terminate.CommandText = """
                SELECT pg_terminate_backend(pid, 5000) FROM pg_locks
                WHERE locktype = 'advisory' AND granted AND objsubid = 1
                  AND classid::bigint = (@key >> 32) & 4294967295 AND objid::bigint = @key & 4294967295
                """;
            terminate.Parameters.AddWithValue("key", key);
            if (terminate.ExecuteScalar() is true)
            {
                Interlocked.Increment(ref sessionsTerminated);
            }
        }

        public void BeforeCommit()
        {
            if (Interlocked.CompareExchange(ref commitArmed, 0, 1) == 1)
            {
                Interlocked.Increment(ref commitFailuresInjected);
                throw new NpgsqlException("injected commit failure", new IOException("connection reset"));
            }
        }

        public void StepRan(Guid sweepId)
        {
            runSweepId = sweepId;
            Interlocked.Increment(ref stepRuns);
            if (pendingStatementPrefix is not null)
            {
                statementPrefix = pendingStatementPrefix;
                Volatile.Write(ref statementFailuresLeft, pendingStatementFailures);
                pendingStatementPrefix = null;
            }

            if (commitArmedByStep)
            {
                commitArmedByStep = false;
                Volatile.Write(ref commitArmed, 1);
            }
        }
    }

    // The sample's outbox step, counting its runs: a retried batch runs it again.
    private sealed class CountingOutboxStep(SampleDbContext db, Faults faults) : IRetentionDependents<PerRowAuditedLog>
    {
        public async Task RetireAsync(PerRowAuditedLog row, RetentionDependentsContext ctx, CancellationToken ct)
        {
            faults.StepRan(ctx.SweepId);
            db.OutboxMessages.Add(
                new OutboxMessage
                {
                    Id = Guid.NewGuid(),
                    TenantId = ctx.TenantId,
                    Type = "log-retired",
                    Payload = row.Id.ToString(),
                    CreatedAt = ctx.At,
                }
            );
            await db.SaveChangesAsync(ct);
        }
    }

    // The counting step, then a statement of its own: after its save, the window a transient
    // failure of the step's own work lands in.
    private sealed class ProbingOutboxStep(SampleDbContext db, Faults faults) : IRetentionDependents<PerRowAuditedLog>
    {
        public async Task RetireAsync(PerRowAuditedLog row, RetentionDependentsContext ctx, CancellationToken ct)
        {
            faults.StepRan(ctx.SweepId);
            db.OutboxMessages.Add(
                new OutboxMessage
                {
                    Id = Guid.NewGuid(),
                    TenantId = ctx.TenantId,
                    Type = "log-retired",
                    Payload = row.Id.ToString(),
                    CreatedAt = ctx.At,
                }
            );
            await db.SaveChangesAsync(ct);
            await db.Database.ExecuteSqlRawAsync(StepProbeStatement, ct);
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

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        private readonly ConcurrentQueue<Exception> exceptions = new();

        public IReadOnlyList<Exception> Exceptions => exceptions.ToArray();

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(exceptions);

        public void Dispose() { }

        private sealed class CapturingLogger(ConcurrentQueue<Exception> exceptions) : ILogger
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
                if (exception is not null)
                {
                    exceptions.Enqueue(exception);
                }
            }
        }
    }
}
