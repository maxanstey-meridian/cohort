using System.Collections.Concurrent;
using Cohort.Domain;
using Cohort.Application;
using Cohort.Hosting;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Cohort.Sample.Tests.ComplianceCorpus;

public sealed class FailureSafetyCorpusTests(PostgresFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Erasure_failure_diagnostics_exclude_subject_data_and_correlate_to_logs()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var personalDataSentinel = $"ACCOUNT-005 Subject-{subjectId:N}-Carol.Example@example.org";
        var functionName = $"corpus_fail_erasure_{noteId:N}";
        var triggerName = $"corpus_fail_erasure_{noteId:N}";
        var observer = new RecordingObserver();
        var logs = new RecordingLogProvider();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = now.AddDays(-60),
                    Body = "erasure diagnostic corpus",
                }
            );
            await db.SaveChangesAsync();
        }

        await ExecuteAsync($"""
            CREATE FUNCTION "{functionName}"() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF OLD."Id" = '{noteId}'::uuid THEN
                    RAISE EXCEPTION '{personalDataSentinel}';
                END IF;
                RETURN OLD;
            END $$;
            CREATE TRIGGER "{triggerName}"
            BEFORE DELETE ON "notes"
            FOR EACH ROW EXECUTE FUNCTION "{functionName}"();
            """);

        try
        {
            using var host = new CohortTestHost(
                ConnectionString,
                configureServices: services =>
                {
                    services.AddSingleton<IRetentionAuditObserver>(observer);
                    services.AddSingleton<ILoggerProvider>(logs);
                }
            );
            var result = await host.RunErasureAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
                now
            );

            var resultDiagnostic = result.EntityFailures.Should().ContainSingle().Which;
            AssertSafeDiagnostic(resultDiagnostic, "Npgsql.PostgresException", "sqlstate:P0001");
            resultDiagnostic.Should().NotContain(personalDataSentinel);
            observer
                .Events.OfType<SweepEvent.PartiallyFailed>()
                .Should()
                .ContainSingle()
                .Which.Error.Should()
                .Be(resultDiagnostic);

            await using (var connection = new NpgsqlConnection(ConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = "SELECT \"Error\" FROM \"sweep_run\" WHERE \"SweepId\" = @sweepId";
                command.Parameters.AddWithValue("sweepId", result.SweepId);
                ((string)(await command.ExecuteScalarAsync())!).Should().Be(resultDiagnostic);
            }

            var diagnosticId = resultDiagnostic.Split("diagnosticId=")[1];
            AssertSingleCorrelatedExceptionLog(logs.Entries, personalDataSentinel, diagnosticId);
        }
        finally
        {
            await ExecuteAsync(
                $"DROP TRIGGER IF EXISTS \"{triggerName}\" ON \"notes\"; DROP FUNCTION IF EXISTS \"{functionName}\"();"
            );
        }
    }

    [Fact]
    public async Task Handler_failure_diagnostics_exclude_personal_data_and_correlate_to_logs()
    {
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var personalDataSentinel = $"ACCOUNT-005 Bob.Example+{Guid.NewGuid():N}@example.org";
        var logs = new RecordingLogProvider();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = now.AddDays(-60),
                    Body = "handler diagnostic corpus",
                }
            );
            await db.SaveChangesAsync();
        }

        using var host = new CohortTestHost(
            ConnectionString,
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton<IRetentionHandler<Note>>(
                    new PersonalDataFailingHandler(personalDataSentinel)
                );
                services.AddSingleton<ILoggerProvider>(logs);
            }
        );
        var result = await host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now
        );
        await host.RunWithServicesAsync(async services =>
            await services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );

        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT status."LastError"
            FROM "sweep_row_handler_status" AS status
            INNER JOIN "sweep_run_row_detail" AS detail
                ON detail."Id" = status."SweepRunRowDetailId"
            WHERE detail."SweepId" = @sweepId
            """;
        command.Parameters.AddWithValue("sweepId", result.SweepId);
        var persistedError = (string)(await command.ExecuteScalarAsync())!;

        AssertSafeDiagnostic(
            persistedError,
            "System.InvalidOperationException",
            "hresult:0x80131509"
        );
        persistedError.Should().NotContain(personalDataSentinel);
        var diagnosticId = persistedError.Split("diagnosticId=")[1];
        AssertSingleCorrelatedExceptionLog(logs.Entries, personalDataSentinel, diagnosticId);
    }

    [Fact]
    public async Task An_entity_failure_preserves_committed_work_and_reports_only_committed_totals()
    {
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var contactId = Guid.NewGuid();
        var personalDataSentinel = $"ACCOUNT-005 Alice.Example+{Guid.NewGuid():N}@example.org";
        var functionName = $"corpus_fail_anonymise_{contactId:N}";
        var triggerName = $"corpus_fail_anonymise_{contactId:N}";
        var observer = new RecordingObserver();
        var logs = new RecordingLogProvider();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(new Note { Id = noteId, TenantId = tenantId, CreatedAt = now.AddDays(-60), Body = "commits" });
            db.AnonymisedContacts.Add(new AnonymisedContact
            {
                Id = contactId,
                TenantId = tenantId,
                CreatedAt = now.AddDays(-60),
                EmailAddress = "failure@example.org",
                GivenName = "Failure",
                Surname = "Target",
                Notes = "rolls back",
            });
            await db.SaveChangesAsync();
        }

        await ExecuteAsync($"""
            CREATE FUNCTION "{functionName}"() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW."Id" = '{contactId}'::uuid THEN
                    RAISE EXCEPTION '{personalDataSentinel}';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER "{triggerName}"
            BEFORE UPDATE ON "anonymised_contacts"
            FOR EACH ROW EXECUTE FUNCTION "{functionName}"();
            """);

        try
        {
            using var host = new CohortTestHost(
                ConnectionString,
                configureServices: services =>
                {
                    services.AddSingleton<IRetentionAuditObserver>(observer);
                    services.AddSingleton<ILoggerProvider>(logs);
                }
            );
            var result = await host.RunSweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                now
            );

            var resultDiagnostic = result.EntityFailures.Should().ContainSingle().Which;
            AssertSafeDiagnostic(resultDiagnostic, "Npgsql.PostgresException", "sqlstate:P0001");
            resultDiagnostic.Should().NotContain(personalDataSentinel);
            result.Counts.Should().Contain(count =>
                count.EntityType == typeof(Note) && count.Affected == 1
            );
            result.Counts.Should().NotContain(count => count.EntityType == typeof(AnonymisedContact));
            result.Counts.Sum(count => count.Affected).Should().Be(1);

            var terminal = observer.Events.OfType<SweepEvent.PartiallyFailed>().Should().ContainSingle().Which;
            terminal.Error.Should().Be(resultDiagnostic);
            terminal.Error.Should().NotContain(personalDataSentinel);
            terminal.TotalAffected.Should().Be(1);

            await using (var connection = new NpgsqlConnection(ConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = """
                    SELECT run."Status", run."TotalAffected", run."Error",
                           (SELECT COUNT(*) FROM "sweep_run_entity_summary" summary
                            WHERE summary."SweepId" = run."SweepId"
                              AND summary."EntityType" = @successfulEntity
                              AND summary."Affected" = 1),
                           (SELECT COALESCE(SUM(summary."Affected"), 0) FROM "sweep_run_entity_summary" summary
                            WHERE summary."SweepId" = run."SweepId"),
                           (SELECT COUNT(*) FROM "sweep_run_entity_summary" summary
                            WHERE summary."SweepId" = run."SweepId"
                              AND summary."EntityType" = @failedEntity),
                           (SELECT COUNT(*) FROM "sweep_run_row_detail" detail
                            WHERE detail."SweepId" = run."SweepId")
                    FROM "sweep_run" run
                    WHERE run."SweepId" = @sweepId
                    """;
                command.Parameters.AddWithValue("sweepId", result.SweepId);
                command.Parameters.AddWithValue("successfulEntity", typeof(Note).FullName!);
                command.Parameters.AddWithValue("failedEntity", typeof(AnonymisedContact).FullName!);
                await using var reader = await command.ExecuteReaderAsync();
                (await reader.ReadAsync()).Should().BeTrue();
                reader.GetInt32(0).Should().Be((int)SweepRunStatus.PartiallyFailed);
                reader.GetInt64(1).Should().Be(1);
                var persistedError = reader.GetString(2);
                persistedError.Should().Be(resultDiagnostic);
                persistedError.Should().NotContain(personalDataSentinel);
                reader.GetInt64(3).Should().Be(1);
                reader.GetInt64(4).Should().Be(1);
                reader.GetInt64(5).Should().Be(0);
                reader.GetInt64(6).Should().Be(0);
            }

            var diagnosticId = resultDiagnostic.Split("diagnosticId=")[1];
            AssertSingleCorrelatedExceptionLog(logs.Entries, personalDataSentinel, diagnosticId);

            await using var verify = Host.CreateDbContext();
            (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeFalse();
            var contact = await verify.AnonymisedContacts.SingleAsync(row => row.Id == contactId);
            contact.Id.Should().Be(contactId);
            contact.TenantId.Should().Be(tenantId);
            contact.SubjectId.Should().BeNull();
            contact.CreatedAt.Should().Be(now.AddDays(-60));
            contact.EmailAddress.Should().Be("failure@example.org");
            contact.GivenName.Should().Be("Failure");
            contact.Surname.Should().Be("Target");
            contact.Notes.Should().Be("rolls back");
            contact.AnonymisedAt.Should().BeNull();
        }
        finally
        {
            await ExecuteAsync($"DROP TRIGGER IF EXISTS \"{triggerName}\" ON \"anonymised_contacts\"; DROP FUNCTION IF EXISTS \"{functionName}\"();");
        }
    }

    private static void AssertSafeDiagnostic(string value, string type, string code)
    {
        value.Should().MatchRegex(
            $"^type={System.Text.RegularExpressions.Regex.Escape(type)};code={code};diagnosticId=[0-9a-f]{{32}}$"
        );
    }

    private static void AssertSingleCorrelatedExceptionLog(
        IReadOnlyList<LogEntry> entries,
        string sentinel,
        string diagnosticId
    )
    {
        var occurrences = entries.Where(entry =>
            entry.RenderedMessage.Contains(sentinel, StringComparison.Ordinal)
            || entry.Exception?.ToString().Contains(sentinel, StringComparison.Ordinal) == true
            || entry.Properties.Values.Any(value =>
                value?.ToString()?.Contains(sentinel, StringComparison.Ordinal) == true
            )
        ).ToArray();

        var occurrence = occurrences.Should().ContainSingle().Which;
        occurrence.Exception.Should().NotBeNull();
        occurrence.Exception!.ToString().Should().Contain(sentinel);
        occurrence.Properties.Should().ContainKey("DiagnosticId")
            .WhoseValue.Should().Be(diagnosticId);
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
                var properties = state is IEnumerable<KeyValuePair<string, object?>> values
                    ? values.ToDictionary(pair => pair.Key, pair => pair.Value)
                    : new Dictionary<string, object?>();
                entries.Enqueue(new LogEntry(formatter(state, exception), exception, properties));
            }
        }
    }

    private sealed record LogEntry(
        string RenderedMessage,
        Exception? Exception,
        IReadOnlyDictionary<string, object?> Properties
    );

    private sealed class PersonalDataFailingHandler(string personalDataSentinel)
        : IRetentionHandler<Note>
    {
        public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct) =>
            throw new InvalidOperationException(personalDataSentinel);
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }
}
