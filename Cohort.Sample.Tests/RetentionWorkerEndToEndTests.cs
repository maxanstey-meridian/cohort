using System.Diagnostics;
using System.Threading.Channels;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Xunit.Sdk;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class RetentionWorkerEndToEndTests(PostgresFixture fixture) : IAsyncLifetime
{
    public async Task InitializeAsync()
    {
        await using var connection = new Npgsql.NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await fixture.Respawner.ResetAsync(connection);
    }

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }

    [Fact]
    public async Task Worker_Persists_Scheduled_DryRun_For_The_Tenanted_Entity_Scope_Without_Mutating_Rows()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: true,
            killSwitch: false
        );
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
            }
        );
        await SeedOldNoteAsync(tenant.Id, "scheduled-dry-run");

        await host.Host.StartAsync();
        await WaitUntilAsync(
            async () =>
            {
                var run = await LoadLatestRunAsync(tenant.Id);
                return run is { Status: SweepRunStatus.Succeeded }
                    && run.Value.EntityTypes.Contains(typeof(Note).FullName!);
            },
            TimeSpan.FromSeconds(8)
        );
        await host.Host.StopAsync();

        var run = await LoadLatestRunAsync(tenant.Id);
        run.Should().NotBeNull();
        run!.Value.Trigger.Should().Be(SweepTriggerKind.Scheduled);
        run.Value.DryRun.Should().BeTrue();
        run.Value.Status.Should().Be(SweepRunStatus.Succeeded);
        run.Value.EntityTypes.Should().Contain(typeof(Note).FullName!);
        run.Value.EntityTypes.Should().NotContain(typeof(TenantlessLog).FullName!);
        (await NoteExistsAsync("scheduled-dry-run")).Should().BeTrue();
    }

    [Fact]
    public async Task Worker_Isolates_A_Tenant_Failure_And_Still_Runs_Later_And_Tenantless_Passes()
    {
        var failingTenant = CreateTenant();
        var healthyTenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        using var host = BuildHost(
            settings,
            failingTenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider>(
                    new TenantFailingRuleProvider(failingTenant.Id)
                );
                services.AddSingleton<IRetentionTenantSource>(
                    new StaticTenantSource(failingTenant, healthyTenant)
                );
            }
        );
        await SeedOldNoteAsync(failingTenant.Id, "isolated-failing-tenant");
        await SeedOldNoteAsync(healthyTenant.Id, "isolated-healthy-tenant");
        await SeedOldTenantlessLogAsync("isolated-tenantless");

        await host.Host.StartAsync();
        await WaitUntilAsync(
            async () =>
                !await NoteExistsAsync("isolated-healthy-tenant")
                && !await TenantlessLogExistsAsync("isolated-tenantless"),
            TimeSpan.FromSeconds(8)
        );
        await host.Host.StopAsync();

        (await NoteExistsAsync("isolated-failing-tenant")).Should().BeTrue();
        (await NoteExistsAsync("isolated-healthy-tenant")).Should().BeFalse();
        (await TenantlessLogExistsAsync("isolated-tenantless")).Should().BeFalse();
    }

    [Fact]
    public async Task Worker_Skips_Occurrences_While_Another_Instance_Holds_The_Sweep_Lock()
    {
        const long sweepAdvisoryLockKey = 0x636F_686F_7274_3031;
        var skippedOccurrenceLog = new SkippedOccurrenceLogProvider();
        var tenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
                services.AddSingleton<ILoggerProvider>(skippedOccurrenceLog);
            }
        );
        await SeedOldNoteAsync(tenant.Id, "lock-guarded-note");

        await using var lockConnection = new NpgsqlConnection(fixture.ConnectionString);
        await lockConnection.OpenAsync();
        await using (var acquire = lockConnection.CreateCommand())
        {
            acquire.CommandText = "SELECT pg_advisory_lock(@key)";
            acquire.Parameters.AddWithValue("key", sweepAdvisoryLockKey);
            await acquire.ExecuteScalarAsync();
        }

        await host.Host.StartAsync();
        await skippedOccurrenceLog.WaitForOccurrencesAsync(2).WaitAsync(TimeSpan.FromSeconds(8));

        skippedOccurrenceLog.OccurrenceCount.Should().BeGreaterThanOrEqualTo(2);
        (await NoteExistsAsync("lock-guarded-note")).Should().BeTrue();

        await using (var release = lockConnection.CreateCommand())
        {
            release.CommandText = "SELECT pg_advisory_unlock(@key)";
            release.Parameters.AddWithValue("key", sweepAdvisoryLockKey);
            await release.ExecuteScalarAsync();
        }

        await WaitUntilAsync(
            async () => !await NoteExistsAsync("lock-guarded-note"),
            TimeSpan.FromSeconds(8)
        );

        await host.Host.StopAsync();

        (await NoteExistsAsync("lock-guarded-note")).Should().BeFalse();
    }

    [Fact]
    public async Task Two_Replicas_Firing_For_The_Same_Occurrence_Record_One_Scheduled_Run()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "0 0 0 1 1 *",
            dryRun: false,
            killSwitch: false
        );
        using var first = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
            }
        );
        using var second = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
            }
        );
        await SeedOldNoteAsync(tenant.Id, "once-per-occurrence");
        var occurrence = DateTimeOffset.UtcNow.AddSeconds(-1);

        await GetWorker(first).RunIterationAsync(occurrence, dryRun: false, CancellationToken.None);
        await GetWorker(second).RunIterationAsync(occurrence, dryRun: false, CancellationToken.None);

        (await CountScheduledRunsAsync(tenant.Id)).Should().Be(1);
        (await NoteExistsAsync("once-per-occurrence")).Should().BeFalse();

        var nextOccurrence = DateTimeOffset.UtcNow;
        await GetWorker(second).RunIterationAsync(nextOccurrence, dryRun: false, CancellationToken.None);

        (await CountScheduledRunsAsync(tenant.Id)).Should().Be(2);
    }

    [Fact]
    public async Task A_Dry_Run_Replica_Does_Not_Take_The_Occurrence_From_A_Real_Sweep()
    {
        // Mid rolling deploy one replica may still be configured to dry run.
        var tenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "0 0 0 1 1 *",
            dryRun: false,
            killSwitch: false
        );
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
            }
        );
        await SeedOldNoteAsync(tenant.Id, "dry-run-replica");
        var occurrence = DateTimeOffset.UtcNow.AddSeconds(-1);

        await GetWorker(host).RunIterationAsync(occurrence, dryRun: true, CancellationToken.None);
        (await NoteExistsAsync("dry-run-replica")).Should().BeTrue();

        await GetWorker(host).RunIterationAsync(occurrence, dryRun: false, CancellationToken.None);

        (await NoteExistsAsync("dry-run-replica")).Should().BeFalse();
        (await CountScheduledRunsAsync(tenant.Id)).Should().Be(2);
    }

    [Fact]
    public async Task Worker_Survives_A_Failing_Iteration_And_Sweeps_On_A_Later_Tick()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        var categoryRepository = new FailingOnceCategoryRepository(
            new SampleRetentionRuleProvider()
        );
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider>(categoryRepository);
            }
        );
        await SeedOldNoteAsync(tenant.Id, "resilient-delete");

        await host.Host.StartAsync();
        await categoryRepository.FailedIteration.WaitAsync(TimeSpan.FromSeconds(8));
        await categoryRepository.LaterSuccessfulIteration.WaitAsync(TimeSpan.FromSeconds(8));
        await WaitUntilAsync(
            async () => !await NoteExistsAsync("resilient-delete"),
            TimeSpan.FromSeconds(8)
        );

        await host.Host.StopAsync();

        (await NoteExistsAsync("resilient-delete")).Should().BeFalse();
    }

    private WorkerTestHost BuildHost(
        IReadOnlyDictionary<string, string?> settings,
        TenantContext tenant,
        Action<IServiceCollection> configureServices
    )
    {
        var connectionString = settings[$"{CohortOptions.SectionName}:ConnectionString"]!;
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(settings);

        builder.Services.AddDbContext<SampleDbContext>(options => options.UseNpgsql(connectionString));
        builder.Services.AddSingleton(tenant);
        builder.Services.AddSingleton<GuidTombstoneFactory>();
        builder.Services.AddSingleton<OriginalValueTombstoneFactory>();
        builder.Services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<GuidTombstoneFactory>()
        );
        builder.Services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<OriginalValueTombstoneFactory>()
        );
        builder.Services.AddCohort<SampleDbContext>();
        configureServices(builder.Services);

        return new WorkerTestHost(builder.Build(), builder.Configuration);
    }

    private static RetentionWorker GetWorker(WorkerTestHost host) =>
        host.Host.Services.GetServices<IHostedService>().OfType<RetentionWorker>().Single();

    private async Task<long> CountScheduledRunsAsync(Guid tenantId)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*) FROM "sweep_run"
            WHERE "TenantId" = @tenantId AND "TriggerKind" = @scheduled
            """;
        command.Parameters.AddWithValue("tenantId", tenantId);
        command.Parameters.AddWithValue("scheduled", (int)SweepTriggerKind.Scheduled);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static IReadOnlyDictionary<string, string?> CreateSettings(
        string connectionString,
        string? schedule,
        bool dryRun,
        bool killSwitch
    )
    {
        return new Dictionary<string, string?>
        {
            [$"{CohortOptions.SectionName}:ConnectionString"] = connectionString,
            [$"{CohortOptions.SectionName}:Schedule"] = schedule,
            [$"{CohortOptions.SectionName}:DryRun"] = dryRun.ToString(),
            [$"{CohortOptions.SectionName}:KillSwitch"] = killSwitch.ToString(),
        };
    }

    private TenantContext CreateTenant()
    {
        return new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
    }

    private async Task SeedOldNoteAsync(Guid tenantId, string body)
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(fixture.ConnectionString)
            .Options;

        await using var db = new SampleDbContext(options);
        db.Notes.Add(
            new Note
            {
                Id = Guid.NewGuid(),
                TenantId = tenantId,
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-120),
                Body = body,
            }
        );
        await db.SaveChangesAsync();
    }

    private async Task<bool> NoteExistsAsync(string body)
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(fixture.ConnectionString)
            .Options;

        await using var db = new SampleDbContext(options);
        return await db.Notes.AnyAsync(note => note.Body == body);
    }

    private async Task SeedOldTenantlessLogAsync(string payload)
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(fixture.ConnectionString)
            .Options;

        await using var db = new SampleDbContext(options);
        db.TenantlessLogs.Add(
            new TenantlessLog
            {
                Id = Guid.NewGuid(),
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-120),
                Payload = payload,
            }
        );
        await db.SaveChangesAsync();
    }

    private async Task<bool> TenantlessLogExistsAsync(string payload)
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(fixture.ConnectionString)
            .Options;

        await using var db = new SampleDbContext(options);
        return await db.TenantlessLogs.AnyAsync(log => log.Payload == payload);
    }

    private async Task<(
        SweepTriggerKind Trigger,
        bool DryRun,
        SweepRunStatus Status,
        IReadOnlyList<string> EntityTypes
    )?> LoadLatestRunAsync(Guid tenantId)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT run."TriggerKind", run."DryRun", run."Status", summary."EntityType"
            FROM "sweep_run" run
            LEFT JOIN "sweep_run_entity_summary" summary ON summary."SweepId" = run."SweepId"
            WHERE run."SweepId" = (
                SELECT "SweepId"
                FROM "sweep_run"
                WHERE "TenantId" = @tenantId
                ORDER BY "StartedAt" DESC
                LIMIT 1
            )
            ORDER BY summary."EntityType"
            """;
        command.Parameters.AddWithValue("tenantId", tenantId);

        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return null;
        }

        var trigger = (SweepTriggerKind)reader.GetInt32(0);
        var dryRun = reader.GetBoolean(1);
        var status = (SweepRunStatus)reader.GetInt32(2);
        var entityTypes = new List<string>();
        do
        {
            if (!reader.IsDBNull(3))
            {
                entityTypes.Add(reader.GetString(3));
            }
        } while (await reader.ReadAsync());

        return (trigger, dryRun, status, entityTypes);
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> predicate, TimeSpan timeout)
    {
        var stopwatch = Stopwatch.StartNew();

        while (stopwatch.Elapsed < timeout)
        {
            if (await predicate())
            {
                return;
            }

            await Task.Delay(100);
        }

        throw new XunitException("Condition was not met within the allotted timeout.");
    }

    private sealed class CountingCategoryRepository(IRetentionRuleProvider inner)
        : IRetentionRuleProvider
    {
        public int GetAsyncCount => getAsyncCount;

        private int getAsyncCount;

        public RetentionCategoryCapabilities? GetCapabilities(string category)
        {
            Interlocked.Increment(ref getAsyncCount);
            return inner.GetCapabilities(category);
        }

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        ) => inner.ResolveAsync(context, ct);
    }

    private sealed class StaticTenantSource(params TenantContext[] tenants) : IRetentionTenantSource
    {
        public Task<IReadOnlyList<TenantContext>> GetTenantsAsync(CancellationToken ct)
        {
            return Task.FromResult<IReadOnlyList<TenantContext>>(tenants);
        }
    }

    private sealed class FailingOnceCategoryRepository(IRetentionRuleProvider inner)
        : IRetentionRuleProvider
    {
        private readonly TaskCompletionSource failedIteration = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private readonly TaskCompletionSource laterSuccessfulIteration = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private int failureInjected;

        public Task FailedIteration => failedIteration.Task;

        public Task LaterSuccessfulIteration => laterSuccessfulIteration.Task;

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            inner.GetCapabilities(category);

        public async Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            if (Interlocked.CompareExchange(ref failureInjected, 1, 0) == 0)
            {
                failedIteration.TrySetResult();
                throw new InvalidOperationException(
                    "Simulated transient category resolver failure."
                );
            }

            var rule = await inner.ResolveAsync(context, ct);
            laterSuccessfulIteration.TrySetResult();
            return rule;
        }
    }

    private sealed class TenantFailingRuleProvider(Guid failingTenantId)
        : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            inner.GetCapabilities(category);

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            if (context.Tenant.Id == failingTenantId)
            {
                throw new InvalidOperationException("Simulated tenant-specific policy failure.");
            }

            return inner.ResolveAsync(context, ct);
        }
    }

    private sealed class WorkerTestHost(IHost host, IConfigurationRoot configuration) : IDisposable
    {
        private readonly TaskCompletionSource reloaded = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        public IHost Host => host;

        public Task Reloaded => reloaded.Task;

        public void Reload(bool killSwitch, bool dryRun, string? schedule = null)
        {
            configuration[$"{CohortOptions.SectionName}:KillSwitch"] = killSwitch.ToString();
            configuration[$"{CohortOptions.SectionName}:DryRun"] = dryRun.ToString();
            if (schedule is not null)
            {
                configuration[$"{CohortOptions.SectionName}:Schedule"] = schedule;
            }
            configuration.Reload();
            reloaded.TrySetResult();
        }

        public void Dispose()
        {
            host.Dispose();
        }
    }

    private sealed class SkippedOccurrenceLogProvider : ILoggerProvider
    {
        private const string WorkerCategory = "Cohort.Hosting.RetentionWorker";
        private const string SkipMessage =
            "Cohort worker skipped this occurrence: another instance holds the sweep advisory lock.";
        private readonly Channel<int> occurrences = Channel.CreateUnbounded<int>();
        private int occurrenceCount;

        public int OccurrenceCount => Volatile.Read(ref occurrenceCount);

        public ILogger CreateLogger(string categoryName)
        {
            return new SkippedOccurrenceLogger(this, categoryName);
        }

        public async Task WaitForOccurrencesAsync(int expectedCount)
        {
            while (OccurrenceCount < expectedCount)
            {
                await occurrences.Reader.ReadAsync();
            }
        }

        public void Dispose() { }

        private void Record(LogLevel level, string categoryName, string message)
        {
            if (
                level != LogLevel.Information
                || categoryName != WorkerCategory
                || message != SkipMessage
            )
            {
                return;
            }

            var count = Interlocked.Increment(ref occurrenceCount);
            occurrences.Writer.TryWrite(count);
        }

        private sealed class SkippedOccurrenceLogger(
            SkippedOccurrenceLogProvider provider,
            string categoryName
        ) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull
            {
                return null;
            }

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter
            )
            {
                provider.Record(logLevel, categoryName, formatter(state, exception));
            }
        }
    }

    private sealed class TemporaryDatabase(string connectionString, string databaseName)
        : IAsyncDisposable
    {
        public string ConnectionString => connectionString;

        public static async Task<TemporaryDatabase> CreateAsync(string baseConnectionString)
        {
            var databaseName = $"cohort_worker_{Guid.NewGuid():N}";
            var adminConnectionString = CreateAdminConnectionString(baseConnectionString);

            await using var connection = new NpgsqlConnection(adminConnectionString);
            await connection.OpenAsync();

            await using (var command = connection.CreateCommand())
            {
                command.CommandText = $"CREATE DATABASE \"{databaseName}\"";
                await command.ExecuteNonQueryAsync();
            }

            var builder = new NpgsqlConnectionStringBuilder(baseConnectionString)
            {
                Database = databaseName,
            };

            return new TemporaryDatabase(builder.ConnectionString, databaseName);
        }

        public async ValueTask DisposeAsync()
        {
            var adminConnectionString = CreateAdminConnectionString(connectionString);

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

        private static string CreateAdminConnectionString(string originalConnectionString)
        {
            var builder = new NpgsqlConnectionStringBuilder(originalConnectionString)
            {
                Database = "postgres",
            };

            return builder.ConnectionString;
        }
    }
}
