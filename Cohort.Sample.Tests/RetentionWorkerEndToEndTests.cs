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
using Microsoft.Extensions.Options;

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
    public void AddCohort_Allows_Host_Overrides_For_Category_Audit_And_Holds_Repositories()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: null,
            dryRun: false,
            killSwitch: false
        );
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionCategoryRepository, CustomCategoryRepository>();
                services.AddScoped<IRetentionAuditWriter, CustomAuditWriter>();
                services.AddScoped<IRetentionHoldsRepository, CustomHoldsRepository>();
            }
        );

        using var scope = host.Host.Services.CreateScope();

        scope.ServiceProvider.GetRequiredService<IRetentionCategoryRepository>()
            .Should()
            .BeOfType<CustomCategoryRepository>();
        scope.ServiceProvider.GetRequiredService<IRetentionAuditWriter>()
            .Should()
            .BeOfType<CustomAuditWriter>();
        scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
            .Should()
            .BeOfType<CustomHoldsRepository>();
    }

    [Fact]
    public async Task AddCohort_Validates_Invalid_Cron_At_Startup()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "not-a-cron",
            dryRun: false,
            killSwitch: false
        );
        using var host = BuildHost(settings, tenant, services =>
        {
            services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
        });

        var act = async () => await host.Host.StartAsync();

        await act
            .Should()
            .ThrowAsync<OptionsValidationException>()
            .WithMessage("*schedule*invalid*");
    }

    [Fact]
    public async Task AddCohort_Rejects_Applying_Migrations_During_DryRun_At_Startup()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: true,
            killSwitch: false,
            applyMigrations: true
        );
        using var host = BuildHost(settings, tenant, services =>
        {
            services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
        });

        var act = async () => await host.Host.StartAsync();

        await act
            .Should()
            .ThrowAsync<OptionsValidationException>()
            .WithMessage("*apply migrations*DryRun*");
    }

    [Fact]
    public async Task AddCohort_Rejects_Applying_Migrations_When_KillSwitch_Is_Enabled_At_Startup()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: true,
            applyMigrations: true
        );
        using var host = BuildHost(settings, tenant, services =>
        {
            services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
        });

        var act = async () => await host.Host.StartAsync();

        await act
            .Should()
            .ThrowAsync<OptionsValidationException>()
            .WithMessage("*apply migrations*KillSwitch*");
    }

    [Fact]
    public async Task Worker_Runs_On_Schedule_And_Deletes_Eligible_Rows()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        var categoryRepository = new CountingCategoryRepository(new SampleCategoryRepository());
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionCategoryRepository>(categoryRepository);
            }
        );
        await SeedOldNoteAsync(tenant.Id, "scheduled-delete");

        await host.Host.StartAsync();
        await WaitUntilAsync(
            async () => categoryRepository.GetAsyncCount > 0 && !await NoteExistsAsync("scheduled-delete"),
            TimeSpan.FromSeconds(8)
        );

        await host.Host.StopAsync();

        (await NoteExistsAsync("scheduled-delete")).Should().BeFalse();
    }

    [Fact]
    public async Task Worker_Uses_Preview_When_DryRun_Is_Enabled_And_Leaves_Rows_Untouched()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: true,
            killSwitch: false
        );
        var categoryRepository = new CountingCategoryRepository(new SampleCategoryRepository());
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionCategoryRepository>(categoryRepository);
            }
        );
        await SeedOldNoteAsync(tenant.Id, "dry-run-note");

        await host.Host.StartAsync();
        await WaitUntilAsync(
            () => Task.FromResult(categoryRepository.GetAsyncCount > 0),
            TimeSpan.FromSeconds(8)
        );

        await host.Host.StopAsync();

        (await NoteExistsAsync("dry-run-note")).Should().BeTrue();
    }

    [Fact]
    public async Task Worker_Pauses_When_KillSwitch_Is_On_And_Resumes_On_The_Next_Iteration_Boundary()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: true
        );
        var categoryRepository = new CountingCategoryRepository(new SampleCategoryRepository());
        var optionsMonitor = new MutableCohortOptionsMonitor(
            new CohortOptions
            {
                Schedule = "*/1 * * * * *",
                DryRun = false,
                KillSwitch = true,
                ApplyMigrations = false,
            }
        );
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionCategoryRepository>(categoryRepository);
                services.AddSingleton<IOptionsMonitor<CohortOptions>>(optionsMonitor);
            }
        );
        await SeedOldNoteAsync(tenant.Id, "paused-note");

        await host.Host.StartAsync();
        await Task.Delay(TimeSpan.FromSeconds(2));

        categoryRepository.GetAsyncCount.Should().Be(0);
        (await NoteExistsAsync("paused-note")).Should().BeTrue();

        optionsMonitor.Update(
            new CohortOptions
            {
                Schedule = "*/1 * * * * *",
                DryRun = false,
                KillSwitch = false,
                ApplyMigrations = false,
            }
        );

        await WaitUntilAsync(
            async () => categoryRepository.GetAsyncCount > 0 && !await NoteExistsAsync("paused-note"),
            TimeSpan.FromSeconds(8)
        );

        await host.Host.StopAsync();

        (await NoteExistsAsync("paused-note")).Should().BeFalse();
    }

    [Fact]
    public async Task Worker_Finishes_The_Current_Sweep_Before_The_KillSwitch_Pauses_Later_Iterations()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        var optionsMonitor = new MutableCohortOptionsMonitor(
            new CohortOptions
            {
                Schedule = "*/1 * * * * *",
                DryRun = false,
                KillSwitch = false,
                ApplyMigrations = false,
            }
        );
        var blockingWriterState = new BlockingAuditWriterState();
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
                services.AddSingleton<IOptionsMonitor<CohortOptions>>(optionsMonitor);
                services.AddSingleton(blockingWriterState);
                services.AddScoped<IRetentionAuditWriter>(sp =>
                    new BlockingAuditWriter(sp.GetRequiredService<BlockingAuditWriterState>())
                );
            }
        );
        await SeedOldNoteAsync(tenant.Id, "in-flight-note");

        await host.Host.StartAsync();
        await blockingWriterState.CompletedReached.Task.WaitAsync(TimeSpan.FromSeconds(8));

        optionsMonitor.Update(
            new CohortOptions
            {
                Schedule = "*/1 * * * * *",
                DryRun = false,
                KillSwitch = true,
                ApplyMigrations = false,
            }
        );
        blockingWriterState.ReleaseCurrentIteration();

        await WaitUntilAsync(
            async () => !await NoteExistsAsync("in-flight-note"),
            TimeSpan.FromSeconds(8)
        );
        await Task.Delay(TimeSpan.FromSeconds(2));

        await host.Host.StopAsync();

        (await NoteExistsAsync("in-flight-note")).Should().BeFalse();
        blockingWriterState.StartedCount.Should().Be(1);
    }

    [Fact]
    public async Task Worker_Applies_Migrations_At_Startup_When_Configured()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenant = CreateTenant();
        var settings = CreateSettings(
            database.ConnectionString,
            schedule: null,
            dryRun: false,
            killSwitch: false,
            applyMigrations: true
        );
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
            }
        );

        await host.Host.StartAsync();
        await WaitUntilAsync(
            () => TableExistsAsync(database.ConnectionString, "notes"),
            TimeSpan.FromSeconds(8)
        );
        await host.Host.StopAsync();

        (await TableExistsAsync(database.ConnectionString, "notes")).Should().BeTrue();
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
        builder.Services.AddSingleton<IAnonymiseValueFactory>(sp => sp.GetRequiredService<GuidTombstoneFactory>());
        builder.Services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<OriginalValueTombstoneFactory>()
        );
        builder.Services.AddCohort<SampleDbContext>();
        configureServices(builder.Services);

        return new WorkerTestHost(builder.Build());
    }

    private static IReadOnlyDictionary<string, string?> CreateSettings(
        string connectionString,
        string? schedule,
        bool dryRun,
        bool killSwitch,
        bool applyMigrations = false
    )
    {
        return new Dictionary<string, string?>
        {
            [$"{CohortOptions.SectionName}:ConnectionString"] = connectionString,
            [$"{CohortOptions.SectionName}:Schedule"] = schedule,
            [$"{CohortOptions.SectionName}:DryRun"] = dryRun.ToString(),
            [$"{CohortOptions.SectionName}:KillSwitch"] = killSwitch.ToString(),
            [$"{CohortOptions.SectionName}:ApplyMigrations"] = applyMigrations.ToString(),
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

    private static async Task<bool> TableExistsAsync(string connectionString, string tableName)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = @tableName
            )
            """;
        command.Parameters.AddWithValue("tableName", tableName);

        return (bool)(await command.ExecuteScalarAsync())!;
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

    private sealed class CountingCategoryRepository(IRetentionCategoryRepository inner)
        : IRetentionCategoryRepository
    {
        public int GetAsyncCount => getAsyncCount;

        private int getAsyncCount;

        public async Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            Interlocked.Increment(ref getAsyncCount);
            return await inner.GetAsync(category, ct);
        }
    }

    private sealed class CustomCategoryRepository : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            return Task.FromResult<IRetentionRuleResolver?>(null);
        }
    }

    private sealed class CustomAuditWriter : IRetentionAuditWriter
    {
        public Task WriteAsync(SweepEvent evt, CancellationToken ct)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class BlockingAuditWriter(BlockingAuditWriterState state) : IRetentionAuditWriter
    {
        public async Task WriteAsync(SweepEvent evt, CancellationToken ct)
        {
            if (evt is SweepEvent.Started)
            {
                state.RecordStarted();
                return;
            }

            if (evt is SweepEvent.Completed && state.TryBlockCurrentIteration())
            {
                state.CompletedReached.TrySetResult(true);
                await state.WaitForReleaseAsync(ct);
            }
        }
    }

    private sealed class CustomHoldsRepository : IRetentionHoldsRepository
    {
        public Task CreateAsync(RetentionHoldRequest request, CancellationToken ct) => Task.CompletedTask;

        public Task RemoveAsync(Guid holdId, DateTimeOffset removedAt, CancellationToken ct) => Task.CompletedTask;

        public Task<IReadOnlyList<RetentionHold>> ListActiveAsync(DateTimeOffset asOf, CancellationToken ct)
        {
            return Task.FromResult<IReadOnlyList<RetentionHold>>([]);
        }

        public Task<bool> HasActiveHoldAsync(
            string tableName,
            string recordId,
            Guid tenantId,
            DateTimeOffset asOf,
            CancellationToken ct
        )
        {
            return Task.FromResult(false);
        }
    }

    private sealed class WorkerTestHost(IHost host) : IDisposable
    {
        public IHost Host => host;

        public void Dispose()
        {
            host.Dispose();
        }
    }

    private sealed class MutableCohortOptionsMonitor(CohortOptions currentValue)
        : IOptionsMonitor<CohortOptions>
    {
        private CohortOptions options = currentValue;

        public CohortOptions CurrentValue => options;

        public CohortOptions Get(string? name) => options;

        public IDisposable? OnChange(Action<CohortOptions, string?> listener)
        {
            return null;
        }

        public void Update(CohortOptions next)
        {
            options = next;
        }
    }

    private sealed class BlockingAuditWriterState
    {
        private readonly Channel<bool> releaseChannel = Channel.CreateBounded<bool>(1);
        private int startedCount;
        private int blocked;

        public TaskCompletionSource<bool> CompletedReached { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int StartedCount => startedCount;

        public void RecordStarted()
        {
            Interlocked.Increment(ref startedCount);
        }

        public bool TryBlockCurrentIteration()
        {
            return Interlocked.CompareExchange(ref blocked, 1, 0) == 0;
        }

        public void ReleaseCurrentIteration()
        {
            releaseChannel.Writer.TryWrite(true);
        }

        public async Task WaitForReleaseAsync(CancellationToken ct)
        {
            await releaseChannel.Reader.ReadAsync(ct);
        }
    }

    private sealed class TemporaryDatabase(string connectionString, string databaseName) : IAsyncDisposable
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
                terminate.CommandText =
                    $"""
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
