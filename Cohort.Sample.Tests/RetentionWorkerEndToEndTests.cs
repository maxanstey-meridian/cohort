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

        scope
            .ServiceProvider.GetRequiredService<IRetentionCategoryRepository>()
            .Should()
            .BeOfType<CustomCategoryRepository>();
        scope
            .ServiceProvider.GetRequiredService<IRetentionAuditWriter>()
            .Should()
            .BeOfType<CustomAuditWriter>();
        scope
            .ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
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
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
            }
        );

        var act = async () => await host.Host.StartAsync();

        await act.Should()
            .ThrowAsync<OptionsValidationException>()
            .WithMessage("*schedule*invalid*");
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
            async () =>
                categoryRepository.GetAsyncCount > 0 && !await NoteExistsAsync("scheduled-delete"),
            TimeSpan.FromSeconds(8)
        );

        await host.Host.StopAsync();

        (await NoteExistsAsync("scheduled-delete")).Should().BeFalse();
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
                services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
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
    public async Task Worker_Reloads_Schedule_And_Persists_The_New_DryRun_Without_Mutating_Rows()
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
                services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
            }
        );
        await SeedOldNoteAsync(tenant.Id, "reloaded-dry-run");

        await host.Host.StartAsync();
        host.Reload(killSwitch: false, dryRun: true, schedule: "*/1 * * * * *");
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
        (await NoteExistsAsync("reloaded-dry-run")).Should().BeTrue();
    }

    [Fact]
    public async Task Worker_Sweeps_Every_Tenant_Returned_By_The_Tenant_Source()
    {
        var tenantA = CreateTenant();
        var tenantB = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        using var host = BuildHost(
            settings,
            tenantA,
            services =>
            {
                services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
                services.AddSingleton<IRetentionTenantSource>(
                    new StaticTenantSource(tenantA, tenantB)
                );
            }
        );
        await SeedOldNoteAsync(tenantA.Id, "multi-tenant-a");
        await SeedOldNoteAsync(tenantB.Id, "multi-tenant-b");

        await host.Host.StartAsync();
        await WaitUntilAsync(
            async () =>
                !await NoteExistsAsync("multi-tenant-a")
                && !await NoteExistsAsync("multi-tenant-b"),
            TimeSpan.FromSeconds(8)
        );

        await host.Host.StopAsync();

        (await NoteExistsAsync("multi-tenant-a")).Should().BeFalse();
        (await NoteExistsAsync("multi-tenant-b")).Should().BeFalse();
    }

    [Fact]
    public async Task Worker_Sweeps_Tenantless_Entities_Once_Under_The_Tenantless_Context()
    {
        var tenantA = CreateTenant();
        var tenantB = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        using var host = BuildHost(
            settings,
            tenantA,
            services =>
            {
                services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
                services.AddSingleton<IRetentionTenantSource>(
                    new StaticTenantSource(tenantA, tenantB)
                );
            }
        );
        await SeedOldTenantlessLogAsync("tenantless-worker-purge");

        await host.Host.StartAsync();
        await WaitUntilAsync(
            async () => !await TenantlessLogExistsAsync("tenantless-worker-purge"),
            TimeSpan.FromSeconds(8)
        );
        await host.Host.StopAsync();

        (await TenantlessLogExistsAsync("tenantless-worker-purge")).Should().BeFalse();

        // Tenantless audit summaries are attributed to the dedicated tenantless context,
        // never to whichever tenant's pass happened to reach the shared table first.
        var summaryTenantIds = await LoadEntitySummaryTenantIdsAsync(typeof(TenantlessLog));
        summaryTenantIds.Should().NotBeEmpty();
        summaryTenantIds.Should().AllSatisfy(tenantId => tenantId.Should().Be(Guid.Empty));
    }

    [Fact]
    public async Task Worker_Sweeps_Tenantless_Entities_Even_When_The_Tenant_Source_Is_Empty()
    {
        // A tenantless-only deployment legitimately has no tenants; the tenantless pass
        // must not be gated on the tenant source returning at least one.
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        using var host = BuildHost(
            settings,
            CreateTenant(),
            services =>
            {
                services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
                services.AddSingleton<IRetentionTenantSource>(new StaticTenantSource());
            }
        );
        await SeedOldTenantlessLogAsync("tenantless-no-tenants-purge");

        await host.Host.StartAsync();
        await WaitUntilAsync(
            async () => !await TenantlessLogExistsAsync("tenantless-no-tenants-purge"),
            TimeSpan.FromSeconds(8)
        );
        await host.Host.StopAsync();

        (await TenantlessLogExistsAsync("tenantless-no-tenants-purge")).Should().BeFalse();

        var summaryTenantIds = await LoadEntitySummaryTenantIdsAsync(typeof(TenantlessLog));
        summaryTenantIds.Should().NotBeEmpty();
        summaryTenantIds.Should().AllSatisfy(tenantId => tenantId.Should().Be(Guid.Empty));
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
                services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
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
    public async Task Worker_Survives_A_Failing_Iteration_And_Sweeps_On_A_Later_Tick()
    {
        var tenant = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        var categoryRepository = new FailingOnceCategoryRepository(new SampleCategoryRepository());
        using var host = BuildHost(
            settings,
            tenant,
            services =>
            {
                services.AddSingleton<IRetentionCategoryRepository>(categoryRepository);
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

    [Fact]
    public async Task Worker_DryRun_Leaves_Rows_Untouched_And_Writes_An_Audited_DryRun_Run()
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

        CompletedSweepRun? completedRun = null;
        await host.Host.StartAsync();
        await WaitUntilAsync(
            async () =>
            {
                completedRun = await LoadCompletedDryRunAsync(
                    tenant.Id,
                    SweepTriggerKind.Scheduled
                );
                return categoryRepository.GetAsyncCount > 0 && completedRun is not null;
            },
            TimeSpan.FromSeconds(8)
        );

        await host.Host.StopAsync();

        (await NoteExistsAsync("dry-run-note")).Should().BeTrue();

        // Scheduled dry runs leave a real audit trail: a sweep_run row with DryRun set
        // and per-entity summaries carrying the predicted counts.
        completedRun.Should().NotBeNull();
        completedRun!.TenantId.Should().Be(tenant.Id);
        completedRun.Trigger.Should().Be(SweepTriggerKind.Scheduled);

        var dryRunSummaries = await LoadDryRunNoteSummariesAsync(completedRun.SweepId);
        dryRunSummaries.Should().NotBeEmpty();
        dryRunSummaries.Should().Contain(affected => affected >= 1);
    }

    private async Task<CompletedSweepRun?> LoadCompletedDryRunAsync(
        Guid tenantId,
        SweepTriggerKind trigger
    )
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT "SweepId", "TenantId", "TriggerKind"
            FROM "sweep_run"
            WHERE "DryRun" = TRUE
              AND "Status" = 1
              AND "SettledAt" IS NOT NULL
              AND "TenantId" = @tenantId
              AND "TriggerKind" = @triggerKind
            ORDER BY "SettledAt" DESC
            LIMIT 1
            """;
        command.Parameters.AddWithValue("tenantId", tenantId);
        command.Parameters.AddWithValue("triggerKind", (int)trigger);

        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return null;
        }

        return new CompletedSweepRun(
            reader.GetGuid(0),
            reader.GetGuid(1),
            (SweepTriggerKind)reader.GetInt32(2)
        );
    }

    private async Task<IReadOnlyList<long>> LoadDryRunNoteSummariesAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT summary."Affected"
            FROM "sweep_run_entity_summary" AS summary
            WHERE summary."SweepId" = @sweepId AND summary."EntityType" = @entityType
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("entityType", typeof(Note).FullName ?? nameof(Note));

        var affectedCounts = new List<long>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            affectedCounts.Add(reader.GetInt64(0));
        }

        return affectedCounts;
    }

    private sealed record CompletedSweepRun(
        Guid SweepId,
        Guid TenantId,
        SweepTriggerKind Trigger
    );

    [Fact]
    public async Task Worker_Reloads_KillSwitch_Between_Passes()
    {
        var tenantA = CreateTenant();
        var tenantB = CreateTenant();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        WorkerTestHost? host = null;
        host = BuildHost(
            settings,
            tenantA,
            services =>
            {
                services.AddSingleton<IRetentionCategoryRepository, SampleCategoryRepository>();
                services.AddSingleton<IRetentionTenantSource>(
                    new ReloadingTenantSource(
                        tenantA,
                        tenantB,
                        () => host!.Reload(killSwitch: true, dryRun: true)
                    )
                );
            }
        );
        using (host)
        {
            await SeedOldNoteAsync(tenantA.Id, "before-kill-switch");
            await SeedOldNoteAsync(tenantB.Id, "after-kill-switch");

            await host.Host.StartAsync();
            await WaitUntilAsync(
                async () => !await NoteExistsAsync("before-kill-switch"),
                TimeSpan.FromSeconds(8)
            );
            await host.Reloaded.WaitAsync(TimeSpan.FromSeconds(8));
            await host.Host.StopAsync();

            (await NoteExistsAsync("before-kill-switch")).Should().BeFalse();
            (await NoteExistsAsync("after-kill-switch")).Should().BeTrue();
        }
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

    private async Task<IReadOnlyList<Guid>> LoadEntitySummaryTenantIdsAsync(Type entityType)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT "TenantId"
            FROM "sweep_run_entity_summary"
            WHERE "EntityType" = @entityType
            """;
        command.Parameters.AddWithValue("entityType", entityType.FullName ?? entityType.Name);

        var tenantIds = new List<Guid>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            tenantIds.Add(reader.GetGuid(0));
        }

        return tenantIds;
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

    private sealed class StaticTenantSource(params TenantContext[] tenants) : IRetentionTenantSource
    {
        public Task<IReadOnlyList<TenantContext>> GetTenantsAsync(CancellationToken ct)
        {
            return Task.FromResult<IReadOnlyList<TenantContext>>(tenants);
        }
    }

    private sealed class ReloadingTenantSource(
        TenantContext first,
        TenantContext second,
        Action reload
    ) : IRetentionTenantSource
    {
        public Task<IReadOnlyList<TenantContext>> GetTenantsAsync(CancellationToken ct)
        {
            return Task.FromResult<IReadOnlyList<TenantContext>>(
                new ReloadingTenantList(first, second, reload)
            );
        }
    }

    private sealed class ReloadingTenantList(
        TenantContext first,
        TenantContext second,
        Action reload
    ) : IReadOnlyList<TenantContext>
    {
        public int Count => 2;

        public TenantContext this[int index] => index switch
        {
            0 => first,
            1 => second,
            _ => throw new ArgumentOutOfRangeException(nameof(index)),
        };

        public IEnumerator<TenantContext> GetEnumerator()
        {
            yield return first;
            reload();
            yield return second;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() =>
            GetEnumerator();
    }

    private sealed class FailingOnceCategoryRepository(IRetentionCategoryRepository inner)
        : IRetentionCategoryRepository
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

        public async Task<IRetentionRuleResolver?> GetAsync(
            string category,
            CancellationToken ct
        )
        {
            var resolver = await inner.GetAsync(category, ct);
            return resolver is null ? null : new FailingOnceResolver(resolver, this);
        }

        private sealed class FailingOnceResolver(
            IRetentionRuleResolver inner,
            FailingOnceCategoryRepository state
        ) : IRetentionRuleResolver
        {
            public async Task<RetentionRule> ResolveAsync(
                RetentionResolutionContext ctx,
                CancellationToken ct
            )
            {
                if (Interlocked.CompareExchange(ref state.failureInjected, 1, 0) == 0)
                {
                    state.failedIteration.TrySetResult();
                    throw new InvalidOperationException(
                        "Simulated transient category resolver failure."
                    );
                }

                var rule = await inner.ResolveAsync(ctx, ct);
                state.laterSuccessfulIteration.TrySetResult();
                return rule;
            }

            public RetentionRule? TryResolveAtStartup() => inner.TryResolveAtStartup();
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
        public Task CreateAsync(RetentionHoldRequest request, CancellationToken ct) =>
            Task.CompletedTask;

        public Task RemoveAsync(Guid holdId, DateTimeOffset removedAt, CancellationToken ct) =>
            Task.CompletedTask;

        public Task<IReadOnlyList<RetentionHold>> ListActiveAsync(
            DateTimeOffset asOf,
            CancellationToken ct
        )
        {
            return Task.FromResult<IReadOnlyList<RetentionHold>>([]);
        }

        public Task<bool> HasActiveHoldAsync(
            Guid retentionEntityId,
            string recordId,
            Guid? tenantId,
            DateTimeOffset asOf,
            CancellationToken ct
        )
        {
            return Task.FromResult(false);
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

    private sealed class BlockingAuditWriterState
    {
        private readonly Channel<bool> releaseChannel = Channel.CreateBounded<bool>(1);
        private int startedCount;
        private int blocked;

        public TaskCompletionSource<bool> CompletedReached { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int StartedCount => Volatile.Read(ref startedCount);

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
