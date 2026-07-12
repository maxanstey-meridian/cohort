using System.Collections.Concurrent;
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
    public void AddCohort_Allows_Host_Rule_Holds_And_Audit_Observer_Registrations()
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
                services.AddSingleton<IRetentionRuleProvider, CustomCategoryRepository>();
                services.AddSingleton<IRetentionAuditObserver, CustomAuditObserver>();
                services.AddScoped<IRetentionHoldsRepository, CustomHoldsRepository>();
            }
        );

        using var scope = host.Host.Services.CreateScope();

        scope
            .ServiceProvider.GetRequiredService<IRetentionRuleProvider>()
            .Should()
            .BeOfType<CustomCategoryRepository>();
        scope
            .ServiceProvider.GetRequiredService<IRetentionAuditObserver>()
            .Should()
            .BeOfType<CustomAuditObserver>();
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
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
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
        var categoryRepository = new CountingCategoryRepository(
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
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
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
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
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
    public async Task Worker_Rejects_A_Null_Tenant_Before_Any_Tenant_Or_Tenantless_Pass()
    {
        var tenant = CreateTenant();
        var logs = new WorkerLogProvider();
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
                services.AddSingleton<IRetentionTenantSource>(
                    new StaticTenantSource(tenant, null!)
                );
                services.AddSingleton<ILoggerProvider>(logs);
            }
        );
        await SeedOldNoteAsync(tenant.Id, "null-source-tenant");
        await SeedOldTenantlessLogAsync("null-source-tenantless");

        await host.Host.StartAsync();
        await WaitUntilAsync(
            () => Task.FromResult(logs.Entries.Any(entry =>
                entry.Exception is InvalidOperationException
                && entry.Exception.Message
                    == "IRetentionTenantSource returned a null tenant context."
            )),
            TimeSpan.FromSeconds(8)
        );
        await host.Host.StopAsync();

        (await NoteExistsAsync("null-source-tenant")).Should().BeTrue();
        (await TenantlessLogExistsAsync("null-source-tenantless")).Should().BeTrue();
    }

    [Fact]
    public async Task Worker_Cancellation_From_The_Tenant_Source_Propagates_Without_Later_Passes()
    {
        var tenant = CreateTenant();
        var source = new CancellationBlockingTenantSource();
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
                services.AddSingleton<IRetentionTenantSource>(source);
            }
        );
        await SeedOldNoteAsync(tenant.Id, "cancelled-source-tenant");
        await SeedOldTenantlessLogAsync("cancelled-source-tenantless");

        await host.Host.StartAsync();
        await source.Entered.WaitAsync(TimeSpan.FromSeconds(8));
        var stop = Stopwatch.StartNew();
        await host.Host.StopAsync().WaitAsync(TimeSpan.FromSeconds(2));

        stop.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(2));
        source.CancellationObserved.Should().BeTrue();
        (await NoteExistsAsync("cancelled-source-tenant")).Should().BeTrue();
        (await TenantlessLogExistsAsync("cancelled-source-tenantless")).Should().BeTrue();
    }

    [Fact]
    public async Task Worker_Cancellation_During_Tenant_Iteration_Propagates_Without_Any_Pass()
    {
        var first = CreateTenant();
        var later = CreateTenant();
        var source = new CancellationBlockingIterationTenantSource(first, later);
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        using var host = BuildHost(
            settings,
            first,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
                services.AddSingleton<IRetentionTenantSource>(source);
            }
        );
        await SeedOldNoteAsync(first.Id, "cancelled-iteration-first");
        await SeedOldNoteAsync(later.Id, "cancelled-iteration-later");
        await SeedOldTenantlessLogAsync("cancelled-iteration-tenantless");

        await host.Host.StartAsync();
        await source.IterationBlocked.WaitAsync(TimeSpan.FromSeconds(8));
        var stop = Stopwatch.StartNew();
        await host.Host.StopAsync().WaitAsync(TimeSpan.FromSeconds(2));

        stop.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(2));
        source.CancellationObserved.Should().BeTrue();
        (await NoteExistsAsync("cancelled-iteration-first")).Should().BeTrue();
        (await NoteExistsAsync("cancelled-iteration-later")).Should().BeTrue();
        (await TenantlessLogExistsAsync("cancelled-iteration-tenantless")).Should().BeTrue();
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
    public async Task Worker_Deduplicates_Tenants_In_First_Seen_Order_And_Uses_The_First_Context()
    {
        var tenantId = Guid.NewGuid();
        var first = new TenantContext(
            tenantId,
            "first",
            new Dictionary<string, string> { ["source"] = "first" }
        );
        var conflictingDuplicate = new TenantContext(
            tenantId,
            "second",
            new Dictionary<string, string> { ["source"] = "second" }
        );
        var provider = new CapturingRuleProvider(tenantId);
        var logs = new WorkerLogProvider();
        var settings = CreateSettings(
            fixture.ConnectionString,
            schedule: "*/1 * * * * *",
            dryRun: false,
            killSwitch: false
        );
        using var host = BuildHost(
            settings,
            first,
            services =>
            {
                services.AddSingleton<IRetentionRuleProvider>(provider);
                services.AddSingleton<IRetentionTenantSource>(
                    new StaticTenantSource(first, conflictingDuplicate)
                );
                services.AddSingleton<ILoggerProvider>(logs);
            }
        );
        await SeedOldNoteAsync(tenantId, "deduplicated-tenant");

        await host.Host.StartAsync();
        await provider.FirstResolution.WaitAsync(TimeSpan.FromSeconds(8));
        await WaitUntilAsync(
            async () => !await NoteExistsAsync("deduplicated-tenant"),
            TimeSpan.FromSeconds(8)
        );
        await host.Host.StopAsync();

        provider.FirstContext.Should().NotBeNull();
        provider.FirstContext!.Jurisdiction.Should().Be("first");
        provider.FirstContext.Tags.Should().Contain("source", "first");
        provider.ResolutionCount.Should().Be(1);
        logs.Entries.Should().Contain(entry =>
            entry.Level == LogLevel.Warning
            && entry.Message
                == $"IRetentionTenantSource returned conflicting contexts for tenant {tenantId}; Cohort will use the first context."
        );
    }

    [Fact]
    public async Task Worker_Executes_Tenant_Passes_Sequentially()
    {
        var tenantA = CreateTenant();
        var tenantB = CreateTenant();
        var provider = new SequentialRuleProvider(tenantA.Id, tenantB.Id);
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
                services.AddSingleton<IRetentionRuleProvider>(provider);
                services.AddSingleton<IRetentionTenantSource>(
                    new StaticTenantSource(tenantA, tenantB)
                );
            }
        );

        await host.Host.StartAsync();
        try
        {
            await provider.FirstTenantEntered.WaitAsync(TimeSpan.FromSeconds(8));
            await Task.Delay(250);

            provider.SecondTenantEntered.IsCompleted.Should().BeFalse();
            provider.MaximumConcurrency.Should().Be(1);

            provider.ReleaseFirstTenant();
            await provider.SecondTenantEntered.WaitAsync(TimeSpan.FromSeconds(8));
        }
        finally
        {
            provider.ReleaseFirstTenant();
            await host.Host.StopAsync();
        }

        provider.MaximumConcurrency.Should().Be(1);
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
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
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
                services.AddSingleton<IRetentionRuleProvider, SampleRetentionRuleProvider>();
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
        var categoryRepository = new CountingCategoryRepository(
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
                services.AddSingleton<IRetentionRuleProvider>(
                    new ReloadingRuleProvider(
                        tenantA.Id,
                        () => host!.Reload(killSwitch: true, dryRun: true)
                    )
                );
                services.AddSingleton<IRetentionTenantSource>(
                    new StaticTenantSource(tenantA, tenantB)
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

    private sealed class CancellationBlockingTenantSource : IRetentionTenantSource
    {
        private readonly TaskCompletionSource entered = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private int cancellationObserved;

        public Task Entered => entered.Task;

        public bool CancellationObserved => Volatile.Read(ref cancellationObserved) == 1;

        public async Task<IReadOnlyList<TenantContext>> GetTenantsAsync(CancellationToken ct)
        {
            entered.TrySetResult();
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                return [];
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                Interlocked.Exchange(ref cancellationObserved, 1);
                throw;
            }
        }
    }

    private sealed class CancellationBlockingIterationTenantSource(
        TenantContext first,
        TenantContext later
    ) : IRetentionTenantSource
    {
        private readonly TaskCompletionSource iterationBlocked = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private int cancellationObserved;

        public Task IterationBlocked => iterationBlocked.Task;

        public bool CancellationObserved => Volatile.Read(ref cancellationObserved) == 1;

        public Task<IReadOnlyList<TenantContext>> GetTenantsAsync(CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<TenantContext>>(
                new CancellationBlockingTenantList(
                    first,
                    later,
                    ct,
                    iterationBlocked,
                    () => Interlocked.Exchange(ref cancellationObserved, 1)
                )
            );
    }

    private sealed class CancellationBlockingTenantList(
        TenantContext first,
        TenantContext later,
        CancellationToken ct,
        TaskCompletionSource iterationBlocked,
        Action cancellationObserved
    ) : IReadOnlyList<TenantContext>
    {
        public int Count => 2;

        public TenantContext this[int index] => index switch
        {
            0 => first,
            1 => later,
            _ => throw new ArgumentOutOfRangeException(nameof(index)),
        };

        public IEnumerator<TenantContext> GetEnumerator()
        {
            yield return first;
            iterationBlocked.TrySetResult();
            ct.WaitHandle.WaitOne();
            cancellationObserved();
            ct.ThrowIfCancellationRequested();
            yield return later;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() =>
            GetEnumerator();
    }

    private sealed class ReloadingRuleProvider(Guid firstTenantId, Action reload)
        : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();
        private int reloaded;

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            inner.GetCapabilities(category);

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            if (
                context.Tenant.Id == firstTenantId
                && Interlocked.CompareExchange(ref reloaded, 1, 0) == 0
            )
            {
                reload();
            }

            return inner.ResolveAsync(context, ct);
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

    private sealed class CapturingRuleProvider(Guid tenantId) : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();
        private readonly TaskCompletionSource firstResolution = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private int resolutionCount;

        public Task FirstResolution => firstResolution.Task;

        public TenantContext? FirstContext { get; private set; }

        public int ResolutionCount => Volatile.Read(ref resolutionCount);

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            inner.GetCapabilities(category);

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            if (context.Category == "short-lived" && context.Tenant.Id == tenantId)
            {
                Interlocked.Increment(ref resolutionCount);
                FirstContext ??= context.Tenant;
                firstResolution.TrySetResult();
            }

            return inner.ResolveAsync(context, ct);
        }
    }

    private sealed class SequentialRuleProvider(Guid firstTenantId, Guid secondTenantId)
        : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();
        private readonly TaskCompletionSource firstTenantEntered = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private readonly TaskCompletionSource secondTenantEntered = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private readonly TaskCompletionSource releaseFirstTenant = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private int concurrency;
        private int maximumConcurrency;

        public Task FirstTenantEntered => firstTenantEntered.Task;

        public Task SecondTenantEntered => secondTenantEntered.Task;

        public int MaximumConcurrency => Volatile.Read(ref maximumConcurrency);

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            inner.GetCapabilities(category);

        public async Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            var current = Interlocked.Increment(ref concurrency);
            var observedMaximum = Volatile.Read(ref maximumConcurrency);
            while (
                current > observedMaximum
                && Interlocked.CompareExchange(
                    ref maximumConcurrency,
                    current,
                    observedMaximum
                ) != observedMaximum
            )
            {
                observedMaximum = Volatile.Read(ref maximumConcurrency);
            }
            try
            {
                if (context.Tenant.Id == firstTenantId && context.Category == "short-lived")
                {
                    firstTenantEntered.TrySetResult();
                    await releaseFirstTenant.Task.WaitAsync(ct);
                }
                else if (context.Tenant.Id == secondTenantId)
                {
                    secondTenantEntered.TrySetResult();
                }

                return await inner.ResolveAsync(context, ct);
            }
            finally
            {
                Interlocked.Decrement(ref concurrency);
            }
        }

        public void ReleaseFirstTenant() => releaseFirstTenant.TrySetResult();
    }

    private sealed class CustomCategoryRepository : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) => null;

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        ) => Task.FromResult<RetentionRule?>(null);
    }

    private sealed class CustomAuditObserver : IRetentionAuditObserver
    {
        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class BlockingAuditObserver(BlockingAuditWriterState state)
        : IRetentionAuditObserver
    {
        public async Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
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

    private sealed class WorkerLogProvider : ILoggerProvider
    {
        private readonly ConcurrentQueue<WorkerLogEntry> entries = new();

        public IReadOnlyList<WorkerLogEntry> Entries => entries.ToArray();

        public ILogger CreateLogger(string categoryName) => new WorkerLogger(entries);

        public void Dispose() { }

        private sealed class WorkerLogger(ConcurrentQueue<WorkerLogEntry> entries) : ILogger
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
                entries.Enqueue(
                    new WorkerLogEntry(logLevel, formatter(state, exception), exception)
                );
            }
        }
    }

    private sealed record WorkerLogEntry(
        LogLevel Level,
        string Message,
        Exception? Exception
    );

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
