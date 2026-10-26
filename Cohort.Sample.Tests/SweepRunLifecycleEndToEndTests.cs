using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class SweepRunLifecycleEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Theory]
    [InlineData(RunPath.Sweep)]
    public async Task Active_Run_Holds_Ownership_Lock_From_Durable_Started_Through_Settlement(
        RunPath path
    )
    {
        var tenantId = Guid.NewGuid();
        var repository = new BlockingCategoryRepository();
        using var host = new CohortTestHost(ConnectionString, repository);
        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        var runTask = StartRunAsync(host, path, tenant, asOf);

        await repository.ResolutionEntered.WaitAsync(TimeSpan.FromSeconds(10));
        var sweepId = await LoadStartedSweepIdAsync(tenantId);

        try
        {
            await BackdateRunAsync(sweepId);
            await host.RunWithServicesAsync(async services =>
            {
                await services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync();
            });

            (await LoadRunStatusAsync(sweepId)).Should().Be(SweepRunStatus.Started);
        }
        finally
        {
            repository.ReleaseResolution();
        }

        (await runTask).Should().Be(sweepId);
        (await LoadRunStatusAsync(sweepId)).Should().Be(SweepRunStatus.Succeeded);
    }

    [Fact]
    public async Task Run_Is_Locked_Before_Started_Reaches_Observers()
    {
        // A slow observer delays the run between Started and its first entity. Recovery must
        // still see a live owner, or it would fail a run that is about to proceed.
        var tenantId = Guid.NewGuid();
        var observer = new StartedBlockingObserver();
        using var host = new CohortTestHost(
            ConnectionString,
            configurationOverrides: new Dictionary<string, string?>
            {
                ["Cohort:AuditObservers:Timeout"] = "00:01:00",
            },
            configureServices: services => services.AddSingleton<IRetentionAuditObserver>(observer)
        );
        var runTask = host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero)
        );

        await observer.StartedDelivered.WaitAsync(TimeSpan.FromSeconds(10));
        var sweepId = await LoadStartedSweepIdAsync(tenantId);
        try
        {
            await BackdateRunAsync(sweepId);
            await host.RunWithServicesAsync(services =>
                services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
            );

            (await LoadRunStatusAsync(sweepId)).Should().Be(SweepRunStatus.Started);
        }
        finally
        {
            observer.Release();
        }

        (await runTask).SweepId.Should().Be(sweepId);
        (await LoadRunStatusAsync(sweepId)).Should().Be(SweepRunStatus.Succeeded);
    }

    [Fact]
    public async Task Manual_Sweep_Runs_As_Requested_When_The_Worker_Is_Configured_To_Dry_Run()
    {
        // Cohort:DryRun only sets what the scheduled worker does; an explicit request is
        // honoured as written.
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        using var host = new CohortTestHost(
            ConnectionString,
            configurationOverrides: new Dictionary<string, string?> { ["Cohort:DryRun"] = "true" }
        );
        await using (var db = host.CreateDbContext())
        {
            db.Notes.Add(new Cohort.Sample.Entities.Note { Id = noteId, TenantId = tenantId, CreatedAt = asOf.AddDays(-120), Body = "configured-dry-run" });
            await db.SaveChangesAsync();
        }

        var result = await host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        (await LoadRunStatusAsync(result.SweepId)).Should().Be(SweepRunStatus.Succeeded);
        await using var verify = host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeFalse();
    }

    [Fact]
    public async Task A_Run_Whose_Session_Is_Lost_Commits_No_Further_Batch()
    {
        // The run's session (and the run lock it holds) dies between two batches. EF reopens a
        // closed connection on its next use, so without a fence the run would carry on, on a
        // session holding no lock, committing the next entity's rows.
        var tenantId = Guid.NewGuid();
        var createdAt = DateTimeOffset.UtcNow.AddDays(-120);
        var observer = SessionKillingObserver.FirstNoteBatch(ConnectionString);
        using var host = new CohortTestHost(
            ConnectionString,
            configurationOverrides: new Dictionary<string, string?> { ["Cohort:SweepBatchSize"] = "1" },
            configureServices: services => services.AddSingleton<IRetentionAuditObserver>(observer)
        );
        var softDeleteId = Guid.NewGuid();
        await using (var db = host.CreateDbContext())
        {
            for (var i = 0; i < 3; i++)
            {
                db.Notes.Add(new Cohort.Sample.Entities.Note { Id = Guid.NewGuid(), TenantId = tenantId, CreatedAt = createdAt, Body = "lost-session" });
            }

            // Swept after Note: entities run in name order when no foreign key orders them.
            db.SoftDeleteRecords.Add(new Cohort.Sample.Entities.SoftDeleteRecord { Id = softDeleteId, TenantId = tenantId, CreatedAt = createdAt, Body = "lost-session" });
            await db.SaveChangesAsync();
        }

        var sweep = () => host.RunSweepAsync(new TenantContext(tenantId, "uk", new Dictionary<string, string>()));
        var thrown = await Record.ExceptionAsync(sweep);

        observer.Terminated.Should().BeTrue();
        await using var verify = host.CreateDbContext();
        (await verify.Notes.CountAsync(note => note.TenantId == tenantId)).Should().Be(2);
        (await verify.SoftDeleteRecords.SingleAsync(record => record.Id == softDeleteId)).IsDeleted.Should().BeFalse();
        // The run fails as a whole rather than moving on to its next entity.
        thrown.Should().NotBeNull();
        (await LoadRunStatusAsync(observer.SweepId)).Should().Be(SweepRunStatus.Failed);
    }

    [Fact]
    public async Task A_Dry_Run_Whose_Session_Is_Lost_Commits_No_Further_Summary()
    {
        // A dry run has no batches to fence. Its session (and the run lock it holds) dies once
        // the Note summary has committed; EF reopens the connection on its next use, so
        // without a fence on each summary the run would carry on beside whichever owner took
        // its locks, recording the next entity's prediction.
        var tenantId = Guid.NewGuid();
        var createdAt = DateTimeOffset.UtcNow.AddDays(-120);
        var observer = SessionKillingObserver.NoteSummary(ConnectionString);
        using var host = new CohortTestHost(
            ConnectionString,
            configureServices: services => services.AddSingleton<IRetentionAuditObserver>(observer)
        );
        await using (var db = host.CreateDbContext())
        {
            db.Notes.Add(new Cohort.Sample.Entities.Note { Id = Guid.NewGuid(), TenantId = tenantId, CreatedAt = createdAt, Body = "lost-session-dry" });
            // Summarised after Note: entities run in name order when no foreign key orders them.
            db.SoftDeleteRecords.Add(new Cohort.Sample.Entities.SoftDeleteRecord { Id = Guid.NewGuid(), TenantId = tenantId, CreatedAt = createdAt, Body = "lost-session-dry" });
            await db.SaveChangesAsync();
        }

        var dryRun = () => host.RunWithServicesAsync(services =>
            services
                .GetRequiredService<RetentionSweepEngine>()
                .RunAsync(
                    new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                    DateTimeOffset.UtcNow,
                    SweepEntityScope.TenantedOnly,
                    dryRun: true
                )
        );
        var thrown = await Record.ExceptionAsync(dryRun);

        observer.Terminated.Should().BeTrue();
        (await LoadSummarisedEntityTypesAsync(observer.SweepId))
            .Should().Contain(typeof(Cohort.Sample.Entities.Note).FullName)
            .And.NotContain(typeof(Cohort.Sample.Entities.SoftDeleteRecord).FullName);
        thrown.Should().NotBeNull();
        (await LoadRunStatusAsync(observer.SweepId)).Should().Be(SweepRunStatus.Failed);
    }

    /// <summary>
    /// Ends the run's session once the first event <paramref name="fireOn"/> matches has
    /// committed: finds the backend holding the run lock and waits for it to exit.
    /// </summary>
    private sealed class SessionKillingObserver(string connectionString, Func<SweepEvent, bool> fireOn)
        : IRetentionAuditObserver
    {
        private int fired;

        public bool Terminated { get; private set; }

        public Guid SweepId { get; private set; }

        /// <summary>Fires once the run's first Note batch that changed rows has committed.</summary>
        public static SessionKillingObserver FirstNoteBatch(string connectionString) =>
            new(
                connectionString,
                evt => evt is SweepEvent.EntityProgress { Affected: > 0 } progress
                    && progress.EntityType == typeof(Cohort.Sample.Entities.Note)
            );

        /// <summary>Fires once the run's Note summary has committed.</summary>
        public static SessionKillingObserver NoteSummary(string connectionString) =>
            new(
                connectionString,
                evt => evt is SweepEvent.EntitySummary summary
                    && summary.EntityType == typeof(Cohort.Sample.Entities.Note)
            );

        public async Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            if (!fireOn(evt) || Interlocked.Exchange(ref fired, 1) != 0)
            {
                return;
            }

            SweepId = evt.SweepId;
            var key = RetentionRunAdvisoryLock.KeyFor(evt.SweepId);
            await using var connection = new NpgsqlConnection($"{connectionString};Pooling=false");
            await connection.OpenAsync(ct);
            await using var terminate = connection.CreateCommand();
            // A bigint advisory key is held as classid (high 32 bits) and objid (low 32 bits).
            terminate.CommandText = """
                SELECT pg_terminate_backend(pid, 5000) FROM pg_locks
                WHERE locktype = 'advisory' AND granted AND objsubid = 1
                  AND classid::bigint = (@key >> 32) & 4294967295 AND objid::bigint = @key & 4294967295
                """;
            terminate.Parameters.AddWithValue("key", key);
            Terminated = await terminate.ExecuteScalarAsync(ct) is true;
        }
    }

    private sealed class StartedBlockingObserver : IRetentionAuditObserver
    {
        private readonly TaskCompletionSource delivered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task StartedDelivered => delivered.Task;

        public void Release() => released.TrySetResult();

        public async Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            if (evt is SweepEvent.Started)
            {
                delivered.TrySetResult();
                await released.Task.WaitAsync(ct);
            }
        }
    }

    private static Task<Guid> StartRunAsync(
        CohortTestHost host,
        RunPath path,
        TenantContext tenant,
        DateTimeOffset asOf
    )
    {
        return path switch
        {
            RunPath.Sweep => RunSweepAsync(),
            RunPath.AuditedDryRun => RunDryRunAsync(),
            RunPath.Erasure => RunErasureAsync(),
            _ => throw new ArgumentOutOfRangeException(nameof(path)),
        };

        async Task<Guid> RunSweepAsync()
        {
            var result = await host.RunSweepAsync(tenant, asOf);
            return result.SweepId;
        }

        async Task<Guid> RunDryRunAsync()
        {
            var result = await host.RunWithServicesAsync(services =>
                services
                    .GetRequiredService<RetentionSweepEngine>()
                    .RunAsync(
                        tenant,
                        asOf,
                        SweepEntityScope.TenantedOnly,
                        dryRun: true
                    )
            );
            return result.SweepId;
        }

        async Task<Guid> RunErasureAsync()
        {
            var result = await host.RunErasureAsync(
                tenant,
                new ErasureScope("user", Guid.NewGuid(), allowSoftDeleteAsErasure: true),
                asOf
            );
            return result.SweepId;
        }
    }

    private async Task<Guid> LoadStartedSweepIdAsync(Guid tenantId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT "SweepId"
            FROM "sweep_run"
            WHERE "TenantId" = @tenantId AND "Status" = @status
            """;
        command.Parameters.AddWithValue("tenantId", tenantId);
        command.Parameters.AddWithValue("status", (int)SweepRunStatus.Started);
        return (Guid)(await command.ExecuteScalarAsync())!;
    }

    private async Task BackdateRunAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            "UPDATE \"sweep_run\" SET \"StartedAt\" = @startedAt WHERE \"SweepId\" = @sweepId";
        command.Parameters.AddWithValue("startedAt", DateTimeOffset.UtcNow.AddDays(-1));
        command.Parameters.AddWithValue("sweepId", sweepId);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<IReadOnlyList<string>> LoadSummarisedEntityTypesAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT \"EntityType\" FROM \"sweep_run_entity_summary\" WHERE \"SweepId\" = @sweepId";
        command.Parameters.AddWithValue("sweepId", sweepId);
        var types = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            types.Add(reader.GetString(0));
        }

        return types;
    }

    private async Task<SweepRunStatus> LoadRunStatusAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT \"Status\" FROM \"sweep_run\" WHERE \"SweepId\" = @sweepId";
        command.Parameters.AddWithValue("sweepId", sweepId);
        return (SweepRunStatus)(int)(await command.ExecuteScalarAsync())!;
    }

    public enum RunPath
    {
        Sweep,
        AuditedDryRun,
        Erasure,
    }

    private sealed class BlockingCategoryRepository : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();
        private readonly TaskCompletionSource resolutionEntered = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private readonly TaskCompletionSource releaseResolution = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        public Task ResolutionEntered => resolutionEntered.Task;

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            inner.GetCapabilities(category);

        public async Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            resolutionEntered.TrySetResult();
            await releaseResolution.Task.WaitAsync(ct);
            return await inner.ResolveAsync(context, ct);
        }

        public void ReleaseResolution() => releaseResolution.TrySetResult();
    }
}
