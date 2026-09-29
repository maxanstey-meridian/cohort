using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
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
                    .DryRunAsync(
                        tenant,
                        asOf,
                        SweepTriggerKind.Manual,
                        SweepEntityScope.TenantedOnly
                    )
            );
            return result.SweepId;
        }

        async Task<Guid> RunErasureAsync()
        {
            var result = await host.RunErasureAsync(
                tenant,
                new ErasureScope(Guid.NewGuid(), allowSoftDeleteAsErasure: true),
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
