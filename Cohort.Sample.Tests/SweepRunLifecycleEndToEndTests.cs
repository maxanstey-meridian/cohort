using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Audit;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class SweepRunLifecycleEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Theory]
    [InlineData(RunPath.Sweep)]
    [InlineData(RunPath.AuditedDryRun)]
    [InlineData(RunPath.Erasure)]
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
    public async Task Audit_Writer_Rejects_Duplicate_And_Late_Terminal_Transitions()
    {
        var sweepId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var startedAt = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        var completedAt = startedAt.AddSeconds(1);

        await using var db = Host.CreateDbContext();
        var writer = new EfRetentionAuditWriter(db);
        await writer.WriteAsync(
            new SweepEvent.Started(
                sweepId,
                startedAt,
                SweepTriggerKind.Manual,
                DryRun: false,
                tenantId
            ),
            CancellationToken.None
        );
        await writer.WriteAsync(
            new SweepEvent.Completed(sweepId, completedAt, TimeSpan.FromSeconds(1), 0),
            CancellationToken.None
        );

        var duplicate = () =>
            writer.WriteAsync(
                new SweepEvent.Completed(sweepId, completedAt, TimeSpan.FromSeconds(1), 0),
                CancellationToken.None
            );
        var lateFailure = () =>
            writer.WriteAsync(
                new SweepEvent.Failed(sweepId, completedAt.AddSeconds(1), "late failure"),
                CancellationToken.None
            );

        await duplicate.Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("Sweep run does not exist or is no longer in the Started state.");
        await lateFailure.Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("Sweep run does not exist or is no longer in the Started state.");
    }

    [Fact]
    public async Task PostgreSql_Rejects_Invalid_Sweep_Run_Lifecycle_Values()
    {
        await AssertConstraintViolationAsync(
            "CK_sweep_run_Status_Range",
            status: 5,
            settledAt: DateTimeOffset.UtcNow,
            duration: TimeSpan.Zero,
            totalAffected: 0
        );
        await AssertConstraintViolationAsync(
            "CK_sweep_run_Started_Unsettled",
            status: (int)SweepRunStatus.Started,
            settledAt: DateTimeOffset.UtcNow,
            duration: null,
            totalAffected: 0
        );
        await AssertConstraintViolationAsync(
            "CK_sweep_run_Terminal_Settled",
            status: (int)SweepRunStatus.Succeeded,
            settledAt: null,
            duration: TimeSpan.Zero,
            totalAffected: 0
        );
        await AssertConstraintViolationAsync(
            "CK_sweep_run_TotalAffected_Nonnegative",
            status: (int)SweepRunStatus.Started,
            settledAt: null,
            duration: null,
            totalAffected: -1
        );
        await AssertConstraintViolationAsync(
            "CK_sweep_run_Duration_Nonnegative",
            status: (int)SweepRunStatus.Succeeded,
            settledAt: DateTimeOffset.UtcNow,
            duration: TimeSpan.FromMilliseconds(-1),
            totalAffected: 0
        );
    }

    private async Task AssertConstraintViolationAsync(
        string constraintName,
        int status,
        DateTimeOffset? settledAt,
        TimeSpan? duration,
        long? totalAffected
    )
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO "sweep_run"
                ("SweepId", "StartedAt", "Status", "SettledAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected")
            VALUES
                (@sweepId, @startedAt, @status, @settledAt, @duration, @triggerKind, FALSE, @tenantId, @totalAffected)
            """;
        command.Parameters.AddWithValue("sweepId", Guid.NewGuid());
        command.Parameters.AddWithValue("startedAt", DateTimeOffset.UtcNow);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("settledAt", settledAt ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("duration", duration ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("triggerKind", (int)SweepTriggerKind.Manual);
        command.Parameters.AddWithValue("tenantId", Guid.NewGuid());
        command.Parameters.AddWithValue("totalAffected", totalAffected ?? (object)DBNull.Value);

        var act = () => command.ExecuteNonQueryAsync();

        var exception = await act.Should().ThrowAsync<PostgresException>();
        exception.Which.SqlState.Should().Be(PostgresErrorCodes.CheckViolation);
        exception.Which.ConstraintName.Should().Be(constraintName);
        await transaction.RollbackAsync();
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
