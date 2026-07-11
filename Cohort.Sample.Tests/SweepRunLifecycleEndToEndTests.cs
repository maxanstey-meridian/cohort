using Cohort.Application;
using Cohort.Infrastructure.Audit;
using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class SweepRunLifecycleEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
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
}
