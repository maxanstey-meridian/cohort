using System.Diagnostics;
using Cohort.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class AdvisoryLockCleanupEndToEndTests(PostgresFixture fixture)
{
    [Fact]
    public async Task False_Unlock_Is_Surfaced_When_No_Primary_Failure_Exists()
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();

        var act = async () =>
            await RetentionRunAdvisoryLock.ReleaseAsync(connection, Guid.NewGuid());

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*not owned*");
    }

    [Fact]
    public async Task Primary_Failure_Wins_Over_Unlock_And_Close_Failures()
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        var primary = new PrimaryOperationException();

        var act = async () =>
        {
            try
            {
                throw primary;
            }
            catch (Exception ex)
            {
                await OperationalConnectionCleanup.RunAsync(
                    ct =>
                        RetentionRunAdvisoryLock.ReleaseAsync(connection, Guid.NewGuid(), ct),
                    _ => throw new CloseFailureException(),
                    ex,
                    NullLogger.Instance
                );
                throw;
            }
        };

        (await act.Should().ThrowAsync<PrimaryOperationException>()).Which.Should().BeSameAs(primary);
    }

    [Fact]
    public async Task Unlock_Failure_Wins_Over_Close_Failure_Without_A_Primary_Failure()
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();

        var act = async () =>
            await OperationalConnectionCleanup.RunAsync(
                ct => RetentionRunAdvisoryLock.ReleaseAsync(connection, Guid.NewGuid(), ct),
                _ => throw new CloseFailureException(),
                primaryException: null,
                NullLogger.Instance
            );

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*not owned*");
    }

    [Fact]
    public async Task Close_Failure_Is_Surfaced_When_Unlock_Succeeds_And_No_Primary_Failure_Exists()
    {
        var closeFailure = new CloseFailureException();

        var act = async () => await OperationalConnectionCleanup.RunAsync(
            _ => Task.CompletedTask,
            _ => Task.FromException(closeFailure),
            primaryException: null,
            NullLogger.Instance
        );

        (await act.Should().ThrowAsync<CloseFailureException>()).Which.Should().BeSameAs(closeFailure);
    }

    [Fact]
    public async Task Primary_Failure_Wins_When_Only_Close_Fails()
    {
        var primary = new PrimaryOperationException();

        var act = async () =>
        {
            try
            {
                throw primary;
            }
            catch (Exception ex)
            {
                await OperationalConnectionCleanup.RunAsync(
                    unlock: null,
                    _ => Task.FromException(new CloseFailureException()),
                    ex,
                    NullLogger.Instance
                );
                throw;
            }
        };

        (await act.Should().ThrowAsync<PrimaryOperationException>()).Which.Should().BeSameAs(primary);
    }

    [Fact]
    public async Task Cleanup_Uses_An_Independent_Bounded_Cancellation_Token()
    {
        CancellationToken observedToken = default;
        var stopwatch = Stopwatch.StartNew();

        var act = async () => await OperationalConnectionCleanup.RunAsync(
            async ct =>
            {
                observedToken = ct;
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            },
            close: null,
            primaryException: null,
            NullLogger.Instance
        );

        await act.Should().ThrowAsync<OperationCanceledException>();
        stopwatch.Stop();

        observedToken.CanBeCanceled.Should().BeTrue();
        observedToken.IsCancellationRequested.Should().BeTrue();
        stopwatch.Elapsed.Should().BeGreaterThanOrEqualTo(TimeSpan.FromSeconds(29));
        stopwatch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(40));
    }

    private sealed class PrimaryOperationException : Exception;

    private sealed class CloseFailureException : Exception;
}
