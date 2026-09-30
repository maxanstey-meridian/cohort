using Cohort.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class AdvisoryLockCleanupEndToEndTests(PostgresFixture fixture)
{
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
                    connection,
                    ct =>
                        RetentionRunAdvisoryLock.ReleaseAsync(connection, RetentionRunAdvisoryLock.KeyFor(Guid.NewGuid()), ct),
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
    public async Task A_Timed_Out_Unlock_Does_Not_Return_The_Lock_Holding_Session_To_The_Pool()
    {
        // Its own pool, so discarding it cannot disturb other tests' connections.
        var connectionString = new NpgsqlConnectionStringBuilder(fixture.ConnectionString)
        {
            ApplicationName = $"cohort-cleanup-{Guid.NewGuid():N}",
        }.ConnectionString;
        var key = RetentionRunAdvisoryLock.KeyFor(Guid.NewGuid());
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await RetentionRunAdvisoryLock.AcquireAsync(connection, key, CancellationToken.None);
        using var timedOut = new CancellationTokenSource();
        await timedOut.CancelAsync();

        await OperationalConnectionCleanup.RunAsync(
            connection,
            _ => RetentionRunAdvisoryLock.ReleaseAsync(connection, key, timedOut.Token),
            _ => connection.CloseAsync(),
            null,
            NullLogger.Instance
        );

        // Another session can take the lock: the session that held it did not survive in the pool.
        await using var other = new NpgsqlConnection(fixture.ConnectionString);
        await other.OpenAsync();
        (await RetentionRunAdvisoryLock.TryAcquireAsync(other, key, CancellationToken.None)).Should().BeTrue();
        await RetentionRunAdvisoryLock.ReleaseAsync(other, key, CancellationToken.None);
    }

    private sealed class PrimaryOperationException : Exception;

    private sealed class CloseFailureException : Exception;
}
