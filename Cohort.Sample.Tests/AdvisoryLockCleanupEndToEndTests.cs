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
            catch (Exception)
            {
                await OperationalConnectionCleanup.RunAsync(
                    connection,
                    ct =>
                        RetentionRunAdvisoryLock.ReleaseAsync(connection, RetentionRunAdvisoryLock.KeyFor(Guid.NewGuid()), ct),
                    _ => throw new CloseFailureException(),
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
            NullLogger.Instance
        );

        // Another session can take the lock: the session that held it did not survive in the pool.
        await WaitForSessionsToEndAsync(connectionString);
        await using var other = new NpgsqlConnection(fixture.ConnectionString);
        await other.OpenAsync();
        (await RetentionRunAdvisoryLock.TryAcquireAsync(other, key, CancellationToken.None)).Should().BeTrue();
        await RetentionRunAdvisoryLock.ReleaseAsync(other, key, CancellationToken.None);
    }

    [Fact]
    public async Task A_Failed_Release_All_Does_Not_Return_The_Sessions_Locks_To_The_Pool()
    {
        // The worker's iteration cleanup: its coordination connection took session locks
        // outside any transaction, and releases every lock it holds when the iteration ends.
        var connectionString = new NpgsqlConnectionStringBuilder(fixture.ConnectionString)
        {
            ApplicationName = $"cohort-release-all-cleanup-{Guid.NewGuid():N}",
        }.ConnectionString;
        long[] keys = [RetentionRunAdvisoryLock.KeyFor(Guid.NewGuid()), RetentionRunAdvisoryLock.KeyFor(Guid.NewGuid())];
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        foreach (var key in keys)
        {
            (await RetentionRunAdvisoryLock.TryAcquireAsync(connection, key, CancellationToken.None)).Should().BeTrue();
        }

        using var timedOut = new CancellationTokenSource();
        await timedOut.CancelAsync();

        await OperationalConnectionCleanup.RunAsync(
            connection,
            _ => RetentionRunAdvisoryLock.ReleaseAllAsync(connection, timedOut.Token),
            _ => connection.CloseAsync(),
            NullLogger.Instance
        );

        // Another session can take every key: the session that held them did not survive in the pool.
        await WaitForSessionsToEndAsync(connectionString);
        await using var other = new NpgsqlConnection(fixture.ConnectionString);
        await other.OpenAsync();
        foreach (var key in keys)
        {
            (await RetentionRunAdvisoryLock.TryAcquireAsync(other, key, CancellationToken.None)).Should().BeTrue();
            await RetentionRunAdvisoryLock.ReleaseAsync(other, key, CancellationToken.None);
        }
    }

    /// <summary>
    /// Waits for every backend of <paramref name="connectionString"/>'s application name to exit.
    /// A discarded session's locks are freed only once its backend has gone, which can lag the
    /// client closing it; a session the pool kept never goes, so the wait times out.
    /// </summary>
    private async Task WaitForSessionsToEndAsync(string connectionString)
    {
        var applicationName = new NpgsqlConnectionStringBuilder(connectionString).ApplicationName;
        await using var probe = new NpgsqlConnection(fixture.ConnectionString);
        await probe.OpenAsync();
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (true)
        {
            await using var command = new NpgsqlCommand(
                "SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity WHERE application_name = @name",
                probe
            );
            command.Parameters.AddWithValue("name", applicationName!);
            if ((long)(await command.ExecuteScalarAsync())! == 0)
            {
                return;
            }

            DateTime.UtcNow.Should().BeBefore(deadline, "the discarded session should have ended");
            await Task.Delay(20);
        }
    }

    private sealed class PrimaryOperationException : Exception;

    private sealed class CloseFailureException : Exception;
}
