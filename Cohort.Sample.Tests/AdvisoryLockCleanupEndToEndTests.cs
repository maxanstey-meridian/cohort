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

    private sealed class PrimaryOperationException : Exception;

    private sealed class CloseFailureException : Exception;
}
