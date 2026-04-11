using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public abstract class IntegrationTestBase(PostgresFixture fixture) : IAsyncLifetime
{
    protected CohortTestHost Host { get; private set; } = default!;

    public async Task InitializeAsync()
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await fixture.Respawner.ResetAsync(connection);

        Host = new CohortTestHost(fixture.ConnectionString);
    }

    public Task DisposeAsync()
    {
        Host.Dispose();
        return Task.CompletedTask;
    }
}
