using Microsoft.EntityFrameworkCore;

using Npgsql;

using Respawn;

using Testcontainers.PostgreSql;

namespace Cohort.Sample.Tests;

public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public string ConnectionString => _container.GetConnectionString();
    public Respawner Respawner { get; private set; } = default!;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();

        var options = new DbContextOptionsBuilder<SampleDbContext>().UseNpgsql(ConnectionString).Options;
        await using var db = new SampleDbContext(options);
        await db.Database.MigrateAsync();

        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();

        Respawner = await Respawner.CreateAsync(
            connection,
            new RespawnerOptions
            {
                DbAdapter = DbAdapter.Postgres,
                SchemasToInclude = ["public"],
                TablesToIgnore = [new("__EFMigrationsHistory")],
            }
        );
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}
