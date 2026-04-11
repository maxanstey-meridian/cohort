using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class SampleMigrationsEndToEndTests(PostgresFixture fixture) : IAsyncLifetime
{
    private readonly string databaseName = $"cohort_migration_{Guid.NewGuid():N}";
    private string connectionString = "";

    public async Task InitializeAsync()
    {
        var adminConnectionString = CreateAdminConnectionString(fixture.ConnectionString);

        await using var connection = new NpgsqlConnection(adminConnectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = $"CREATE DATABASE \"{databaseName}\"";
        await command.ExecuteNonQueryAsync();

        var builder = new NpgsqlConnectionStringBuilder(fixture.ConnectionString)
        {
            Database = databaseName,
        };
        connectionString = builder.ConnectionString;
    }

    public async Task DisposeAsync()
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var adminConnectionString = CreateAdminConnectionString(fixture.ConnectionString);

        await using var connection = new NpgsqlConnection(adminConnectionString);
        await connection.OpenAsync();

        await using (var terminate = connection.CreateCommand())
        {
            terminate.CommandText =
                $"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{databaseName}'
                  AND pid <> pg_backend_pid()
                """;
            await terminate.ExecuteNonQueryAsync();
        }

        await using var drop = connection.CreateCommand();
        drop.CommandText = $"DROP DATABASE IF EXISTS \"{databaseName}\"";
        await drop.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Add_Note_Tenant_Migration_Drops_Legacy_Notes_Instead_Of_Backfilling_Guid_Empty()
    {
        var options = CreateOptions();

        await using (var db = new SampleDbContext(options))
        {
            var migrator = db.Database.GetService<IMigrator>();
            await migrator.MigrateAsync("20260411230000_AddExemptDocument");

            await db.Database.ExecuteSqlRawAsync(
                """
                INSERT INTO "notes" ("Id", "CreatedAt", "Body")
                VALUES ({0}, {1}, {2})
                """,
                Guid.NewGuid(),
                new DateTimeOffset(2026, 4, 10, 12, 0, 0, TimeSpan.Zero),
                "legacy-note"
            );

            await migrator.MigrateAsync();
        }

        await using var verify = new SampleDbContext(options);
        var legacyCount = await verify.Notes.CountAsync();

        legacyCount.Should().Be(0);
    }

    private DbContextOptions<SampleDbContext> CreateOptions()
    {
        return new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(connectionString)
            .Options;
    }

    private static string CreateAdminConnectionString(string originalConnectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(originalConnectionString)
        {
            Database = "postgres",
        };

        return builder.ConnectionString;
    }
}
