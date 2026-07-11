using Cohort.Application;
using Cohort.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class CohortSchemaValidatorEndToEndTests(PostgresFixture fixture) : IAsyncLifetime
{
    private readonly string databaseName = $"cohort_schema_validation_{Guid.NewGuid():N}";
    private string connectionString = "";

    public async Task InitializeAsync()
    {
        var adminConnectionString = CreateAdminConnectionString(fixture.ConnectionString);
        await using var connection = new NpgsqlConnection(adminConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"CREATE DATABASE \"{databaseName}\"";
        await command.ExecuteNonQueryAsync();

        connectionString = new NpgsqlConnectionStringBuilder(fixture.ConnectionString)
        {
            Database = databaseName,
        }.ConnectionString;

        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsql(connectionString)
            .Options;
        await using var db = new SampleDbContext(options);
        await db.Database.MigrateAsync();
    }

    public async Task DisposeAsync()
    {
        var adminConnectionString = CreateAdminConnectionString(fixture.ConnectionString);
        await using var connection = new NpgsqlConnection(adminConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"DROP DATABASE IF EXISTS \"{databaseName}\" WITH (FORCE)";
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Validation_Accepts_The_Current_Migrated_Schema()
    {
        using var host = new CohortTestHost(connectionString);

        await host.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<CohortSchemaValidator>().ValidateAsync(default)
        );
    }

    [Theory]
    [InlineData(
        "ALTER TABLE \"sweep_run\" DROP CONSTRAINT \"CK_sweep_run_Terminal_Settled\"",
        "sweep_run.CK_sweep_run_Terminal_Settled"
    )]
    [InlineData(
        "ALTER TABLE \"sweep_row_handler_status\" DROP CONSTRAINT \"CK_sweep_row_handler_status_Claim\"",
        "sweep_row_handler_status.CK_sweep_row_handler_status_Claim"
    )]
    public async Task Validation_Rejects_Partial_Schemas_Missing_Required_Checks(
        string mutation,
        string expectedCapability
    )
    {
        await ExecuteAsync(mutation);
        using var host = new CohortTestHost(connectionString);

        var act = () => host.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<CohortSchemaValidator>().ValidateAsync(default)
        );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error => error.Contains(expectedCapability));
    }

    [Theory]
    [InlineData(
        "ALTER TABLE \"sweep_run\" DROP CONSTRAINT \"CK_sweep_run_Status_Range\"; ALTER TABLE \"sweep_run\" ADD CONSTRAINT \"CK_sweep_run_Status_Range\" CHECK (\"Status\" BETWEEN 0 AND 5)",
        "sweep_run.CK_sweep_run_Status_Range"
    )]
    [InlineData(
        "ALTER TABLE \"sweep_row_handler_status\" DROP CONSTRAINT \"CK_sweep_row_handler_status_Completion\"; ALTER TABLE \"sweep_row_handler_status\" ADD CONSTRAINT \"CK_sweep_row_handler_status_Completion\" CHECK (\"CompletedAt\" IS NULL)",
        "sweep_row_handler_status.CK_sweep_row_handler_status_Completion"
    )]
    public async Task Validation_Rejects_Adopted_Schemas_With_Malformed_Checks(
        string mutation,
        string expectedCapability
    )
    {
        await ExecuteAsync(mutation);
        using var host = new CohortTestHost(connectionString);

        var act = () => host.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<CohortSchemaValidator>().ValidateAsync(default)
        );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle(error => error.Contains(expectedCapability));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Validation_Rejects_Missing_Or_NonCascading_Row_Handler_Foreign_Key(
        bool replaceWithoutCascade
    )
    {
        const string lookup = """
            SELECT conname
            FROM pg_constraint
            WHERE contype = 'f'
              AND conrelid = 'sweep_row_handler_status'::regclass
            """;
        var constraintName = (string)(await ExecuteScalarAsync(lookup))!;
        await ExecuteAsync(
            $"ALTER TABLE \"sweep_row_handler_status\" DROP CONSTRAINT \"{constraintName.Replace("\"", "\"\"")}\""
        );
        if (replaceWithoutCascade)
        {
            await ExecuteAsync(
                "ALTER TABLE \"sweep_row_handler_status\" ADD CONSTRAINT \"FK_adopted_row_detail\" FOREIGN KEY (\"SweepRunRowDetailId\") REFERENCES \"sweep_run_row_detail\" (\"Id\") ON DELETE RESTRICT"
            );
        }

        using var host = new CohortTestHost(connectionString);
        var act = () => host.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<CohortSchemaValidator>().ValidateAsync(default)
        );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .ContainSingle(error => error.Contains("sweep_row_handler_status(SweepRunRowDetailId) -> sweep_run_row_detail(Id) ON DELETE CASCADE"));
    }

    [Fact]
    public async Task Validation_Reports_Restrictive_Foreign_Keys_With_Their_Required_Action()
    {
        const string lookup = """
            SELECT conname
            FROM pg_constraint
            WHERE contype = 'f'
              AND conrelid = 'sweep_run_entity_summary'::regclass
            """;
        var constraintName = (string)(await ExecuteScalarAsync(lookup))!;
        await ExecuteAsync(
            $"ALTER TABLE \"sweep_run_entity_summary\" DROP CONSTRAINT \"{constraintName.Replace("\"", "\"\"")}\""
        );

        using var host = new CohortTestHost(connectionString);
        var act = () => host.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<CohortSchemaValidator>().ValidateAsync(default)
        );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .ContainSingle(error => error.Contains("sweep_run_entity_summary(SweepId) -> sweep_run(SweepId) ON DELETE RESTRICT"));
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private async Task<object?> ExecuteScalarAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync();
    }

    private static string CreateAdminConnectionString(string originalConnectionString)
    {
        return new NpgsqlConnectionStringBuilder(originalConnectionString)
        {
            Database = "postgres",
        }.ConnectionString;
    }
}
