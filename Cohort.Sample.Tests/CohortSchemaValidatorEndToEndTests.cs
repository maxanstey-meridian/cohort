using System.Collections.Concurrent;
using Cohort.Application;
using Cohort.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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

    [Theory]
    [InlineData(
        "ALTER TABLE \"retention_holds\" DROP CONSTRAINT \"PK_retention_holds\"",
        "primary key capability 'retention_holds(HoldId)' on table '\"public\".\"retention_holds\"'"
    )]
    [InlineData(
        "DROP INDEX \"IX_sweep_run_row_detail_StableIdentity\"",
        "index capability 'sweep_run_row_detail(SweepId, RetentionEntityId, RecordId, Category, Strategy, TenantId)' on table '\"public\".\"sweep_run_row_detail\"'"
    )]
    public async Task Validation_Rejects_Missing_Key_And_Unique_Index_Capabilities(
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

    [Fact]
    public async Task Validation_Warns_But_Passes_When_A_Recommended_Index_Is_Missing()
    {
        await ExecuteAsync("DROP INDEX \"IX_retention_holds_RetentionEntityId_RecordId\"");
        var logs = new RecordingLogProvider();
        using var host = new CohortTestHost(
            connectionString,
            configureServices: services => services.AddSingleton<ILoggerProvider>(logs)
        );

        var act = () => host.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<CohortSchemaValidator>().ValidateAsync(default)
        );

        await act.Should().NotThrowAsync();
        logs.Entries.Should().ContainSingle(entry =>
            entry.Level == LogLevel.Warning
            && entry.Message.Contains(
                "index 'retention_holds(RetentionEntityId, RecordId)' on table '\"public\".\"retention_holds\"'"
            )
        );
    }

    [Theory]
    [InlineData(
        "ALTER TABLE \"sweep_run\" DROP CONSTRAINT \"CK_sweep_run_Status_Range\"; ALTER TABLE \"sweep_run\" ADD CONSTRAINT \"CK_sweep_run_Status_Range\" CHECK (\"Status\" BETWEEN 0 AND 4) NOT VALID",
        "sweep_run.CK_sweep_run_Status_Range"
    )]
    [InlineData(
        "ALTER TABLE \"sweep_row_handler_status\" DROP CONSTRAINT \"CK_sweep_row_handler_status_Completion\"",
        "sweep_row_handler_status.CK_sweep_row_handler_status_Completion"
    )]
    public async Task Validation_Rejects_Missing_Or_Unvalidated_Checks(
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
    [InlineData("ALTER TABLE \"sweep_run\" ALTER COLUMN \"DryRun\" TYPE text USING \"DryRun\"::text", "sweep_run.\"DryRun\" bool NOT NULL")]
    [InlineData("ALTER TABLE \"sweep_run_row_detail\" ALTER COLUMN \"CapturedPayload\" SET NOT NULL", "sweep_run_row_detail.\"CapturedPayload\" text NULL")]
    [InlineData("ALTER TABLE \"sweep_row_handler_status\" ALTER COLUMN \"QueuedAt\" DROP NOT NULL", "sweep_row_handler_status.\"QueuedAt\" timestamptz NOT NULL")]
    public async Task Validation_Rejects_Runtime_Critical_Column_Shape(
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
    public async Task Validation_Rejects_A_Deferrable_Summary_Key_Because_On_Conflict_Cannot_Use_It()
    {
        await ExecuteAsync("""
            ALTER TABLE "sweep_run_entity_summary" DROP CONSTRAINT "PK_sweep_run_entity_summary";
            ALTER TABLE "sweep_run_entity_summary" ADD CONSTRAINT "PK_sweep_run_entity_summary"
                PRIMARY KEY ("SweepId", "RetentionEntityId", "Category", "TenantId", "Strategy")
                DEFERRABLE INITIALLY IMMEDIATE
            """);
        using var host = new CohortTestHost(connectionString);

        var act = () => host.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<CohortSchemaValidator>().ValidateAsync(default)
        );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .ContainSingle(error => error.Contains(
                "primary key capability 'sweep_run_entity_summary(SweepId, RetentionEntityId, Category, TenantId, Strategy)'"
            ));
    }

    [Fact]
    public async Task Validation_Accepts_A_Migrated_Schema_Under_A_NonDefault_IntervalStyle()
    {
        // pg_get_expr deparses the Duration check's INTERVAL '0' literal through the session
        // IntervalStyle: '00:00:00'::interval under the default, 'PT0S'::interval under iso_8601.
        await ExecuteAsync($"ALTER DATABASE \"{databaseName}\" SET intervalstyle = 'iso_8601'");
        // Database-level settings only apply to new backends, so drop pooled sessions.
        NpgsqlConnection.ClearAllPools();
        var deparsed = (string)(await ExecuteScalarAsync("""
            SELECT pg_get_expr(conbin, conrelid)
            FROM pg_constraint
            WHERE conname = 'CK_sweep_run_Duration_Nonnegative'
            """))!;
        deparsed.Should().Contain("PT0S");
        using var host = new CohortTestHost(connectionString);

        var act = () => host.RunWithServicesAsync(serviceProvider =>
            serviceProvider.GetRequiredService<CohortSchemaValidator>().ValidateAsync(default)
        );

        await act.Should().NotThrowAsync();
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

    private sealed class RecordingLogProvider : ILoggerProvider
    {
        private readonly ConcurrentQueue<LogEntry> entries = new();

        public IReadOnlyList<LogEntry> Entries => entries.ToArray();

        public ILogger CreateLogger(string categoryName) => new RecordingLogger(entries);

        public void Dispose() { }

        private sealed class RecordingLogger(ConcurrentQueue<LogEntry> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter
            )
            {
                entries.Enqueue(new LogEntry(logLevel, formatter(state, exception)));
            }
        }
    }

    private sealed record LogEntry(LogLevel Level, string Message);

    private static string CreateAdminConnectionString(string originalConnectionString)
    {
        return new NpgsqlConnectionStringBuilder(originalConnectionString)
        {
            Database = "postgres",
        }.ConnectionString;
    }
}
