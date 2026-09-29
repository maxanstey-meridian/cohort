using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class HostCompositionEndToEndTests(PostgresFixture fixture)
{
    [Fact]
    public async Task StartAsync_Rejects_Legacy_Cohort_Schema_When_Host_Migrations_Are_Not_Applied()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await LegacyCohortSchema.BootstrapPreRowDispatchAsync(database.ConnectionString);
        using var host = BuildHost<ValidRetentionDbContext>(
            options => options.UseNpgsql(database.ConnectionString),
            new SingleCategoryRepository(
                "valid",
                new RetentionRule(
                    TimeSpan.FromDays(30),
                    Strategy.Purge,
                    AuditRowDetail: AuditRowDetail.PerRow
                )
            )
        );

        var act = async () => await host.StartAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .ContainSingle(error =>
                error.Contains("\"public\".\"sweep_run\"", StringComparison.Ordinal)
                && error.Contains("Status", StringComparison.Ordinal)
                && error.Contains("\"public\".\"sweep_run_row_detail\"", StringComparison.Ordinal)
                && error.Contains("RetentionEntityId", StringComparison.Ordinal)
                && error.Contains("sweep_row_handler_status", StringComparison.Ordinal)
                && error.Contains("pending EF Core migrations", StringComparison.Ordinal)
            );
    }

    [Fact]
    public async Task Sample_Migrates_A_Fresh_Database_Before_Host_Schema_Validation()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            [$"{SampleOptions.SectionName}:{nameof(SampleOptions.ConnectionString)}"] =
                database.ConnectionString,
        });
        builder.Services.AddSampleRetentionServices();
        using var host = builder.Build();

        await using (var scope = host.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<SampleDbContext>();
            await db.Database.MigrateAsync();
        }

        await host.StartAsync();
        await host.StopAsync();
    }

    [Fact]
    public void AddCohort_Execution_Settings_Keep_The_Last_Valid_Snapshot_After_Invalid_Reload()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    [$"{CohortOptions.SectionName}:RowHandlerDispatch:BatchSize"] = "20",
                }
            )
            .Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(configuration);
        services.AddCohort<ValidRetentionDbContext>();
        using var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredService<IRetentionExecutionSettings>();

        configuration[$"{CohortOptions.SectionName}:RowHandlerDispatch:BatchSize"] = "10001";
        var reload = () => configuration.Reload();

        reload.Should().Throw<OptionsValidationException>();
        settings.RowHandlerDispatch.BatchSize.Should().Be(20);
    }

    private static IHost BuildHost<TContext>(
        Action<DbContextOptionsBuilder> configureDb,
        ITestRetentionRuleProvider categoryRepository,
        IReadOnlyDictionary<string, string?>? settings = null,
        Action<IServiceCollection>? configureServices = null
    )
        where TContext : DbContext
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(settings ?? new Dictionary<string, string?>());
        builder.Services.AddDbContext<TContext>(configureDb);
        builder.Services.AddSingleton<IRetentionRuleProvider>(categoryRepository);
        configureServices?.Invoke(builder.Services);
        builder.Services.AddCohort<TContext>();
        return builder.Build();
    }

    private sealed class SingleCategoryRepository(string category, RetentionRule rule)
        : ITestRetentionRuleProvider
    {
        public Task<ITestRetentionRule?> GetAsync(
            string requestedCategory,
            CancellationToken ct
        ) =>
            Task.FromResult<ITestRetentionRule?>(
                requestedCategory == category ? new StaticTestRetentionRule(rule) : null
            );
    }

    private sealed class ValidRetentionDbContext(DbContextOptions<ValidRetentionDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ValidRetentionRecord>().HasKey(record => record.Id);
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("valid", nameof(ValidRetentionRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000000a")]
    private sealed class ValidRetentionRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }
}
