using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using Npgsql;

namespace Cohort.Sample.Tests;

// Pattern #1 (end-to-end): readiness reads the real catalog, so a hold relation's link column
// is checked against a real table. The schema is created from the test-local model, drifted with
// raw DDL, and the public IRetentionSweep refuses to run.
[Collection("Integration")]
public sealed class HeldWithReadinessEndToEndTests(PostgresFixture fixture)
{
    private static readonly DateTimeOffset AsOf = new(2026, 11, 2, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task A_Schema_Matching_The_Model_Passes_Readiness()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildMigratedServicesAsync(database.ConnectionString);

        var result = await SweepAsync(services);

        result.EntityFailures.Should().BeEmpty();
    }

    [Theory]
    [InlineData("held_with_links", "CaseId", "[HeldWith(typeof(", nameof(ReadinessLink))]
    [InlineData("held_with_cases", "SourceSubmissionId", "[HeldWithReferencing(typeof(", nameof(ReadinessSubmission))]
    public async Task A_Link_Column_Missing_From_The_Real_Table_Fails_Readiness(
        string table,
        string column,
        string attribute,
        string declaringType
    )
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildMigratedServicesAsync(database.ConnectionString);
        await ExecuteAsync(database.ConnectionString, $"ALTER TABLE public.{table} DROP COLUMN \"{column}\"");

        var act = () => SweepAsync(services);

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        var error = exception.Which.Errors.Should().ContainSingle().Which;
        error.Should().Contain(attribute);
        error.Should().Contain($"on {typeof(HeldWithReadinessEndToEndTests).FullName}+{declaringType}");
        error.Should().Contain($"link column \"public\".\"{table}\".\"{column}\" was not found in the database");
    }

    [Theory]
    [InlineData("held_with_links", "CaseId", "text", "uuid", nameof(ReadinessLink))]
    [InlineData("held_with_cases", "SourceSubmissionId", "bigint", "uuid", nameof(ReadinessSubmission))]
    public async Task A_Link_Column_Whose_Catalog_Type_Differs_From_The_Target_Record_Id_Fails_Readiness(
        string table,
        string column,
        string linkType,
        string recordIdType,
        string declaringType
    )
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildMigratedServicesAsync(database.ConnectionString);
        await ExecuteAsync(
            database.ConnectionString,
            $"ALTER TABLE public.{table} ALTER COLUMN \"{column}\" TYPE {linkType} USING NULL"
        );

        var act = () => SweepAsync(services);

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        var error = exception.Which.Errors.Should().ContainSingle().Which;
        error.Should().Contain($"on {typeof(HeldWithReadinessEndToEndTests).FullName}+{declaringType}");
        error.Should().Contain($"link column \"public\".\"{table}\".\"{column}\" is '{linkType}'");
        error.Should().Contain($"is '{recordIdType}'");
    }

    [Fact]
    public async Task A_Link_Column_Built_On_The_Record_Ids_Type_Through_A_Domain_Passes_Readiness()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildMigratedServicesAsync(database.ConnectionString);
        await ExecuteAsync(
            database.ConnectionString,
            """
            CREATE DOMAIN public.link_id AS uuid;
            CREATE DOMAIN public.nested_link_id AS public.link_id;
            ALTER TABLE public.held_with_links ALTER COLUMN "CaseId" TYPE public.nested_link_id;
            """
        );

        var result = await SweepAsync(services);

        result.EntityFailures.Should().BeEmpty();
    }

    private static Task<RetentionSweepResult> SweepAsync(ServiceProvider services) =>
        services
            .GetRequiredService<IRetentionSweep>()
            .ExecuteAsync(
                RetentionSweepRequest.Tenanted(
                    new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                    AsOf
                )
            );

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<ServiceProvider> BuildMigratedServicesAsync(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().AddInMemoryCollection().Build());
        services.AddLogging();
        services.AddDbContext<HeldWithReadinessDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(new PurgeCategoryProvider());
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<HeldWithReadinessDbContext>();
        var provider = services.BuildServiceProvider(validateScopes: true);

        await using var scope = provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<HeldWithReadinessDbContext>().Database.EnsureCreatedAsync();
        return provider;
    }

    private sealed class PurgeCategoryProvider : ITestRetentionRuleProvider
    {
        private static readonly ITestRetentionRule Resolver = new StaticTestRetentionRule(
            new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge)
        );

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct) =>
            Task.FromResult<ITestRetentionRule?>(category == "held-with-readiness" ? Resolver : null);
    }

    private sealed class HeldWithReadinessDbContext(DbContextOptions<HeldWithReadinessDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ReadinessCase>(entity => entity.ToTable("held_with_cases"));
            modelBuilder.Entity<ReadinessLink>(entity => entity.ToTable("held_with_links"));
            modelBuilder.Entity<ReadinessSubmission>(entity => entity.ToTable("held_with_submissions"));
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("held-with-readiness", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0005-000000000001")]
    private sealed class ReadinessCase
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid? SourceSubmissionId { get; init; }
    }

    [Retain("held-with-readiness", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0005-000000000002")]
    [HeldWith(typeof(ReadinessCase), nameof(CaseId))]
    private sealed class ReadinessLink
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid? CaseId { get; init; }
    }

    [Retain("held-with-readiness", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0005-000000000003")]
    [HeldWithReferencing(typeof(ReadinessCase), nameof(ReadinessCase.SourceSubmissionId))]
    private sealed class ReadinessSubmission
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }
}
