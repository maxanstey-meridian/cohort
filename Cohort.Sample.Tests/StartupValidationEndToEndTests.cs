using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Cohort.Sample.Tests;

// ─── EXEMPLAR #3 — end-to-end test ──────────────────────────────────────────
//
// Pattern: end-to-end test. THIS IS THE PATTERN.
//
// Feed real data in the front. Run the real code path. Assert what comes out
// the back. Use this whenever the code under test touches a port (DbContext,
// IOptions with real config binding, IHostedService, file/HTTP I/O).
//
// Copy this file. Rename it. Edit the seed and assertions.
//
// Do NOT abstract.
// Do NOT share a base class beyond IntegrationTestBase.
// Do NOT add mocks — NSubstitute is intentionally absent from this project.
//
// When you add a new port `IFoo`, the same PR adds an end-to-end test here that
// exercises the REAL implementation against PostgresFixture. Non-negotiable.
// See CLAUDE.md.
// ────────────────────────────────────────────────────────────────────────────

public sealed class StartupValidationEndToEndTests : IntegrationTestBase
{
    private readonly string connectionString;

    public StartupValidationEndToEndTests(PostgresFixture fixture)
        : base(fixture)
    {
        connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task Startup_Path_Runs_Validation_Before_Returning_Registry_Entries()
    {
        var entries = await Host.RunStartupAsync();

        entries
            .Should()
            .Contain(kvp =>
                kvp.Key == typeof(Note)
                && kvp.Value.Category == "short-lived"
                && kvp.Value.AnchorMember == nameof(Note.CreatedAt)
            );
        entries
            .Should()
            .Contain(kvp =>
                kvp.Key == typeof(SoftDeleteRecord)
                && kvp.Value.Category == "soft-delete"
                && kvp.Value.AnchorMember == nameof(SoftDeleteRecord.CreatedAt)
            );
        entries
            .Should()
            .Contain(kvp =>
                kvp.Key == typeof(AnonymisedContact)
                && kvp.Value.Category == "anonymise"
                && kvp.Value.AnchorMember == nameof(AnonymisedContact.CreatedAt)
            );
    }

    [Fact]
    public async Task Startup_Path_Fails_When_Category_Resolution_Is_Misconfigured()
    {
        await using var db = Host.CreateDbContext();
        var connectionString = db.Database.GetConnectionString()!;
        using var host = new CohortTestHost(connectionString, new EmptyCategoryRepository());

        var act = async () => await host.RunStartupAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().HaveCount(6);
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'short-lived' for entity {typeof(Note).FullName} could not be resolved."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'soft-delete' for entity {typeof(SoftDeleteRecord).FullName} could not be resolved."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'anonymise' for entity {typeof(AnonymisedContact).FullName} could not be resolved."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'tenantless-purge' for entity {typeof(TenantlessLog).FullName} could not be resolved."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'tenantless-softdelete' for entity {typeof(TenantlessSoftDelete).FullName} could not be resolved."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'per-row-audit-override' for entity {typeof(PerRowAuditedLog).FullName} could not be resolved."
            );
    }

    [Fact]
    public async Task Shared_Test_Host_Uses_The_Cohort_Di_Entry_Point()
    {
        await Host.RunWithServicesAsync(
            serviceProvider =>
            {
                serviceProvider.GetServices<IHostedService>()
                    .Should()
                    .ContainSingle(service => service.GetType() == typeof(RetentionWorker));

                return Task.CompletedTask;
            }
        );
    }

    [Fact]
    public async Task Startup_Path_Fails_When_AnonymiseWith_Uses_A_Type_That_Does_Not_Implement_The_Factory_Port()
    {
        var act = async () =>
            await RunFactoryValidationStartupAsync<InvalidFactoryTypeStartupDbContext>(
                new SingleCategoryRepository(
                    "invalid-factory-type",
                    new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    )
                )
            );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Anonymise convention on {typeof(InvalidFactoryTypeStartupRecord).FullName}: [AnonymiseWith] member ExternalId specifies factory type {typeof(NotAFactory).FullName} which does not implement {nameof(IAnonymiseValueFactory)}."
            );
    }

    [Fact]
    public async Task Startup_Path_Fails_When_AnonymiseWith_Factory_Is_Not_Registered_In_Di()
    {
        var act = async () =>
            await RunFactoryValidationStartupAsync<UnregisteredFactoryStartupDbContext>(
                new SingleCategoryRepository(
                    "unregistered-factory",
                    new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    )
                )
            );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Anonymise convention on {typeof(UnregisteredFactoryStartupRecord).FullName}: [AnonymiseWith] member ExternalId specifies factory type {typeof(RegisteredFactory).FullName} but no matching {nameof(IAnonymiseValueFactory)} is registered in DI."
            );
    }

    [Fact]
    public async Task Startup_Path_Allows_Registered_FactoryBacked_Anonymise_Metadata_Without_Literal_Type_Checks()
    {
        var entries = await RunFactoryValidationStartupAsync<RegisteredFactoryStartupDbContext>(
            new SingleCategoryRepository(
                "registered-factory",
                new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                )
            ),
            services => services.AddSingleton<IAnonymiseValueFactory, RegisteredFactory>()
        );

        entries.Should().ContainKey(typeof(RegisteredFactoryStartupRecord));
        entries[typeof(RegisteredFactoryStartupRecord)]
            .AnonymiseFields.Should()
            .ContainSingle(field => field is AnonymiseFactoryField);
    }

    private async Task<IReadOnlyDictionary<Type, RetentionEntry>> RunFactoryValidationStartupAsync<TContext>(
        IRetentionCategoryRepository categoryRepository,
        Action<IServiceCollection>? registerServices = null
    )
        where TContext : DbContext
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>())
            .Build();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging();
        services.AddDbContext<TContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton(categoryRepository);
        registerServices?.Invoke(services);
        services.AddCohort<TContext>();
        services.AddScoped<SampleRetentionStartupService>();

        await using var serviceProvider = services.BuildServiceProvider(validateScopes: true);
        await using var scope = serviceProvider.CreateAsyncScope();
        var startup = scope.ServiceProvider.GetRequiredService<SampleRetentionStartupService>();
        return await startup.RunAsync();
    }

    private sealed class EmptyCategoryRepository : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct) =>
            Task.FromResult<IRetentionRuleResolver?>(null);
    }

    private sealed class SingleCategoryRepository(string category, IRetentionRuleResolver resolver)
        : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string requestedCategory, CancellationToken ct)
        {
            return Task.FromResult<IRetentionRuleResolver?>(
                requestedCategory == category
                    ? resolver
                    : throw new InvalidOperationException(
                        $"Unexpected category lookup for '{requestedCategory}'."
                    )
            );
        }
    }

    private sealed class InvalidFactoryTypeStartupDbContext(
        DbContextOptions<InvalidFactoryTypeStartupDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<InvalidFactoryTypeStartupRecord>(entity =>
            {
                entity.ToTable("invalid_factory_type_startup_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.ExternalId).HasColumnName("external_id");
            });
        }
    }

    private sealed class UnregisteredFactoryStartupDbContext(
        DbContextOptions<UnregisteredFactoryStartupDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<UnregisteredFactoryStartupRecord>(entity =>
            {
                entity.ToTable("unregistered_factory_startup_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.ExternalId).HasColumnName("external_id");
            });
        }
    }

    private sealed class RegisteredFactoryStartupDbContext(
        DbContextOptions<RegisteredFactoryStartupDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<RegisteredFactoryStartupRecord>(entity =>
            {
                entity.ToTable("registered_factory_startup_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.ExternalId).HasColumnName("external_id");
            });
        }
    }

    [Retain("invalid-factory-type", nameof(InvalidFactoryTypeStartupRecord.CreatedAt))]
    private sealed class InvalidFactoryTypeStartupRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [AnonymiseWith(typeof(NotAFactory))]
        public Guid ExternalId { get; init; }
    }

    [Retain("unregistered-factory", nameof(UnregisteredFactoryStartupRecord.CreatedAt))]
    private sealed class UnregisteredFactoryStartupRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [AnonymiseWith(typeof(RegisteredFactory))]
        public Guid ExternalId { get; init; }
    }

    [Retain("registered-factory", nameof(RegisteredFactoryStartupRecord.CreatedAt))]
    private sealed class RegisteredFactoryStartupRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [AnonymiseWith(typeof(RegisteredFactory))]
        public Guid ExternalId { get; init; }
    }

    private sealed class RegisteredFactory : IAnonymiseValueFactory
    {
        public object? Create(AnonymiseValueContext context) => Guid.Empty;
    }

    private sealed class NotAFactory;
}
