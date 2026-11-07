using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Migrations;
using Cohort.Infrastructure.Sweep;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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
    public async Task Validation_Checks_Every_Strategy_Declared_For_A_Dynamic_Category()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                [$"{SampleOptions.SectionName}:{nameof(SampleOptions.ConnectionString)}"] =
                    connectionString,
            }
        );
        builder.Services.AddSampleRetentionServices();
        builder.Services.RemoveAll<IRetentionRuleProvider>();
        builder.Services.AddSingleton<IRetentionRuleProvider, MultiStrategyRuleProvider>();
        using var host = builder.Build();

        var act = () => host.StartAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .Contain(error =>
                error.Contains(typeof(SoftDeleteRecord).FullName!, StringComparison.Ordinal)
                && error.Contains("Anonymise", StringComparison.Ordinal)
            );
    }

    // Only AddRetentionDependents makes a dependents step: a step service it didn't register
    // would never run while its rows are still mutated, so startup refuses it, and so does a
    // sweep's own readiness check on a host that was never started. Nothing is constructed to
    // find out: the step's constructor throws.
    [Theory]
    [InlineData("type")]
    [InlineData("instance")]
    [InlineData("factory")]
    [InlineData("duplicate")]
    [InlineData("singleton-first")]
    [InlineData("factory-first")]
    [InlineData("service-removed")]
    [InlineData("keyed")]
    [InlineData("open-generic")]
    public async Task Validation_Refuses_A_Dependents_Step_Not_Registered_With_AddRetentionDependents(string registration)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                [$"{SampleOptions.SectionName}:{nameof(SampleOptions.ConnectionString)}"] =
                    connectionString,
            }
        );
        builder.Services.AddSampleRetentionServices();
        switch (registration)
        {
            case "type":
                builder.Services.AddScoped<IRetentionDependents<Note>, UnconstructableNoteStep>();
                break;
            case "instance":
                builder.Services.AddSingleton<IRetentionDependents<Note>>(new InstanceNoteStep());
                break;
            case "factory":
                builder.Services.AddScoped<IRetentionDependents<Note>>(_ => new UnconstructableNoteStep());
                break;
            case "duplicate":
                builder.Services.AddRetentionDependents<Note, UnconstructableNoteStep>();
                builder.Services.AddScoped<IRetentionDependents<Note>, UnconstructableNoteStep>();
                break;
            case "singleton-first":
                builder.Services.AddSingleton<IRetentionDependents<Note>, UnconstructableNoteStep>();
                builder.Services.AddRetentionDependents<Note, UnconstructableNoteStep>();
                break;
            case "factory-first":
                builder.Services.AddScoped<IRetentionDependents<Note>, UnconstructableNoteStep>(_ => new UnconstructableNoteStep());
                builder.Services.AddRetentionDependents<Note, UnconstructableNoteStep>();
                break;
            case "service-removed":
                builder.Services.AddRetentionDependents<Note, UnconstructableNoteStep>();
                builder.Services.RemoveAll<IRetentionDependents<Note>>();
                break;
            case "keyed":
                builder.Services.AddKeyedScoped<IRetentionDependents<Note>, UnconstructableNoteStep>("k");
                break;
            case "open-generic":
                builder.Services.AddScoped(typeof(IRetentionDependents<>), typeof(OpenStep<>));
                break;
        }

        using var host = builder.Build();

        var start = () => host.StartAsync();
        var sweep = () =>
            host.Services.GetRequiredService<IRetentionSweep>()
                .ExecuteAsync(RetentionSweepRequest.Tenantless(dryRun: true));

        var named = registration switch
        {
            "factory" or "factory-first" or "keyed" => $"IRetentionDependents<{typeof(Note).FullName}>",
            "open-generic" => "IRetentionDependents<>",
            "instance" => typeof(InstanceNoteStep).FullName!,
            _ => typeof(UnconstructableNoteStep).FullName!,
        };
        foreach (var act in new[] { start, sweep })
        {
            var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
            exception
                .Which.Errors.Should()
                .ContainSingle(error =>
                    error.Contains(named, StringComparison.Ordinal)
                    && error.Contains("AddRetentionDependents", StringComparison.Ordinal)
                );
        }

        UnconstructableNoteStep.Constructed.Should().BeFalse();
    }

    // A container can resolve a step the IServiceCollection scan never saw; resolving it
    // refuses it the same way instead of failing on a missing registration.
    [Fact]
    public void Resolving_A_Step_Without_A_Registration_Refuses_It()
    {
        var services = new ServiceCollection();
        services.AddRetentionDependents<Note, InstanceNoteStep>();
        services.AddScoped<IRetentionDependents<Note>, StrayNoteStep>();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var act = () => RetentionHandlerSupport.ResolveDependents(scope.ServiceProvider, typeof(Note));

        act.Should().Throw<RetentionConfigurationException>()
            .Which.Errors.Should().ContainSingle(error =>
                error.Contains(typeof(StrayNoteStep).FullName!, StringComparison.Ordinal)
                && error.Contains("AddRetentionDependents", StringComparison.Ordinal)
            );
    }

    // Startup refuses both shapes (a removed service, and a duplicate), so only a provider
    // the IServiceCollection scan never validated (another container) reaches them here.
    [Theory]
    [InlineData("removed", 0)]
    [InlineData("several", 2)]
    public void Resolving_A_Registered_Step_Without_Exactly_One_Service_Refuses_It(string registration, int resolved)
    {
        var services = new ServiceCollection();
        services.AddRetentionDependents<Note, InstanceNoteStep>();
        if (registration == "removed")
        {
            services.RemoveAll<IRetentionDependents<Note>>();
        }
        else
        {
            services.AddScoped<IRetentionDependents<Note>, InstanceNoteStep>();
        }

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var act = () => RetentionHandlerSupport.ResolveDependents(scope.ServiceProvider, typeof(Note));

        act.Should().Throw<RetentionConfigurationException>()
            .Which.Errors.Should().ContainSingle(error =>
                error.Contains(typeof(InstanceNoteStep).FullName!, StringComparison.Ordinal)
                && error.Contains($"resolves {resolved} IRetentionDependents", StringComparison.Ordinal)
            );
    }

    [Fact]
    public void Startup_Names_A_Retained_Entity_Whose_RetentionEntityId_Is_Malformed()
    {
        var options = new DbContextOptionsBuilder<MalformedEntityIdDbContext>()
            .UseNpgsqlMetadataModel($"malformed-entity-id-{Guid.NewGuid()}")
            .Options;
        using var db = new MalformedEntityIdDbContext(options);

        var build = () => new RetentionEntryBuilder(new CohortConventions())
            .TryBuild(db.Model.FindEntityType(typeof(MalformedEntityIdRecord))!);

        build.Should().Throw<InvalidOperationException>()
            .WithMessage($"*{typeof(MalformedEntityIdRecord).FullName}*[RetentionEntityId] is invalid*non-empty UUID*");
    }

    private sealed class MalformedEntityIdDbContext(DbContextOptions<MalformedEntityIdDbContext> options)
        : DbContext(options)
    {
        public DbSet<MalformedEntityIdRecord> Records => Set<MalformedEntityIdRecord>();
    }

    [Retain("short-lived", nameof(CreatedAt))]
    [RetentionEntityId("not-a-uuid")]
    private sealed class MalformedEntityIdRecord
    {
        public Guid Id { get; set; }

        public DateTimeOffset CreatedAt { get; set; }
    }

    // NULL marks the rows still to anonymise. A default or value generation mapped on the marker
    // makes EF leave it out of an insert, so the database fills it in and every row is born
    // marked as anonymised: startup refuses it, since no database check could tell.
    [Theory]
    [InlineData("default-sql")]
    [InlineData("value-generated")]
    public async Task Startup_refuses_an_anonymised_at_marker_with_a_mapped_default(string mapping)
    {
        var errors = mapping switch
        {
            "default-sql" => await StartupErrorsAsync<MarkerDefaultSqlDbContext>(),
            _ => await StartupErrorsAsync<MarkerValueGeneratedDbContext>(),
        };

        errors.Should().ContainSingle(error =>
            error.Contains(typeof(MarkerRecord).FullName!, StringComparison.Ordinal)
            && error.Contains("the AnonymisedAt marker AnonymisedAt has a default or value generation mapped", StringComparison.Ordinal)
        );
    }

    private async Task<IReadOnlyList<string>> StartupErrorsAsync<TContext>()
        where TContext : DbContext
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddDbContext<TContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(new MarkerRuleProvider());
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<TContext>();
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        var start = () => provider
            .GetServices<IHostedService>()
            .Single(service => service.GetType().Name == "RetentionValidationHostedService")
            .StartAsync(CancellationToken.None);

        return (await start.Should().ThrowAsync<RetentionConfigurationException>()).Which.Errors;
    }

    private sealed class MarkerDefaultSqlDbContext(DbContextOptions<MarkerDefaultSqlDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder
                .Entity<MarkerRecord>()
                .ToTable("marker_records")
                .Property(record => record.AnonymisedAt)
                .HasDefaultValueSql("now()");
            modelBuilder.ConfigureCohortTables();
        }
    }

    private sealed class MarkerValueGeneratedDbContext(DbContextOptions<MarkerValueGeneratedDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder
                .Entity<MarkerRecord>()
                .ToTable("marker_records")
                .Property(record => record.AnonymisedAt)
                .ValueGeneratedOnAdd();
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("marker", nameof(CreatedAt))]
    [RetentionEntityId("d4a1f8e3-6b25-4c97-8e0d-3a7c9b2f5e18")]
    [RetentionTenantless]
    private sealed class MarkerRecord
    {
        public Guid Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }

        [Anonymise(AnonymiseMethod.EmptyString)]
        public string Label { get; set; } = "";

        public DateTimeOffset? AnonymisedAt { get; set; }
    }

    private sealed class MarkerRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == "marker" ? new RetentionCategoryCapabilities([Strategy.Anonymise]) : null;

        public Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct) =>
            Task.FromResult<RetentionRule?>(new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Anonymise));
    }

    private sealed class UnconstructableNoteStep : IRetentionDependents<Note>
    {
        public static bool Constructed { get; private set; }

        public UnconstructableNoteStep()
        {
            Constructed = true;
            throw new InvalidOperationException("Startup must not construct a dependents step.");
        }

        public Task RetireAsync(Note row, RetentionDependentsContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class InstanceNoteStep : IRetentionDependents<Note>
    {
        public Task RetireAsync(Note row, RetentionDependentsContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class StrayNoteStep : IRetentionDependents<Note>
    {
        public Task RetireAsync(Note row, RetentionDependentsContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class OpenStep<TEntity> : IRetentionDependents<TEntity>
    {
        public Task RetireAsync(TEntity row, RetentionDependentsContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class MultiStrategyRuleProvider : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == "soft-delete"
                ? new([Strategy.SoftDelete, Strategy.Anonymise])
                : inner.GetCapabilities(category);

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        ) => inner.ResolveAsync(context, ct);
    }

    private sealed class SingleCategoryRepository(string category, ITestRetentionRule resolver)
        : ITestRetentionRuleProvider
    {
        public Task<ITestRetentionRule?> GetAsync(
            string requestedCategory,
            CancellationToken ct
        )
        {
            return Task.FromResult<ITestRetentionRule?>(
                requestedCategory == category
                    ? resolver
                    : throw new InvalidOperationException(
                        $"Unexpected category lookup for '{requestedCategory}'."
                    )
            );
        }
    }

    private sealed class DeferredRuleResolver(RetentionRule rule) : ITestRetentionRule
    {
        public Task<RetentionRule> ResolveAsync(
            RetentionResolutionContext ctx,
            CancellationToken ct
        ) => Task.FromResult(rule);
    }

    private sealed class NotAFactory;
}
