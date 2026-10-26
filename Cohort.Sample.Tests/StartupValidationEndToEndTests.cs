using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Sweep;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

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

    // Only AddRowHandler makes a row handler: a handler service it didn't register would
    // never run while its rows are still mutated, so startup refuses it, and so does a
    // sweep's own readiness check on a host that was never started. Nothing is constructed
    // to find out: the handler's constructor throws.
    [Theory]
    [InlineData("type")]
    [InlineData("instance")]
    [InlineData("factory")]
    [InlineData("duplicate")]
    [InlineData("singleton-first")]
    [InlineData("instance-first")]
    [InlineData("keyed")]
    [InlineData("open-generic")]
    public async Task Validation_Refuses_A_Handler_Service_Not_Registered_With_AddRowHandler(string registration)
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
                builder.Services.AddScoped<IRetentionHandler<Note>, UnconstructableNoteHandler>();
                break;
            case "instance":
                builder.Services.AddSingleton<IRetentionHandler<Note>>(new InstanceNoteHandler());
                break;
            case "factory":
                builder.Services.AddScoped<IRetentionHandler<Note>>(_ => new UnconstructableNoteHandler());
                break;
            case "duplicate":
                builder.Services.AddRowHandler<Note, UnconstructableNoteHandler>();
                builder.Services.AddScoped<IRetentionHandler<Note>, UnconstructableNoteHandler>();
                break;
            // A singleton or instance registered first makes AddRowHandler's own add a no-op.
            case "singleton-first":
                builder.Services.AddSingleton<IRetentionHandler<Note>, UnconstructableNoteHandler>();
                builder.Services.AddRowHandler<Note, UnconstructableNoteHandler>();
                break;
            case "instance-first":
                builder.Services.AddSingleton<IRetentionHandler<Note>>(new InstanceNoteHandler());
                builder.Services.AddRowHandler<Note, InstanceNoteHandler>();
                break;
            case "keyed":
                builder.Services.AddKeyedScoped<IRetentionHandler<Note>, UnconstructableNoteHandler>("k");
                break;
            case "open-generic":
                builder.Services.AddScoped(typeof(IRetentionHandler<>), typeof(OpenHandler<>));
                break;
        }

        using var host = builder.Build();

        var start = () => host.StartAsync();
        var sweep = () =>
            host.Services.GetRequiredService<IRetentionSweep>()
                .ExecuteAsync(RetentionSweepRequest.Tenantless(dryRun: true));

        var named = registration switch
        {
            "factory" or "keyed" => $"IRetentionHandler<{typeof(Note).FullName}>",
            "open-generic" => "IRetentionHandler<>",
            "instance" or "instance-first" => typeof(InstanceNoteHandler).FullName!,
            _ => typeof(UnconstructableNoteHandler).FullName!,
        };
        foreach (var act in new[] { start, sweep })
        {
            var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
            exception
                .Which.Errors.Should()
                .ContainSingle(error =>
                    error.Contains(named, StringComparison.Ordinal)
                    && error.Contains("AddRowHandler", StringComparison.Ordinal)
                );
        }

        UnconstructableNoteHandler.Constructed.Should().BeFalse();
    }

    // A container can resolve a handler the IServiceCollection scan never saw; resolving it
    // refuses it the same way instead of failing on a missing registration.
    [Fact]
    public void Resolving_A_Handler_Without_A_Registration_Refuses_It()
    {
        var services = new ServiceCollection();
        services.AddRowHandler<Note, InstanceNoteHandler>();
        services.AddScoped<IRetentionHandler<Note>, StrayNoteHandler>();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var act = () => RetentionHandlerSupport.ResolveHandlers(scope.ServiceProvider, typeof(Note));

        act.Should().Throw<RetentionConfigurationException>()
            .Which.Errors.Should().ContainSingle(error =>
                error.Contains(typeof(StrayNoteHandler).FullName!, StringComparison.Ordinal)
                && error.Contains("AddRowHandler", StringComparison.Ordinal)
            );
    }

    // The same for a dependents step: only AddRetentionDependents makes one, and startup and a
    // never-started host's own readiness check refuse any other shape without constructing it.
    [Theory]
    [InlineData("type")]
    [InlineData("instance")]
    [InlineData("factory")]
    [InlineData("duplicate")]
    [InlineData("singleton-first")]
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
            "factory" or "keyed" => $"IRetentionDependents<{typeof(Note).FullName}>",
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

    // The scrub clock starts when a row is mutated and a row's handlers run one after another,
    // so PayloadRetention has to cover every handler's retry schedule on the busiest entity.
    // Each schedule here is 30 minutes and 10 seconds: one 30-minute claim plus one poll.
    [Theory]
    [InlineData(1, true)]
    [InlineData(2, false)]
    public void Options_Refuse_A_PayloadRetention_The_Row_Handlers_Of_One_Entity_Outlast(
        int handlerCount,
        bool accepted
    )
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                [$"{SampleOptions.SectionName}:{nameof(SampleOptions.ConnectionString)}"] = connectionString,
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:PayloadRetention"] = "01:00:00",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "1",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:ClaimTimeout"] = "00:30:00",
            }
        );
        builder.Services.AddSampleRetentionServices();
        builder.Services.AddRowHandler<Note, InstanceNoteHandler>();
        if (handlerCount == 2)
        {
            builder.Services.AddRowHandler<Note, SecondNoteHandler>();
        }

        using var host = builder.Build();

        var options = () => host.Services.GetRequiredService<IOptions<CohortOptions>>().Value;

        if (accepted)
        {
            options.Should().NotThrow();
            return;
        }

        options.Should().Throw<OptionsValidationException>()
            .Which.Failures.Should().ContainSingle(failure =>
                failure.Contains("PayloadRetention", StringComparison.Ordinal)
                && failure.Contains(typeof(Note).FullName!, StringComparison.Ordinal)
                && failure.Contains("2 row handlers", StringComparison.Ordinal)
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

    [RowHandlerId("3a4e2f9b-7c61-4d08-b5e2-91f0c6d7a843")]
    private sealed class SecondNoteHandler : IRetentionHandler<Note>;

    [RowHandlerId("c70b90fd-ebbb-4631-8ce9-6f11d4bf7081")]
    private sealed class UnconstructableNoteHandler : IRetentionHandler<Note>
    {
        public static bool Constructed { get; private set; }

        public UnconstructableNoteHandler()
        {
            Constructed = true;
            throw new InvalidOperationException("Startup must not construct a row handler.");
        }
    }

    [RowHandlerId("d0f60a6d-922d-46c0-8100-c4a358f7802b")]
    private sealed class InstanceNoteHandler : IRetentionHandler<Note>;

    private sealed class StrayNoteHandler : IRetentionHandler<Note>;

    [RowHandlerId("b4137b7d-6960-43b5-9cbe-560cc7af6a63")]
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

    [RowHandlerId("8e903300-3e52-4f2c-98c8-4c9976907fed")]
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

    private sealed class OpenHandler<TEntity> : IRetentionHandler<TEntity>;

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
