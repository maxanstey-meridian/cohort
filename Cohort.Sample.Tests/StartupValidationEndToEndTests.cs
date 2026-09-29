using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

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
