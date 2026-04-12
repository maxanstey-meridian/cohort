using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Tests;

public sealed class StaticRetentionRuleResolverTests
{
    [Fact]
    public async Task Resolve_Async_Returns_Configured_Rule_From_Context()
    {
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge);
        var resolver = new StaticRetentionRuleResolver(rule);
        var context = CreateContext();

        var resolved = await resolver.ResolveAsync(context, CancellationToken.None);

        resolved.Should().Be(rule);
    }

    [Fact]
    public void Try_Resolve_At_Startup_Returns_Configured_Rule()
    {
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge);
        var resolver = new StaticRetentionRuleResolver(rule);

        var resolved = resolver.TryResolveAtStartup();

        resolved.Should().Be(rule);
    }

    [Fact]
    public void Get_Possible_Strategies_At_Startup_Returns_Configured_Strategy()
    {
        var resolver = new StaticRetentionRuleResolver(
            new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
        );

        var strategies = resolver.GetPossibleStrategiesAtStartup();

        strategies.Should().Equal(Strategy.SoftDelete);
    }

    [Fact]
    public void Try_Resolve_At_Startup_Remains_Optional_For_Effectful_Resolvers()
    {
        IRetentionRuleResolver resolver = new DeferredResolver(
            new RetentionRule(TimeSpan.FromDays(90), Strategy.Exempt)
        );

        var resolved = resolver.TryResolveAtStartup();

        resolved.Should().BeNull();
    }

    [Fact]
    public void Get_Possible_Strategies_At_Startup_Remains_Optional_For_Effectful_Resolvers()
    {
        IRetentionRuleResolver resolver = new DeferredResolver(
            new RetentionRule(TimeSpan.FromDays(90), Strategy.Exempt)
        );

        var strategies = resolver.GetPossibleStrategiesAtStartup();

        strategies.Should().BeNull();
    }

    [Fact]
    public async Task Category_Repository_Returns_Resolver_By_Category()
    {
        var expected = new StaticRetentionRuleResolver(
            new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
        );
        IRetentionCategoryRepository repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["short-lived"] = expected,
            }
        );

        var resolved = await repository.GetAsync("short-lived", CancellationToken.None);
        var missing = await repository.GetAsync("missing", CancellationToken.None);

        resolved.Should().BeSameAs(expected);
        missing.Should().BeNull();
    }

    [Fact]
    public void Retention_Alias_Cycle_Exception_Is_Public_And_Constructible()
    {
        var inner = new InvalidOperationException("inner");
        var exception = new RetentionAliasCycleException("Alias cycle detected.", inner);

        exception.Should().BeOfType<RetentionAliasCycleException>();
        exception.Message.Should().Be("Alias cycle detected.");
        exception.InnerException.Should().BeSameAs(inner);
    }

    private static RetentionResolutionContext CreateContext()
    {
        return new RetentionResolutionContext(
            "short-lived",
            new TenantContext(Guid.NewGuid(), "uk-england", new Dictionary<string, string>()),
            DateTimeOffset.Parse("2026-01-01T00:00:00+00:00"),
            []
        );
    }

    private sealed class DeferredResolver(RetentionRule rule) : IRetentionRuleResolver
    {
        public Task<RetentionRule> ResolveAsync(RetentionResolutionContext ctx, CancellationToken ct) =>
            Task.FromResult(rule);
    }

    private sealed class InMemoryCategoryRepository(
        IReadOnlyDictionary<string, IRetentionRuleResolver> resolvers
    ) : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            resolvers.TryGetValue(category, out var resolver);
            return Task.FromResult(resolver);
        }
    }
}
