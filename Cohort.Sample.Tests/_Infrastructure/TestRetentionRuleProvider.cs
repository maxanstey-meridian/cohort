using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Sample.Tests;

internal interface ITestRetentionRule
{
    public Task<RetentionRule> ResolveAsync(
        RetentionResolutionContext context,
        CancellationToken ct
    );

    public RetentionRule? TryResolveAtStartup() => null;
}

// Existing focused fixtures use small rule objects to vary one behavior at a time.
// This test-only provider exposes them through the real public provider contract.
internal interface ITestRetentionRuleProvider : IRetentionRuleProvider
{
    public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct);

    RetentionCategoryCapabilities? IRetentionRuleProvider.GetCapabilities(string category)
    {
        var resolver = GetAsync(category, CancellationToken.None).GetAwaiter().GetResult();
        if (resolver is null)
        {
            return null;
        }

        var rule = resolver.TryResolveAtStartup();
        if (rule is not null)
        {
            return new RetentionCategoryCapabilities([rule.Strategy]);
        }

        if (resolver.GetType().Name.Contains("OpaqueSoftDelete", StringComparison.Ordinal))
        {
            return new RetentionCategoryCapabilities([Strategy.Anonymise, Strategy.SoftDelete]);
        }

        var strategy =
            category.Contains("anonymise", StringComparison.OrdinalIgnoreCase)
            || category.Contains("factory", StringComparison.OrdinalIgnoreCase)
            ? Strategy.Anonymise
            : category.Contains("soft-delete", StringComparison.OrdinalIgnoreCase)
                || category.Contains("softdelete", StringComparison.OrdinalIgnoreCase)
                ? Strategy.SoftDelete
                : Strategy.Purge;
        return new RetentionCategoryCapabilities([strategy]);
    }

    async Task<RetentionRule?> IRetentionRuleProvider.ResolveAsync(
        RetentionResolutionContext context,
        CancellationToken ct
    )
    {
        var resolver = await GetAsync(context.Category, ct);
        return resolver is null ? null : await resolver.ResolveAsync(context, ct);
    }
}

internal sealed class StaticTestRetentionRule(RetentionRule rule) : ITestRetentionRule
{
    public Task<RetentionRule> ResolveAsync(
        RetentionResolutionContext context,
        CancellationToken ct
    ) => Task.FromResult(rule);

    public RetentionRule? TryResolveAtStartup() => rule;
}
