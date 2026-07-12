using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Infrastructure;

internal static class RetentionRuleProviderResolution
{
    internal static async Task<RetentionRule> ResolveAsync(
        IRetentionRuleProvider provider,
        IReadOnlyDictionary<string, RetentionCategoryCapabilities> validatedCapabilities,
        RetentionResolutionContext context,
        CancellationToken ct
    )
    {
        if (!validatedCapabilities.TryGetValue(context.Category, out var capabilities))
        {
            throw new InvalidOperationException(
                $"Retention category '{context.Category}' was not present in the startup-validated capabilities snapshot."
            );
        }

        var rule = await provider.ResolveAsync(context, ct);
        if (rule is null)
        {
            throw new InvalidOperationException(
                $"Retention category '{context.Category}' could not be resolved at runtime."
            );
        }

        if (!capabilities.Strategies.Contains(rule.Strategy))
        {
            throw new InvalidOperationException(
                $"Retention category '{context.Category}' resolved strategy '{rule.Strategy}', which was not declared in its startup-validated capabilities."
            );
        }

        return rule;
    }
}
