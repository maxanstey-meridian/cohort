using Cohort.Domain;

namespace Cohort.Application;

/// Lives in Application/ — not Domain/ — because it implements an Application port
/// (`IRetentionRuleResolver`). Layer is determined by what a type binds to, not by
/// how much code it contains.
public sealed class StaticRetentionRuleResolver(RetentionRule rule) : IRetentionRuleResolver
{
    public Task<RetentionRule> ResolveAsync(RetentionResolutionContext ctx, CancellationToken ct) =>
        Task.FromResult(rule);

    public RetentionRule? TryResolveAtStartup() => rule;

    public IReadOnlySet<Strategy>? GetPossibleStrategiesAtStartup() => new HashSet<Strategy>
    {
        rule.Strategy,
    };
}
