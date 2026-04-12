using Cohort.Domain;

namespace Cohort.Application;

public interface IRetentionRuleResolver
{
    public Task<RetentionRule> ResolveAsync(RetentionResolutionContext ctx, CancellationToken ct);

    /// <summary>
    /// Optional startup hook. Static resolvers return their rule immediately so the
    /// validator can sanity-check it at boot. Effectful resolvers return null.
    /// </summary>
    public RetentionRule? TryResolveAtStartup() => null;

    /// <summary>
    /// Optional startup hint for effectful resolvers. Return the set of strategies this
    /// resolver may produce when startup cannot resolve a concrete rule.
    /// </summary>
    public IReadOnlySet<Strategy>? GetPossibleStrategiesAtStartup() => null;
}
