using Cohort.Domain;

namespace Cohort.Application;

public interface IRetentionRuleResolver
{
    public Task<RetentionRule> ResolveAsync(string category, CancellationToken ct);

    /// <summary>
    /// Optional startup hook. Static resolvers return their rule immediately so the
    /// validator can sanity-check it at boot. Effectful resolvers return null.
    /// </summary>
    public RetentionRule? TryResolveAtStartup() => null;
}
