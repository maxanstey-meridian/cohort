using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Sample;

public sealed class SampleCategoryRepository : IRetentionCategoryRepository
{
    private static readonly IReadOnlyDictionary<string, IRetentionRuleResolver> Resolvers =
        new Dictionary<string, IRetentionRuleResolver>
        {
            ["short-lived"] = new StaticRetentionRuleResolver(
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
            ),
        };

    public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
    {
        Resolvers.TryGetValue(category, out var resolver);
        return Task.FromResult(resolver);
    }
}
