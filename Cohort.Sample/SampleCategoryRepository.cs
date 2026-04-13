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
            ["soft-delete"] = new StaticRetentionRuleResolver(
                new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
            ),
            ["anonymise"] = new StaticRetentionRuleResolver(
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
            ),
            ["tombstone-anonymise"] = new StaticRetentionRuleResolver(
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
            ),
            ["tenantless-purge"] = new StaticRetentionRuleResolver(
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
            ),
            ["tenantless-softdelete"] = new StaticRetentionRuleResolver(
                new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
            ),
            // Category default is SummaryOnly — the entity [Retain] attribute overrides to PerRow,
            // which is exactly what CohortConventionsEndToEndTests / PerRowAuditOverride tests assert.
            ["per-row-audit-override"] = new StaticRetentionRuleResolver(
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge, AuditRowDetail: AuditRowDetail.SummaryOnly)
            ),
        };

    public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
    {
        Resolvers.TryGetValue(category, out var resolver);
        return Task.FromResult(resolver);
    }
}
