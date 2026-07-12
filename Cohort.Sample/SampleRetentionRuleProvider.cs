using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Sample;

public sealed class SampleRetentionRuleProvider : IRetentionRuleProvider
{
    private static readonly IReadOnlyDictionary<string, RetentionRule> Rules =
        new Dictionary<string, RetentionRule>
        {
            ["short-lived"] = new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge),
            ["blob-cleanup"] = new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge),
            ["soft-delete"] = new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete),
            ["anonymise"] = new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise),
            ["tombstone-anonymise"] = new RetentionRule(
                TimeSpan.FromDays(30),
                Strategy.Anonymise
            ),
            ["tenantless-purge"] = new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge),
            ["nullable-anchor-purge"] = new RetentionRule(
                TimeSpan.FromDays(30),
                Strategy.Purge
            ),
            ["tenantless-softdelete"] = new RetentionRule(
                TimeSpan.FromDays(30),
                Strategy.SoftDelete
            ),
            // Category default is SummaryOnly — the entity [Retain] attribute overrides to PerRow,
            // which is exactly what CohortConventionsEndToEndTests / PerRowAuditOverride tests assert.
            ["per-row-audit-override"] = new RetentionRule(
                TimeSpan.FromDays(30),
                Strategy.Purge,
                AuditRowDetail: AuditRowDetail.SummaryOnly
            ),
        };

    public RetentionCategoryCapabilities? GetCapabilities(string category)
    {
        return Rules.TryGetValue(category, out var rule)
            ? new RetentionCategoryCapabilities([rule.Strategy])
            : null;
    }

    public Task<RetentionRule?> ResolveAsync(
        RetentionResolutionContext context,
        CancellationToken ct
    )
    {
        Rules.TryGetValue(context.Category, out var rule);
        return Task.FromResult(rule);
    }
}
