using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Sample;

public sealed class SampleRetentionRuleProvider : IRetentionRuleProvider
{
    private static readonly IReadOnlyDictionary<string, RetentionRule> Rules =
        new Dictionary<string, RetentionRule>
        {
            ["short-lived"] = new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge),
            ["blob-cleanup"] = new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge),
            ["soft-delete"] = new RetentionRule(new RetentionPeriod(Days: 30), Strategy.SoftDelete),
            ["anonymise"] = new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Anonymise),
            // Resolves to soft delete here, but declares anonymise too: another tenant's rule
            // may scrub the same rows, so both markers decide which rows are retired.
            ["soft-delete-or-anonymise"] = new RetentionRule(
                new RetentionPeriod(Days: 30),
                Strategy.SoftDelete
            ),
            ["tombstone-anonymise"] = new RetentionRule(
                new RetentionPeriod(Days: 30),
                Strategy.Anonymise
            ),
            ["tenantless-purge"] = new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge),
            ["nullable-anchor-purge"] = new RetentionRule(
                new RetentionPeriod(Days: 30),
                Strategy.Purge
            ),
            ["tenantless-softdelete"] = new RetentionRule(
                new RetentionPeriod(Days: 30),
                Strategy.SoftDelete
            ),
            // Category default is SummaryOnly — the entity [Retain] attribute overrides to PerRow,
            // which is exactly what CohortConventionsEndToEndTests / PerRowAuditOverride tests assert.
            ["per-row-audit-override"] = new RetentionRule(
                new RetentionPeriod(Days: 30),
                Strategy.Purge,
                AuditRowDetail: AuditRowDetail.SummaryOnly
            ),
        };

    private static readonly IReadOnlyDictionary<string, Strategy[]> DeclaredStrategies =
        new Dictionary<string, Strategy[]>
        {
            ["soft-delete-or-anonymise"] = [Strategy.SoftDelete, Strategy.Anonymise],
        };

    public RetentionCategoryCapabilities? GetCapabilities(string category)
    {
        if (DeclaredStrategies.TryGetValue(category, out var declared))
        {
            return new RetentionCategoryCapabilities(declared);
        }

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
