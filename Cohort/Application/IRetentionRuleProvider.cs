using Cohort.Domain;

namespace Cohort.Application;

public interface IRetentionRuleProvider
{
    public RetentionCategoryCapabilities? GetCapabilities(string category);

    public Task<RetentionRule?> ResolveAsync(
        RetentionResolutionContext context,
        CancellationToken ct
    );
}
