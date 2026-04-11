namespace Cohort.Application;

public interface IRetentionCategoryRepository
{
    public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct);
}
