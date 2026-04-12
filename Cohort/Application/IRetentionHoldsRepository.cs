using Cohort.Domain;

namespace Cohort.Application;

public interface IRetentionHoldsRepository
{
    public Task CreateAsync(RetentionHoldRequest request, CancellationToken ct);

    public Task RemoveAsync(Guid holdId, DateTimeOffset removedAt, CancellationToken ct);

    public Task<IReadOnlyList<RetentionHold>> ListActiveAsync(DateTimeOffset asOf, CancellationToken ct);

    public Task<bool> HasActiveHoldAsync(
        string tableName,
        Guid recordId,
        Guid tenantId,
        DateTimeOffset asOf,
        CancellationToken ct
    );
}
