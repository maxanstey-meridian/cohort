using Cohort.Domain;

namespace Cohort.Application;

public interface IRetentionHoldsRepository
{
    /// <summary>
    /// Creates a hold on an existing row. Refuses an <see cref="RetentionHoldRequest.ExpiresAt"/>
    /// the database clock has already reached, because that hold would protect nothing.
    /// </summary>
    public Task CreateAsync(RetentionHoldRequest request, CancellationToken ct);

    public Task RemoveAsync(Guid holdId, DateTimeOffset removedAt, CancellationToken ct);

    /// <summary>
    /// Holds that protect now, judged on the database clock exactly as sweeps, erasure and
    /// <see cref="IRetentionDeletion"/> judge them.
    /// </summary>
    public Task<IReadOnlyList<RetentionHold>> ListActiveAsync(CancellationToken ct);

    /// <summary>
    /// Whether a hold protects the row now, judged on the database clock exactly as sweeps,
    /// erasure and <see cref="IRetentionDeletion"/> judge it.
    /// </summary>
    public Task<bool> HasActiveHoldAsync(
        Guid retentionEntityId,
        string recordId,
        Guid? tenantId,
        CancellationToken ct
    );
}
