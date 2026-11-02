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
    /// erasure and <see cref="IRetentionDeletion"/> judge it: a hold on the row itself, or on a
    /// related row the entity is declared held with (<see cref="HeldWithAttribute"/>,
    /// <see cref="HeldWithReferencingAttribute"/>), through any chain of relations.
    /// </summary>
    public Task<bool> HasActiveHoldAsync(
        Guid retentionEntityId,
        string recordId,
        Guid? tenantId,
        CancellationToken ct
    );

    /// <summary>
    /// The holds that protect the row now, directly or through a declared relation, each once,
    /// ordered like <see cref="ListActiveAsync"/> and judged exactly as
    /// <see cref="HasActiveHoldAsync"/> judges the row. A hold on the row's id counts even when
    /// the row is gone. Empty when nothing protects it.
    /// </summary>
    public Task<IReadOnlyList<RetentionHold>> ListCoveringAsync(
        Guid retentionEntityId,
        string recordId,
        Guid? tenantId,
        CancellationToken ct
    );

    /// <summary>
    /// The holds that protect any row whose <see cref="ErasureSubjectAttribute"/> column of kind
    /// <paramref name="subjectKind"/> equals <paramref name="subject"/>, directly or through a
    /// declared relation, each once, ordered like <see cref="ListActiveAsync"/>. A null
    /// <paramref name="tenantId"/> reads the tenantless entities, as erasure does. It ignores
    /// age, legal minimum, and strategy, so it answers "is any of this subject's data held", not
    /// whether an erasure would report held rows. One read: no run, ledger row, or observer.
    /// Refuses a kind no retained entity declares and a subject of the wrong type, as erasure does.
    /// </summary>
    public Task<IReadOnlyList<RetentionHold>> ListCoveringSubjectAsync(
        string subjectKind,
        object subject,
        Guid? tenantId,
        CancellationToken ct
    );
}
