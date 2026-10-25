namespace Cohort.Application;

/// <summary>
/// Coordinates host-owned deletion with Cohort retention holds.
/// </summary>
/// <remarks>
/// <para>
/// This service is scoped. Resolve it and the DbContext registered by AddCohort from the same
/// dependency-injection scope, and make every change through that context: writes through
/// another context or connection commit outside the transaction. Cohort starts and commits the
/// transaction; the callback runs on that scoped DbContext inside it.
/// </para>
/// <para>
/// Cohort throws <see cref="InvalidOperationException"/> when the context has unsaved changes
/// on entry, before doing anything. Once the callback has run, Cohort throws and rolls back
/// when the callback returned with unsaved changes, or when any row with an active hold was
/// deleted inside the transaction, listed or not (EF or database cascades and raw SQL
/// included). Callback exceptions are propagated and the transaction is rolled back. After a
/// refusal or failure once the callback has run, the context's change tracker is cleared, so
/// a later save can't apply the callback's changes unguarded.
/// </para>
/// </remarks>
public interface IRetentionDeletion
{
    /// <summary>
    /// Locks every target and runs <paramref name="deletion"/> only when none has an active hold.
    /// </summary>
    public Task<RetentionDeletionOutcome> ExecuteAsync(
        IReadOnlyCollection<RetentionTarget> targets,
        Func<CancellationToken, Task> deletion,
        CancellationToken ct = default
    );
}

public sealed class RetentionTarget
{
    public RetentionTarget(Guid retentionEntityId, string recordId, Guid? tenantId)
    {
        RetentionEntityId = retentionEntityId != Guid.Empty
            ? retentionEntityId
            : throw new ArgumentException(
                "Retention entity ID cannot be empty.",
                nameof(retentionEntityId)
            );
        RecordId = !string.IsNullOrWhiteSpace(recordId)
            ? recordId
            : throw new ArgumentException("Record ID cannot be blank.", nameof(recordId));
        TenantId = tenantId != Guid.Empty
            ? tenantId
            : throw new ArgumentException("Tenant ID cannot be empty.", nameof(tenantId));
    }

    public Guid RetentionEntityId { get; }

    public string RecordId { get; }

    public Guid? TenantId { get; }
}

public enum RetentionDeletionOutcome
{
    Executed,
    Protected,
}
