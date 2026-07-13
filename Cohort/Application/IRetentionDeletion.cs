namespace Cohort.Application;

/// <summary>
/// Coordinates host-owned deletion with Cohort retention holds.
/// </summary>
/// <remarks>
/// This service is scoped. Resolve it and the DbContext registered by AddCohort from the same
/// dependency-injection scope. Cohort starts and commits the transaction; the callback runs on
/// that scoped DbContext inside the transaction and must persist its changes before returning.
/// Callback exceptions are propagated and the transaction is rolled back.
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
