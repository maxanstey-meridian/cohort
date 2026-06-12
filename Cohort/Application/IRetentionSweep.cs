using Cohort.Domain;

namespace Cohort.Application;

/// <summary>
/// Port for the mutating sweep, symmetrical with <see cref="IRetentionPreview"/> and
/// <see cref="IRetentionErasureService"/> so hosts can decorate the most dangerous
/// operation (metrics, approval gates) like the others.
/// </summary>
public interface IRetentionSweep
{
    public Task<RetentionSweepResult> SweepAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    );

    public Task<RetentionSweepResult> SweepAsync(
        TenantContext tenant,
        DateTimeOffset now,
        SweepTriggerKind trigger,
        CancellationToken ct = default
    );
}
