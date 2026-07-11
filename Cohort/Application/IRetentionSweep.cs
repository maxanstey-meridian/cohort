using Cohort.Domain;

namespace Cohort.Application;

/// <summary>
/// Port for the mutating sweep, symmetrical with <see cref="IRetentionPreview"/> and
/// <see cref="IRetentionErasureService"/> so hosts can decorate the most dangerous
/// operation (metrics, approval gates) like the others.
/// </summary>
public interface IRetentionSweep
{
    public Task<RetentionSweepResult> ExecuteAsync(
        RetentionSweepRequest request,
        CancellationToken ct = default
    );

    public Task<RetentionSweepResult> SweepAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    ) => ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, now), ct);

    public Task<RetentionSweepResult> SweepAsync(
        TenantContext tenant,
        DateTimeOffset now,
        SweepTriggerKind trigger,
        CancellationToken ct = default
    ) => ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, now, trigger), ct);
}

public abstract record RetentionSweepRequest
{
    private RetentionSweepRequest(
        DateTimeOffset at,
        SweepTriggerKind trigger,
        bool dryRun
    )
    {
        At = at;
        Trigger = trigger;
        DryRun = dryRun;
    }

    public DateTimeOffset At { get; }

    public SweepTriggerKind Trigger { get; }

    public bool DryRun { get; }

    public static RetentionSweepRequest Tenanted(
        TenantContext tenant,
        DateTimeOffset at,
        SweepTriggerKind trigger = SweepTriggerKind.Manual,
        bool dryRun = false
    ) => new TenantedRequest(tenant, at, trigger, dryRun);

    public static RetentionSweepRequest Tenantless(
        DateTimeOffset at,
        SweepTriggerKind trigger = SweepTriggerKind.Manual,
        bool dryRun = false
    ) => new TenantlessRequest(at, trigger, dryRun);

    public sealed record TenantedRequest : RetentionSweepRequest
    {
        internal TenantedRequest(
            TenantContext tenant,
            DateTimeOffset at,
            SweepTriggerKind trigger,
            bool dryRun
        )
            : base(at, trigger, dryRun)
        {
            ArgumentNullException.ThrowIfNull(tenant);
            Tenant = tenant;
        }

        public TenantContext Tenant { get; }
    }

    public sealed record TenantlessRequest : RetentionSweepRequest
    {
        internal TenantlessRequest(
            DateTimeOffset at,
            SweepTriggerKind trigger,
            bool dryRun
        )
            : base(at, trigger, dryRun) { }
    }
}
