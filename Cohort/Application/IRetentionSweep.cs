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

    /// <summary>
    /// Sweeps the tenant's tenanted entities as of the database clock, audited as
    /// <see cref="SweepTriggerKind.Manual"/>. Pass a <see cref="RetentionSweepRequest"/> to
    /// <see cref="ExecuteAsync"/> for a tenantless, dry or differently triggered run.
    /// </summary>
    public Task<RetentionSweepResult> SweepAsync(
        TenantContext tenant,
        CancellationToken ct = default
    ) => ExecuteAsync(RetentionSweepRequest.Tenanted(tenant), ct);
}

public abstract record RetentionSweepRequest
{
    private RetentionSweepRequest(
        DateTimeOffset? at,
        SweepTriggerKind trigger,
        bool dryRun
    )
    {
        // Npgsql only writes UTC DateTimeOffsets to timestamptz.
        At = at?.ToUniversalTime();
        // An erasure run names the kind of subject it erased, which a sweep has none of.
        Trigger =
            trigger == SweepTriggerKind.Erasure
                ? throw new ArgumentException(
                    "A sweep can't be an erasure run; erase a subject through IRetentionErasureService.",
                    nameof(trigger)
                )
                : trigger;
        DryRun = dryRun;
    }

    /// <summary>
    /// The time the sweep treats as now. Null uses the database clock. A real (non-dry)
    /// sweep refuses a time later than the database clock.
    /// </summary>
    public DateTimeOffset? At { get; }

    public SweepTriggerKind Trigger { get; }

    public bool DryRun { get; }

    /// <summary>The tenant the run is scoped and audited under.</summary>
    internal abstract TenantContext RunTenant { get; }

    public static RetentionSweepRequest Tenanted(
        TenantContext tenant,
        DateTimeOffset? at = null,
        SweepTriggerKind trigger = SweepTriggerKind.Manual,
        bool dryRun = false
    ) => new TenantedRequest(tenant, at, trigger, dryRun);

    public static RetentionSweepRequest Tenantless(
        DateTimeOffset? at = null,
        SweepTriggerKind trigger = SweepTriggerKind.Manual,
        bool dryRun = false
    ) => new TenantlessRequest(at, trigger, dryRun);

    public sealed record TenantedRequest : RetentionSweepRequest
    {
        internal TenantedRequest(
            TenantContext tenant,
            DateTimeOffset? at,
            SweepTriggerKind trigger,
            bool dryRun
        )
            : base(at, trigger, dryRun)
        {
            ArgumentNullException.ThrowIfNull(tenant);
            Tenant = tenant;
        }

        public TenantContext Tenant { get; }

        internal override TenantContext RunTenant => Tenant;
    }

    public sealed record TenantlessRequest : RetentionSweepRequest
    {
        internal TenantlessRequest(
            DateTimeOffset? at,
            SweepTriggerKind trigger,
            bool dryRun
        )
            : base(at, trigger, dryRun) { }

        internal override TenantContext RunTenant => TenantContext.Tenantless;
    }
}
