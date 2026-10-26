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
    /// Sweeps the tenant's tenanted entities as of the database clock. Pass a
    /// <see cref="RetentionSweepRequest"/> to <see cref="ExecuteAsync"/> for a tenantless or
    /// dry run.
    /// </summary>
    public Task<RetentionSweepResult> SweepAsync(
        TenantContext tenant,
        CancellationToken ct = default
    ) => ExecuteAsync(RetentionSweepRequest.Tenanted(tenant), ct);
}

/// <summary>
/// An explicit sweep, always audited as <see cref="SweepTriggerKind.Manual"/>. Only Cohort's
/// scheduled worker writes <see cref="SweepTriggerKind.Scheduled"/> runs, each under the lock
/// that keeps a scheduled pass from running twice.
/// </summary>
public abstract record RetentionSweepRequest
{
    private RetentionSweepRequest(DateTimeOffset? at, bool dryRun)
    {
        // Npgsql only writes UTC DateTimeOffsets to timestamptz.
        At = at?.ToUniversalTime();
        DryRun = dryRun;
    }

    /// <summary>
    /// The time the sweep treats as now. Null uses the database clock. A real (non-dry)
    /// sweep refuses a time later than the database clock.
    /// </summary>
    public DateTimeOffset? At { get; }

    public bool DryRun { get; }

    /// <summary>The tenant the run is scoped and audited under.</summary>
    internal abstract TenantContext RunTenant { get; }

    public static RetentionSweepRequest Tenanted(
        TenantContext tenant,
        DateTimeOffset? at = null,
        bool dryRun = false
    ) => new TenantedRequest(tenant, at, dryRun);

    public static RetentionSweepRequest Tenantless(
        DateTimeOffset? at = null,
        bool dryRun = false
    ) => new TenantlessRequest(at, dryRun);

    public sealed record TenantedRequest : RetentionSweepRequest
    {
        internal TenantedRequest(TenantContext tenant, DateTimeOffset? at, bool dryRun)
            : base(at, dryRun)
        {
            ArgumentNullException.ThrowIfNull(tenant);
            Tenant = tenant;
        }

        public TenantContext Tenant { get; }

        internal override TenantContext RunTenant => Tenant;
    }

    public sealed record TenantlessRequest : RetentionSweepRequest
    {
        internal TenantlessRequest(DateTimeOffset? at, bool dryRun)
            : base(at, dryRun) { }

        internal override TenantContext RunTenant => TenantContext.Tenantless;
    }
}
