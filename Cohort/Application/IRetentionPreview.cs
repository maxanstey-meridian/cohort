using Cohort.Domain;

namespace Cohort.Application;

public interface IRetentionPreview
{
    public Task<RetentionSweepResult> ExecuteAsync(
        RetentionPreviewRequest request,
        CancellationToken ct = default
    );

    /// <summary>
    /// Counts what a sweep of the tenant's tenanted entities would do at <paramref name="at"/>,
    /// or at the database clock when it's omitted. Changes nothing.
    /// </summary>
    public Task<RetentionSweepResult> PreviewAsync(
        TenantContext tenant,
        DateTimeOffset? at = null,
        CancellationToken ct = default
    ) =>
        ExecuteAsync(RetentionPreviewRequest.Tenanted(tenant, at), ct);
}

public abstract record RetentionPreviewRequest
{
    private RetentionPreviewRequest(DateTimeOffset? at)
    {
        // Npgsql only writes UTC DateTimeOffsets to timestamptz.
        At = at?.ToUniversalTime();
    }

    /// <summary>
    /// The time the preview treats as now. Null uses the database clock. A preview changes
    /// nothing, so any time is allowed, including one later than the database clock.
    /// </summary>
    public DateTimeOffset? At { get; }

    /// <summary>The tenant the preview is scoped and counted under.</summary>
    internal abstract TenantContext RunTenant { get; }

    public static RetentionPreviewRequest Tenanted(TenantContext tenant, DateTimeOffset? at = null) =>
        new TenantedRequest(tenant, at);

    public static RetentionPreviewRequest Tenantless(DateTimeOffset? at = null) =>
        new TenantlessRequest(at);

    public sealed record TenantedRequest : RetentionPreviewRequest
    {
        internal TenantedRequest(TenantContext tenant, DateTimeOffset? at)
            : base(at)
        {
            ArgumentNullException.ThrowIfNull(tenant);
            Tenant = tenant;
        }

        public TenantContext Tenant { get; }

        internal override TenantContext RunTenant => Tenant;
    }

    public sealed record TenantlessRequest : RetentionPreviewRequest
    {
        internal TenantlessRequest(DateTimeOffset? at)
            : base(at) { }

        internal override TenantContext RunTenant => TenantContext.Tenantless;
    }
}
