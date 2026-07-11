using Cohort.Domain;

namespace Cohort.Application;

public interface IRetentionPreview
{
    public Task<RetentionSweepResult> ExecuteAsync(
        RetentionPreviewRequest request,
        CancellationToken ct = default
    );

    public Task<RetentionSweepResult> PreviewAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    ) =>
        ExecuteAsync(RetentionPreviewRequest.Tenanted(tenant, now), ct);
}

public abstract record RetentionPreviewRequest
{
    private RetentionPreviewRequest(DateTimeOffset at)
    {
        At = at;
    }

    public DateTimeOffset At { get; }

    public static RetentionPreviewRequest Tenanted(TenantContext tenant, DateTimeOffset at) =>
        new TenantedRequest(tenant, at);

    public static RetentionPreviewRequest Tenantless(DateTimeOffset at) =>
        new TenantlessRequest(at);

    public sealed record TenantedRequest : RetentionPreviewRequest
    {
        internal TenantedRequest(TenantContext tenant, DateTimeOffset at)
            : base(at)
        {
            ArgumentNullException.ThrowIfNull(tenant);
            Tenant = tenant;
        }

        public TenantContext Tenant { get; }
    }

    public sealed record TenantlessRequest : RetentionPreviewRequest
    {
        internal TenantlessRequest(DateTimeOffset at)
            : base(at) { }
    }
}
