using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Sample;

internal sealed class SampleRetentionStartupService(
    IRetentionSweep sweep,
    IRetentionPreview previewService,
    IRetentionErasureService erasureService
)
{
    public Task<RetentionSweepResult> RunSweepAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        return sweep.SweepAsync(tenant, now, ct);
    }

    public Task<RetentionSweepResult> RunPreviewAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        return previewService.PreviewAsync(tenant, now, ct);
    }

    public Task<ErasureResult> RunErasureAsync(
        TenantContext tenant,
        ErasureScope scope,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        return erasureService.EraseAsync(tenant, scope, now, ct);
    }
}
