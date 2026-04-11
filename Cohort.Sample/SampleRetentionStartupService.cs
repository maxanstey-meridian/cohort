using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Sample;

public sealed class SampleRetentionStartupService(
    RetentionRegistry registry,
    RetentionStartupValidator validator,
    RetentionSweepEngine sweepEngine,
    IRetentionPreview previewService
)
{
    public async Task<IReadOnlyDictionary<Type, RetentionEntry>> RunAsync(
        CancellationToken ct = default
    )
    {
        await validator.ValidateAsync(ct);
        return registry.Scan();
    }

    public async Task<RetentionSweepResult> RunSweepAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        await validator.ValidateAsync(ct);
        return await sweepEngine.SweepAsync(tenant, now, ct);
    }

    public async Task<RetentionSweepResult> RunPreviewAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        await validator.ValidateAsync(ct);
        return await previewService.PreviewAsync(tenant, now, ct);
    }
}
