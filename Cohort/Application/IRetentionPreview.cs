using Cohort.Domain;

namespace Cohort.Application;

public interface IRetentionPreview
{
    public Task<RetentionSweepResult> PreviewAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    );
}
