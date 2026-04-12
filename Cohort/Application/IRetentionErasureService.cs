using Cohort.Domain;

namespace Cohort.Application;

public interface IRetentionErasureService
{
    public Task<ErasureResult> EraseAsync(
        TenantContext tenant,
        ErasureScope scope,
        DateTimeOffset now,
        CancellationToken ct = default
    );
}
