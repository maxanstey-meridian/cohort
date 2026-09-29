using Cohort.Application;
using Cohort.Domain;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

internal sealed class ScopeOwnedRetentionErasureService(IServiceScopeFactory scopeFactory)
    : IRetentionErasureService
{
    public async Task<ErasureResult> EraseAsync(
        TenantContext tenant,
        ErasureScope scope,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(scope);

        await using var executionScope = scopeFactory.CreateAsyncScope();
        var service = executionScope.ServiceProvider.GetRequiredService<RetentionErasureService>();
        return await service.EraseAsync(tenant, scope, now, ct);
    }
}
