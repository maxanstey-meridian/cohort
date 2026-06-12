using Cohort.Domain;

namespace Cohort.Application;

/// <summary>
/// Enumerates the tenants the hosted worker sweeps on each scheduled tick. The default
/// implementation adapts a singleton <see cref="TenantContext"/> registration; multi-tenant
/// hosts register their own source so every tenant's data is retired on schedule.
/// </summary>
public interface IRetentionTenantSource
{
    public Task<IReadOnlyList<TenantContext>> GetTenantsAsync(CancellationToken ct);
}
