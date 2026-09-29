using Cohort.Application;
using Cohort.Domain;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

internal sealed class ScopeOwnedRetentionSweep(IServiceScopeFactory scopeFactory) : IRetentionSweep
{
    public Task<RetentionSweepResult> ExecuteAsync(
        RetentionSweepRequest request,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(request);

        var (tenant, scope) = request switch
        {
            RetentionSweepRequest.TenantedRequest tenanted =>
                (tenanted.Tenant, SweepEntityScope.TenantedOnly),
            RetentionSweepRequest.TenantlessRequest =>
                (TenantContext.Tenantless, SweepEntityScope.TenantlessOnly),
            _ => throw new ArgumentOutOfRangeException(nameof(request)),
        };

        return RunAsync(tenant, request, scope, ct);
    }

    private async Task<RetentionSweepResult> RunAsync(
        TenantContext tenant,
        RetentionSweepRequest request,
        SweepEntityScope entities,
        CancellationToken ct
    )
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        return await scope.ServiceProvider
            .GetRequiredService<RetentionSweepEngine>()
            .RunAsync(tenant, request.At, request.Trigger, entities, request.DryRun, ct);
    }
}
