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

        return ExecuteAsync(
            engine =>
                request.DryRun
                    ? engine.DryRunAsync(
                        tenant,
                        request.At,
                        request.Trigger,
                        scope,
                        ct
                    )
                    : engine.SweepAsync(
                        tenant,
                        request.At,
                        request.Trigger,
                        scope,
                        ct
                    ),
            ct
        );
    }

    private async Task<RetentionSweepResult> ExecuteAsync(
        Func<RetentionSweepEngine, Task<RetentionSweepResult>> execute,
        CancellationToken ct
    )
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        await scope.ServiceProvider
            .GetRequiredService<RetentionRuntimeReadinessValidator>()
            .ValidateAsync(ct);
        var engine = scope.ServiceProvider.GetRequiredService<RetentionSweepEngine>();
        return await execute(engine);
    }
}
