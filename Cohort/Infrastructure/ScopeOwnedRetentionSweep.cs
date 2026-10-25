using Cohort.Application;
using Cohort.Domain;

using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

internal sealed class ScopeOwnedRetentionSweep(IServiceScopeFactory scopeFactory) : IRetentionSweep
{
    public async Task<RetentionSweepResult> ExecuteAsync(
        RetentionSweepRequest request,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(request);

        var entities = request is RetentionSweepRequest.TenantlessRequest
            ? SweepEntityScope.TenantlessOnly
            : SweepEntityScope.TenantedOnly;
        await using var scope = await scopeFactory.CreateEnteredScopeAsync(
            RetentionExecutionScopes.For(request.RunTenant),
            ct
        );
        return await scope.ServiceProvider
            .GetRequiredService<RetentionSweepEngine>()
            .RunAsync(request.RunTenant, request.At, request.Trigger, entities, request.DryRun, ct);
    }
}
