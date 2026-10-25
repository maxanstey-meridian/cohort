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

        await using var scope = await scopeFactory.CreateEnteredScopeAsync(
            RetentionExecutionScopes.For(request.RunTenant),
            ct
        );
        return await scope.ServiceProvider
            .GetRequiredService<RetentionSweepEngine>()
            .RunAsync(request.RunTenant, request.At, request.Trigger, SweepEntityScopes.For(request.RunTenant), request.DryRun, ct);
    }
}
