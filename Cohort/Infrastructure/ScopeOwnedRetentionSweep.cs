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

        return scopeFactory.RunInScopeAsync(
            RetentionExecutionScopes.For(request.RunTenant),
            services => services
                .GetRequiredService<RetentionSweepEngine>()
                .RunAsync(request.RunTenant, request.At, SweepEntityScopes.For(request.RunTenant), request.DryRun, ct),
            ct
        );
    }
}
