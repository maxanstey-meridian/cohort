using Cohort.Application;
using Cohort.Infrastructure;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Cohort.Hosting;

internal sealed class RetentionValidationHostedService(IServiceScopeFactory scopeFactory)
    : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await using var scope = await scopeFactory.CreateEnteredScopeAsync(
            new RetentionExecutionContext.System(RetentionSystemWork.Readiness),
            cancellationToken
        );
        await scope
            .ServiceProvider.GetRequiredService<RetentionRuntimeReadinessValidator>()
            .ValidateAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
