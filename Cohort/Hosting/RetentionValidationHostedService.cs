using Cohort.Application;
using Cohort.Infrastructure;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Cohort.Hosting;

internal sealed class RetentionValidationHostedService(IServiceScopeFactory scopeFactory)
    : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) =>
        scopeFactory.RunInScopeAsync(
            new RetentionExecutionContext.System(RetentionSystemWork.Readiness),
            services => services.GetRequiredService<RetentionRuntimeReadinessValidator>().ValidateAsync(cancellationToken),
            cancellationToken
        );

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
