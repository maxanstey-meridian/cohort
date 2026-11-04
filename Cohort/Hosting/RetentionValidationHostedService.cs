using Cohort.Application;
using Cohort.Infrastructure;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Cohort.Hosting;

/// <summary>
/// Readiness at host startup. It runs the model pass in <see cref="RetentionSystemWork.Readiness"/>,
/// then validates the context of each background subsystem scope that will run: the provider and
/// connection checks always, and the model pass when that context is a model instance or
/// connection string not yet validated. The scopes are the payload scrub and the row-handler dispatcher
/// (<see cref="RetentionSystemWork.RowDispatch"/>), the worker
/// (<see cref="RetentionSystemWork.WorkerCoordination"/>, when <see cref="CohortOptions.WorkerRuns"/>),
/// and history pruning (<see cref="RetentionSystemWork.HistoryPruning"/>, when
/// <see cref="CohortOptions.PruningScope"/> isn't null). A refusal stops the host.
/// </summary>
internal sealed class RetentionValidationHostedService(
    IServiceScopeFactory scopeFactory,
    CohortOptionsSnapshot options
) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await ValidateAsync(RetentionSystemWork.Readiness, cancellationToken);

        var current = options.Current;
        await ValidateAsync(RetentionSystemWork.RowDispatch, cancellationToken);

        if (current.WorkerRuns)
        {
            await ValidateAsync(RetentionSystemWork.WorkerCoordination, cancellationToken);
        }

        if (current.PruningScope is not null)
        {
            await ValidateAsync(RetentionSystemWork.HistoryPruning, cancellationToken);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private Task ValidateAsync(RetentionSystemWork work, CancellationToken cancellationToken) =>
        scopeFactory.RunInScopeAsync(
            new RetentionExecutionContext.System(work),
            services => services.GetRequiredService<RetentionRuntimeReadinessValidator>().ValidateAsync(cancellationToken),
            cancellationToken
        );
}
