using Cohort.Application;
using Cohort.Infrastructure;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Cohort.Hosting;

/// <summary>
/// Readiness at host startup. It runs the model pass and an early check of the port groups in
/// <see cref="RetentionSystemWork.Readiness"/>, then checks, in its own scope and under its own
/// role, each background subsystem that will run: the payload scrub and the row-handler
/// dispatcher (<see cref="RetentionSystemWork.RowDispatch"/>, the dispatcher only while the kill
/// switch is off), the worker (<see cref="RetentionSystemWork.WorkerCoordination"/>, when
/// <see cref="CohortOptions.WorkerRuns"/>), and history pruning
/// (<see cref="RetentionSystemWork.HistoryPruning"/>, when <see cref="CohortOptions.PruningScope"/>
/// isn't null). A refusal stops the host.
/// </summary>
internal sealed class RetentionValidationHostedService(
    IServiceScopeFactory scopeFactory,
    CohortOptionsSnapshot options
) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await scopeFactory.RunInScopeAsync(
            new RetentionExecutionContext.System(RetentionSystemWork.Readiness),
            services => services.GetRequiredService<RetentionRuntimeReadinessValidator>().ValidateAtStartupAsync(cancellationToken),
            cancellationToken
        );

        var current = options.Current;
        await ValidateAsync(RetentionSystemWork.RowDispatch, RetentionStatementGroup.Scrub, cancellationToken);
        if (!current.KillSwitch)
        {
            await ValidateAsync(RetentionSystemWork.RowDispatch, RetentionStatementGroup.Dispatch, cancellationToken);
        }

        if (current.WorkerRuns)
        {
            await ValidateAsync(RetentionSystemWork.WorkerCoordination, RetentionStatementGroup.Worker, cancellationToken);
        }

        if (current.PruningScope is { } scope)
        {
            await ValidateAsync(
                RetentionSystemWork.HistoryPruning,
                RetentionStatementGroup.Pruning(scope),
                cancellationToken
            );
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private Task ValidateAsync(
        RetentionSystemWork work,
        RetentionStatementGroup group,
        CancellationToken cancellationToken
    ) =>
        scopeFactory.RunInScopeAsync(
            new RetentionExecutionContext.System(work),
            services => services.GetRequiredService<RetentionRuntimeReadinessValidator>().ValidateAsync(group, cancellationToken),
            cancellationToken
        );
}
