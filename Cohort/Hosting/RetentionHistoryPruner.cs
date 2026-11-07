using Cohort.Application;
using Cohort.Infrastructure;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cohort.Hosting;

/// <summary>
/// Opt-in pruning of Cohort's own history: settled runs (with their entity summaries and row
/// details) past <see cref="HistoryPruningOptions.SucceededRunRetention"/>
/// or <see cref="HistoryPruningOptions.FailedRunRetention"/>, and holds that stopped protecting
/// longer ago than <see cref="HistoryPruningOptions.InactiveHoldRetention"/>. A run is never
/// pruned while it is a scheduled run of the schedule's current occurrence (the worker's ledger
/// for it), and nothing is pruned while
/// <see cref="CohortOptions.KillSwitch"/> is on. Each batch commits on its own and claims with
/// <c>SKIP LOCKED</c>, so replicas prune disjoint batches.
/// </summary>
internal sealed class RetentionHistoryPruner(
    IServiceScopeFactory scopeFactory,
    CohortOptionsSnapshot options,
    ILogger<RetentionHistoryPruner> logger
) : BackgroundService
{
    internal const int BatchSize = 100;

    private static readonly RetentionExecutionContext HistoryPruning =
        new RetentionExecutionContext.System(RetentionSystemWork.HistoryPruning);
    private static readonly TimeSpan Interval = TimeSpan.FromHours(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (options.Current.PrunerRuns)
            {
                try
                {
                    await PruneAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Cohort history pruning pass failed; it retries after the next interval.");
                }
            }

            try
            {
                await OperationalTime.DelayAsync(Interval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Runs one pruning pass: deletes every eligible run, then every eligible hold, in
    /// independently committed batches. Retentions left <c>null</c> prune nothing. The kill
    /// switch is checked before every batch, so engaging it stops the pass after the batch in
    /// progress.
    /// </summary>
    internal async Task<RetentionHistoryPruneResult> PruneAsync(CancellationToken ct = default)
    {
        // One reading of the options decides what is pruned.
        var current = options.Current;
        if (!current.PrunerRuns)
        {
            return new RetentionHistoryPruneResult(0, 0);
        }

        var pruning = current.HistoryPruning;
        var (now, currentOccurrence) = await scopeFactory.RunInScopeAsync(
            HistoryPruning,
            async services =>
            {
                await services
                    .GetRequiredService<RetentionRuntimeReadinessValidator>()
                    .ValidateAsync(ct);
                var databaseNow = await DatabaseClock.NowAsync(
                    services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext),
                    ct
                );
                return (databaseNow, CurrentOccurrence(current.Schedule, databaseNow));
            },
            ct
        );

        var succeededCutoff = Cutoff(now, pruning.SucceededRunRetention);
        var failedCutoff = Cutoff(now, pruning.FailedRunRetention);

        long runs = 0;
        if (pruning.PrunesRuns)
        {
            int deleted;
            do
            {
                ct.ThrowIfCancellationRequested();
                deleted = await PruneRunBatchAsync(succeededCutoff, failedCutoff, currentOccurrence, ct);
                runs += deleted;
            }
            while (deleted == BatchSize && !options.Current.KillSwitch);
        }

        long holds = 0;
        if (pruning.InactiveHoldRetention is { } holdRetention && !options.Current.KillSwitch)
        {
            var holdCutoff = OperationalTime.SubtractSaturating(now, holdRetention);
            int deleted;
            do
            {
                ct.ThrowIfCancellationRequested();
                deleted = await PruneHoldBatchAsync(holdCutoff, ct);
                holds += deleted;
            }
            while (deleted == BatchSize && !options.Current.KillSwitch);
        }

        if (runs > 0 || holds > 0)
        {
            logger.LogInformation(
                "Cohort pruned {RunCount} settled retention runs and {HoldCount} inactive holds.",
                runs,
                holds
            );
        }

        return new RetentionHistoryPruneResult(runs, holds);
    }

    /// <summary>
    /// One batch of settled runs; see <see cref="HistoryPruningSql.PruneRunBatchAsync"/>.
    /// </summary>
    private Task<int> PruneRunBatchAsync(
        DateTimeOffset? succeededCutoff,
        DateTimeOffset? failedCutoff,
        DateTimeOffset? currentOccurrence,
        CancellationToken ct
    ) =>
        scopeFactory.RunInScopeAsync(
            HistoryPruning,
            services => HistoryPruningSql.PruneRunBatchAsync(
                services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext),
                succeededCutoff,
                failedCutoff,
                currentOccurrence,
                BatchSize,
                ct
            ),
            ct
        );

    /// <summary>
    /// The latest occurrence of <paramref name="schedule"/> at or before
    /// <paramref name="databaseNow"/>, the occurrence the worker is catching up; null when no
    /// schedule is set.
    /// </summary>
    private static DateTimeOffset? CurrentOccurrence(string? schedule, DateTimeOffset databaseNow) =>
        string.IsNullOrWhiteSpace(schedule)
            ? null
            : CohortScheduleParser.GetPreviousOccurrence(schedule, databaseNow);

    /// <summary>
    /// One batch of inactive holds; see <see cref="HistoryPruningSql.PruneHoldBatchAsync"/>.
    /// </summary>
    private Task<int> PruneHoldBatchAsync(DateTimeOffset cutoff, CancellationToken ct) =>
        scopeFactory.RunInScopeAsync(
            HistoryPruning,
            services => HistoryPruningSql.PruneHoldBatchAsync(
                services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext),
                cutoff,
                BatchSize,
                ct
            ),
            ct
        );

    private static DateTimeOffset? Cutoff(DateTimeOffset now, TimeSpan? retention) =>
        retention is null ? null : OperationalTime.SubtractSaturating(now, retention.Value);
}

internal sealed record RetentionHistoryPruneResult(long Runs, long Holds);
