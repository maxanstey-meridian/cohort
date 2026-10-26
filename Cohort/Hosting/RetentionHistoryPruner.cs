using Cohort.Application;
using Cohort.Infrastructure;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cohort.Hosting;

/// <summary>
/// Opt-in pruning of Cohort's own history: settled runs (with their entity summaries, row
/// details and handler statuses) past <see cref="HistoryPruningOptions.SucceededRunRetention"/>
/// or <see cref="HistoryPruningOptions.FailedRunRetention"/>, and holds that stopped protecting
/// longer ago than <see cref="HistoryPruningOptions.InactiveHoldRetention"/>. A run is never
/// pruned while any of its handler work is pending or in flight, or while any of its rows still
/// holds a captured snapshot (requeueable dead-lettered work), nor while it is a scheduled run of
/// the schedule's current occurrence (the worker's ledger for it), and nothing is pruned while
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
            if (options.Current.HistoryPruning.Enabled)
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
        var pruning = options.Current.HistoryPruning;
        if (!pruning.Enabled || options.Current.KillSwitch)
        {
            return new RetentionHistoryPruneResult(0, 0);
        }

        var currentOccurrence = await scopeFactory.RunInScopeAsync(
            HistoryPruning,
            async services =>
            {
                await services.GetRequiredService<RetentionRuntimeReadinessValidator>().ValidateAsync(ct);
                return await CurrentOccurrenceAsync(
                    services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext),
                    options.Current.Schedule,
                    ct
                );
            },
            ct
        );

        var now = DateTimeOffset.UtcNow;
        var succeededCutoff = Cutoff(now, pruning.SucceededRunRetention);
        var failedCutoff = Cutoff(now, pruning.FailedRunRetention);
        var holdCutoff = Cutoff(now, pruning.InactiveHoldRetention);

        long runs = 0;
        if (succeededCutoff is not null || failedCutoff is not null)
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
        if (holdCutoff is not null && !options.Current.KillSwitch)
        {
            int deleted;
            do
            {
                ct.ThrowIfCancellationRequested();
                deleted = await PruneHoldBatchAsync(holdCutoff.Value, ct);
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
    /// The latest occurrence of <paramref name="schedule"/> at or before the database clock,
    /// the occurrence the worker is catching up; null without a schedule.
    /// </summary>
    private static async Task<DateTimeOffset?> CurrentOccurrenceAsync(
        DbContext db,
        string? schedule,
        CancellationToken ct
    ) =>
        string.IsNullOrWhiteSpace(schedule)
            ? null
            : CohortScheduleParser.GetPreviousOccurrence(schedule, await DatabaseClock.NowAsync(db, ct));

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
