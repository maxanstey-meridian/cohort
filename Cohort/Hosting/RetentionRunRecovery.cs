using Cohort.Application;
using Cohort.Infrastructure;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cohort.Hosting;

/// <summary>
/// Abandoned-run recovery: settles as Failed every run left Started longer than
/// <see cref="CohortOptions.SweepSettleTimeout"/> whose run lock no process still holds. The
/// background loop recovers once a minute, first at startup, and skips while
/// <see cref="CohortOptions.KillSwitch"/> is on; an explicit <see cref="RecoverAsync"/> call
/// ignores the kill switch. Recovery acts only on runs older than the settle timeout, at least
/// a minute, so a fixed one-minute interval loses nothing.
/// </summary>
internal sealed class RetentionRunRecovery(
    IServiceScopeFactory scopeFactory,
    CohortOptionsSnapshot options,
    ILogger<RetentionRunRecovery> logger
) : BackgroundService, IRetentionRunRecovery
{
    private static readonly RetentionExecutionContext Recovery =
        new RetentionExecutionContext.System(RetentionSystemWork.Recovery);
    private static readonly TimeSpan Interval = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Recovers in one scope: readiness, then the database clock, then the abandoned runs. The
    /// cutoff and a recovered run's settlement both come from the database clock, as a live
    /// run's do.
    /// </summary>
    public Task RecoverAsync(CancellationToken ct = default) =>
        scopeFactory.RunInScopeAsync(
            Recovery,
            async services =>
            {
                await services.GetRequiredService<RetentionRuntimeReadinessValidator>().ValidateAsync(ct);
                var db = services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext);
                var now = await DatabaseClock.NowAsync(db, ct);
                var cutoff = OperationalTime.SubtractSaturating(now, options.Current.SweepSettleTimeout);
                await RetentionRunLedgerSql.RecoverAbandonedAsync(db, cutoff, now, logger, ct);
            },
            ct
        );

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (options.Current.KillSwitch)
            {
                logger.LogDebug("Cohort skipped abandoned-run recovery: the kill switch is on.");
            }
            else
            {
                try
                {
                    await RecoverAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Cohort abandoned-run recovery failed; it retries after the next interval.");
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
}
