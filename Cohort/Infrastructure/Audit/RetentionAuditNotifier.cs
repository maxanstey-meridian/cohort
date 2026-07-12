using Cohort.Application;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Audit;

internal sealed class RetentionAuditNotifier(
    IEnumerable<IRetentionAuditObserver> observers,
    IRetentionExecutionSettings settings,
    ILogger<RetentionAuditNotifier> logger
)
{
    private readonly IRetentionAuditObserver[] observerList = observers.ToArray();
    private readonly TimeSpan timeout = NormalizeTimeout(settings.AuditObserverTimeout);
    private readonly HashSet<IRetentionAuditObserver> quarantined =
        new(ReferenceEqualityComparer.Instance);

    public async Task NotifyCommittedAsync(SweepEvent evt)
    {
        ArgumentNullException.ThrowIfNull(evt);

        foreach (var observer in observerList)
        {
            if (quarantined.Contains(observer))
            {
                continue;
            }

            try
            {
                using var timeoutSource = new CancellationTokenSource(timeout);
                var delivery = observer.OnCommittedAsync(evt, timeoutSource.Token);
                try
                {
                    await delivery.WaitAsync(timeoutSource.Token);
                }
                catch (OperationCanceledException) when (timeoutSource.IsCancellationRequested)
                {
                    quarantined.Add(observer);
                    _ = delivery.ContinueWith(
                        task => _ = task.Exception,
                        CancellationToken.None,
                        TaskContinuationOptions.ExecuteSynchronously
                            | TaskContinuationOptions.OnlyOnFaulted,
                        TaskScheduler.Default
                    );
                    logger.LogWarning(
                        "Cohort audit observer {ObserverType} timed out processing committed event {EventType} for sweep {SweepId} and was quarantined for the remainder of the run.",
                        observer.GetType().FullName,
                        evt.GetType().Name,
                        GetSweepId(evt)
                    );
                }
            }
            catch (Exception ex)
            {
                logger.LogError(
                    ex,
                    "Cohort audit observer {ObserverType} failed processing committed event {EventType} for sweep {SweepId}.",
                    observer.GetType().FullName,
                    evt.GetType().Name,
                    GetSweepId(evt)
                );
            }
        }
    }

    private static TimeSpan NormalizeTimeout(TimeSpan configuredTimeout) =>
        configuredTimeout > TimeSpan.Zero && configuredTimeout <= TimeSpan.FromHours(1)
            ? configuredTimeout
            : TimeSpan.FromSeconds(5);

    private static Guid GetSweepId(SweepEvent evt) =>
        evt switch
        {
            SweepEvent.Started value => value.SweepId,
            SweepEvent.EntityProgress value => value.SweepId,
            SweepEvent.EntitySummary value => value.SweepId,
            SweepEvent.RowDetail value => value.SweepId,
            SweepEvent.Completed value => value.SweepId,
            SweepEvent.PartiallyFailed value => value.SweepId,
            SweepEvent.Failed value => value.SweepId,
            SweepEvent.Cancelled value => value.SweepId,
            _ => Guid.Empty,
        };
}
