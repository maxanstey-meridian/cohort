using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Audit;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure;

internal sealed class RetentionRunLifecycle(
    EfRetentionAuditWriter auditWriter,
    RetentionAuditNotifier? auditNotifier = null,
    ILogger? logger = null
)
{
    private static readonly TimeSpan AuditSettlementTimeout = TimeSpan.FromSeconds(30);

    public List<SweepEvent> AuditEvents { get; } = [];

    public List<EntitySweepCount> EntityCounts { get; } = [];

    public long AccumulatedAffectedTotal => EntityCounts.Count > 0
        ? EntityCounts.Sum(count => count.Affected)
        : AuditEvents.OfType<SweepEvent.EntitySummary>().Sum(summary => summary.Affected);

    public async Task WriteDurableAsync(SweepEvent evt, CancellationToken ct)
    {
        using var timeout = ct.CanBeCanceled
            ? null
            : new CancellationTokenSource(AuditSettlementTimeout);
        await auditWriter.WriteAsync(evt, timeout?.Token ?? ct);
        AuditEvents.Add(evt);
        if (auditNotifier is not null)
        {
            await auditNotifier.NotifyCommittedAsync(evt);
        }
    }

    public Task NotifyCommittedAsync(SweepEvent evt) =>
        auditNotifier?.NotifyCommittedAsync(evt) ?? Task.CompletedTask;

    public async Task TrySettleTerminalAsync(SweepEvent evt, string operation, Guid sweepId)
    {
        try
        {
            using var timeout = new CancellationTokenSource(AuditSettlementTimeout);
            await auditWriter.WriteAsync(evt, timeout.Token);
            if (auditNotifier is not null)
            {
                await auditNotifier.NotifyCommittedAsync(evt);
            }
        }
        catch (Exception settlementException)
        {
            logger?.LogError(
                settlementException,
                "Cohort could not mark {Operation} {SweepId} as terminal.",
                operation,
                sweepId
            );
        }
    }

    public void ReplaceEntityCount(
        RetentionEntry entry,
        Guid tenantId,
        Strategy strategy,
        long affected,
        long heldCount,
        long skippedCount,
        long nullAnchorCount = 0
    )
    {
        var count = new EntitySweepCount(
            entry.EntityType,
            entry.Category,
            tenantId,
            strategy,
            affected,
            heldCount,
            skippedCount,
            nullAnchorCount
        );
        var index = EntityCounts.FindIndex(existing =>
            existing.EntityType == entry.EntityType
            && existing.Category == entry.Category
            && existing.TenantId == tenantId
            && existing.Strategy == strategy
        );

        if (index < 0)
        {
            EntityCounts.Add(count);
        }
        else
        {
            EntityCounts[index] = count;
        }
    }

    public RetentionSweepResult CreateSweepResult(IReadOnlyList<string> entityFailures)
    {
        EnsureCountsFromAuditEvents();
        var (started, settledAt) = GetResultBounds();
        return new RetentionSweepResult(
            started.SweepId,
            started.At,
            settledAt,
            EntityCounts,
            entityFailures
        );
    }

    public ErasureResult CreateErasureResult(
        ErasureScope scope,
        IReadOnlyList<string> entityFailures
    )
    {
        var (started, settledAt) = GetResultBounds();
        return new ErasureResult(
            started.SweepId,
            started.At,
            settledAt,
            scope,
            EntityCounts,
            started.DryRun,
            entityFailures
        );
    }

    public static string TruncateError(string value)
    {
        const int maxLength = 4000;
        if (value.Length <= maxLength)
        {
            return value;
        }

        var completeDiagnosticBoundary = value.LastIndexOf('\n', maxLength - 1, maxLength);
        return completeDiagnosticBoundary > 0
            ? value[..completeDiagnosticBoundary]
            : value[..maxLength];
    }

    private void EnsureCountsFromAuditEvents()
    {
        if (EntityCounts.Count > 0)
        {
            return;
        }

        foreach (var summary in AuditEvents.OfType<SweepEvent.EntitySummary>())
        {
            EntityCounts.Add(
                new EntitySweepCount(
                    summary.EntityType,
                    summary.Category,
                    summary.TenantId,
                    summary.Strategy,
                    summary.Affected,
                    summary.HeldCount,
                    summary.SkippedCount,
                    summary.NullAnchorCount
                )
            );
        }
    }

    private (SweepEvent.Started Started, DateTimeOffset SettledAt) GetResultBounds()
    {
        var started = AuditEvents.OfType<SweepEvent.Started>().Single();
        var settledAt = AuditEvents
            .Where(evt => evt is SweepEvent.Completed or SweepEvent.PartiallyFailed)
            .Select(evt =>
                evt switch
                {
                    SweepEvent.Completed completed => completed.At,
                    SweepEvent.PartiallyFailed partiallyFailed => partiallyFailed.At,
                    _ => throw new InvalidOperationException("The run has no terminal event."),
                }
            )
            .Single();
        return (started, settledAt);
    }
}
