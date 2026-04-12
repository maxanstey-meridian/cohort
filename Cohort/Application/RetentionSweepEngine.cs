using System.Data;

using Cohort.Domain;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Cohort.Application;

public sealed class RetentionSweepEngine(
    DbContext db,
    RetentionRegistry registry,
    IRetentionCategoryRepository categoryRepository,
    RetentionStartupValidator validator,
    IRetentionAuditWriter auditWriter,
    IEnumerable<IRetentionSweepStrategy> sweepStrategies
)
{
    private readonly IReadOnlyDictionary<Strategy, IRetentionSweepStrategy> strategies = sweepStrategies
        .ToDictionary(strategy => strategy.HandlesStrategy);

    public async Task<RetentionSweepResult> SweepAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(tenant);
        await validator.ValidateAsync(ct);

        var sweepId = Guid.NewGuid();
        var startedAt = DateTimeOffset.UtcNow;
        var executionPlan = new List<(RetentionEntry Entry, RetentionResolutionContext Context, RetentionRule Rule)>();
        var auditEvents = new List<SweepEvent>();

        foreach (
            var entry in registry
                .Scan()
                .Values.OrderBy(entry => entry.EntityType.FullName, StringComparer.Ordinal)
        )
        {
            var resolver = await categoryRepository.GetAsync(entry.Category, ct);
            if (resolver is null)
            {
                throw new InvalidOperationException(
                    $"Retention category '{entry.Category}' for entity {entry.EntityType.FullName} could not be resolved at runtime."
                );
            }

            var context = new RetentionResolutionContext(entry.Category, tenant, now, []);
            var rule = await resolver.ResolveAsync(context, ct);
            if (
                rule.Strategy != Strategy.Exempt && !strategies.ContainsKey(rule.Strategy)
            )
            {
                throw new InvalidOperationException(
                    $"Retention strategy '{rule.Strategy}' is not registered for sweep execution."
                );
            }

            executionPlan.Add((entry, context, rule));
        }

        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            await using var transaction = await db.Database.BeginTransactionAsync(ct);
            var dbTransaction = transaction.GetDbTransaction();
            await WriteAuditEventAsync(
                new SweepEvent.Started(
                    sweepId,
                    startedAt,
                    SweepTriggerKind.Scheduled,
                    DryRun: false,
                    tenant.Id
                ),
                auditEvents,
                ct
            );

            foreach (var (entry, context, rule) in executionPlan)
            {
                var eventAt = DateTimeOffset.UtcNow;
                var resolvedPeriod = CutoffCalculator.ResolveEffectivePeriod(rule.Period, rule.LegalMin);
                var execution = rule.Strategy switch
                {
                    Strategy.Exempt => new SweepExecutionResult([], 0),
                    _ => await strategies[rule.Strategy].SweepAsync(
                        entry,
                        rule,
                        context,
                        connection,
                        dbTransaction,
                        ct
                    ),
                };

                if (execution.HeldCount < 0)
                {
                    throw new InvalidOperationException(
                        $"Retention strategy '{rule.Strategy}' produced an invalid held-count for entity {entry.EntityType.FullName}."
                    );
                }

                await WriteAuditEventAsync(
                    new SweepEvent.EntitySummary(
                        sweepId,
                        eventAt,
                        entry.EntityType,
                        entry.Category,
                        tenant.Id,
                        rule.Strategy,
                        resolvedPeriod,
                        execution.AffectedRecordIds.Count,
                        execution.HeldCount
                    ),
                    auditEvents,
                    ct
                );

                var effectiveAuditDetail = entry.AuditRowDetail ?? rule.AuditRowDetail;
                if (effectiveAuditDetail == AuditRowDetail.PerRow)
                {
                    foreach (var recordId in execution.AffectedRecordIds)
                    {
                        await WriteAuditEventAsync(
                            new SweepEvent.RowDetail(
                                sweepId,
                                eventAt,
                                entry.EntityType,
                                recordId,
                                entry.Category,
                                rule.Strategy,
                                tenant.Id
                            ),
                            auditEvents,
                            ct
                        );
                    }
                }
            }

            var completedAt = DateTimeOffset.UtcNow;
            var totalAffected = auditEvents.OfType<SweepEvent.EntitySummary>().Sum(summary => summary.Affected);
            await WriteAuditEventAsync(
                new SweepEvent.Completed(sweepId, completedAt, completedAt - startedAt, totalAffected),
                auditEvents,
                ct
            );

            await transaction.CommitAsync(ct);
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }

        return CreateResult(auditEvents);
    }

    private async Task WriteAuditEventAsync(
        SweepEvent evt,
        ICollection<SweepEvent> auditEvents,
        CancellationToken ct
    )
    {
        await auditWriter.WriteAsync(evt, ct);
        auditEvents.Add(evt);
    }

    private static RetentionSweepResult CreateResult(IEnumerable<SweepEvent> auditEvents)
    {
        var started = auditEvents.OfType<SweepEvent.Started>().Single();
        var completed = auditEvents.OfType<SweepEvent.Completed>().Single();
        var counts = auditEvents
            .OfType<SweepEvent.EntitySummary>()
            .Select(summary =>
                new EntitySweepCount(
                    summary.EntityType,
                    summary.Category,
                    summary.TenantId,
                    summary.Strategy,
                    summary.Affected
                )
            )
            .ToArray();

        return new RetentionSweepResult(started.SweepId, started.At, completed.At, counts);
    }
}
