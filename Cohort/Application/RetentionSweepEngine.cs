using System.Data;

using Cohort.Domain;
using Cohort.Hosting;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Cohort.Application;

public sealed class RetentionSweepEngine(
    DbContext db,
    RetentionRegistry registry,
    IRetentionCategoryRepository categoryRepository,
    RetentionStartupValidator validator,
    IRetentionAuditWriter auditWriter,
    IEnumerable<IRetentionSweepStrategy> sweepStrategies,
    IOptionsMonitor<CohortOptions>? options = null,
    ILogger<RetentionSweepEngine>? logger = null
) : IRetentionSweep
{
    private readonly IReadOnlyDictionary<Strategy, IRetentionSweepStrategy> strategies = sweepStrategies
        .ToDictionary(strategy => strategy.HandlesStrategy);

    public Task<RetentionSweepResult> SweepAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        // Direct calls are recorded as Manual; the hosted worker passes Scheduled.
        return SweepAsync(tenant, now, SweepTriggerKind.Manual, ct);
    }

    public Task<RetentionSweepResult> SweepAsync(
        TenantContext tenant,
        DateTimeOffset now,
        SweepTriggerKind trigger,
        CancellationToken ct = default
    )
    {
        return SweepAsync(tenant, now, trigger, SweepEntityScope.All, ct);
    }

    public async Task<RetentionSweepResult> SweepAsync(
        TenantContext tenant,
        DateTimeOffset now,
        SweepTriggerKind trigger,
        SweepEntityScope scope,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(tenant);

        if (options?.CurrentValue.DryRun == true)
        {
            throw new InvalidOperationException(
                "Cohort is configured with DryRun enabled. RetentionSweepEngine.SweepAsync mutates data and refuses to run as a safety net; use IRetentionPreview for a count-only pass, or clear Cohort:DryRun."
            );
        }

        await validator.ValidateAsync(ct);

        var sweepId = Guid.NewGuid();
        var startedAt = DateTimeOffset.UtcNow;
        var batchSize = Math.Max(1, options?.CurrentValue.SweepBatchSize ?? new CohortOptions().SweepBatchSize);
        var executionPlan = await BuildExecutionPlanAsync(tenant, now, scope, ct);
        var auditEvents = new List<SweepEvent>();
        var entityFailures = new List<string>();

        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            // Started commits immediately (no ambient transaction): a sweep that later
            // fails or crashes still leaves audit evidence that it was attempted.
            await WriteAuditEventAsync(
                new SweepEvent.Started(
                    sweepId,
                    startedAt,
                    trigger,
                    DryRun: false,
                    tenant.Id
                ),
                auditEvents,
                ct
            );

            foreach (
                var (entry, context, rule) in RetentionExecutionPlanOrderer.Order(
                    db,
                    executionPlan,
                    item => item.Entry,
                    logger
                )
            )
            {
                try
                {
                    await SweepEntityAsync(
                        entry,
                        context,
                        rule,
                        sweepId,
                        tenant,
                        batchSize,
                        auditEvents,
                        ct
                    );
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // One entity's failure must not abort retention for every other
                    // entity; the failure is recorded on the run row and surfaced in
                    // the result.
                    entityFailures.Add($"{entry.EntityType.FullName}: {ex.Message}");
                    logger?.LogError(
                        ex,
                        "Cohort sweep {SweepId} failed for entity {EntityType}; continuing with remaining entities.",
                        sweepId,
                        entry.EntityType.FullName
                    );
                }
            }

            var completedAt = DateTimeOffset.UtcNow;
            var totalAffected = auditEvents.OfType<SweepEvent.EntitySummary>().Sum(summary => summary.Affected);
            await WriteAuditEventAsync(
                new SweepEvent.Completed(sweepId, completedAt, completedAt - startedAt, totalAffected),
                auditEvents,
                ct
            );

            if (entityFailures.Count > 0)
            {
                await WriteAuditEventAsync(
                    new SweepEvent.Failed(
                        sweepId,
                        completedAt,
                        Truncate(string.Join("; ", entityFailures), 4000)
                    ),
                    auditEvents,
                    ct
                );
            }
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }

        return CreateResult(auditEvents, entityFailures);
    }

    /// <summary>
    /// Counts what a sweep would do without mutating anything, while writing the same
    /// audit trail as a real sweep (Started with DryRun, per-entity summaries with
    /// predicted affected and measured held counts, Completed). This is the audited
    /// counterpart of <see cref="IRetentionPreview"/>, which writes no audit at all.
    /// </summary>
    public async Task<RetentionSweepResult> DryRunAsync(
        TenantContext tenant,
        DateTimeOffset now,
        SweepTriggerKind trigger,
        SweepEntityScope scope = SweepEntityScope.All,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(tenant);
        await validator.ValidateAsync(ct);

        var sweepId = Guid.NewGuid();
        var startedAt = DateTimeOffset.UtcNow;
        var executionPlan = await BuildExecutionPlanAsync(tenant, now, scope, ct);
        var auditEvents = new List<SweepEvent>();
        var entityFailures = new List<string>();

        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            await WriteAuditEventAsync(
                new SweepEvent.Started(sweepId, startedAt, trigger, DryRun: true, tenant.Id),
                auditEvents,
                ct
            );

            foreach (
                var (entry, context, rule) in RetentionExecutionPlanOrderer.Order(
                    db,
                    executionPlan,
                    item => item.Entry,
                    logger
                )
            )
            {
                try
                {
                    var eventAt = DateTimeOffset.UtcNow;
                    var resolvedPeriod = CutoffCalculator.ResolveEffectivePeriod(rule.Period, rule.LegalMin);
                    var affected = 0L;
                    var heldCount = 0L;
                    var nullAnchorCount = 0L;

                    if (rule.Strategy != Strategy.Exempt)
                    {
                        var strategy = strategies[rule.Strategy];
                        affected = await strategy.PreviewAsync(entry, rule, context, connection, ct);
                        heldCount = await strategy.CountHeldAsync(entry, rule, context, connection, ct);
                        nullAnchorCount = await strategy.CountNullAnchorsAsync(entry, rule, context, connection, ct);
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
                            affected,
                            heldCount,
                            0,
                            nullAnchorCount,
                            rule.Provenance
                        ),
                        auditEvents,
                        ct
                    );
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    entityFailures.Add($"{entry.EntityType.FullName}: {ex.Message}");
                    logger?.LogError(
                        ex,
                        "Cohort dry run {SweepId} failed for entity {EntityType}; continuing with remaining entities.",
                        sweepId,
                        entry.EntityType.FullName
                    );
                }
            }

            var completedAt = DateTimeOffset.UtcNow;
            var totalAffected = auditEvents.OfType<SweepEvent.EntitySummary>().Sum(summary => summary.Affected);
            await WriteAuditEventAsync(
                new SweepEvent.Completed(sweepId, completedAt, completedAt - startedAt, totalAffected),
                auditEvents,
                ct
            );

            if (entityFailures.Count > 0)
            {
                await WriteAuditEventAsync(
                    new SweepEvent.Failed(
                        sweepId,
                        completedAt,
                        Truncate(string.Join("; ", entityFailures), 4000)
                    ),
                    auditEvents,
                    ct
                );
            }
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }

        return CreateResult(auditEvents, entityFailures);
    }

    private async Task<List<(RetentionEntry Entry, RetentionResolutionContext Context, RetentionRule Rule)>> BuildExecutionPlanAsync(
        TenantContext tenant,
        DateTimeOffset now,
        SweepEntityScope scope,
        CancellationToken ct
    )
    {
        var executionPlan = new List<(RetentionEntry Entry, RetentionResolutionContext Context, RetentionRule Rule)>();

        foreach (var entry in registry.Scan().Values)
        {
            if (
                (scope == SweepEntityScope.TenantedOnly && entry.Tenant is null)
                || (scope == SweepEntityScope.TenantlessOnly && entry.Tenant is not null)
            )
            {
                continue;
            }

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

        return executionPlan;
    }

    private async Task SweepEntityAsync(
        RetentionEntry entry,
        RetentionResolutionContext context,
        RetentionRule rule,
        Guid sweepId,
        TenantContext tenant,
        int batchSize,
        List<SweepEvent> auditEvents,
        CancellationToken ct
    )
    {
        var connection = db.Database.GetDbConnection();
        var eventAt = DateTimeOffset.UtcNow;
        var resolvedPeriod = CutoffCalculator.ResolveEffectivePeriod(rule.Period, rule.LegalMin);
        var affectedRecordIds = new List<string>();
        var skippedCount = 0L;
        var heldCount = 0L;
        var nullAnchorCount = 0L;
        var rowDetailsPersisted = false;

        if (rule.Strategy != Strategy.Exempt)
        {
            var strategy = strategies[rule.Strategy];

            // Each batch selects, locks, and mutates at most batchSize rows in its own
            // transaction, so a large backlog is retired incrementally and a failure
            // loses only the current batch.
            while (true)
            {
                ct.ThrowIfCancellationRequested();

                SweepExecutionResult execution;
                await using (var transaction = await db.Database.BeginTransactionAsync(ct))
                {
                    execution = await strategy.SweepAsync(
                        entry,
                        rule,
                        context,
                        connection,
                        transaction.GetDbTransaction(),
                        ct,
                        new SweepMutationContext(sweepId, DateTimeOffset.UtcNow, batchSize)
                    );

                    if (execution.HeldCount < 0)
                    {
                        throw new InvalidOperationException(
                            $"Retention strategy '{rule.Strategy}' produced an invalid held-count for entity {entry.EntityType.FullName}."
                        );
                    }

                    if (execution.SkippedCount < 0)
                    {
                        throw new InvalidOperationException(
                            $"Retention strategy '{rule.Strategy}' produced an invalid skipped-count for entity {entry.EntityType.FullName}."
                        );
                    }

                    await transaction.CommitAsync(ct);
                }

                affectedRecordIds.AddRange(execution.AffectedRecordIds);
                skippedCount += execution.SkippedCount;
                rowDetailsPersisted |= execution.RowDetailsPersisted;

                // A batch that affected nothing cannot shrink the candidate filter
                // (e.g. every row was skipped by a failing OnBefore handler), so
                // looping again would reselect the same rows forever; the remainder
                // is deferred to the next sweep.
                if (execution.CandidateCount < batchSize || execution.AffectedRecordIds.Count == 0)
                {
                    break;
                }
            }

            // Held rows are measured directly (eligible AND actively held) instead of
            // being inferred from candidate arithmetic.
            heldCount = await strategy.CountHeldAsync(entry, rule, context, connection, ct);
            nullAnchorCount = await strategy.CountNullAnchorsAsync(entry, rule, context, connection, ct);
        }

        await using (var summaryTransaction = await db.Database.BeginTransactionAsync(ct))
        {
            await WriteAuditEventAsync(
                new SweepEvent.EntitySummary(
                    sweepId,
                    eventAt,
                    entry.EntityType,
                    entry.Category,
                    tenant.Id,
                    rule.Strategy,
                    resolvedPeriod,
                    affectedRecordIds.Count,
                    heldCount,
                    skippedCount,
                    nullAnchorCount,
                    rule.Provenance
                ),
                auditEvents,
                ct
            );

            var effectiveAuditDetail = entry.AuditRowDetail == AuditRowDetail.Inherit
                ? rule.AuditRowDetail
                : entry.AuditRowDetail;
            if (effectiveAuditDetail == AuditRowDetail.PerRow && !rowDetailsPersisted)
            {
                foreach (var recordId in affectedRecordIds)
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

            await summaryTransaction.CommitAsync(ct);
        }
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

    private static string Truncate(string value, int maxLength)
    {
        return value.Length <= maxLength ? value : value[..maxLength];
    }

    private static RetentionSweepResult CreateResult(
        IEnumerable<SweepEvent> auditEvents,
        IReadOnlyList<string> entityFailures
    )
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
                    summary.Affected,
                    summary.HeldCount,
                    summary.SkippedCount,
                    summary.NullAnchorCount
                )
            )
            .ToArray();

        return new RetentionSweepResult(
            started.SweepId,
            started.At,
            completed.At,
            counts,
            entityFailures
        );
    }
}

/// <summary>
/// Which retained entities a sweep covers. Direct calls sweep everything; the hosted
/// worker sweeps tenanted entities per tenant and tenantless entities exactly once,
/// attributed to <see cref="TenantContext.Tenantless"/>, so a multi-tenant tick does
/// not re-sweep shared tables under every tenant's identity.
/// </summary>
public enum SweepEntityScope
{
    All,
    TenantedOnly,
    TenantlessOnly,
}
