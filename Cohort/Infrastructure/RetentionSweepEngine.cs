using System.Data;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure;

internal sealed class RetentionSweepEngine(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionRegistry registry,
    IRetentionCategoryRepository categoryRepository,
    RetentionStartupValidator validator,
    IRetentionAuditWriter auditWriter,
    IEnumerable<IRetentionSweepStrategy> sweepStrategies,
    IRetentionExecutionSettings? options = null,
    ILogger<RetentionSweepEngine>? logger = null
)
{
    private readonly IReadOnlyDictionary<Strategy, IRetentionSweepStrategy> strategies =
        sweepStrategies.ToDictionary(strategy => strategy.HandlesStrategy);

    public async Task<RetentionSweepResult> SweepAsync(
        TenantContext tenant,
        DateTimeOffset now,
        SweepTriggerKind trigger,
        SweepEntityScope scope,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(tenant);

        var sweepId = Guid.NewGuid();
        var startedAt = DateTimeOffset.UtcNow;
        var batchSize = Math.Max(1, options?.SweepBatchSize ?? 5000);
        var lifecycle = new RetentionRunLifecycle(auditWriter, logger);
        var entityFailures = new List<string>();
        var startedPersisted = false;

        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;
        var runLockAcquired = false;

        try
        {
            // Started commits immediately (no ambient transaction): a sweep that later
            // fails or crashes still leaves audit evidence that it was attempted.
            await lifecycle.WriteDurableAsync(
                new SweepEvent.Started(sweepId, startedAt, trigger, DryRun: false, tenant.Id),
                CancellationToken.None
            );
            startedPersisted = true;

            if (options?.DryRun == true)
            {
                throw new InvalidOperationException(
                    "Cohort is configured with DryRun enabled. RetentionSweepEngine.SweepAsync mutates data and refuses to run as a safety net; use IRetentionPreview for a count-only pass, or clear Cohort:DryRun."
                );
            }

            await validator.ValidateAsync(ct);
            var executionPlan = await BuildExecutionPlanAsync(tenant, now, scope, ct);
            if (shouldCloseConnection)
            {
                await db.Database.OpenConnectionAsync(ct);
            }
            await RetentionRunAdvisoryLock.AcquireAsync(connection, sweepId, ct);
            runLockAcquired = true;

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
                        lifecycle,
                        ct
                    );
                }
                catch (OperationCanceledException)
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

            ct.ThrowIfCancellationRequested();
            var completedAt = DateTimeOffset.UtcNow;
            var totalAffected = lifecycle.AccumulatedAffectedTotal;
            if (entityFailures.Count > 0)
            {
                await lifecycle.WriteDurableAsync(
                    new SweepEvent.PartiallyFailed(
                        sweepId,
                        completedAt,
                        completedAt - startedAt,
                        totalAffected,
                        RetentionRunLifecycle.TruncateError(string.Join("; ", entityFailures))
                    ),
                    CancellationToken.None
                );
            }
            else
            {
                await lifecycle.WriteDurableAsync(
                    new SweepEvent.Completed(
                        sweepId,
                        completedAt,
                        completedAt - startedAt,
                        totalAffected
                    ),
                    CancellationToken.None
                );
            }
        }
        catch (OperationCanceledException ex) when (startedPersisted && ct.IsCancellationRequested)
        {
            var cancelledAt = DateTimeOffset.UtcNow;
            await lifecycle.TrySettleTerminalAsync(
                new SweepEvent.Cancelled(
                    sweepId,
                    cancelledAt,
                    RetentionRunLifecycle.TruncateError(ex.Message),
                    cancelledAt - startedAt,
                    lifecycle.AccumulatedAffectedTotal
                ),
                "cancelled sweep",
                sweepId
            );
            throw;
        }
        catch (Exception ex) when (startedPersisted)
        {
            var failedAt = DateTimeOffset.UtcNow;
            await lifecycle.TrySettleTerminalAsync(
                new SweepEvent.Failed(
                    sweepId,
                    failedAt,
                    RetentionRunLifecycle.TruncateError(ex.Message),
                    failedAt - startedAt,
                    lifecycle.AccumulatedAffectedTotal
                ),
                "failed sweep",
                sweepId
            );
            throw;
        }
        finally
        {
            if (runLockAcquired)
            {
                await RetentionRunAdvisoryLock.ReleaseAsync(
                    connection,
                    sweepId,
                    CancellationToken.None
                );
            }
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }

        return lifecycle.CreateSweepResult(entityFailures);
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
        SweepEntityScope scope,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(tenant);

        var sweepId = Guid.NewGuid();
        var startedAt = DateTimeOffset.UtcNow;
        var lifecycle = new RetentionRunLifecycle(auditWriter, logger);
        var entityFailures = new List<string>();
        var startedPersisted = false;

        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;
        var runLockAcquired = false;

        try
        {
            await lifecycle.WriteDurableAsync(
                new SweepEvent.Started(sweepId, startedAt, trigger, DryRun: true, tenant.Id),
                CancellationToken.None
            );
            startedPersisted = true;
            await validator.ValidateAsync(ct);
            var executionPlan = await BuildExecutionPlanAsync(tenant, now, scope, ct);
            if (shouldCloseConnection)
            {
                await db.Database.OpenConnectionAsync(ct);
            }
            await RetentionRunAdvisoryLock.AcquireAsync(connection, sweepId, ct);
            runLockAcquired = true;

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
                    var resolvedPeriod = CutoffCalculator.ResolveEffectivePeriod(
                        rule.Period,
                        rule.LegalMin
                    );
                    var affected = 0L;
                    var heldCount = 0L;
                    var nullAnchorCount = 0L;

                    if (rule.Strategy != Strategy.Exempt)
                    {
                        var measurement = await RetentionPreviewMeasurement.MeasureAsync(
                            strategies[rule.Strategy],
                            entry,
                            rule,
                            context,
                            connection,
                            ct
                        );
                        affected = measurement.Affected;
                        heldCount = measurement.HeldCount;
                        nullAnchorCount = measurement.NullAnchorCount;
                    }

                    await lifecycle.WriteDurableAsync(
                        new SweepEvent.EntitySummary(
                            sweepId,
                            eventAt,
                            entry.EntityType,
                            entry.EntityId,
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
                        ct
                    );
                }
                catch (OperationCanceledException)
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

            ct.ThrowIfCancellationRequested();
            var completedAt = DateTimeOffset.UtcNow;
            var totalAffected = lifecycle.AccumulatedAffectedTotal;
            if (entityFailures.Count > 0)
            {
                await lifecycle.WriteDurableAsync(
                    new SweepEvent.PartiallyFailed(
                        sweepId,
                        completedAt,
                        completedAt - startedAt,
                        totalAffected,
                        RetentionRunLifecycle.TruncateError(string.Join("; ", entityFailures))
                    ),
                    CancellationToken.None
                );
            }
            else
            {
                await lifecycle.WriteDurableAsync(
                    new SweepEvent.Completed(
                        sweepId,
                        completedAt,
                        completedAt - startedAt,
                        totalAffected
                    ),
                    CancellationToken.None
                );
            }
        }
        catch (OperationCanceledException ex) when (startedPersisted && ct.IsCancellationRequested)
        {
            var cancelledAt = DateTimeOffset.UtcNow;
            await lifecycle.TrySettleTerminalAsync(
                new SweepEvent.Cancelled(
                    sweepId,
                    cancelledAt,
                    RetentionRunLifecycle.TruncateError(ex.Message),
                    cancelledAt - startedAt,
                    lifecycle.AccumulatedAffectedTotal
                ),
                "cancelled dry run",
                sweepId
            );
            throw;
        }
        catch (Exception ex) when (startedPersisted)
        {
            var failedAt = DateTimeOffset.UtcNow;
            await lifecycle.TrySettleTerminalAsync(
                new SweepEvent.Failed(
                    sweepId,
                    failedAt,
                    RetentionRunLifecycle.TruncateError(ex.Message),
                    failedAt - startedAt,
                    lifecycle.AccumulatedAffectedTotal
                ),
                "failed dry run",
                sweepId
            );
            throw;
        }
        finally
        {
            if (runLockAcquired)
            {
                await RetentionRunAdvisoryLock.ReleaseAsync(
                    connection,
                    sweepId,
                    CancellationToken.None
                );
            }
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }

        return lifecycle.CreateSweepResult(entityFailures);
    }

    private async Task<
        List<(RetentionEntry Entry, RetentionResolutionContext Context, RetentionRule Rule)>
    > BuildExecutionPlanAsync(
        TenantContext tenant,
        DateTimeOffset now,
        SweepEntityScope scope,
        CancellationToken ct
    )
    {
        var executionPlan =
            new List<(
                RetentionEntry Entry,
                RetentionResolutionContext Context,
                RetentionRule Rule
            )>();

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
            if (rule.Strategy != Strategy.Exempt && !strategies.ContainsKey(rule.Strategy))
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
        RetentionRunLifecycle lifecycle,
        CancellationToken ct
    )
    {
        var connection = db.Database.GetDbConnection();
        var eventAt = DateTimeOffset.UtcNow;
        var resolvedPeriod = CutoffCalculator.ResolveEffectivePeriod(rule.Period, rule.LegalMin);
        var effectiveAuditDetail =
            entry.AuditRowDetail == AuditRowDetail.Inherit
                ? rule.AuditRowDetail
                : entry.AuditRowDetail;
        var affectedCount = 0L;
        var skippedCount = 0L;
        var heldCount = 0L;
        var nullAnchorCount = 0L;

        if (rule.Strategy != Strategy.Exempt)
        {
            var strategy = strategies[rule.Strategy];
            var excludedRecordIds = new List<string>();

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
                        new SweepMutationContext(
                            sweepId,
                            DateTimeOffset.UtcNow,
                            batchSize,
                            excludedRecordIds
                        )
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

                    // Row-detail audit commits with the batch whose mutations it
                    // documents: a crash later in the run cannot lose evidence for rows
                    // already retired, and affected ids never accumulate in memory.
                    if (
                        effectiveAuditDetail == AuditRowDetail.PerRow
                        && !execution.RowDetailsPersisted
                    )
                    {
                        foreach (var recordId in execution.AffectedRecordIds)
                        {
                            await auditWriter.WriteAsync(
                                new SweepEvent.RowDetail(
                                    sweepId,
                                    eventAt,
                                    entry.EntityType,
                                    entry.EntityId,
                                    recordId,
                                    entry.Category,
                                    rule.Strategy,
                                    tenant.Id
                                ),
                                ct
                            );
                        }
                    }

                    await auditWriter.WriteAsync(
                        new SweepEvent.EntityProgress(
                            sweepId,
                            eventAt,
                            entry.EntityType,
                            entry.EntityId,
                            entry.Category,
                            tenant.Id,
                            rule.Strategy,
                            resolvedPeriod,
                            execution.AffectedRecordIds.Count,
                            execution.SkippedCount,
                            rule.Provenance
                        ),
                        ct
                    );

                    await transaction.CommitAsync(ct);
                }

                affectedCount += execution.AffectedRecordIds.Count;
                skippedCount += execution.SkippedCount;
                lifecycle.ReplaceEntityCount(
                    entry,
                    tenant.Id,
                    rule.Strategy,
                    affectedCount,
                    heldCount,
                    skippedCount,
                    nullAnchorCount
                );
                // Skipped rows stay eligible (their mutation never ran), so later
                // batches of this run must not reselect them: re-running their failed
                // OnBefore would re-insert the same row detail under this sweep id.
                excludedRecordIds.AddRange(execution.SkippedRecordIds);

                // Progress means the candidate filter shrank: rows were mutated, or
                // skipped rows joined the exclusion list. A batch with neither (e.g. a
                // custom strategy that skips without reporting ids) would reselect the
                // same rows forever; the remainder is deferred to the next sweep.
                var madeProgress =
                    execution.AffectedRecordIds.Count > 0 || execution.SkippedRecordIds.Count > 0;
                if (execution.CandidateCount < batchSize || !madeProgress)
                {
                    break;
                }
            }

            // Held rows are measured directly (eligible AND actively held) instead of
            // being inferred from candidate arithmetic.
            heldCount = await strategy.CountHeldAsync(entry, rule, context, connection, ct);
            nullAnchorCount = await strategy.CountNullAnchorsAsync(
                entry,
                rule,
                context,
                connection,
                ct
            );
        }

        lifecycle.ReplaceEntityCount(
            entry,
            tenant.Id,
            rule.Strategy,
            affectedCount,
            heldCount,
            skippedCount,
            nullAnchorCount
        );
        await lifecycle.WriteDurableAsync(
            new SweepEvent.EntitySummary(
                sweepId,
                eventAt,
                entry.EntityType,
                entry.EntityId,
                entry.Category,
                tenant.Id,
                rule.Strategy,
                resolvedPeriod,
                affectedCount,
                heldCount,
                skippedCount,
                nullAnchorCount,
                rule.Provenance
            ),
            ct
        );
    }
}

/// <summary>
/// Which retained entities a sweep covers. Every internal caller must choose explicitly
/// so tenantless rows cannot be swept as an accidental side effect of a tenanted pass.
/// </summary>
internal enum SweepEntityScope
{
    TenantedOnly,
    TenantlessOnly,
}
