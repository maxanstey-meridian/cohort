using System.Data;
using System.Data.Common;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Audit;
using Cohort.Infrastructure.Sweep;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure;

internal sealed class RetentionErasureService(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionRegistry registry,
    IRetentionRuleProvider ruleProvider,
    RetentionRuntimeReadinessValidator readinessValidator,
    RetentionValidationState validationState,
    EfRetentionAuditWriter auditWriter,
    RetentionAuditNotifier auditNotifier,
    IEnumerable<IRetentionSweepStrategy> sweepStrategies,
    IRetentionExecutionSettings options,
    ILogger<RetentionErasureService>? logger = null
)
{
    private readonly IReadOnlyDictionary<Strategy, IRetentionSweepStrategy> strategies =
        sweepStrategies.ToDictionary(strategy => strategy.HandlesStrategy);

    public async Task<ErasureResult> EraseAsync(
        TenantContext tenant,
        ErasureScope scope,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(scope);
        await readinessValidator.ValidateAsync(ct);
        var dryRun = options.DryRun;

        var sweepId = Guid.NewGuid();
        var startedAt = DateTimeOffset.UtcNow;
        var executionPlan =
            new List<(
                RetentionEntry Entry,
                RetentionResolutionContext Context,
                RetentionRule Rule,
                ErasureSubjectPredicate Predicate
            )>();
        var lifecycle = new RetentionRunLifecycle(auditWriter, auditNotifier, logger);
        var startedPersisted = false;
        var entityFailures = new List<string>();
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;
        var runLockAcquired = false;
        var batchSize = Math.Max(1, options.SweepBatchSize);
        Exception? primaryException = null;

        async Task BuildExecutionPlanAsync()
        {
            foreach (var entry in registry.Scan().Values)
            {
                if (entry.Tenant is null)
                {
                    continue;
                }

                var subjectMetadata = validationState.ErasureSubjects[entry.EntityType];
                if (subjectMetadata is null)
                {
                    continue;
                }

                var context = new RetentionResolutionContext(entry.Category, tenant, now, []);
                RetentionRule rule;
                try
                {
                    rule = await RetentionRuleProviderResolution.ResolveAsync(
                        ruleProvider,
                        readinessValidator.ValidatedCapabilities,
                        context,
                        ct
                    );
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    RecordPreparationFailure(entry, ex, sweepId, entityFailures);
                    continue;
                }

                if (rule.Strategy != Strategy.Exempt && !strategies.ContainsKey(rule.Strategy))
                {
                    throw new InvalidOperationException(
                        $"Retention strategy '{rule.Strategy}' is not registered for erasure execution."
                    );
                }

                if (rule.Strategy == Strategy.SoftDelete && !scope.AllowSoftDeleteAsErasure)
                {
                    throw new InvalidOperationException(
                        $"Erasure for entity {entry.EntityType.FullName} (category '{entry.Category}') resolves to the SoftDelete strategy, which only sets the soft-delete flag and leaves personal data in place. If that genuinely satisfies the erasure request, opt in with new ErasureScope(subject, allowSoftDeleteAsErasure: true)."
                    );
                }

                try
                {
                    executionPlan.Add(
                        (entry, context, rule, subjectMetadata.CreatePredicate(scope.Subject))
                    );
                }
                catch (Exception ex)
                {
                    RecordPreparationFailure(entry, ex, sweepId, entityFailures);
                }
            }
        }

        try
        {
            if (!scope.AllowSoftDeleteAsErasure)
            {
                await BuildExecutionPlanAsync();
            }

            // Caller-policy refusal is validated before an attempted run exists. Once the
            // plan is accepted, Started commits immediately so later failures remain visible.
            await lifecycle.WriteDurableAsync(
                new SweepEvent.Started(
                    sweepId,
                    startedAt,
                    SweepTriggerKind.Erasure,
                    DryRun: dryRun,
                    tenant.Id
                ),
                CancellationToken.None
            );
            startedPersisted = true;

            if (shouldCloseConnection)
            {
                await db.Database.OpenConnectionAsync(ct);
            }
            await RetentionRunAdvisoryLock.AcquireAsync(connection, sweepId, ct);
            runLockAcquired = true;

            if (scope.AllowSoftDeleteAsErasure)
            {
                await BuildExecutionPlanAsync();
            }

            foreach (
                var (entry, context, rule, predicate) in RetentionExecutionPlanOrderer.Order(
                    db,
                    executionPlan,
                    item => item.Entry,
                    logger
                )
            )
            {
                _ = context;
                try
                {
                    await EraseEntityAsync(
                        entry,
                        rule,
                        predicate,
                        sweepId,
                        tenant,
                        now,
                        dryRun,
                        batchSize,
                        connection,
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
                    // One entity's failure must not abort erasure of the subject's data in
                    // every other entity; the failure is recorded on the run row and
                    // surfaced in the result.
                    var diagnostic = RetentionFailureDiagnostic.Create(ex);
                    entityFailures.Add(diagnostic.ToString());
                    logger?.LogError(
                        ex,
                        "Cohort erasure {SweepId} failed for entity {EntityType}; continuing with remaining entities. Diagnostic {DiagnosticId}.",
                        sweepId,
                        entry.EntityType.FullName,
                        diagnostic.DiagnosticIdText
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
                        RetentionRunLifecycle.TruncateError(string.Join("\n", entityFailures))
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
            primaryException = ex;
            var cancelledAt = DateTimeOffset.UtcNow;
            var diagnostic = RetentionFailureDiagnostic.Create(ex);
            logger?.LogWarning(
                ex,
                "Cohort erasure {SweepId} was cancelled. Diagnostic {DiagnosticId}.",
                sweepId,
                diagnostic.DiagnosticIdText
            );
            await lifecycle.TrySettleTerminalAsync(
                new SweepEvent.Cancelled(
                    sweepId,
                    cancelledAt,
                    diagnostic.ToString(),
                    cancelledAt - startedAt,
                    lifecycle.AccumulatedAffectedTotal
                ),
                "cancelled erasure",
                sweepId
            );
            throw;
        }
        catch (Exception ex) when (startedPersisted)
        {
            primaryException = ex;
            var failedAt = DateTimeOffset.UtcNow;
            var diagnostic = RetentionFailureDiagnostic.Create(ex);
            logger?.LogError(
                ex,
                "Cohort erasure {SweepId} failed. Diagnostic {DiagnosticId}.",
                sweepId,
                diagnostic.DiagnosticIdText
            );
            await lifecycle.TrySettleTerminalAsync(
                new SweepEvent.Failed(
                    sweepId,
                    failedAt,
                    diagnostic.ToString(),
                    failedAt - startedAt,
                    lifecycle.AccumulatedAffectedTotal
                ),
                "failed erasure",
                sweepId
            );
            throw;
        }
        catch (Exception ex)
        {
            primaryException = ex;
            throw;
        }
        finally
        {
            await OperationalConnectionCleanup.RunAsync(
                runLockAcquired
                    ? cleanupToken =>
                        RetentionRunAdvisoryLock.ReleaseAsync(connection, sweepId, cleanupToken)
                    : null,
                shouldCloseConnection
                    ? cleanupToken => db.Database.CloseConnectionAsync().WaitAsync(cleanupToken)
                    : null,
                primaryException,
                logger
            );
        }

        return lifecycle.CreateErasureResult(scope, entityFailures);
    }

    private void RecordPreparationFailure(
        RetentionEntry entry,
        Exception exception,
        Guid sweepId,
        ICollection<string> entityFailures
    )
    {
        var diagnostic = RetentionFailureDiagnostic.Create(exception);
        entityFailures.Add(diagnostic.ToString());
        logger?.LogError(
            exception,
            "Cohort erasure {SweepId} failed to prepare entity {EntityType}; continuing with remaining entities. Diagnostic {DiagnosticId}.",
            sweepId,
            entry.EntityType.FullName,
            diagnostic.DiagnosticIdText
        );
    }

    private async Task EraseEntityAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        Guid sweepId,
        TenantContext tenant,
        DateTimeOffset now,
        bool dryRun,
        int batchSize,
        DbConnection connection,
        RetentionRunLifecycle lifecycle,
        CancellationToken ct
    )
    {
        var eventAt = DateTimeOffset.UtcNow;
        var resolvedPeriod = CutoffCalculator.ResolveErasureMinimumAge(rule.LegalMin);
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

            if (dryRun)
            {
                affectedCount = await strategy.PreviewEraseAsync(
                    entry,
                    rule,
                    predicate,
                    tenant,
                    now,
                    connection,
                    ct
                );
            }
            else
            {
                // Each batch selects, locks, and mutates at most batchSize rows in its own
                // transaction, mirroring the sweep engine: a failure loses only the
                // current batch and earlier batches stay erased.
                while (true)
                {
                    ct.ThrowIfCancellationRequested();

                    SweepExecutionResult execution;
                    List<SweepEvent> committedEvents = [];
                    await using (var transaction = await db.Database.BeginTransactionAsync(ct))
                    {
                        execution = await strategy.EraseAsync(
                            entry,
                            rule,
                            predicate,
                            tenant,
                            now,
                            connection,
                            transaction.GetDbTransaction(),
                            ct,
                            new SweepMutationContext(
                                sweepId,
                                DateTimeOffset.UtcNow,
                                batchSize
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
                        // documents: a crash later in the run cannot lose evidence for
                        // rows already erased, and affected ids never accumulate in
                        // memory.
                        if (
                            effectiveAuditDetail == AuditRowDetail.PerRow
                            && !execution.RowDetailsPersisted
                        )
                        {
                            foreach (var recordId in execution.AffectedRecordIds)
                            {
                                var rowDetail = new SweepEvent.RowDetail(
                                    sweepId,
                                    eventAt,
                                    entry.EntityType,
                                    entry.RetentionEntityId,
                                    recordId,
                                    entry.Category,
                                    rule.Strategy,
                                    tenant.Id
                                );
                                await auditWriter.WriteAsync(rowDetail, ct);
                                committedEvents.Add(rowDetail);
                            }
                        }
                        else if (effectiveAuditDetail == AuditRowDetail.PerRow)
                        {
                            committedEvents.AddRange(
                                execution.AffectedRecordIds.Select(recordId =>
                                    new SweepEvent.RowDetail(
                                        sweepId,
                                        eventAt,
                                        entry.EntityType,
                                        entry.RetentionEntityId,
                                        recordId,
                                        entry.Category,
                                        rule.Strategy,
                                        tenant.Id
                                    )
                                )
                            );
                        }

                        var progress = new SweepEvent.EntityProgress(
                            sweepId,
                            eventAt,
                            entry.EntityType,
                            entry.RetentionEntityId,
                            entry.Category,
                            tenant.Id,
                            rule.Strategy,
                            resolvedPeriod,
                            execution.AffectedRecordIds.Count,
                            execution.SkippedCount,
                            rule.Provenance
                        );
                        await auditWriter.WriteAsync(progress, ct);
                        committedEvents.Add(progress);

                        await transaction.CommitAsync(ct);
                    }

                    foreach (var committedEvent in committedEvents)
                    {
                        await lifecycle.NotifyCommittedAsync(committedEvent);
                    }

                    affectedCount += execution.AffectedRecordIds.Count;
                    skippedCount += execution.SkippedCount;
                    lifecycle.ReplaceEntityCount(
                        entry,
                        tenant.Id,
                        rule.Strategy,
                        affectedCount,
                        heldCount,
                        skippedCount
                    );
                    // Progress means the candidate filter shrank: rows were mutated, or
                    // skipped rows committed row-detail evidence. A batch with neither
                    // (e.g. a custom strategy that skips without reporting ids) would
                    // reselect the same rows forever.
                    var madeProgress =
                        execution.AffectedRecordIds.Count > 0
                        || execution.SkippedRecordIds.Count > 0;
                    if (execution.CandidateCount < batchSize || !madeProgress)
                    {
                        break;
                    }
                }
            }

            // Held rows are measured directly using the same subject, tenant,
            // strategy, and optional legal-minimum predicates as mutation.
            heldCount = await strategy.CountHeldForEraseAsync(
                entry,
                rule,
                predicate,
                tenant,
                now,
                connection,
                ct
            );
            if (CutoffCalculator.ComputeErasureCutoff(now, rule.LegalMin) is not null)
            {
                nullAnchorCount = await strategy.CountNullAnchorsForEraseAsync(
                    entry,
                    rule,
                    predicate,
                    tenant,
                    connection,
                    ct
                );
            }
        }

        if (!dryRun)
        {
            lifecycle.ReplaceEntityCount(
                entry,
                tenant.Id,
                rule.Strategy,
                affectedCount,
                heldCount,
                skippedCount,
                nullAnchorCount
            );
        }
        await lifecycle.WriteDurableAsync(
            new SweepEvent.EntitySummary(
                sweepId,
                eventAt,
                entry.EntityType,
                entry.RetentionEntityId,
                entry.Category,
                tenant.Id,
                rule.Strategy,
                resolvedPeriod,
                affectedCount,
                heldCount,
                skippedCount,
                nullAnchorCount,
                Provenance: rule.Provenance
            ),
            ct
        );
        if (dryRun)
        {
            lifecycle.ReplaceEntityCount(
                entry,
                tenant.Id,
                rule.Strategy,
                affectedCount,
                heldCount,
                skippedCount,
                nullAnchorCount
            );
        }
    }

}
