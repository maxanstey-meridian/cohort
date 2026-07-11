using System.Data;
using System.Data.Common;
using System.Reflection;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure;

internal sealed class RetentionErasureService(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionRegistry registry,
    IRetentionCategoryRepository categoryRepository,
    RetentionStartupValidator validator,
    IRetentionAuditWriter auditWriter,
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
        var lifecycle = new RetentionRunLifecycle(auditWriter, logger);
        var startedPersisted = false;
        var entityFailures = new List<string>();
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;
        var runLockAcquired = false;
        var batchSize = Math.Max(1, options.SweepBatchSize);

        try
        {
            // Started commits immediately (no ambient transaction): an erasure that later
            // fails or crashes still leaves audit evidence that it was attempted.
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

            await validator.ValidateAsync(ct);
            foreach (var entry in registry.Scan().Values)
            {
                if (entry.Tenant is null)
                {
                    continue;
                }

                var predicate = ResolveMatch(entry, scope);
                if (predicate is null)
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
                        $"Retention strategy '{rule.Strategy}' is not registered for erasure execution."
                    );
                }

                if (rule.Strategy == Strategy.SoftDelete && !scope.AllowSoftDeleteAsErasure)
                {
                    throw new InvalidOperationException(
                        $"Erasure for entity {entry.EntityType.FullName} (category '{entry.Category}') resolves to the SoftDelete strategy, which only sets the soft-delete flag and leaves personal data in place. If that genuinely satisfies the erasure request, opt in with new ErasureScope(subject, allowSoftDeleteAsErasure: true)."
                    );
                }

                executionPlan.Add((entry, context, rule, predicate));
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
                    entityFailures.Add($"{entry.EntityType.FullName}: {ex.Message}");
                    logger?.LogError(
                        ex,
                        "Cohort erasure {SweepId} failed for entity {EntityType}; continuing with remaining entities.",
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
                "cancelled erasure",
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
                "failed erasure",
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

        return lifecycle.CreateErasureResult(scope, entityFailures);
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
        var resolvedPeriod = CutoffCalculator.ResolveEffectivePeriod(rule.Period, rule.LegalMin);
        var effectiveAuditDetail =
            entry.AuditRowDetail == AuditRowDetail.Inherit
                ? rule.AuditRowDetail
                : entry.AuditRowDetail;
        var affectedCount = 0L;
        var skippedCount = 0L;
        var heldCount = 0L;

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
                var excludedRecordIds = new List<string>();
                while (true)
                {
                    ct.ThrowIfCancellationRequested();

                    SweepExecutionResult execution;
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
                        skippedCount
                    );
                    // Skipped rows stay eligible (their mutation never ran), so later
                    // batches of this run must not reselect them: re-running their
                    // failed OnBefore would re-insert the same row detail under this
                    // sweep id.
                    excludedRecordIds.AddRange(execution.SkippedRecordIds);

                    // Progress means the candidate filter shrank: rows were mutated, or
                    // skipped rows joined the exclusion list. A batch with neither
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

            // Held rows are measured directly (subject-matching, past cutoff, actively
            // held) instead of being inferred from candidate arithmetic.
            heldCount = await strategy.CountHeldForEraseAsync(
                entry,
                rule,
                predicate,
                tenant,
                now,
                connection,
                ct
            );
        }

        if (!dryRun)
        {
            lifecycle.ReplaceEntityCount(
                entry,
                tenant.Id,
                rule.Strategy,
                affectedCount,
                heldCount,
                skippedCount
            );
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
                affectedCount,
                heldCount,
                skippedCount,
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
                skippedCount
            );
        }
    }

    private ErasureSubjectPredicate? ResolveMatch(RetentionEntry entry, ErasureScope scope)
    {
        var subjectProperties = entry
            .EntityType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(property => property.IsDefined(typeof(ErasureSubjectAttribute), inherit: false))
            .OrderBy(property => property.Name, StringComparer.Ordinal)
            .ToArray();

        if (subjectProperties.Length == 0)
        {
            return null;
        }

        var entityType =
            db.Model.FindEntityType(entry.EntityType)
            ?? throw new InvalidOperationException(
                $"Entity {entry.EntityType.FullName} is not mapped by the current EF model."
            );
        var storeObject =
            StoreObjectIdentifier.Create(entityType, StoreObjectType.Table)
            ?? throw new InvalidOperationException(
                $"Entity {entry.EntityType.FullName} does not have a mapped table for erasure."
            );

        var effectiveTypes = subjectProperties
            .Select(property =>
                Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType
            )
            .Distinct()
            .ToArray();
        if (effectiveTypes.Length > 1)
        {
            throw new InvalidOperationException(
                $"Entity {entry.EntityType.FullName} defines incompatible [ErasureSubject] properties. All marked properties must share the same effective CLR type after nullable unwrapping. Found: {string.Join(", ", subjectProperties.Select(property => $"{property.Name}:{(Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType).Name}"))}."
            );
        }

        var matches = subjectProperties
            .Select(subjectProperty =>
            {
                var efProperty =
                    entityType.FindProperty(subjectProperty.Name)
                    ?? throw new InvalidOperationException(
                        $"[ErasureSubject] on {entry.EntityType.FullName}.{subjectProperty.Name}: property is not mapped by EF."
                    );
                var subjectColumn =
                    efProperty.GetColumnName(storeObject)
                    ?? throw new InvalidOperationException(
                        $"[ErasureSubject] on {entry.EntityType.FullName}.{subjectProperty.Name}: property has no mapped table column."
                    );

                return new ErasureSubjectMatch(
                    subjectProperty.Name,
                    subjectColumn,
                    ConvertSubjectValue(
                        entry.EntityType,
                        subjectProperty,
                        efProperty,
                        scope.Subject
                    )
                );
            })
            .ToArray();

        return new ErasureSubjectPredicate(matches);
    }

    private static object ConvertSubjectValue(
        Type entityType,
        PropertyInfo property,
        IProperty efProperty,
        object subject
    )
    {
        var targetType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
        if (targetType.IsInstanceOfType(subject))
        {
            var converter = efProperty.GetTypeMapping().Converter;
            return converter?.ConvertToProvider(subject) ?? subject;
        }

        throw new InvalidOperationException(
            $"Erasure scope subject value of type {subject.GetType().Name} cannot be expressed against [ErasureSubject] property '{property.Name}' on {entityType.FullName}, which expects {targetType.Name}."
        );
    }

}
