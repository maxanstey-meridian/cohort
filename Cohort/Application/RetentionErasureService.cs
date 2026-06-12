using System.Data;
using System.Data.Common;
using System.Reflection;

using Cohort.Domain;
using Cohort.Hosting;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Cohort.Application;

public sealed class RetentionErasureService(
    DbContext db,
    RetentionRegistry registry,
    IRetentionCategoryRepository categoryRepository,
    RetentionStartupValidator validator,
    IRetentionAuditWriter auditWriter,
    IEnumerable<IRetentionSweepStrategy> sweepStrategies,
    IOptionsMonitor<CohortOptions> options,
    ILogger<RetentionErasureService>? logger = null
) : IRetentionErasureService
{
    private readonly IReadOnlyDictionary<Strategy, IRetentionSweepStrategy> strategies = sweepStrategies
        .ToDictionary(strategy => strategy.HandlesStrategy);

    public async Task<ErasureResult> EraseAsync(
        TenantContext tenant,
        ErasureScope scope,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(scope);
        await validator.ValidateAsync(ct);
        var dryRun = options.CurrentValue.DryRun;

        var sweepId = Guid.NewGuid();
        var startedAt = DateTimeOffset.UtcNow;
        var executionPlan = new List<(
            RetentionEntry Entry,
            RetentionResolutionContext Context,
            RetentionRule Rule,
            ErasureSubjectPredicate Predicate
        )>();
        var auditEvents = new List<SweepEvent>();

        foreach (var entry in registry.Scan().Values)
        {
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

        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        var batchSize = Math.Max(1, options.CurrentValue.SweepBatchSize);
        var entityFailures = new List<string>();

        try
        {
            // Started commits immediately (no ambient transaction): an erasure that later
            // fails or crashes still leaves audit evidence that it was attempted.
            await WriteAuditEventAsync(
                new SweepEvent.Started(
                    sweepId,
                    startedAt,
                    SweepTriggerKind.Erasure,
                    DryRun: dryRun,
                    tenant.Id
                ),
                auditEvents,
                ct
            );

            foreach (
                var (entry, context, rule, predicate) in RetentionExecutionPlanOrderer.Order(
                    db,
                    executionPlan,
                    item => item.Entry
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

        return CreateResult(auditEvents, scope, entityFailures);
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
        List<SweepEvent> auditEvents,
        CancellationToken ct
    )
    {
        var eventAt = DateTimeOffset.UtcNow;
        var resolvedPeriod = CutoffCalculator.ResolveEffectivePeriod(rule.Period, rule.LegalMin);
        var affectedRecordIds = new List<string>();
        var affectedCount = 0L;
        var skippedCount = 0L;
        var heldCount = 0L;
        var rowDetailsPersisted = false;

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
                    // looping again would reselect the same rows forever.
                    if (execution.CandidateCount < batchSize || execution.AffectedRecordIds.Count == 0)
                    {
                        break;
                    }
                }

                affectedCount = affectedRecordIds.Count;
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

        await WriteAuditEventAsync(
            new SweepEvent.EntitySummary(
                sweepId,
                eventAt,
                entry.EntityType,
                entry.Category,
                tenant.Id,
                rule.Strategy,
                resolvedPeriod,
                affectedCount,
                heldCount,
                skippedCount,
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
    }

    private static string Truncate(string value, int maxLength)
    {
        return value.Length <= maxLength ? value : value[..maxLength];
    }

    private ErasureSubjectPredicate? ResolveMatch(RetentionEntry entry, ErasureScope scope)
    {
        var subjectProperties = entry.EntityType
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
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
            .Select(property => Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType)
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
                    ConvertSubjectValue(entry.EntityType, subjectProperty, efProperty, scope.Subject)
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

    private async Task WriteAuditEventAsync(
        SweepEvent evt,
        ICollection<SweepEvent> auditEvents,
        CancellationToken ct
    )
    {
        await auditWriter.WriteAsync(evt, ct);
        auditEvents.Add(evt);
    }

    private static ErasureResult CreateResult(
        IEnumerable<SweepEvent> auditEvents,
        ErasureScope scope,
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
                    summary.SkippedCount
                )
            )
            .ToArray();

        return new ErasureResult(
            started.SweepId,
            started.At,
            completed.At,
            scope,
            counts,
            started.DryRun,
            entityFailures
        );
    }
}
