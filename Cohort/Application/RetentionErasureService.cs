using System.Data;
using System.Reflection;

using Cohort.Domain;
using Cohort.Hosting;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;

namespace Cohort.Application;

public sealed class RetentionErasureService(
    DbContext db,
    RetentionRegistry registry,
    IRetentionCategoryRepository categoryRepository,
    RetentionStartupValidator validator,
    IRetentionAuditWriter auditWriter,
    IEnumerable<IRetentionSweepStrategy> sweepStrategies,
    IOptionsMonitor<CohortOptions> options
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

        foreach (
            var entry in registry
                .Scan()
                .Values.OrderBy(entry => entry.EntityType.FullName, StringComparer.Ordinal)
        )
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

            executionPlan.Add((entry, context, rule, predicate));
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
                    SweepTriggerKind.Erasure,
                    DryRun: dryRun,
                    tenant.Id
                ),
                auditEvents,
                ct
            );

            foreach (var (entry, context, rule, predicate) in executionPlan)
            {
                var eventAt = DateTimeOffset.UtcNow;
                var resolvedPeriod = CutoffCalculator.ResolveEffectivePeriod(rule.Period, rule.LegalMin);
                var (execution, affectedCount) = rule.Strategy switch
                {
                    Strategy.Exempt => (new SweepExecutionResult([], 0), 0),
                    _ when dryRun => (
                        new SweepExecutionResult([], 0),
                        await strategies[rule.Strategy].PreviewEraseAsync(
                            entry,
                            rule,
                            predicate,
                            tenant,
                            now,
                            connection,
                            ct
                        )
                    ),
                    _ => (
                        await strategies[rule.Strategy].EraseAsync(
                            entry,
                            rule,
                            predicate,
                            tenant,
                            now,
                            connection,
                            dbTransaction,
                            ct,
                            new SweepMutationContext(sweepId, eventAt)
                        ),
                        -1
                    ),
                };

                if (affectedCount < 0)
                {
                    affectedCount = execution.AffectedRecordIds.Count;
                }

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
                        execution.HeldCount,
                        execution.SkippedCount,
                        rule.Provenance
                    ),
                    auditEvents,
                    ct
                );

                var effectiveAuditDetail = entry.AuditRowDetail == AuditRowDetail.Inherit
                    ? rule.AuditRowDetail
                    : entry.AuditRowDetail;
                if (
                    effectiveAuditDetail == AuditRowDetail.PerRow
                    && !execution.RowDetailsPersisted
                )
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

        return CreateResult(auditEvents, scope);
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
        ErasureScope scope
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
                    summary.Affected
                )
            )
            .ToArray();

        return new ErasureResult(started.SweepId, started.At, completed.At, scope, counts);
    }
}
