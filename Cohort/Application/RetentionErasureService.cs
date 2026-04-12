using System.Data;
using System.Reflection;

using Cohort.Domain;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace Cohort.Application;

public sealed class RetentionErasureService(
    DbContext db,
    RetentionRegistry registry,
    IRetentionCategoryRepository categoryRepository,
    RetentionStartupValidator validator,
    IRetentionAuditWriter auditWriter,
    IEnumerable<IRetentionSweepStrategy> sweepStrategies
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

        var sweepId = Guid.NewGuid();
        var startedAt = DateTimeOffset.UtcNow;
        var executionPlan = new List<(
            RetentionEntry Entry,
            RetentionResolutionContext Context,
            RetentionRule Rule,
            ErasureSubjectMatch Match
        )>();
        var auditEvents = new List<SweepEvent>();

        foreach (
            var entry in registry
                .Scan()
                .Values.OrderBy(entry => entry.EntityType.FullName, StringComparer.Ordinal)
        )
        {
            var match = ResolveMatch(entry, scope);
            if (match is null)
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

            executionPlan.Add((entry, context, rule, match));
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
                    DryRun: false,
                    tenant.Id
                ),
                auditEvents,
                ct
            );

            foreach (var (entry, context, rule, match) in executionPlan)
            {
                var eventAt = DateTimeOffset.UtcNow;
                var resolvedPeriod = CutoffCalculator.ResolveEffectivePeriod(rule.Period, rule.LegalMin);
                var execution = rule.Strategy switch
                {
                    Strategy.Exempt => new SweepExecutionResult([], 0),
                    _ => await strategies[rule.Strategy].EraseAsync(
                        entry,
                        rule,
                        match,
                        tenant,
                        now,
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

                if (rule.AuditRowDetail == AuditRowDetail.PerRow)
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

    private ErasureSubjectMatch? ResolveMatch(RetentionEntry entry, ErasureScope scope)
    {
        var subjectProperties = entry.EntityType
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(property => property.IsDefined(typeof(ErasureSubjectAttribute), inherit: false))
            .ToArray();

        if (subjectProperties.Length == 0)
        {
            return null;
        }

        if (subjectProperties.Length > 1)
        {
            throw new InvalidOperationException(
                $"Entity {entry.EntityType.FullName} defines multiple [ErasureSubject] properties. Exactly one is required."
            );
        }

        var subjectProperty = subjectProperties[0];
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
            ConvertSubjectValue(entry.EntityType, subjectProperty, scope.Subject)
        );
    }

    private static object ConvertSubjectValue(Type entityType, PropertyInfo property, object subject)
    {
        var targetType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
        if (targetType.IsInstanceOfType(subject))
        {
            return subject;
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
