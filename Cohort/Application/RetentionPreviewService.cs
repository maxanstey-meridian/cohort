using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;

using Cohort.Domain;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Application;

public sealed class RetentionPreviewService(
    DbContext db,
    RetentionRegistry registry,
    IRetentionCategoryRepository categoryRepository
) : IRetentionPreview
{
    private static readonly MethodInfo DbContextSetMethod = typeof(DbContext)
        .GetMethods(BindingFlags.Public | BindingFlags.Instance)
        .Single(method =>
            method.Name == nameof(DbContext.Set)
            && method.IsGenericMethodDefinition
            && method.GetParameters().Length == 0
        );

    private static readonly MethodInfo CountAsyncMethod = typeof(EntityFrameworkQueryableExtensions)
        .GetMethods(BindingFlags.Public | BindingFlags.Static)
        .Single(method =>
            method.Name == nameof(EntityFrameworkQueryableExtensions.CountAsync)
            && method.IsGenericMethodDefinition
            && method.GetParameters().Length == 2
            && method.GetParameters()[1].ParameterType == typeof(CancellationToken)
        );

    public async Task<RetentionSweepResult> PreviewAsync(
        TenantContext tenant,
        DateTimeOffset now,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(tenant);

        var startedAt = DateTimeOffset.UtcNow;
        var counts = new List<EntitySweepCount>();

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
                rule.Strategy is not Strategy.Purge
                    and not Strategy.SoftDelete
                    and not Strategy.Exempt
            )
            {
                throw new InvalidOperationException(
                    $"Retention strategy '{rule.Strategy}' is not supported by the preview path."
                );
            }

            var affected = rule.Strategy switch
            {
                Strategy.Purge => await CountCandidatesAsync(entry, rule, context, ct),
                Strategy.SoftDelete => await CountCandidatesAsync(entry, rule, context, ct),
                Strategy.Exempt => 0,
                _ => throw new UnreachableException(
                    "Preview execution should only contain supported strategies."
                ),
            };

            counts.Add(
                new EntitySweepCount(
                    entry.EntityType,
                    entry.Category,
                    tenant.Id,
                    rule.Strategy,
                    affected
                )
            );
        }

        return new RetentionSweepResult(Guid.NewGuid(), startedAt, DateTimeOffset.UtcNow, counts);
    }

    private async Task<int> CountCandidatesAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext context,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(context);

        if (rule.Strategy is not Strategy.Purge and not Strategy.SoftDelete)
        {
            throw new InvalidOperationException(
                $"RetentionPreviewService cannot preview {rule.Strategy} rules."
            );
        }

        var tenant = entry.Tenant
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose tenant metadata for purge previews."
            );
        var isDeletedMember = rule.Strategy == Strategy.SoftDelete
            ? GetSoftDeleteEligibilityMember(entry)
            : null;

        var query = (IQueryable)
            DbContextSetMethod.MakeGenericMethod(entry.EntityType).Invoke(db, null)!;
        var predicate = BuildEligibilityPredicate(
            entry.EntityType,
            entry.AnchorMember,
            tenant.TenantMember,
            context.Tenant.Id,
            CutoffCalculator.Compute(context.Now, rule.Period, rule.LegalMin),
            isDeletedMember
        );
        var filtered = query.Provider.CreateQuery(
            Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Where),
                [entry.EntityType],
                query.Expression,
                predicate
            )
        );

        var countTask = (Task<int>)
            CountAsyncMethod.MakeGenericMethod(entry.EntityType).Invoke(null, [filtered, ct])!;

        return await countTask;
    }

    private static string GetSoftDeleteEligibilityMember(RetentionEntry entry)
    {
        var softDelete = entry.SoftDelete
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose soft-delete metadata for soft-delete previews."
            );
        var isDeletedMember = entry.EntityType.GetProperty(
            softDelete.IsDeletedMember,
            BindingFlags.Public | BindingFlags.Instance
        );

        if (isDeletedMember is null || isDeletedMember.PropertyType != typeof(bool))
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose a public bool {softDelete.IsDeletedMember} property for soft-delete previews."
            );
        }

        return softDelete.IsDeletedMember;
    }

    private static LambdaExpression BuildEligibilityPredicate(
        Type entityType,
        string anchorMember,
        string tenantMember,
        Guid tenantId,
        DateTimeOffset cutoff,
        string? isDeletedMember
    )
    {
        var entity = Expression.Parameter(entityType, "entity");
        var tenantAccess = Expression.Property(entity, tenantMember);
        var anchorAccess = Expression.Property(entity, anchorMember);

        var tenantPredicate = Expression.Equal(
            tenantAccess,
            tenantAccess.Type == typeof(Guid?)
                ? Expression.Convert(Expression.Constant(tenantId), typeof(Guid?))
                : Expression.Constant(tenantId)
        );
        var cutoffPredicate = CreateCutoffPredicate(anchorAccess, cutoff);
        var predicate = Expression.AndAlso(cutoffPredicate, tenantPredicate);

        if (isDeletedMember is not null)
        {
            var isDeletedAccess = Expression.Property(entity, isDeletedMember);
            predicate = Expression.AndAlso(
                predicate,
                Expression.Equal(isDeletedAccess, Expression.Constant(false))
            );
        }

        return Expression.Lambda(predicate, entity);
    }

    private static Expression CreateCutoffPredicate(
        MemberExpression anchorAccess,
        DateTimeOffset cutoff
    )
    {
        if (anchorAccess.Type == typeof(DateTimeOffset))
        {
            return Expression.LessThan(anchorAccess, Expression.Constant(cutoff));
        }

        if (anchorAccess.Type == typeof(DateTimeOffset?))
        {
            return Expression.AndAlso(
                Expression.Property(anchorAccess, nameof(Nullable<DateTimeOffset>.HasValue)),
                Expression.LessThan(
                    Expression.Property(anchorAccess, nameof(Nullable<DateTimeOffset>.Value)),
                    Expression.Constant(cutoff)
                )
            );
        }

        if (anchorAccess.Type == typeof(DateTime))
        {
            return Expression.LessThan(anchorAccess, Expression.Constant(cutoff.UtcDateTime));
        }

        if (anchorAccess.Type == typeof(DateTime?))
        {
            return Expression.AndAlso(
                Expression.Property(anchorAccess, nameof(Nullable<DateTime>.HasValue)),
                Expression.LessThan(
                    Expression.Property(anchorAccess, nameof(Nullable<DateTime>.Value)),
                    Expression.Constant(cutoff.UtcDateTime)
                )
            );
        }

        throw new InvalidOperationException(
            $"Anchor member '{anchorAccess.Member.Name}' must be DateTime or DateTimeOffset (nullable allowed), got {anchorAccess.Type.Name}."
        );
    }
}
