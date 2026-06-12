using Cohort.Domain;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Cohort.Application;

internal static class RetentionExecutionPlanOrderer
{
    internal static IReadOnlyList<TPlan> Order<TPlan>(
        DbContext db,
        IReadOnlyList<TPlan> plan,
        Func<TPlan, RetentionEntry> entrySelector,
        ILogger? logger = null
    )
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(entrySelector);

        if (plan.Count <= 1)
        {
            return plan;
        }

        var orderedItems = plan
            .OrderBy(
                item => entrySelector(item).EntityType.FullName,
                StringComparer.Ordinal
            )
            .ToArray();
        var itemByEntityType = orderedItems.ToDictionary(
            item => entrySelector(item).EntityType,
            item => item
        );
        var outgoingEdges = new Dictionary<Type, HashSet<Type>>();
        var incomingCounts = new Dictionary<Type, int>();

        foreach (var item in orderedItems)
        {
            var entityType = entrySelector(item).EntityType;
            outgoingEdges[entityType] = [];
            incomingCounts[entityType] = 0;
        }

        foreach (var item in orderedItems)
        {
            var entityType = entrySelector(item).EntityType;
            var modelEntity =
                db.Model.FindEntityType(entityType)
                ?? throw new InvalidOperationException(
                    $"Retention entry for {entityType.FullName} is not mapped in the DbContext model."
                );

            foreach (var foreignKey in modelEntity.GetForeignKeys())
            {
                var principalClrType = ResolvePrincipalClrType(foreignKey.PrincipalEntityType);
                if (
                    principalClrType is null
                    || principalClrType == entityType
                    || !itemByEntityType.ContainsKey(principalClrType)
                )
                {
                    continue;
                }

                if (!outgoingEdges[entityType].Add(principalClrType))
                {
                    continue;
                }

                incomingCounts[principalClrType]++;
            }
        }

        var ready = new PriorityQueue<Type, string>(StringComparer.Ordinal);
        foreach (var entityType in incomingCounts
                     .Where(pair => pair.Value == 0)
                     .Select(pair => pair.Key))
        {
            ready.Enqueue(entityType, entityType.FullName ?? entityType.Name);
        }

        var orderedEntityTypes = new List<Type>(orderedItems.Length);
        while (ready.Count > 0)
        {
            var entityType = ready.Dequeue();
            orderedEntityTypes.Add(entityType);

            foreach (var dependency in outgoingEdges[entityType])
            {
                incomingCounts[dependency]--;
                if (incomingCounts[dependency] == 0)
                {
                    ready.Enqueue(dependency, dependency.FullName ?? dependency.Name);
                }
            }
        }

        if (orderedEntityTypes.Count != orderedItems.Length)
        {
            var resolved = new HashSet<Type>(orderedEntityTypes);
            var cycleMembers = orderedItems
                .Select(item => entrySelector(item).EntityType)
                .Where(entityType => !resolved.Contains(entityType))
                .ToArray();

            // A dependency-respecting order does not exist for these entities; they are
            // appended alphabetically and the operator is told, because a Purge sweep
            // inside an FK cycle can fail on constraint violations depending on order.
            logger?.LogWarning(
                "Retention entities {CycleMembers} form a foreign-key cycle; no dependency-respecting sweep order exists, so they are processed alphabetically.",
                string.Join(", ", cycleMembers.Select(entityType => entityType.FullName))
            );

            orderedEntityTypes.AddRange(cycleMembers);
        }

        return orderedEntityTypes
            .Select(entityType => itemByEntityType[entityType])
            .ToArray();
    }

    private static Type? ResolvePrincipalClrType(IEntityType entityType)
    {
        var clrType = entityType.ClrType;
        if (clrType != typeof(object))
        {
            return clrType;
        }

        return entityType.BaseType is null
            ? null
            : ResolvePrincipalClrType(entityType.BaseType);
    }
}
