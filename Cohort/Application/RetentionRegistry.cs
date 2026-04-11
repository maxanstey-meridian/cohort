using System.Reflection;

using Cohort.Domain;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Application;

/// Walks an EF Core model, reads `[Retain]` attributes, validates anchors, and returns
/// an immutable list of `RetentionEntry` records.
///
/// Takes `DbContext` as a port-shaped dependency: it's the host's "here is my model"
/// contract. The registry never touches `DbSet`, never issues SQL — it only reads
/// metadata. SQL belongs in `Infrastructure/`.
///
/// Crude error handling: throws `InvalidOperationException` on the first failure.
/// Multi-error aggregation via `RetentionConfigurationException` is Milestone A.
public sealed class RetentionRegistry(DbContext db)
{
    private static readonly Type[] AllowedAnchorTypes =
    [
        typeof(DateTime),
        typeof(DateTime?),
        typeof(DateTimeOffset),
        typeof(DateTimeOffset?),
    ];

    public IReadOnlyList<RetentionEntry> Scan()
    {
        var entries = new List<RetentionEntry>();

        foreach (var entityType in db.Model.GetEntityTypes())
        {
            var clrType = entityType.ClrType;
            var retain = clrType.GetCustomAttribute<RetainAttribute>(inherit: false);
            if (retain is null)
            {
                continue;
            }

            var anchor = clrType.GetProperty(
                retain.AnchorMember,
                BindingFlags.Public | BindingFlags.Instance
            );
            if (anchor is null)
            {
                throw new InvalidOperationException(
                    $"[Retain] on {clrType.FullName}: anchor member '{retain.AnchorMember}' not found as a public instance property."
                );
            }
            if (!AllowedAnchorTypes.Contains(anchor.PropertyType))
            {
                throw new InvalidOperationException(
                    $"[Retain] on {clrType.FullName}: anchor '{retain.AnchorMember}' must be DateTime or DateTimeOffset (nullable allowed), got {anchor.PropertyType.Name}."
                );
            }

            var tableName =
                entityType.GetTableName()
                ?? throw new InvalidOperationException(
                    $"[Retain] on {clrType.FullName}: EF entity has no mapped table name."
                );

            entries.Add(new RetentionEntry(clrType, tableName, retain.Category, retain.AnchorMember));
        }

        return entries;
    }
}
