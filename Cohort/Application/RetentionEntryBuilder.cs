using System.Reflection;

using Cohort.Domain;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Cohort.Application;

public sealed class RetentionEntryBuilder
{
    private static readonly Type[] AllowedAnchorTypes =
    [
        typeof(DateTime),
        typeof(DateTime?),
        typeof(DateTimeOffset),
        typeof(DateTimeOffset?),
    ];

    public RetentionEntry? TryBuild(IEntityType entityType)
    {
        var clrType = entityType.ClrType;
        var retain = clrType.GetCustomAttribute<RetainAttribute>(inherit: false);
        if (retain is null)
        {
            return null;
        }

        var storeObject =
            StoreObjectIdentifier.Create(entityType, StoreObjectType.Table)
            ?? throw new InvalidOperationException(
                $"[Retain] on {clrType.FullName}: EF entity has no mapped table name."
            );

        var tableName =
            entityType.GetTableName()
            ?? throw new InvalidOperationException(
                $"[Retain] on {clrType.FullName}: EF entity has no mapped table name."
            );

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

        var anchorProperty =
            entityType.FindProperty(retain.AnchorMember)
            ?? throw new InvalidOperationException(
                $"[Retain] on {clrType.FullName}: anchor member '{retain.AnchorMember}' is not mapped by EF."
            );

        var anchorColumn =
            anchorProperty.GetColumnName(storeObject)
            ?? throw new InvalidOperationException(
                $"[Retain] on {clrType.FullName}: anchor member '{retain.AnchorMember}' has no mapped table column."
            );

        return new RetentionEntry(
            clrType,
            tableName,
            retain.Category,
            retain.AnchorMember,
            anchorColumn,
            BuildAnonymiseFields(clrType, entityType, storeObject),
            BuildSoftDeleteConvention(entityType, storeObject)
        );
    }

    private static IReadOnlyList<AnonymiseField> BuildAnonymiseFields(
        Type clrType,
        IEntityType entityType,
        StoreObjectIdentifier storeObject
    )
    {
        var fields = new List<AnonymiseField>();

        foreach (var property in clrType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var anonymise = property.GetCustomAttribute<AnonymiseAttribute>(inherit: false);
            if (anonymise is null)
            {
                continue;
            }

            var efProperty =
                entityType.FindProperty(property.Name)
                ?? throw new InvalidOperationException(
                    $"[Anonymise] on {clrType.FullName}.{property.Name}: property is not mapped by EF."
                );

            var columnName =
                efProperty.GetColumnName(storeObject)
                ?? throw new InvalidOperationException(
                    $"[Anonymise] on {clrType.FullName}.{property.Name}: property has no mapped table column."
                );

            fields.Add(new AnonymiseField(property.Name, columnName, anonymise.Method, anonymise.Literal));
        }

        return fields.ToArray();
    }

    private static SoftDeleteConvention? BuildSoftDeleteConvention(
        IEntityType entityType,
        StoreObjectIdentifier storeObject
    )
    {
        var clrType = entityType.ClrType;
        var isDeletedMember = clrType.GetProperty("IsDeleted", BindingFlags.Public | BindingFlags.Instance);
        if (isDeletedMember is null)
        {
            return null;
        }

        var isDeletedProperty =
            entityType.FindProperty(isDeletedMember.Name)
            ?? throw new InvalidOperationException(
                $"Soft-delete convention on {clrType.FullName}: IsDeleted is not mapped by EF."
            );

        var isDeletedColumn =
            isDeletedProperty.GetColumnName(storeObject)
            ?? throw new InvalidOperationException(
                $"Soft-delete convention on {clrType.FullName}: IsDeleted has no mapped table column."
            );

        var deletedAtMember = clrType.GetProperty("DeletedAt", BindingFlags.Public | BindingFlags.Instance);
        var deletedAtProperty = deletedAtMember is null ? null : entityType.FindProperty(deletedAtMember.Name);
        var deletedAtColumn = deletedAtProperty?.GetColumnName(storeObject);

        return new SoftDeleteConvention(
            isDeletedProperty.Name,
            isDeletedColumn,
            deletedAtMember?.Name,
            deletedAtColumn
        );
    }
}
