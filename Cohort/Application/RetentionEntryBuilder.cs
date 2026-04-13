using System.Reflection;

using Cohort.Domain;
using Cohort.Hosting;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Cohort.Application;

public sealed class RetentionEntryBuilder(CohortConventions conventions)
{
    private static readonly Type[] AllowedAnchorTypes =
    [
        typeof(DateTime),
        typeof(DateTime?),
        typeof(DateTimeOffset),
        typeof(DateTimeOffset?),
    ];
    private static readonly Type[] AllowedTenantTypes = [typeof(Guid), typeof(Guid?)];

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

        var anchor = ReflectionMemberResolver.FindPropertyByName(clrType, retain.AnchorMember);
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
            BuildRecordIdConvention(entityType, storeObject),
            BuildAnonymiseFields(clrType, entityType, storeObject),
            BuildTenantConvention(entityType, storeObject),
            BuildSoftDeleteConvention(entityType, storeObject),
            retain.AuditRowDetail
        );
    }

    private RecordIdConvention BuildRecordIdConvention(
        IEntityType entityType,
        StoreObjectIdentifier storeObject
    )
    {
        var clrType = entityType.ClrType;
        var recordIdMember = clrType
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(p => p.GetCustomAttribute<RetentionRecordIdAttribute>() is not null)
            ?? ReflectionMemberResolver.FindPropertyByName(clrType, conventions.RecordIdPropertyName);
        if (recordIdMember is null)
        {
            throw new InvalidOperationException(
                $"Record-id convention on {clrType.FullName}: no public Id property found and no property marked with [RetentionRecordId]."
            );
        }

        var recordIdProperty =
            entityType.FindProperty(recordIdMember.Name)
            ?? throw new InvalidOperationException(
                $"Record-id convention on {clrType.FullName}: '{recordIdMember.Name}' is not mapped by EF."
            );

        var recordIdColumn =
            recordIdProperty.GetColumnName(storeObject)
            ?? throw new InvalidOperationException(
                $"Record-id convention on {clrType.FullName}: '{recordIdMember.Name}' has no mapped table column."
            );

        return new RecordIdConvention(recordIdProperty.Name, recordIdColumn, recordIdMember.PropertyType);
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
            var anonymiseWith = property.GetCustomAttribute<AnonymiseWithAttribute>(inherit: false);

            if (anonymise is not null && anonymiseWith is not null)
            {
                throw new InvalidOperationException(
                    $"[Anonymise] and [AnonymiseWith] on {clrType.FullName}.{property.Name}: exactly one is allowed per property."
                );
            }

            if (anonymise is null && anonymiseWith is null)
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

            if (anonymise is not null)
            {
                fields.Add(
                    new AnonymiseLiteralField(
                        property.Name,
                        columnName,
                        anonymise.Method,
                        anonymise.Literal
                    )
                );
                continue;
            }

            fields.Add(
                new AnonymiseFactoryField(
                    property.Name,
                    columnName,
                    anonymiseWith!.FactoryType
                )
            );
        }

        return fields.ToArray();
    }

    private TenantConvention? BuildTenantConvention(
        IEntityType entityType,
        StoreObjectIdentifier storeObject
    )
    {
        var clrType = entityType.ClrType;
        var tenantMember = clrType
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(p => p.GetCustomAttribute<RetentionTenantAttribute>() is not null)
            ?? ReflectionMemberResolver.FindPropertyByName(clrType, conventions.TenantPropertyName);
        if (tenantMember is null)
        {
            return null;
        }

        if (!AllowedTenantTypes.Contains(tenantMember.PropertyType))
        {
            throw new InvalidOperationException(
                $"Tenant convention on {clrType.FullName}: TenantId must be Guid or nullable Guid, got {tenantMember.PropertyType.Name}."
            );
        }

        var tenantProperty =
            entityType.FindProperty(tenantMember.Name)
            ?? throw new InvalidOperationException(
                $"Tenant convention on {clrType.FullName}: TenantId is not mapped by EF."
            );

        var tenantColumn =
            tenantProperty.GetColumnName(storeObject)
            ?? throw new InvalidOperationException(
                $"Tenant convention on {clrType.FullName}: TenantId has no mapped table column."
            );

        return new TenantConvention(tenantProperty.Name, tenantColumn);
    }

    private SoftDeleteConvention? BuildSoftDeleteConvention(
        IEntityType entityType,
        StoreObjectIdentifier storeObject
    )
    {
        var clrType = entityType.ClrType;
        var isDeletedMember = clrType
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(p => p.GetCustomAttribute<RetentionSoftDeleteAttribute>() is not null)
            ?? ReflectionMemberResolver.FindPropertyByName(clrType, conventions.SoftDeletePropertyName);
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

        var deletedAtMember = clrType
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(p => p.GetCustomAttribute<RetentionDeletedAtAttribute>() is not null)
            ?? ReflectionMemberResolver.FindPropertyByName(clrType, conventions.DeletedAtPropertyName);
        var deletedAtProperty =
            deletedAtMember is null
                ? null
                : entityType.FindProperty(deletedAtMember.Name)
                    ?? throw new InvalidOperationException(
                        $"Soft-delete convention on {clrType.FullName}: DeletedAt is not mapped by EF."
                    );
        var deletedAtColumn =
            deletedAtProperty?.GetColumnName(storeObject)
            ?? (deletedAtProperty is null
                ? null
                : throw new InvalidOperationException(
                    $"Soft-delete convention on {clrType.FullName}: DeletedAt has no mapped table column."
                ));

        return new SoftDeleteConvention(
            isDeletedProperty.Name,
            isDeletedColumn,
            deletedAtMember?.Name,
            deletedAtColumn
        );
    }
}
