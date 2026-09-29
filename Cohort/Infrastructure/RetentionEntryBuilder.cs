using System.Collections.Frozen;
using System.Reflection;
using System.Runtime.CompilerServices;
using Cohort.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Cohort.Infrastructure;

internal sealed class RetentionEntryBuilder(CohortConventions conventions)
{
    private static readonly Type[] AllowedAnchorTypes =
    [
        typeof(DateTime),
        typeof(DateTime?),
        typeof(DateTimeOffset),
        typeof(DateTimeOffset?),
    ];
    private static readonly Type[] AllowedTenantTypes = [typeof(Guid)];

    private readonly ConditionalWeakTable<IModel, FrozenDictionary<Type, RetentionEntry>> entriesByModel = new();

    public string ExpectedTenantPropertyName => conventions.TenantPropertyName;

    /// <summary>Every retained entity in <paramref name="model"/>, built once per model.</summary>
    public FrozenDictionary<Type, RetentionEntry> BuildAll(IModel model) =>
        entriesByModel.GetValue(
            model,
            m => m.GetEntityTypes()
                .Select(TryBuild)
                .OfType<RetentionEntry>()
                .ToFrozenDictionary(entry => entry.EntityType)
        );

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
            clrType.GetCustomAttribute<RetentionEntityIdAttribute>(inherit: false)?.Id
                ?? throw new InvalidOperationException(
                    $"[Retain] on {clrType.FullName}: retained entities must declare [RetentionEntityId(\"uuid\")] with a stable non-empty UUID."
                ),
            new RelationalObjectName(
                entityType.GetSchema() ?? entityType.Model.GetDefaultSchema() ?? "public",
                tableName
            ),
            CohortStoreTables.FromModel(entityType.Model),
            retain.Category,
            retain.AnchorMember,
            anchorColumn,
            BuildRecordIdConvention(entityType, storeObject),
            BuildAnonymiseFields(clrType, entityType, storeObject),
            BuildMaterializationColumns(entityType, storeObject),
            BuildTenantConvention(entityType, storeObject),
            BuildSoftDeleteConvention(entityType, storeObject),
            clrType.GetCustomAttribute<RetentionTenantlessAttribute>(inherit: false) is not null,
            retain.AuditRowDetail,
            BuildAnonymisedAtConvention(entityType, storeObject)
        );
    }

    // Every mapped column, complex-property columns and system columns such as xmin included:
    // FromSql materialization fails if any is missing, and SELECT * omits system columns.
    private static IReadOnlyList<string> BuildMaterializationColumns(
        IEntityType entityType,
        StoreObjectIdentifier storeObject
    ) =>
        entityType
            .GetFlattenedProperties()
            .Select(property => property.GetColumnName(storeObject))
            .Where(columnName => columnName is not null)
            .Select(columnName => columnName!)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();

    private RecordIdConvention BuildRecordIdConvention(
        IEntityType entityType,
        StoreObjectIdentifier storeObject
    )
    {
        var (recordIdProperty, recordIdColumn, recordIdMember) =
            ResolveMarked<RetentionRecordIdAttribute>(entityType, storeObject, conventions.RecordIdPropertyName, "Record-id", "'{0}'")
            ?? throw new InvalidOperationException(
                $"Record-id convention on {entityType.ClrType.FullName}: no public Id property found and no property marked with [RetentionRecordId]."
            );

        return new RecordIdConvention(
            recordIdProperty.Name,
            recordIdColumn,
            recordIdMember.PropertyType,
            TryGetStoreType(recordIdProperty)
        );
    }

    /// <summary>
    /// The property marked with <typeparamref name="TMarker"/>, else the one named by
    /// convention, with its mapped column; null when neither exists. <paramref name="label"/>
    /// names it in errors ("{0}" is replaced by the member name).
    /// </summary>
    private static (IProperty Property, string Column, PropertyInfo Member)? ResolveMarked<TMarker>(
        IEntityType entityType,
        StoreObjectIdentifier storeObject,
        string conventionName,
        string role,
        string label
    )
        where TMarker : Attribute
    {
        var clrType = entityType.ClrType;
        var member =
            clrType
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(property => property.GetCustomAttribute<TMarker>() is not null)
            ?? ReflectionMemberResolver.FindPropertyByName(clrType, conventionName);
        if (member is null)
        {
            return null;
        }

        var name = string.Format(System.Globalization.CultureInfo.InvariantCulture, label, member.Name);
        var property =
            entityType.FindProperty(member.Name)
            ?? throw new InvalidOperationException($"{role} convention on {clrType.FullName}: {name} is not mapped by EF.");
        var column =
            property.GetColumnName(storeObject)
            ?? throw new InvalidOperationException($"{role} convention on {clrType.FullName}: {name} has no mapped table column.");
        return (property, column, member);
    }

    private static string? TryGetStoreType(IProperty property)
    {
        // Null on non-relational providers (metadata-only scans); SQL builders fall back
        // to casting the column to text instead of casting the parameter.
        try
        {
            return property.GetColumnType();
        }
        catch (Exception ex) when (ex is InvalidOperationException or InvalidCastException)
        {
            // Non-relational providers (metadata-only scans) have no store type.
            return null;
        }
    }

    private AnonymisedAtConvention? BuildAnonymisedAtConvention(
        IEntityType entityType,
        StoreObjectIdentifier storeObject
    )
    {
        if (
            ResolveMarked<RetentionAnonymisedAtAttribute>(entityType, storeObject, conventions.AnonymisedAtPropertyName, "AnonymisedAt", "'{0}'")
            is not var (anonymisedAtProperty, anonymisedAtColumn, anonymisedAtMember)
        )
        {
            return null;
        }

        if (anonymisedAtMember.PropertyType != typeof(DateTimeOffset?))
        {
            throw new InvalidOperationException(
                $"AnonymisedAt convention on {entityType.ClrType.FullName}: '{anonymisedAtMember.Name}' must be a nullable DateTimeOffset — NULL marks rows not yet anonymised, got {anonymisedAtMember.PropertyType.Name}."
            );
        }

        return new AnonymisedAtConvention(anonymisedAtProperty.Name, anonymisedAtColumn);
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

            var storeType = efProperty.GetColumnType(storeObject);
            if (anonymise is not null)
            {
                fields.Add(
                    new AnonymiseLiteralField(
                        property.Name,
                        columnName,
                        storeType,
                        anonymise.Method,
                        anonymise.Literal
                    )
                );
                continue;
            }

            fields.Add(
                new AnonymiseFactoryField(property.Name, columnName, storeType, anonymiseWith!.FactoryType)
            );
        }

        return fields.ToArray();
    }

    private TenantConvention? BuildTenantConvention(
        IEntityType entityType,
        StoreObjectIdentifier storeObject
    )
    {
        if (
            ResolveMarked<RetentionTenantAttribute>(entityType, storeObject, conventions.TenantPropertyName, "Tenant", "TenantId")
            is not var (tenantProperty, tenantColumn, tenantMember)
        )
        {
            return null;
        }

        if (!AllowedTenantTypes.Contains(tenantMember.PropertyType))
        {
            throw new InvalidOperationException(
                $"Tenant convention on {entityType.ClrType.FullName}: TenantId must be a non-nullable Guid, got {tenantMember.PropertyType.Name}."
            );
        }

        return new TenantConvention(tenantProperty.Name, tenantColumn);
    }

    private SoftDeleteConvention? BuildSoftDeleteConvention(
        IEntityType entityType,
        StoreObjectIdentifier storeObject
    )
    {
        if (
            ResolveMarked<RetentionSoftDeleteAttribute>(entityType, storeObject, conventions.SoftDeletePropertyName, "Soft-delete", "IsDeleted")
            is not var (isDeletedProperty, isDeletedColumn, _)
        )
        {
            return null;
        }

        var deletedAt = ResolveMarked<RetentionDeletedAtAttribute>(entityType, storeObject, conventions.DeletedAtPropertyName, "Soft-delete", "DeletedAt");
        return new SoftDeleteConvention(isDeletedProperty.Name, isDeletedColumn, deletedAt?.Member.Name, deletedAt?.Column);
    }
}
