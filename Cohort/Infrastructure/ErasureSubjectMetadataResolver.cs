using System.Reflection;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure;

internal sealed class ErasureSubjectMetadataResolver(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db
)
{
    /// <summary>
    /// Resolves the entity's <c>[ErasureSubject]</c> columns, one metadata per kind, ordered
    /// by kind. Whether each kind keeps one CLR type is checked across the whole model by
    /// the startup validator.
    /// </summary>
    internal IReadOnlyList<ErasureSubjectMetadata> Resolve(RetentionEntry entry)
    {
        var subjectProperties = entry
            .EntityType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(property =>
                (Property: property, Attribute: property.GetCustomAttribute<ErasureSubjectAttribute>(inherit: false))
            )
            .Where(marked => marked.Attribute is not null)
            .ToArray();

        if (subjectProperties.Length == 0)
        {
            return [];
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

        return subjectProperties
            .GroupBy(marked => marked.Attribute!.Kind, StringComparer.Ordinal)
            .OrderBy(group => group.Key, StringComparer.Ordinal)
            .Select(group => new ErasureSubjectMetadata(
                entry.EntityType,
                group.Key,
                group
                    .OrderBy(marked => marked.Property.Name, StringComparer.Ordinal)
                    .Select(marked => ResolveMember(entry.EntityType, entityType, storeObject, marked.Property))
                    .ToArray()
            ))
            .ToArray();
    }

    private static ErasureSubjectMember ResolveMember(
        Type clrType,
        IEntityType entityType,
        StoreObjectIdentifier storeObject,
        PropertyInfo property
    )
    {
        var efProperty =
            entityType.FindProperty(property.Name)
            ?? throw new InvalidOperationException(
                $"[ErasureSubject] on {clrType.FullName}.{property.Name}: property is not mapped by EF."
            );
        var column =
            efProperty.GetColumnName(storeObject)
            ?? throw new InvalidOperationException(
                $"[ErasureSubject] on {clrType.FullName}.{property.Name}: property has no mapped table column."
            );

        _ = efProperty.GetTypeMapping();
        return new ErasureSubjectMember(
            property.Name,
            Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType,
            column,
            efProperty.GetColumnType(storeObject),
            efProperty
        );
    }
}

internal sealed record ErasureSubjectMetadata(
    Type EntityType,
    string Kind,
    IReadOnlyList<ErasureSubjectMember> Members
)
{
    /// <summary>
    /// The erasure service has already checked the subject against its kind's CLR type.
    /// </summary>
    internal ErasureSubjectPredicate CreatePredicate(object subject) =>
        new(
            Members
                .Select(member =>
                    new ErasureSubjectMatch(
                        member.Name,
                        member.Column,
                        member.StoreType,
                        member.Property.GetTypeMapping().Converter?.ConvertToProvider(subject) ?? subject
                    )
                )
                .ToArray()
        );
}

internal sealed record ErasureSubjectMember(
    string Name,
    Type SubjectType,
    string Column,
    string? StoreType,
    IProperty Property
);
