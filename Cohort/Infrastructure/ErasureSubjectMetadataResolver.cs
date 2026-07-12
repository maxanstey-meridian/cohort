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
    internal ErasureSubjectMetadata? Resolve(RetentionEntry entry)
    {
        var subjectProperties = entry
            .EntityType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
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
            .Select(property =>
                Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType
            )
            .Distinct()
            .ToArray();
        if (effectiveTypes.Length > 1)
        {
            throw new InvalidOperationException(
                $"Entity {entry.EntityType.FullName} defines incompatible [ErasureSubject] properties. All marked properties must share the same effective CLR type after nullable unwrapping. Found: {string.Join(", ", subjectProperties.Select(property => $"{property.Name}:{(Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType).Name}"))}."
            );
        }

        var members = subjectProperties
            .Select(property => ResolveMember(entry.EntityType, entityType, storeObject, property))
            .ToArray();
        return new ErasureSubjectMetadata(entry.EntityType, effectiveTypes[0], members);
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
        return new ErasureSubjectMember(property.Name, column, efProperty);
    }
}

internal sealed record ErasureSubjectMetadata(
    Type EntityType,
    Type SubjectType,
    IReadOnlyList<ErasureSubjectMember> Members
)
{
    internal ErasureSubjectPredicate CreatePredicate(object subject)
    {
        if (!SubjectType.IsInstanceOfType(subject))
        {
            var subjectDescription =
                Members.Count == 1
                    ? $"property '{Members[0].Name}'"
                    : $"properties {string.Join(", ", Members.Select(member => $"'{member.Name}'"))}";
            throw new InvalidOperationException(
                $"Erasure scope subject value of type {subject.GetType().Name} cannot be expressed against [ErasureSubject] {subjectDescription} on {EntityType.FullName}, which expects {SubjectType.Name}."
            );
        }

        return new ErasureSubjectPredicate(
            Members
                .Select(member =>
                    new ErasureSubjectMatch(
                        member.Name,
                        member.Column,
                        member.Property.GetTypeMapping().Converter?.ConvertToProvider(subject) ?? subject
                    )
                )
                .ToArray()
        );
    }
}

internal sealed record ErasureSubjectMember(string Name, string Column, IProperty Property);
