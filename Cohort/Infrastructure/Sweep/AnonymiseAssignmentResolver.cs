using Cohort.Application;
using Cohort.Domain;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Cohort.Infrastructure.Sweep;

internal sealed class AnonymiseAssignmentResolver(
    DbContext db,
    IEnumerable<IAnonymiseValueFactory>? anonymiseValueFactories = null
)
{
    private readonly IReadOnlyDictionary<Type, IAnonymiseValueFactory> factories =
        (anonymiseValueFactories ?? Array.Empty<IAnonymiseValueFactory>())
        .GroupBy(factory => factory.GetType())
        .ToDictionary(group => group.Key, group => group.Last());
    private readonly DbContext modelDb = db ?? throw new ArgumentNullException(nameof(db));

    internal bool RequiresPerRowExecution(RetentionEntry entry)
    {
        return entry.AnonymiseFields
            .OfType<AnonymiseFactoryField>()
            .Any(field => ResolveFactory(field).RequiresPerRowExecution);
    }

    internal IReadOnlyList<AnonymiseFactoryField> GetOriginalValueFields(RetentionEntry entry)
    {
        return entry.AnonymiseFields
            .OfType<AnonymiseFactoryField>()
            .Where(field => ResolveFactory(field).RequiresOriginalValue)
            .ToArray();
    }

    internal IReadOnlyDictionary<string, object?> CreateStaticAssignments(
        RetentionEntry entry,
        Guid tenantId,
        DateTimeOffset now
    )
    {
        var values = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var field in entry.AnonymiseFields)
        {
            values[field.MemberName] = field switch
            {
                AnonymiseLiteralField literalField => CreateLiteralAssignmentValue(literalField),
                AnonymiseFactoryField factoryField when !ResolveFactory(factoryField).RequiresPerRowExecution
                    => ResolveFactory(factoryField)
                        .Create(
                            new AnonymiseValueContext(
                                entry.EntityType,
                                factoryField.MemberName,
                                null,
                                now,
                                tenantId
                            )
                        ),
                AnonymiseFactoryField => null,
                _ => throw new InvalidOperationException(
                    $"Anonymise field '{field.MemberName}' is not supported."
                ),
            };
        }

        return values;
    }

    internal IReadOnlyList<object?> CreateSetBasedAssignmentValues(
        RetentionEntry entry,
        Guid tenantId,
        DateTimeOffset now
    )
    {
        var staticAssignments = CreateStaticAssignments(entry, tenantId, now);
        return entry.AnonymiseFields
            .Select(field =>
                ConvertAssignmentValueToProvider(entry, field, staticAssignments[field.MemberName])
            )
            .ToArray();
    }

    internal IReadOnlyList<object?> CreatePerRowAssignmentValues(
        RetentionEntry entry,
        TenantContext tenant,
        DateTimeOffset now,
        IReadOnlyDictionary<string, object?> originalValues,
        IReadOnlyDictionary<string, object?> staticAssignments
    )
    {
        return entry.AnonymiseFields
            .Select(field =>
                ConvertAssignmentValueToProvider(
                    entry,
                    field,
                    ResolvePerRowAssignmentValue(
                        entry,
                        field,
                        tenant,
                        now,
                        originalValues,
                        staticAssignments
                    )
                )
            )
            .ToArray();
    }

    internal IReadOnlyDictionary<string, object?> CreateOriginalValuesFromEntity<TEntity>(
        RetentionEntry entry,
        TEntity row
    )
        where TEntity : class
    {
        var originalValues = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var field in GetOriginalValueFields(entry))
        {
            var property =
                entry.EntityType.GetProperty(field.MemberName)
                ?? throw new InvalidOperationException(
                    $"Property '{field.MemberName}' on {entry.EntityType.FullName} is not mapped by the current EF model."
                );
            originalValues[field.MemberName] = property.GetValue(row);
        }

        return originalValues;
    }

    internal object? ConvertOriginalValueFromProvider(
        RetentionEntry entry,
        AnonymiseFactoryField field,
        object? providerValue
    )
    {
        if (providerValue is null)
        {
            return providerValue;
        }

        var property = ResolveEfProperty(entry, field.MemberName);
        var converter = property.GetTypeMapping().Converter;
        return converter?.ConvertFromProvider(providerValue) ?? providerValue;
    }

    private object? ResolvePerRowAssignmentValue(
        RetentionEntry entry,
        AnonymiseField field,
        TenantContext tenant,
        DateTimeOffset now,
        IReadOnlyDictionary<string, object?> originalValues,
        IReadOnlyDictionary<string, object?> staticAssignments
    )
    {
        return field switch
        {
            AnonymiseLiteralField literalField => CreateLiteralAssignmentValue(literalField),
            AnonymiseFactoryField factoryField when ResolveFactory(factoryField).RequiresPerRowExecution
                => ResolveFactory(factoryField)
                    .Create(
                        new AnonymiseValueContext(
                            entry.EntityType,
                            factoryField.MemberName,
                            originalValues.TryGetValue(factoryField.MemberName, out var originalValue)
                                ? originalValue
                                : null,
                            now,
                            tenant.Id
                        )
                    ),
            AnonymiseFactoryField factoryField => staticAssignments[factoryField.MemberName],
            _ => throw new InvalidOperationException(
                $"Anonymise field '{field.MemberName}' is not supported."
            ),
        };
    }

    private object? ConvertAssignmentValueToProvider(
        RetentionEntry entry,
        AnonymiseField field,
        object? value
    )
    {
        if (value is null or DBNull)
        {
            return value is DBNull ? null : value;
        }

        var property = ResolveEfProperty(entry, field.MemberName);
        var converter = property.GetTypeMapping().Converter;
        return converter?.ConvertToProvider(value) ?? value;
    }

    private IAnonymiseValueFactory ResolveFactory(AnonymiseFactoryField field)
    {
        if (!factories.TryGetValue(field.FactoryType, out var factory))
        {
            throw new InvalidOperationException(
                $"Anonymise field '{field.MemberName}' requires factory type {field.FactoryType.FullName}, but no matching {nameof(IAnonymiseValueFactory)} is registered."
            );
        }

        return factory;
    }

    private IProperty ResolveEfProperty(RetentionEntry entry, string memberName)
    {
        var entityType =
            modelDb.Model.FindEntityType(entry.EntityType)
            ?? throw new InvalidOperationException(
                $"Entity {entry.EntityType.FullName} is not mapped by the current EF model."
            );
        return entityType.FindProperty(memberName)
            ?? throw new InvalidOperationException(
                $"Property '{memberName}' on {entry.EntityType.FullName} is not mapped by the current EF model."
            );
    }

    private static object CreateLiteralAssignmentValue(AnonymiseLiteralField literalField)
    {
        return literalField.Method switch
        {
            AnonymiseMethod.Null => DBNull.Value,
            AnonymiseMethod.EmptyString => string.Empty,
            AnonymiseMethod.FixedLiteral => literalField.Literal
                ?? throw new InvalidOperationException(
                    $"Anonymise field '{literalField.MemberName}' requires a literal value."
                ),
            _ => throw new InvalidOperationException(
                $"Anonymise method '{literalField.Method}' is not supported."
            ),
        };
    }
}
