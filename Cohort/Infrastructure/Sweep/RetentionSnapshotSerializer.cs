using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;

namespace Cohort.Infrastructure.Sweep;

internal static class RetentionSnapshotSerializer
{
    private const string EncodedTypeProperty = "$cohortType";
    private const string EncodedValueProperty = "$cohortValue";

    // Persisted payloads name CLR types; resolving arbitrary names AppDomain-wide turns a
    // tampered payload into a deserialization gadget. Only well-known scalars and the
    // swept entity's own property types are ever materialised.
    private static readonly Type[] WellKnownSnapshotTypes =
    [
        typeof(bool),
        typeof(byte),
        typeof(sbyte),
        typeof(short),
        typeof(ushort),
        typeof(int),
        typeof(uint),
        typeof(long),
        typeof(ulong),
        typeof(float),
        typeof(double),
        typeof(decimal),
        typeof(char),
        typeof(string),
        typeof(Guid),
        typeof(DateTime),
        typeof(DateTimeOffset),
        typeof(TimeSpan),
        typeof(DateOnly),
        typeof(TimeOnly),
        typeof(byte[]),
    ];

    private static readonly ConcurrentDictionary<
        Type,
        IReadOnlyDictionary<string, Type>
    > AllowedSnapshotTypes = new();

    public static string Serialize(IReadOnlyDictionary<string, object?> snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        var encoded = new Dictionary<string, object?>(snapshot.Count, StringComparer.Ordinal);
        foreach (var (key, value) in snapshot)
        {
            encoded[key] = EncodeValue(value);
        }

        return JsonSerializer.Serialize(encoded);
    }

    public static IReadOnlyDictionary<string, object?> Deserialize(
        string? capturedPayload,
        Type entityType,
        IEnumerable<Assembly> handlerAssemblies
    )
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentNullException.ThrowIfNull(handlerAssemblies);

        if (string.IsNullOrWhiteSpace(capturedPayload))
        {
            throw new InvalidOperationException(
                "Retention row handler dispatch payload is missing from the captured row detail."
            );
        }

        using var document = JsonDocument.Parse(capturedPayload);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException(
                "Retention row handler dispatch payload must be a JSON object."
            );
        }

        var resolver = new SnapshotTypeResolver(
            entityType,
            AllowedSnapshotTypes.GetOrAdd(entityType, BuildAllowedSnapshotTypes),
            handlerAssemblies.Append(entityType.Assembly).Distinct().ToArray()
        );

        return document
            .RootElement.EnumerateObject()
            .ToDictionary(
                property => property.Name,
                property => DecodeValue(property.Value, resolver),
                StringComparer.Ordinal
            );
    }

    private static IReadOnlyDictionary<string, Type> BuildAllowedSnapshotTypes(Type entityType)
    {
        var allowed = new Dictionary<string, Type>(StringComparer.Ordinal);
        var candidates = WellKnownSnapshotTypes.Concat(
            entityType
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .SelectMany(property =>
                {
                    var underlying = Nullable.GetUnderlyingType(property.PropertyType);
                    return underlying is null
                        ? new[] { property.PropertyType }
                        : [property.PropertyType, underlying];
                })
        );

        foreach (var candidate in candidates)
        {
            allowed[RetentionTypeIdentity.GetPersistedName(candidate)] = candidate;
        }

        return allowed;
    }

    private sealed class SnapshotTypeResolver(
        Type entityType,
        IReadOnlyDictionary<string, Type> allowedTypes,
        Assembly[] allowedAssemblies
    )
    {
        public Type Resolve(string typeName)
        {
            var normalized = RetentionTypeIdentity.Normalize(typeName);
            if (allowedTypes.TryGetValue(normalized, out var known))
            {
                return known;
            }

            // Handler-stashed payload types resolve only inside the entity's and the
            // registered handlers' own assemblies; framework and third-party assemblies
            // (where deserialization gadget types live) stay out of reach.
            var resolved = Type.GetType(
                normalized,
                assemblyName =>
                    allowedAssemblies.FirstOrDefault(assembly =>
                        string.Equals(
                            assembly.GetName().Name,
                            assemblyName.Name,
                            StringComparison.Ordinal
                        )
                    ),
                typeResolver: null,
                throwOnError: false
            );

            return resolved
                ?? throw new InvalidOperationException(
                    $"Retention snapshot payload names type '{typeName}', which is not in the snapshot type allow-list for entity {entityType.FullName}. Snapshot values round-trip only as well-known scalars, property types of the swept entity, or types declared in the entity's or its registered handlers' assemblies."
                );
        }
    }

    private static object? EncodeValue(object? value)
    {
        return value switch
        {
            null => null,
            string => value,
            bool => value,
            byte byteValue => EncodeTypedValue(
                typeof(byte),
                JsonSerializer.SerializeToElement(byteValue)
            ),
            sbyte sbyteValue => EncodeTypedValue(
                typeof(sbyte),
                JsonSerializer.SerializeToElement(sbyteValue)
            ),
            short shortValue => EncodeTypedValue(
                typeof(short),
                JsonSerializer.SerializeToElement(shortValue)
            ),
            ushort ushortValue => EncodeTypedValue(
                typeof(ushort),
                JsonSerializer.SerializeToElement(ushortValue)
            ),
            int intValue => EncodeTypedValue(
                typeof(int),
                JsonSerializer.SerializeToElement(intValue)
            ),
            uint uintValue => EncodeTypedValue(
                typeof(uint),
                JsonSerializer.SerializeToElement(uintValue)
            ),
            long longValue => EncodeTypedValue(
                typeof(long),
                JsonSerializer.SerializeToElement(longValue)
            ),
            ulong ulongValue => EncodeTypedValue(
                typeof(ulong),
                JsonSerializer.SerializeToElement(ulongValue)
            ),
            float floatValue => EncodeTypedValue(
                typeof(float),
                JsonSerializer.SerializeToElement(floatValue)
            ),
            double doubleValue => EncodeTypedValue(
                typeof(double),
                JsonSerializer.SerializeToElement(doubleValue)
            ),
            decimal decimalValue => EncodeTypedValue(
                typeof(decimal),
                JsonSerializer.SerializeToElement(decimalValue)
            ),
            Guid guid => EncodeTypedValue(typeof(Guid), JsonSerializer.SerializeToElement(guid)),
            DateTime dateTime => EncodeTypedValue(
                typeof(DateTime),
                JsonSerializer.SerializeToElement(dateTime)
            ),
            DateTimeOffset dateTimeOffset => EncodeTypedValue(
                typeof(DateTimeOffset),
                JsonSerializer.SerializeToElement(dateTimeOffset)
            ),
            Enum enumeration => EncodeTypedValue(
                enumeration.GetType(),
                JsonSerializer.SerializeToElement(enumeration, enumeration.GetType())
            ),
            IDictionary<string, object?> dictionary => dictionary.ToDictionary(
                pair => pair.Key,
                pair => EncodeValue(pair.Value),
                StringComparer.Ordinal
            ),
            IReadOnlyDictionary<string, object?> dictionary => dictionary.ToDictionary(
                pair => pair.Key,
                pair => EncodeValue(pair.Value),
                StringComparer.Ordinal
            ),
            object?[] array => array.Select(EncodeValue).ToArray(),
            IEnumerable<object?> enumerable => enumerable.Select(EncodeValue).ToArray(),
            _ => EncodeTypedValue(
                value.GetType(),
                JsonSerializer.SerializeToElement(value, value.GetType())
            ),
        };
    }

    private static object EncodeTypedValue(Type type, JsonElement serializedValue)
    {
        return new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            [EncodedTypeProperty] = RetentionTypeIdentity.GetPersistedName(type),
            [EncodedValueProperty] = serializedValue,
        };
    }

    private static object? DecodeValue(JsonElement element, SnapshotTypeResolver resolver)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.String => element.GetString(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Number when element.TryGetInt64(out var int64Value) => int64Value,
            JsonValueKind.Number when element.TryGetDecimal(out var decimalValue) => decimalValue,
            JsonValueKind.Number => element.GetDouble(),
            JsonValueKind.Object => DecodeObject(element, resolver),
            JsonValueKind.Array => element
                .EnumerateArray()
                .Select(item => DecodeValue(item, resolver))
                .ToArray(),
            _ => element.GetRawText(),
        };
    }

    private static object? DecodeObject(JsonElement element, SnapshotTypeResolver resolver)
    {
        if (TryDecodeTypedValue(element, resolver, out var decoded))
        {
            return decoded;
        }

        return element
            .EnumerateObject()
            .ToDictionary(
                property => property.Name,
                property => DecodeValue(property.Value, resolver),
                StringComparer.Ordinal
            );
    }

    private static bool TryDecodeTypedValue(
        JsonElement element,
        SnapshotTypeResolver resolver,
        out object? decoded
    )
    {
        decoded = null;

        if (!element.TryGetProperty(EncodedTypeProperty, out var typeProperty))
        {
            return false;
        }

        if (!element.TryGetProperty(EncodedValueProperty, out var valueProperty))
        {
            throw new InvalidOperationException(
                "Retention snapshot payload is missing its encoded value."
            );
        }

        if (typeProperty.ValueKind != JsonValueKind.String)
        {
            throw new InvalidOperationException(
                "Retention snapshot encoded type metadata must be a JSON string."
            );
        }

        var typeName = typeProperty.GetString();
        if (string.IsNullOrWhiteSpace(typeName))
        {
            throw new InvalidOperationException(
                "Retention snapshot encoded type metadata must not be empty."
            );
        }

        var resolvedType = resolver.Resolve(typeName);
        decoded = JsonSerializer.Deserialize(valueProperty.GetRawText(), resolvedType);
        return true;
    }
}
