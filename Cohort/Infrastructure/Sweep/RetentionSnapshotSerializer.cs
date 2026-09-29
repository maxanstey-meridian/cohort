using System.Collections;
using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;

namespace Cohort.Infrastructure.Sweep;

internal static class RetentionSnapshotSerializer
{
    private const string EncodedTypeProperty = "$cohortType";
    private const string EncodedValueProperty = "$cohortValue";
    private const string EncodedCollectionProperty = "$cohortCollection";
    private const string ArrayCollectionKind = "array";
    private const string ListCollectionKind = "list";
    private static readonly string ObjectTypeName = RetentionTypeIdentity.GetPersistedName(
        typeof(object)
    );

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

    /// <summary>
    /// Encodes the OnBefore snapshot for post-commit dispatch, refusing (before the row is
    /// mutated) any value that would not decode back to an equal value of the same type.
    /// </summary>
    public static string Capture(
        IEnumerable<KeyValuePair<string, object?>> snapshot,
        Type entityType,
        IEnumerable<Assembly> handlerAssemblies
    )
    {
        var resolver = CreateResolver(entityType, handlerAssemblies);
        var encoded = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var (key, value) in snapshot)
        {
            JsonElement element;
            object? decoded;
            try
            {
                element = JsonSerializer.SerializeToElement(EncodeValue(value));
                decoded = DecodeValue(element, resolver);
            }
            catch (Exception ex)
                when (ex
                        is JsonException
                            or NotSupportedException
                            or InvalidOperationException
                            or ArgumentException
                )
            {
                throw UnsupportedValue(key, value, ex);
            }

            if (!RoundTrips(value, decoded))
            {
                throw UnsupportedValue(key, value, inner: null);
            }

            encoded[key] = element;
        }

        return JsonSerializer.Serialize(encoded);
    }

    private static NotSupportedException UnsupportedValue(
        string key,
        object? value,
        Exception? inner
    ) =>
        new(
            $"Retention snapshot value '{key}' of type {value?.GetType().FullName} cannot round-trip exactly to OnAfterAsync. Snapshot values must be null, well-known scalars, enums or value-equal types from the entity's or its handlers' assemblies, one-dimensional arrays or List<T> of those, object?[], List<object?>, or nested IDictionary<string, object?>.",
            inner
        );

    public static IReadOnlyDictionary<string, object?> Deserialize(
        string? capturedPayload,
        Type entityType,
        IEnumerable<Assembly> handlerAssemblies
    )
    {
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

        var resolver = CreateResolver(entityType, handlerAssemblies);

        return document
            .RootElement.EnumerateObject()
            .ToDictionary(
                property => property.Name,
                property => DecodeValue(property.Value, resolver),
                StringComparer.Ordinal
            );
    }

    private static SnapshotTypeResolver CreateResolver(
        Type entityType,
        IEnumerable<Assembly> handlerAssemblies
    ) =>
        new(
            entityType,
            AllowedSnapshotTypes.GetOrAdd(entityType, BuildAllowedSnapshotTypes),
            handlerAssemblies.Append(entityType.Assembly).Distinct().ToArray()
        );

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
            // Exactly object?[]: array covariance would otherwise route string[] here and
            // decode it as object?[].
            object?[] array when value.GetType() == typeof(object[]) => array
                .Select(EncodeValue)
                .ToArray(),
            byte[] => EncodeTypedValue(
                value.GetType(),
                JsonSerializer.SerializeToElement(value, value.GetType())
            ),
            Array array when array.Rank == 1 => EncodeCollection(
                value.GetType().GetElementType()!,
                ArrayCollectionKind,
                array
            ),
            IList list
                when value.GetType().IsGenericType
                    && value.GetType().GetGenericTypeDefinition() == typeof(List<>) =>
                EncodeCollection(value.GetType().GetGenericArguments()[0], ListCollectionKind, list),
            _ => EncodeTypedValue(
                value.GetType(),
                JsonSerializer.SerializeToElement(value, value.GetType())
            ),
        };
    }

    private static object EncodeCollection(Type elementType, string kind, IList items)
    {
        return new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            [EncodedTypeProperty] = RetentionTypeIdentity.GetPersistedName(elementType),
            [EncodedCollectionProperty] = kind,
            [EncodedValueProperty] =
                elementType == typeof(object)
                    ? items.Cast<object?>().Select(EncodeValue).ToArray()
                    : JsonSerializer.SerializeToElement(items, items.GetType()),
        };
    }

    private static bool RoundTrips(object? original, object? decoded)
    {
        if (original is null || decoded is null)
        {
            return original is null && decoded is null;
        }

        if (original.GetType() != decoded.GetType())
        {
            return false;
        }

        return original switch
        {
            IDictionary<string, object?> dictionary => decoded
                is IDictionary<string, object?> other
                && dictionary.Count == other.Count
                && dictionary.All(pair =>
                    other.TryGetValue(pair.Key, out var value) && RoundTrips(pair.Value, value)
                ),
            IReadOnlyDictionary<string, object?> dictionary => decoded
                is IReadOnlyDictionary<string, object?> other
                && dictionary.Count == other.Count
                && dictionary.All(pair =>
                    other.TryGetValue(pair.Key, out var value) && RoundTrips(pair.Value, value)
                ),
            IList list => decoded is IList other
                && list.Count == other.Count
                && Enumerable.Range(0, list.Count).All(index => RoundTrips(list[index], other[index])),
            _ => original.Equals(decoded),
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

        if (element.TryGetProperty(EncodedCollectionProperty, out var collectionProperty))
        {
            decoded = DecodeCollection(typeName, collectionProperty, valueProperty, resolver);
            return true;
        }

        var resolvedType = resolver.Resolve(typeName);
        decoded = JsonSerializer.Deserialize(valueProperty.GetRawText(), resolvedType);
        return true;
    }

    private static object DecodeCollection(
        string elementTypeName,
        JsonElement collectionProperty,
        JsonElement valueProperty,
        SnapshotTypeResolver resolver
    )
    {
        if (valueProperty.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException(
                "Retention snapshot encoded collection value must be a JSON array."
            );
        }

        var elementType =
            RetentionTypeIdentity.Normalize(elementTypeName) == ObjectTypeName
                ? typeof(object)
                : resolver.Resolve(elementTypeName);
        var items = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(elementType))!;
        foreach (var item in valueProperty.EnumerateArray())
        {
            items.Add(
                elementType == typeof(object)
                    ? DecodeValue(item, resolver)
                    : JsonSerializer.Deserialize(item.GetRawText(), elementType)
            );
        }

        switch (collectionProperty.ValueKind == JsonValueKind.String ? collectionProperty.GetString() : null)
        {
            case ListCollectionKind:
                return items;
            case ArrayCollectionKind:
                var array = Array.CreateInstance(elementType, items.Count);
                items.CopyTo(array, 0);
                return array;
            default:
                throw new InvalidOperationException(
                    "Retention snapshot encoded collection kind must be 'array' or 'list'."
                );
        }
    }
}
