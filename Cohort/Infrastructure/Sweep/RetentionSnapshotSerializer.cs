using System.Text.Json;

namespace Cohort.Infrastructure.Sweep;

internal static class RetentionSnapshotSerializer
{
    private const string EncodedTypeProperty = "$cohortType";
    private const string EncodedValueProperty = "$cohortValue";

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

    public static IReadOnlyDictionary<string, object?> Deserialize(string? capturedPayload)
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

        return document.RootElement.EnumerateObject().ToDictionary(
            property => property.Name,
            property => DecodeValue(property.Value),
            StringComparer.Ordinal
        );
    }

    private static object? EncodeValue(object? value)
    {
        return value switch
        {
            null => null,
            string => value,
            bool => value,
            byte byteValue => EncodeTypedValue(typeof(byte), JsonSerializer.SerializeToElement(byteValue)),
            sbyte sbyteValue => EncodeTypedValue(typeof(sbyte), JsonSerializer.SerializeToElement(sbyteValue)),
            short shortValue => EncodeTypedValue(typeof(short), JsonSerializer.SerializeToElement(shortValue)),
            ushort ushortValue => EncodeTypedValue(typeof(ushort), JsonSerializer.SerializeToElement(ushortValue)),
            int intValue => EncodeTypedValue(typeof(int), JsonSerializer.SerializeToElement(intValue)),
            uint uintValue => EncodeTypedValue(typeof(uint), JsonSerializer.SerializeToElement(uintValue)),
            long longValue => EncodeTypedValue(typeof(long), JsonSerializer.SerializeToElement(longValue)),
            ulong ulongValue => EncodeTypedValue(typeof(ulong), JsonSerializer.SerializeToElement(ulongValue)),
            float floatValue => EncodeTypedValue(typeof(float), JsonSerializer.SerializeToElement(floatValue)),
            double doubleValue => EncodeTypedValue(typeof(double), JsonSerializer.SerializeToElement(doubleValue)),
            decimal decimalValue => EncodeTypedValue(typeof(decimal), JsonSerializer.SerializeToElement(decimalValue)),
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
            [EncodedTypeProperty] =
                type.AssemblyQualifiedName ?? throw new InvalidOperationException(
                    $"Retention snapshot value type '{type.FullName}' cannot be persisted without an assembly-qualified type name."
                ),
            [EncodedValueProperty] = serializedValue,
        };
    }

    private static object? DecodeValue(JsonElement element)
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
            JsonValueKind.Object => DecodeObject(element),
            JsonValueKind.Array => element.EnumerateArray().Select(DecodeValue).ToArray(),
            _ => element.GetRawText(),
        };
    }

    private static object? DecodeObject(JsonElement element)
    {
        if (TryDecodeTypedValue(element, out var decoded))
        {
            return decoded;
        }

        return element.EnumerateObject().ToDictionary(
            property => property.Name,
            property => DecodeValue(property.Value),
            StringComparer.Ordinal
        );
    }

    private static bool TryDecodeTypedValue(JsonElement element, out object? decoded)
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

        var resolvedType = Type.GetType(typeName, throwOnError: false, ignoreCase: false);
        if (resolvedType is null)
        {
            throw new InvalidOperationException(
                $"Retention snapshot encoded type '{typeName}' could not be resolved."
            );
        }

        decoded = JsonSerializer.Deserialize(valueProperty.GetRawText(), resolvedType);
        return true;
    }
}
