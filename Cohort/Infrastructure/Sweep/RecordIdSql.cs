using Cohort.Domain;

namespace Cohort.Infrastructure.Sweep;

/// <summary>
/// Builds record-id match predicates that cast the parameter to the column's store type
/// (index-friendly) instead of casting the column to text (which forces a sequential
/// scan). Falls back to the column cast when no relational store type is known.
/// </summary>
internal static class RecordIdSql
{
    internal static string EqualsParameter(
        string targetAlias,
        RecordIdConvention recordId,
        string parameterName
    )
    {
        return recordId.RecordIdStoreType is { } storeType
            ? $"{targetAlias}.{Quote(recordId.RecordIdColumn)} = CAST(@{parameterName} AS {storeType})"
            : $"CAST({targetAlias}.{Quote(recordId.RecordIdColumn)} AS text) = @{parameterName}";
    }

    internal static string EqualsAnyParameter(
        string targetAlias,
        RecordIdConvention recordId,
        string parameterName
    )
    {
        return recordId.RecordIdStoreType is { } storeType
            ? $"{targetAlias}.{Quote(recordId.RecordIdColumn)} = ANY(CAST(@{parameterName} AS {storeType}[]))"
            : $"CAST({targetAlias}.{Quote(recordId.RecordIdColumn)} AS text) = ANY(@{parameterName})";
    }

    private static string Quote(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }
}
