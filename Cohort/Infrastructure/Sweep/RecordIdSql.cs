using System.Data.Common;

namespace Cohort.Infrastructure.Sweep;

/// <summary>
/// Builds record-id match predicates that cast the parameter to the column's store type
/// (index-friendly) instead of casting the column to text (which forces a sequential
/// scan). Falls back to the column cast when no relational store type is known.
/// </summary>
internal static class RecordIdSql
{
    internal static string TextExpression(string targetAlias, RecordIdConvention recordId)
    {
        return $"CAST({targetAlias}.{Quote(recordId.RecordIdColumn)} AS text)";
    }

    internal static string EqualsParameter(
        string targetAlias,
        RecordIdConvention recordId,
        string parameterName
    )
    {
        return PostgresStoreTypeSql.Validate(recordId.RecordIdStoreType) is { } storeType
            ? $"{targetAlias}.{Quote(recordId.RecordIdColumn)} = CAST(@{parameterName} AS {storeType})"
            : $"CAST({targetAlias}.{Quote(recordId.RecordIdColumn)} AS text) = @{parameterName}";
    }

    internal static string EqualsAnyParameter(
        string targetAlias,
        RecordIdConvention recordId,
        string parameterName
    )
    {
        return PostgresStoreTypeSql.Validate(recordId.RecordIdStoreType) is { } storeType
            ? $"{targetAlias}.{Quote(recordId.RecordIdColumn)} = ANY(CAST(@{parameterName} AS {storeType}[]))"
            : $"CAST({targetAlias}.{Quote(recordId.RecordIdColumn)} AS text) = ANY(@{parameterName})";
    }

    internal static async Task<string> CanonicalizeAsync(
        DbConnection connection,
        DbTransaction transaction,
        RecordIdConvention recordId,
        object value,
        CancellationToken ct
    )
    {
        var storeType = PostgresStoreTypeSql.Validate(recordId.RecordIdStoreType);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            storeType is null
                ? "SELECT CAST(@recordId AS text)"
                : $"SELECT CAST(CAST(@recordId AS {storeType}) AS text)";
        command.Parameters.Add(
            RelationalSweepStrategyCore.CreateParameter(command, "recordId", value)
        );
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    private static string Quote(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }
}
