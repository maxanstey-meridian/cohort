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
        return $"CAST({targetAlias}.{PostgreSqlIdentifier.Quote(recordId.RecordIdColumn)} AS text)";
    }

    internal static string EqualsParameter(
        string targetAlias,
        RecordIdConvention recordId,
        string parameterName
    )
    {
        return PostgresStoreTypeSql.CastType(recordId.RecordIdStoreType) is { } storeType
            ? $"{targetAlias}.{PostgreSqlIdentifier.Quote(recordId.RecordIdColumn)} = CAST(@{parameterName} AS {storeType})"
            : $"CAST({targetAlias}.{PostgreSqlIdentifier.Quote(recordId.RecordIdColumn)} AS text) = CAST(@{parameterName} AS text)";
    }

    internal static string EqualsAnyParameter(
        string targetAlias,
        RecordIdConvention recordId,
        string parameterName
    )
    {
        return PostgresStoreTypeSql.CastType(recordId.RecordIdStoreType) is { } storeType
            ? $"{targetAlias}.{PostgreSqlIdentifier.Quote(recordId.RecordIdColumn)} = ANY(CAST(@{parameterName} AS {storeType}[]))"
            : $"CAST({targetAlias}.{PostgreSqlIdentifier.Quote(recordId.RecordIdColumn)} AS text) = ANY(@{parameterName})";
    }

    internal static async Task<string> CanonicalizeAsync(
        DbConnection connection,
        DbTransaction transaction,
        RecordIdConvention recordId,
        object value,
        CancellationToken ct
    )
    {
        var storeType = PostgresStoreTypeSql.CastType(recordId.RecordIdStoreType);
        await using var command = new SqlParams { ["recordId"] = value }.CreateCommand(
            connection,
            transaction,
            storeType is null
                ? "SELECT CAST(@recordId AS text)"
                : $"SELECT CAST(CAST(@recordId AS {storeType}) AS text)"
        );
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }
}
