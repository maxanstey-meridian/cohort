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
            : $"{TextExpression(targetAlias, recordId)} = CAST(@{parameterName} AS text)";
    }

    /// <summary>
    /// Like <see cref="EqualsParameter"/>, for a text SQL expression such as a hold's stored
    /// <c>RecordId</c>. A value that doesn't parse as the store type fails the statement.
    /// </summary>
    internal static string EqualsExpression(
        string targetAlias,
        RecordIdConvention recordId,
        string textExpression
    )
    {
        return PostgresStoreTypeSql.CastType(recordId.RecordIdStoreType) is { } storeType
            ? $"{targetAlias}.{PostgreSqlIdentifier.Quote(recordId.RecordIdColumn)} = CAST({textExpression} AS {storeType})"
            : $"{TextExpression(targetAlias, recordId)} = {textExpression}";
    }

    internal static string EqualsAnyParameter(
        string targetAlias,
        RecordIdConvention recordId,
        string parameterName
    )
    {
        return PostgresStoreTypeSql.CastType(recordId.RecordIdStoreType) is { } storeType
            ? $"{targetAlias}.{PostgreSqlIdentifier.Quote(recordId.RecordIdColumn)} = ANY(CAST(@{parameterName} AS {storeType}[]))"
            : $"{TextExpression(targetAlias, recordId)} = ANY(@{parameterName})";
    }

    /// <summary>
    /// S11: the canonical text of a record key, through the store type. It reads no host table:
    /// it only needs the store type to be one PostgreSQL knows.
    /// </summary>
    internal static SweepStatement CanonicalizeStatement(RetentionEntry entry, object? value)
    {
        var storeType = PostgresStoreTypeSql.CastType(entry.RecordId.RecordIdStoreType);
        return new SweepStatement(
            "canonicalize-record-id",
            entry.EntityType,
            storeType is null
                ? "SELECT CAST(@recordId AS text)"
                : $"SELECT CAST(CAST(@recordId AS {storeType}) AS text)",
            new SqlParams { ["recordId"] = value },
            []
        );
    }

    internal static async Task<string> CanonicalizeAsync(
        DbConnection connection,
        DbTransaction? transaction,
        RetentionEntry entry,
        object value,
        CancellationToken ct
    )
    {
        var statement = CanonicalizeStatement(entry, value);
        await using var command = statement.Parameters.CreateCommand(connection, transaction, statement.Sql);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }
}
