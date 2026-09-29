using Cohort.Domain;
using Cohort.Infrastructure.Sweep;

namespace Cohort.Infrastructure;

internal abstract record AnonymiseField(string MemberName, string ColumnName, string? StoreType)
{
    // Cast to the column's store type so text-shaped values can land in jsonb, citext, etc.
    internal string AssignmentSql(string parameterName)
    {
        var column = PostgreSqlIdentifier.Quote(ColumnName);
        return PostgresStoreTypeSql.Validate(StoreType) is { } storeType
            ? $"{column} = CAST(@{parameterName} AS {storeType})"
            : $"{column} = @{parameterName}";
    }
}

internal sealed record AnonymiseLiteralField(
    string MemberName,
    string ColumnName,
    string? StoreType,
    AnonymiseMethod Method,
    string? Literal = null
) : AnonymiseField(MemberName, ColumnName, StoreType);

internal sealed record AnonymiseFactoryField(
    string MemberName,
    string ColumnName,
    string? StoreType,
    Type FactoryType
) : AnonymiseField(MemberName, ColumnName, StoreType);
