using Cohort.Domain;

namespace Cohort.Infrastructure;

internal abstract record AnonymiseField(string MemberName, string ColumnName);

internal sealed record AnonymiseLiteralField(
    string MemberName,
    string ColumnName,
    AnonymiseMethod Method,
    string? Literal = null
) : AnonymiseField(MemberName, ColumnName);

internal sealed record AnonymiseFactoryField(string MemberName, string ColumnName, Type FactoryType)
    : AnonymiseField(MemberName, ColumnName);
