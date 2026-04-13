namespace Cohort.Domain;

public abstract record AnonymiseField(string MemberName, string ColumnName);

public sealed record AnonymiseLiteralField(
    string MemberName,
    string ColumnName,
    AnonymiseMethod Method,
    string? Literal = null
) : AnonymiseField(MemberName, ColumnName);

public sealed record AnonymiseFactoryField(
    string MemberName,
    string ColumnName,
    Type FactoryType
) : AnonymiseField(MemberName, ColumnName);
