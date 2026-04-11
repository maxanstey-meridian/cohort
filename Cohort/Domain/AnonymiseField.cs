namespace Cohort.Domain;

public sealed record AnonymiseField(
    string MemberName,
    string ColumnName,
    AnonymiseMethod Method,
    string? Literal = null
);
