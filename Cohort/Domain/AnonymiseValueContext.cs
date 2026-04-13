namespace Cohort.Domain;

public sealed record AnonymiseValueContext(
    Type EntityType,
    string MemberName,
    object? OriginalValue,
    DateTimeOffset Now,
    Guid TenantId
);
