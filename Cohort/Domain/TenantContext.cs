namespace Cohort.Domain;

public sealed record TenantContext(
    Guid Id,
    string? Jurisdiction,
    IReadOnlyDictionary<string, string> Tags
);
