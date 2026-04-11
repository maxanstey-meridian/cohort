namespace Cohort.Domain;

public sealed record RetentionResolutionContext(
    string Category,
    TenantContext Tenant,
    DateTimeOffset Now,
    IReadOnlyList<string> AliasPath
);
