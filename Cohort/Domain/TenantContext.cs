namespace Cohort.Domain;

public sealed record TenantContext(
    Guid Id,
    string? Jurisdiction,
    IReadOnlyDictionary<string, string> Tags
)
{
    /// <summary>
    /// The identity tenantless sweeps run under. Tenantless tables hold data no tenant
    /// owns, so their audit rows are attributed to <see cref="Guid.Empty"/> rather than
    /// to whichever tenant's pass happened to reach them first.
    /// </summary>
    public static TenantContext Tenantless { get; } =
        new(Guid.Empty, null, new Dictionary<string, string>());
}
