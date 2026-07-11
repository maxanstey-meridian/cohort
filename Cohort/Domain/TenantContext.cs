using System.Collections.ObjectModel;

namespace Cohort.Domain;

public sealed record TenantContext
{
    private IReadOnlyDictionary<string, string> tags;

    public TenantContext(
        Guid id,
        string? jurisdiction,
        IReadOnlyDictionary<string, string> tags
    ) : this(id, jurisdiction, tags, allowTenantless: false) { }

    private TenantContext(
        Guid id,
        string? jurisdiction,
        IReadOnlyDictionary<string, string> tags,
        bool allowTenantless
    )
    {
        if (!allowTenantless && id == Guid.Empty)
        {
            throw new ArgumentException("Tenant ID cannot be empty.", nameof(id));
        }

        Id = id;
        Jurisdiction = jurisdiction;
        this.tags = CopyTags(tags);
    }

    public Guid Id { get; }

    public string? Jurisdiction { get; }

    public IReadOnlyDictionary<string, string> Tags
    {
        get => tags;
        init => tags = CopyTags(value);
    }

    /// <summary>
    /// The identity tenantless sweeps run under. Tenantless tables hold data no tenant
    /// owns, so their audit rows are attributed to <see cref="Guid.Empty"/> rather than
    /// to whichever tenant's pass happened to reach them first.
    /// </summary>
    internal static TenantContext Tenantless { get; } =
        new(Guid.Empty, null, new Dictionary<string, string>(), allowTenantless: true);

    private static IReadOnlyDictionary<string, string> CopyTags(
        IReadOnlyDictionary<string, string> tags
    ) =>
        new ReadOnlyDictionary<string, string>(
            new Dictionary<string, string>(
                tags ?? throw new ArgumentNullException(nameof(tags))
            )
        );
}
