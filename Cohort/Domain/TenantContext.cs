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

    /// <summary>Whether this is <see cref="Tenantless"/>, the only context with an empty ID.</summary>
    internal bool IsTenantless => Id == Guid.Empty;

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

    /// <summary>
    /// Equal when the ID, the jurisdiction, and the tags match: the tags by the same keys with
    /// the same values, all compared ordinally. The ID alone would equate contexts that resolve
    /// different rules.
    /// </summary>
    public bool Equals(TenantContext? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return Id == other.Id
            && string.Equals(Jurisdiction, other.Jurisdiction, StringComparison.Ordinal)
            && tags.Count == other.tags.Count
            && tags.All(pair =>
                other.tags.TryGetValue(pair.Key, out var value)
                && string.Equals(pair.Value, value, StringComparison.Ordinal)
            );
    }

    // The tags are left out: equal contexts have equal IDs and jurisdictions, which is all a hash needs.
    public override int GetHashCode() =>
        HashCode.Combine(Id, Jurisdiction is null ? 0 : StringComparer.Ordinal.GetHashCode(Jurisdiction));

    private static IReadOnlyDictionary<string, string> CopyTags(
        IReadOnlyDictionary<string, string> tags
    ) =>
        new ReadOnlyDictionary<string, string>(
            new Dictionary<string, string>(
                tags ?? throw new ArgumentNullException(nameof(tags))
            )
        );
}
