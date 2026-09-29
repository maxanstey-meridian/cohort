using System.Collections.ObjectModel;

namespace Cohort.Domain;

public sealed record RetentionCategoryCapabilities
{
    public RetentionCategoryCapabilities(IEnumerable<Strategy> strategies)
    {
        ArgumentNullException.ThrowIfNull(strategies);

        var copy = new HashSet<Strategy>(strategies);
        if (copy.Count == 0)
        {
            throw new ArgumentException(
                "Retention category capabilities must declare at least one strategy.",
                nameof(strategies)
            );
        }

        foreach (var strategy in copy)
        {
            if (!Enum.IsDefined(strategy))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(strategies),
                    strategy,
                    "Retention category capabilities may only declare defined strategies."
                );
            }
        }

        Strategies = new ReadOnlySet<Strategy>(copy);
    }

    public IReadOnlySet<Strategy> Strategies { get; }

    public bool Equals(RetentionCategoryCapabilities? other) =>
        other is not null && Strategies.SetEquals(other.Strategies);

    public override int GetHashCode()
    {
        var hash = 0;
        foreach (var strategy in Strategies)
        {
            hash |= 1 << (int)strategy;
        }

        return hash;
    }
}
