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

        Strategies = new ReadOnlyStrategySet(copy);
    }

    public IReadOnlySet<Strategy> Strategies { get; }

    private sealed class ReadOnlyStrategySet(HashSet<Strategy> values) : IReadOnlySet<Strategy>
    {
        public int Count => values.Count;

        public bool Contains(Strategy item) => values.Contains(item);

        public IEnumerator<Strategy> GetEnumerator() => values.GetEnumerator();

        public bool IsProperSubsetOf(IEnumerable<Strategy> other) =>
            values.IsProperSubsetOf(other);

        public bool IsProperSupersetOf(IEnumerable<Strategy> other) =>
            values.IsProperSupersetOf(other);

        public bool IsSubsetOf(IEnumerable<Strategy> other) => values.IsSubsetOf(other);

        public bool IsSupersetOf(IEnumerable<Strategy> other) => values.IsSupersetOf(other);

        public bool Overlaps(IEnumerable<Strategy> other) => values.Overlaps(other);

        public bool SetEquals(IEnumerable<Strategy> other) => values.SetEquals(other);

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() =>
            GetEnumerator();
    }
}
