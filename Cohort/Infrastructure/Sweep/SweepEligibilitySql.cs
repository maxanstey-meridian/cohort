using Cohort.Domain;

namespace Cohort.Infrastructure.Sweep;

/// <summary>
/// The one definition of which rows each strategy could still act on. Sweeps narrow their
/// scope with it, and the worker's tenant coverage check counts only the rows it admits, so
/// the two can't disagree about which rows are retired.
/// </summary>
internal static class SweepEligibilitySql
{
    /// <summary>
    /// The rows <paramref name="strategy"/> could still act on, over alias <c>target</c>:
    /// "" for every row, <c>FALSE</c> for none.
    /// </summary>
    internal static string Predicate(Strategy strategy, RetentionEntry entry) => strategy switch
    {
        Strategy.Purge => "",
        Strategy.SoftDelete =>
            $"target.{PostgreSqlIdentifier.Quote(RequireSoftDelete(entry).IsDeletedColumn)} = FALSE",
        // The AnonymisedAt marker keeps anonymisation idempotent: rows scrubbed by an earlier
        // run fall out of scope instead of being re-anonymised forever.
        Strategy.Anonymise =>
            $"target.{PostgreSqlIdentifier.Quote(RequireAnonymisedAt(entry).AnonymisedAtColumn)} IS NULL",
        Strategy.Exempt => "FALSE",
        _ => throw new ArgumentOutOfRangeException(nameof(strategy), strategy, null),
    };

    /// <summary>
    /// The rows any of <paramref name="strategies"/> could still act on: "" when one acts on
    /// every row, null when none acts on any (skip the table), otherwise the ORed predicates.
    /// </summary>
    internal static string? AnyPredicate(IEnumerable<Strategy> strategies, RetentionEntry entry)
    {
        var predicates = new List<string>();
        foreach (var strategy in strategies.Distinct().Order())
        {
            switch (Predicate(strategy, entry))
            {
                case "":
                    return "";
                case "FALSE":
                    break;
                case var predicate:
                    predicates.Add($"({predicate})");
                    break;
            }
        }

        return predicates.Count == 0 ? null : string.Join(" OR ", predicates);
    }

    private static SoftDeleteConvention RequireSoftDelete(RetentionEntry entry) =>
        entry.SoftDelete
        ?? throw new InvalidOperationException(
            $"Retention entry for {entry.EntityType.FullName} must expose soft-delete metadata for soft-delete operations."
        );

    private static AnonymisedAtConvention RequireAnonymisedAt(RetentionEntry entry) =>
        entry.AnonymisedAt
        ?? throw new InvalidOperationException(
            $"Retention entry for {entry.EntityType.FullName} has no AnonymisedAt marker, so anonymisation cannot tell scrubbed rows from pending ones."
        );
}
