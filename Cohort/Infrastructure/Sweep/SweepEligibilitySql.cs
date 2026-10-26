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
    /// The rows <paramref name="strategy"/> could still act on, over alias <c>target</c>
    /// ("" for every row). The marker a strategy needs is guaranteed by startup validation and
    /// by the strategy's own <c>Validate</c>, both of which run first.
    /// </summary>
    internal static string Predicate(Strategy strategy, RetentionEntry entry) => strategy switch
    {
        Strategy.Purge => "",
        Strategy.SoftDelete =>
            $"target.{PostgreSqlIdentifier.Quote(entry.SoftDelete!.IsDeletedColumn)} = FALSE",
        // The AnonymisedAt marker keeps anonymisation idempotent: rows scrubbed by an earlier
        // run fall out of scope instead of being re-anonymised forever.
        Strategy.Anonymise =>
            $"target.{PostgreSqlIdentifier.Quote(entry.AnonymisedAt!.AnonymisedAtColumn)} IS NULL",
        _ => throw new ArgumentOutOfRangeException(nameof(strategy), strategy, null),
    };

    /// <summary>
    /// The rows any of <paramref name="strategies"/> could still act on: "" when Purge is among
    /// them, null when none acts on any row (only Exempt, so skip the table), otherwise the
    /// ORed predicates of the rest.
    /// </summary>
    internal static string? AnyPredicate(IEnumerable<Strategy> strategies, RetentionEntry entry)
    {
        var acting = strategies.Distinct().Where(strategy => strategy != Strategy.Exempt).Order().ToList();
        if (acting.Contains(Strategy.Purge))
        {
            return "";
        }

        return acting.Count == 0
            ? null
            : string.Join(" OR ", acting.Select(strategy => $"({Predicate(strategy, entry)})"));
    }
}
