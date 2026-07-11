namespace Cohort.Infrastructure.Sweep;

internal static class AnonymiseFilterBuilder
{
    internal static SqlFilter CreateCutoffFilter(RetentionEntry entry, DateTimeOffset cutoff)
    {
        // The AnonymisedAt marker keeps anonymisation idempotent: rows scrubbed by an
        // earlier sweep fall out of the filter instead of being re-anonymised forever.
        var anonymisedAtClause = entry.AnonymisedAt is { } anonymisedAt
            ? $" AND target.{AnonymiseSqlBuilder.QuoteIdentifier(anonymisedAt.AnonymisedAtColumn)} IS NULL"
            : "";

        return new SqlFilter(
            $"target.{AnonymiseSqlBuilder.QuoteIdentifier(entry.AnchorColumn)} < @cutoff{anonymisedAtClause}",
            [new SqlFilterParameter("cutoff", cutoff)]
        );
    }

    internal static SqlFilter CreateNullAnchorFilter(RetentionEntry entry)
    {
        // Rows already scrubbed (AnonymisedAt set) are excluded: a NULL-anchor count
        // reports rows anonymisation could otherwise still act on.
        var anonymisedAtClause = entry.AnonymisedAt is { } anonymisedAt
            ? $" AND target.{AnonymiseSqlBuilder.QuoteIdentifier(anonymisedAt.AnonymisedAtColumn)} IS NULL"
            : "";

        return new SqlFilter(
            $"target.{AnonymiseSqlBuilder.QuoteIdentifier(entry.AnchorColumn)} IS NULL{anonymisedAtClause}",
            []
        );
    }

    internal static SqlFilter CreateSubjectFilter(ErasureSubjectPredicate predicate)
    {
        return new SqlFilter(
            "("
                + string.Join(
                    " OR ",
                    predicate.Matches.Select(
                        (match, index) =>
                            $"target.{AnonymiseSqlBuilder.QuoteIdentifier(match.SubjectColumn)} = @subjectValue{index}"
                    )
                )
                + ")",
            predicate
                .Matches.Select(
                    (match, index) =>
                        new SqlFilterParameter($"subjectValue{index}", match.SubjectValue)
                )
                .ToArray()
        );
    }

    internal static SqlFilter Combine(params SqlFilter[] filters)
    {
        return new SqlFilter(
            string.Join(" AND ", filters.Select(filter => $"({filter.PredicateSql})")),
            filters.SelectMany(filter => filter.Parameters).ToArray()
        );
    }
}

internal sealed record SqlFilter(string PredicateSql, IReadOnlyList<SqlFilterParameter> Parameters);

internal sealed record SqlFilterParameter(string Name, object? Value);
