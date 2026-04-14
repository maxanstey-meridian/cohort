using Cohort.Application;

namespace Cohort.Infrastructure.Sweep;

internal static class AnonymiseFilterBuilder
{
    internal static SqlFilter CreateCutoffFilter(string anchorColumn, DateTimeOffset cutoff)
    {
        return new SqlFilter(
            $"target.{AnonymiseSqlBuilder.QuoteIdentifier(anchorColumn)} < @cutoff",
            [new SqlFilterParameter("cutoff", cutoff)]
        );
    }

    internal static SqlFilter CreateSubjectFilter(ErasureSubjectPredicate predicate)
    {
        return new SqlFilter(
            "("
                + string.Join(
                    " OR ",
                    predicate.Matches.Select((match, index) =>
                        $"target.{AnonymiseSqlBuilder.QuoteIdentifier(match.SubjectColumn)} = @subjectValue{index}"
                    )
                )
                + ")",
            predicate.Matches
                .Select((match, index) => new SqlFilterParameter($"subjectValue{index}", match.SubjectValue))
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
