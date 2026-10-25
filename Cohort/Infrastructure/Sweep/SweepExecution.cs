using System.Data.Common;

namespace Cohort.Infrastructure.Sweep;

internal sealed record SweepExecutionResult(
    IReadOnlyList<string> AffectedRecordIds,
    int HeldCount,
    bool RowDetailsPersisted = false,
    int SkippedCount = 0,
    int CandidateCount = 0,
    IReadOnlyList<string>? SkippedRecordIds = null,
    IReadOnlyList<string>? BlockedRecordIds = null,
    IReadOnlyList<DbException>? BlockedFailures = null
)
{
    /// <summary>
    /// Record ids the strategy selected but deliberately did not mutate (e.g. rows whose
    /// OnBefore handler failed). The engine excludes them from later batches of the same
    /// run: a skipped row stays eligible, so reselecting it would re-fail it forever and
    /// re-insert its audit row detail under the same sweep id.
    /// </summary>
    public IReadOnlyList<string> SkippedRecordIds { get; init; } = SkippedRecordIds ?? [];

    /// <summary>
    /// Record ids an integrity constraint (SQLSTATE class 23) refused to let the strategy
    /// change. Not persisted: the engine counts them and excludes them from later batches of
    /// the same run.
    /// </summary>
    public IReadOnlyList<string> BlockedRecordIds { get; init; } = BlockedRecordIds ?? [];

    /// <summary>
    /// The refusal behind each of <see cref="BlockedRecordIds"/>, in the same order: the
    /// engine reads them to tell a schema-wide refusal from row-level ones.
    /// </summary>
    public IReadOnlyList<DbException> BlockedFailures { get; init; } = BlockedFailures ?? [];
}

/// <summary>
/// One batch of a real run. <c>ExcludedRecordIds</c> are rows earlier batches of the run
/// found blocked, left out of discovery so they aren't tried again in the same run.
/// </summary>
internal sealed record SweepMutationContext(
    Guid SweepId,
    DateTimeOffset At,
    int BatchSize,
    IReadOnlyCollection<string> ExcludedRecordIds
);

internal sealed record ErasureSubjectPredicate
{
    public ErasureSubjectPredicate(IReadOnlyList<ErasureSubjectMatch> matches)
    {
        ArgumentNullException.ThrowIfNull(matches);
        if (matches.Count == 0)
        {
            throw new ArgumentException(
                "Erasure subject predicates must contain at least one subject match.",
                nameof(matches)
            );
        }

        Matches = matches;
    }

    public IReadOnlyList<ErasureSubjectMatch> Matches { get; }
}

internal sealed record ErasureSubjectMatch(
    string SubjectMember,
    string SubjectColumn,
    string? SubjectStoreType,
    object SubjectValue
)
{
    // Compare under the column's own type so citext and similar types keep their semantics.
    internal string EqualsParameterSql(string targetAlias, string parameterName)
    {
        var column = $"{targetAlias}.{PostgreSqlIdentifier.Quote(SubjectColumn)}";
        return PostgresStoreTypeSql.CastType(SubjectStoreType) is { } storeType
            ? $"{column} = CAST(@{parameterName} AS {storeType})"
            : $"{column} = @{parameterName}";
    }
}
