namespace Cohort.Infrastructure.Sweep;

internal sealed record SweepExecutionResult(
    IReadOnlyList<string> AffectedRecordIds,
    int HeldCount,
    bool RowDetailsPersisted = false,
    int SkippedCount = 0,
    int CandidateCount = 0,
    IReadOnlyList<string>? SkippedRecordIds = null
)
{
    /// <summary>
    /// Record ids the strategy selected but deliberately did not mutate (e.g. rows whose
    /// OnBefore handler failed). The engine excludes them from later batches of the same
    /// run: a skipped row stays eligible, so reselecting it would re-fail it forever and
    /// re-insert its audit row detail under the same sweep id.
    /// </summary>
    public IReadOnlyList<string> SkippedRecordIds { get; init; } = SkippedRecordIds ?? [];
}

internal sealed record SweepMutationContext(
    Guid SweepId,
    DateTimeOffset At,
    int? BatchSize = null
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
        return PostgresStoreTypeSql.Validate(SubjectStoreType) is { } storeType
            ? $"{column} = CAST(@{parameterName} AS {storeType})"
            : $"{column} = @{parameterName}";
    }
}
