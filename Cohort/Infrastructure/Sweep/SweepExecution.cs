using System.Data.Common;

namespace Cohort.Infrastructure.Sweep;

internal sealed record SweepExecutionResult(
    IReadOnlyList<string> AffectedRecordIds,
    bool RowDetailsPersisted = false,
    int SkippedCount = 0,
    int CandidateCount = 0,
    IReadOnlyList<string>? BlockedRecordIds = null,
    IReadOnlyList<DbException>? BlockedFailures = null
)
{
    /// <summary>
    /// Record ids an integrity constraint (SQLSTATE class 23) refused to let the strategy
    /// change. Not persisted: the engine counts them, and later batches of the run continue
    /// past them (<see cref="Reached"/>).
    /// </summary>
    public IReadOnlyList<string> BlockedRecordIds { get; init; } = BlockedRecordIds ?? [];

    /// <summary>
    /// The refusal behind each of <see cref="BlockedRecordIds"/>, in the same order: the
    /// engine reads them to tell a schema-wide refusal from row-level ones.
    /// </summary>
    public IReadOnlyList<DbException> BlockedFailures { get; init; } = BlockedFailures ?? [];

    /// <summary>
    /// Where the run's next batch for the entity continues: the last row this batch's
    /// discovery reached, or where it started when it found none.
    /// </summary>
    public SweepCursor? Reached { get; init; }
}

/// <summary>
/// One batch of a real run. Discovery continues after <paramref name="After"/>, the row the
/// run's previous batch for the entity reached (from the start when null).
/// </summary>
internal sealed record SweepMutationContext(
    Guid SweepId,
    DateTimeOffset At,
    int BatchSize,
    SweepCursor? After
);

/// <summary>
/// A position in discovery's order (anchor ascending with NULLs last, then record-id text).
/// Every row before it was tried once by the run: changed, skipped, blocked, or found locked
/// and left for the next run. The anchor is bound as a timestamptz, like the cutoff, so the
/// comparison never goes through text. Null when the row's anchor is NULL, which only an
/// erasure without a legal minimum reaches.
/// </summary>
internal sealed record SweepCursor(DateTimeOffset? Anchor, string RecordId);

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
