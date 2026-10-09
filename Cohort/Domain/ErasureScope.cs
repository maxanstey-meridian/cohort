namespace Cohort.Domain;

public sealed record ErasureScope
{
    public ErasureScope(
        string kind,
        object subject,
        bool allowSoftDeleteAsErasure = false,
        bool dryRun = false
    )
    {
        Kind = string.IsNullOrWhiteSpace(kind)
            ? throw new ArgumentException("Erasure subject kind cannot be blank.", nameof(kind))
            : kind;
        Subject = subject ?? throw new ArgumentNullException(nameof(subject));
        AllowSoftDeleteAsErasure = allowSoftDeleteAsErasure;
        DryRun = dryRun;
    }

    /// <summary>
    /// The kind of subject being erased. Only columns marked
    /// <c>[ErasureSubject(kind)]</c> with this kind are matched.
    /// </summary>
    public string Kind { get; }

    /// <summary>
    /// The subject's identifier, of the CLR type its kind's columns share.
    /// </summary>
    public object Subject { get; }

    /// <summary>
    /// Erasure under a SoftDelete category only sets the soft-delete flag — personal data
    /// stays in the row. That is rarely what a right-to-erasure request means, so it is
    /// refused unless the caller explicitly opts in here.
    /// </summary>
    public bool AllowSoftDeleteAsErasure { get; }

    /// <summary>
    /// Counts what the erasure would change and writes its audit trail without changing
    /// any row.
    /// </summary>
    public bool DryRun { get; }
}
