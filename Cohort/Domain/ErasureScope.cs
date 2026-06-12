namespace Cohort.Domain;

public sealed record ErasureScope
{
    public ErasureScope(object subject, bool allowSoftDeleteAsErasure = false)
    {
        Subject = subject ?? throw new ArgumentNullException(nameof(subject));
        AllowSoftDeleteAsErasure = allowSoftDeleteAsErasure;
    }

    public object Subject { get; }

    /// <summary>
    /// Erasure under a SoftDelete category only sets the soft-delete flag — personal data
    /// stays in the row. That is rarely what a right-to-erasure request means, so it is
    /// refused unless the caller explicitly opts in here.
    /// </summary>
    public bool AllowSoftDeleteAsErasure { get; }
}
