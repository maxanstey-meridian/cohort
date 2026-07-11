namespace Cohort.Application;

/// <summary>
/// Indicates that a dispatcher could not settle handler work because its fenced claim
/// was replaced before the status update completed.
/// </summary>
internal sealed class RetentionRowDispatchClaimLostException(long statusId, Exception? innerException = null)
    : InvalidOperationException(
        $"Retention row handler status {statusId} could not be settled because its claim is no longer owned by this dispatcher.",
        innerException
    )
{
    /// <summary>The handler status row whose claim was lost.</summary>
    public long StatusId { get; } = statusId;
}
