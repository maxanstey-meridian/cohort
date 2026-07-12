namespace Cohort.Hosting;

public sealed class AuditObserverOptions
{
    internal static TimeSpan MaximumTimeout { get; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Maximum time Cohort waits for each observer to process one committed event.
    /// A timeout abandons that delivery attempt and does not affect the retention run.
    /// </summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(5);
}
