namespace Cohort.Infrastructure;

/// <summary>
/// The host settings Infrastructure reads. Implemented in Hosting over the last valid
/// <c>CohortOptions</c>, because Infrastructure cannot see the options types.
/// </summary>
internal interface IRetentionExecutionSettings
{
    public int SweepBatchSize { get; }

    public TimeSpan AuditObserverTimeout { get; }

    /// <summary>The zone whose calendar retention periods are counted in.</summary>
    public TimeZoneInfo TimeZone { get; }
}
