using Cohort.Infrastructure;
using Microsoft.Extensions.Options;

namespace Cohort.Hosting;

/// <summary>
/// The one owner of the effective <see cref="CohortOptions"/>. A reload that fails
/// validation never reaches <c>OnChange</c>, so the last valid options stay in force;
/// reading <see cref="IOptionsMonitor{TOptions}.CurrentValue"/> directly would throw instead.
/// </summary>
internal sealed class CohortOptionsSnapshot : IRetentionExecutionSettings, IDisposable
{
    private CohortOptions current;
    private readonly IDisposable? reloadSubscription;

    public CohortOptionsSnapshot(IOptionsMonitor<CohortOptions> options)
    {
        current = options.CurrentValue;
        reloadSubscription = options.OnChange(updated => Volatile.Write(ref current, updated));
    }

    public CohortOptions Current => Volatile.Read(ref current);

    public bool DryRun => Current.DryRun;

    public int SweepBatchSize => Current.SweepBatchSize;

    public TimeSpan AuditObserverTimeout => Current.AuditObservers.Timeout;

    public void Dispose()
    {
        reloadSubscription?.Dispose();
    }
}
