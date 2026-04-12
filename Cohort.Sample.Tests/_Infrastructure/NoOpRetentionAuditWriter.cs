using Cohort.Application;

namespace Cohort.Sample.Tests;

public sealed class NoOpRetentionAuditWriter : IRetentionAuditWriter
{
    public Task WriteAsync(SweepEvent evt, CancellationToken ct)
    {
        return Task.CompletedTask;
    }
}
