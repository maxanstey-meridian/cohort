namespace Cohort.Application;

public interface IRetentionAuditWriter
{
    public Task WriteAsync(SweepEvent evt, CancellationToken ct);
}
