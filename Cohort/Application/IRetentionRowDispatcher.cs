namespace Cohort.Application;

public interface IRetentionRowDispatcher
{
    public Task FlushAsync(CancellationToken ct = default);
}
