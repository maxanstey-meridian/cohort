namespace Cohort.Hosting;

public sealed class RowHandlerDispatchOptions
{
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(10);

    public int BatchSize { get; init; } = 50;

    public int MaxAttempts { get; init; } = 10;

    public int MaxParallelism { get; init; } = 4;

    public TimeSpan BaseBackoff { get; init; } = TimeSpan.FromSeconds(1);
}
