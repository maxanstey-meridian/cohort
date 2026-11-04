using System.Collections.Concurrent;

using Cohort.Application;

namespace Cohort.Sample.Tests;

/// <summary>
/// An <see cref="IRetentionExecutionScope"/> that records every context it was asked to run
/// work in, then runs the work unchanged.
/// </summary>
internal sealed class RecordingExecutionScope : IRetentionExecutionScope
{
    private readonly ConcurrentQueue<RetentionExecutionContext> ran = new();

    public IReadOnlyCollection<RetentionExecutionContext> Ran => ran;

    public Task RunAsync(RetentionExecutionContext context, Func<Task> work, CancellationToken ct)
    {
        ran.Enqueue(context);
        return work();
    }
}
