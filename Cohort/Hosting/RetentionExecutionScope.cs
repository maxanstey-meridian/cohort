using Cohort.Application;

namespace Cohort.Hosting;

public static class RetentionExecutionScope
{
    /// <summary>
    /// For hosts without tenant isolation: it just runs the work, so Cohort's work runs with
    /// whatever access the host's connection already has. Register it with
    /// <c>services.AddSingleton(RetentionExecutionScope.Unisolated)</c>.
    /// </summary>
    public static IRetentionExecutionScope Unisolated { get; } = new UnisolatedExecutionScope();

    private sealed class UnisolatedExecutionScope : IRetentionExecutionScope
    {
        public Task RunAsync(RetentionExecutionContext context, Func<Task> work, CancellationToken ct) =>
            work();
    }
}
