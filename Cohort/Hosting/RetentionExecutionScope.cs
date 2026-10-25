using Cohort.Application;

namespace Cohort.Hosting;

public static class RetentionExecutionScope
{
    /// <summary>
    /// For hosts without tenant isolation: entering a scope changes nothing, so Cohort's work
    /// runs with whatever access the host's connection already has. Register it with
    /// <c>services.AddSingleton(RetentionExecutionScope.Unisolated)</c>.
    /// </summary>
    public static IRetentionExecutionScope Unisolated { get; } = new UnisolatedExecutionScope();

    private sealed class UnisolatedExecutionScope : IRetentionExecutionScope
    {
        public ValueTask EnterAsync(RetentionExecutionContext context, CancellationToken ct) =>
            ValueTask.CompletedTask;
    }
}
