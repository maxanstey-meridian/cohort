using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure;

/// <summary>
/// Releases a session advisory lock and closes an owned connection. Failures are logged, never
/// thrown: the run's outcome is already recorded, and a dropped session releases its locks
/// anyway, so a cleanup error must not replace the real result or exception.
/// </summary>
internal static class OperationalConnectionCleanup
{
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(30);

    internal static async Task RunAsync(
        Func<CancellationToken, Task>? unlock,
        Func<CancellationToken, Task>? close,
        Exception? primaryException,
        ILogger? logger
    )
    {
        using var cleanup = new CancellationTokenSource(CleanupTimeout);

        if (unlock is not null)
        {
            try
            {
                await unlock(cleanup.Token);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(
                    ex,
                    "Cohort advisory-lock cleanup failed{PrimaryFailureContext}.",
                    primaryException is null ? "" : " after the primary operation failed"
                );
            }
        }

        if (close is not null)
        {
            try
            {
                await close(cleanup.Token);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(
                    ex,
                    "Cohort owned-connection cleanup failed{PrimaryFailureContext}.",
                    primaryException is null ? "" : " after the primary operation failed"
                );
            }
        }
    }
}
