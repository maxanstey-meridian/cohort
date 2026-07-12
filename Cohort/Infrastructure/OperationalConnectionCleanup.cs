using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure;

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
        Exception? unlockException = null;
        Exception? closeException = null;
        using var cleanup = new CancellationTokenSource(CleanupTimeout);

        if (unlock is not null)
        {
            try
            {
                await unlock(cleanup.Token);
            }
            catch (Exception ex)
            {
                unlockException = ex;
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
                closeException = ex;
                logger?.LogWarning(
                    ex,
                    "Cohort owned-connection cleanup failed{PrimaryFailureContext}.",
                    primaryException is null ? "" : " after the primary operation failed"
                );
            }
        }

        if (primaryException is not null)
        {
            return;
        }

        if ((unlockException ?? closeException) is { } cleanupException)
        {
            ExceptionDispatchInfo.Capture(cleanupException).Throw();
        }
    }
}
