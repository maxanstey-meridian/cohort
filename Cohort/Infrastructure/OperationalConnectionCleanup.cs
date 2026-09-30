using System.Data.Common;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Cohort.Infrastructure;

/// <summary>
/// Releases a session advisory lock and closes an owned connection. Failures are logged, never
/// thrown: the run's outcome is already recorded, and a dropped session releases its locks
/// anyway, so a cleanup error must not replace the real result or exception. A session whose
/// unlock failed may still hold the lock, so it is discarded rather than returned to the pool.
/// </summary>
internal static class OperationalConnectionCleanup
{
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(30);

    internal static async Task RunAsync(
        DbConnection connection,
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
                DiscardWhenClosed(connection);
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

    /// <summary>
    /// Npgsql resets a pooled session (DISCARD ALL releases advisory locks) only when the
    /// connection is next used, so a lock-holding session could sit idle in the pool and keep
    /// the lock. Clearing the pool closes this physical connection when it is returned; other
    /// pooled connections are reopened on demand.
    /// </summary>
    internal static void DiscardWhenClosed(DbConnection connection)
    {
        if (connection is NpgsqlConnection npgsqlConnection)
        {
            NpgsqlConnection.ClearPool(npgsqlConnection);
        }
    }
}
