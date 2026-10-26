using System.Buffers.Binary;
using System.Data.Common;
using System.Security.Cryptography;
using System.Text;

namespace Cohort.Infrastructure;

/// <summary>
/// Session-level PostgreSQL advisory locks that mark a live run, worker or row-handler
/// delivery. They live as long as the session that took them, so they need direct connections
/// or session-mode pooling: a transaction-mode pooler hands the session to someone else.
/// </summary>
internal static class RetentionRunAdvisoryLock
{
    internal static long KeyFor(Guid sweepId)
    {
        Span<byte> bytes = stackalloc byte[16];
        sweepId.TryWriteBytes(bytes, bigEndian: true, out _);
        return BinaryPrimitives.ReadInt64BigEndian(bytes);
    }

    /// <summary>
    /// The scheduled worker's key for one Cohort install. Advisory locks are per database, so
    /// the key is derived from the install's schema-qualified <c>sweep_run</c> table: installs
    /// in different schemas sweep independently, replicas of one install share the key.
    /// </summary>
    internal static long WorkerKeyFor(RelationalObjectName sweepRun)
    {
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Encoding.UTF8.GetBytes($"cohort-worker:{PostgreSqlIdentifier.Format(sweepRun)}"), hash);
        return BinaryPrimitives.ReadInt64BigEndian(hash);
    }

    /// <summary>
    /// The key a dispatcher holds while it runs one row-handler status. Status ids are only
    /// unique within one install's <c>sweep_row_handler_status</c> table, so the key is derived
    /// from the schema-qualified table as well, the same way as <see cref="WorkerKeyFor"/>.
    /// </summary>
    internal static long RowHandlerKeyFor(RelationalObjectName statusTable, long statusId)
    {
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(
            Encoding.UTF8.GetBytes($"cohort-row-handler:{PostgreSqlIdentifier.Format(statusTable)}:{statusId}"),
            hash
        );
        return BinaryPrimitives.ReadInt64BigEndian(hash);
    }

    internal static Task AcquireAsync(DbConnection connection, long key, CancellationToken ct) =>
        ExecuteAsync(connection, "SELECT pg_catalog.pg_advisory_lock(@key)", key, ct);

    internal static async Task<bool> TryAcquireAsync(DbConnection connection, long key, CancellationToken ct) =>
        await ExecuteAsync(connection, "SELECT pg_catalog.pg_try_advisory_lock(@key)", key, ct) is true;

    internal static async Task ReleaseAsync(DbConnection connection, long key, CancellationToken ct)
    {
        if (await ExecuteAsync(connection, "SELECT pg_catalog.pg_advisory_unlock(@key)", key, ct) is not true)
        {
            throw new InvalidOperationException(
                $"PostgreSQL reported that Cohort advisory lock {key} was not owned by this connection."
            );
        }
    }

    /// <summary>
    /// Takes every key on the connection, in key order so two sessions taking overlapping sets
    /// can't deadlock, inside <paramref name="transaction"/>. A session lock survives the
    /// transaction's commit, and also its rollback, so a caller whose transaction fails must
    /// still release them.
    /// </summary>
    internal static async Task AcquireAllAsync(
        DbConnection connection,
        DbTransaction transaction,
        IReadOnlyCollection<long> keys,
        CancellationToken ct
    )
    {
        await using var command = new SqlParams { ["keys"] = keys.ToArray() }.CreateCommand(
            connection,
            transaction,
            "SELECT pg_catalog.pg_advisory_lock(k) FROM pg_catalog.unnest(@keys) AS k ORDER BY k"
        );
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Releases every session advisory lock the connection holds.</summary>
    internal static async Task ReleaseAllAsync(DbConnection connection, CancellationToken ct)
    {
        await using var command = new SqlParams().CreateCommand(
            connection,
            null,
            "SELECT pg_catalog.pg_advisory_unlock_all()"
        );
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ExecuteAsync(DbConnection connection, string sql, long key, CancellationToken ct)
    {
        await using var command = new SqlParams { ["key"] = key }.CreateCommand(connection, null, sql);
        return await command.ExecuteScalarAsync(ct);
    }
}
