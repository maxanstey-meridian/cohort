using System.Buffers.Binary;
using System.Data.Common;
using System.Security.Cryptography;
using System.Text;

namespace Cohort.Infrastructure;

/// <summary>Session-level PostgreSQL advisory locks that mark a live run or worker.</summary>
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

    private static async Task<object?> ExecuteAsync(DbConnection connection, string sql, long key, CancellationToken ct)
    {
        await using var command = new SqlParams { ["key"] = key }.CreateCommand(connection, null, sql);
        return await command.ExecuteScalarAsync(ct);
    }
}
