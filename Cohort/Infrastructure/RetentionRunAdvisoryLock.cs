using System.Buffers.Binary;
using System.Data.Common;

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
