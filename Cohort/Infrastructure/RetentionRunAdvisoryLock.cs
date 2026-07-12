using System.Buffers.Binary;
using System.Data.Common;

namespace Cohort.Infrastructure;

internal static class RetentionRunAdvisoryLock
{
    internal static long GetKey(Guid sweepId)
    {
        Span<byte> bytes = stackalloc byte[16];
        sweepId.TryWriteBytes(bytes, bigEndian: true, out _);
        return BinaryPrimitives.ReadInt64BigEndian(bytes);
    }

    internal static Task AcquireAsync(DbConnection connection, Guid sweepId, CancellationToken ct) =>
        ExecuteAsync(connection, "SELECT pg_catalog.pg_advisory_lock(@key)", sweepId, ct);

    internal static async Task<bool> TryAcquireAsync(
        DbConnection connection,
        Guid sweepId,
        CancellationToken ct
    )
    {
        return await TryAcquireAsync(connection, GetKey(sweepId), ct);
    }

    internal static async Task<bool> TryAcquireAsync(
        DbConnection connection,
        long key,
        CancellationToken ct
    )
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT pg_catalog.pg_try_advisory_lock(@key)";
        AddKey(command, key);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    internal static async Task ReleaseAsync(
        DbConnection connection,
        Guid sweepId,
        CancellationToken ct = default
    )
    {
        await ReleaseAsync(connection, GetKey(sweepId), ct);
    }

    internal static async Task ReleaseAsync(
        DbConnection connection,
        long key,
        CancellationToken ct = default
    )
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT pg_catalog.pg_advisory_unlock(@key)";
        AddKey(command, key);
        if (await command.ExecuteScalarAsync(ct) is not true)
        {
            throw new InvalidOperationException(
                $"PostgreSQL reported that Cohort advisory lock {key} was not owned by this connection."
            );
        }
    }

    private static async Task ExecuteAsync(
        DbConnection connection,
        string sql,
        Guid sweepId,
        CancellationToken ct
    )
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        AddKey(command, sweepId);
        await command.ExecuteScalarAsync(ct);
    }

    private static void AddKey(DbCommand command, Guid sweepId)
    {
        AddKey(command, GetKey(sweepId));
    }

    private static void AddKey(DbCommand command, long key)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = "key";
        parameter.Value = key;
        command.Parameters.Add(parameter);
    }
}
