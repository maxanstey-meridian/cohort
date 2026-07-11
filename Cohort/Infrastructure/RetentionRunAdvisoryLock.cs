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
        ExecuteAsync(connection, "SELECT pg_advisory_lock(@key)", sweepId, ct);

    internal static async Task<bool> TryAcquireAsync(
        DbConnection connection,
        Guid sweepId,
        CancellationToken ct
    )
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT pg_try_advisory_lock(@key)";
        AddKey(command, sweepId);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    internal static Task ReleaseAsync(
        DbConnection connection,
        Guid sweepId,
        CancellationToken ct = default
    ) => ExecuteAsync(connection, "SELECT pg_advisory_unlock(@key)", sweepId, ct);

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
        var parameter = command.CreateParameter();
        parameter.ParameterName = "key";
        parameter.Value = GetKey(sweepId);
        command.Parameters.Add(parameter);
    }
}
