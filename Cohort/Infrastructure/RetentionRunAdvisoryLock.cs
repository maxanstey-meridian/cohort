using System.Buffers.Binary;
using System.Data.Common;
using System.Security.Cryptography;
using System.Text;

namespace Cohort.Infrastructure;

/// <summary>
/// Session-level PostgreSQL advisory locks that mark a live run, worker, scheduled pass or
/// row-handler delivery. They live as long as the session that took them, so they need direct
/// connections or session-mode pooling: a transaction-mode pooler hands the session to someone
/// else, and Npgsql multiplexing runs each command outside a transaction on whichever session
/// is free (readiness refuses a multiplexed connection).
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
    internal static long WorkerKeyFor(RelationalObjectName sweepRun) =>
        HashKey($"cohort-worker:{PostgreSqlIdentifier.Format(sweepRun)}");

    /// <summary>
    /// The key a scheduled pass holds on its own connection from its ledger re-read to its
    /// settlement: one per install (the schema-qualified <c>sweep_run</c> table, as for
    /// <see cref="WorkerKeyFor"/>), tenant (<see cref="Guid.Empty"/> for the tenantless pass)
    /// and kind, matching the ledger's identity of a pass. The occurrence is left out, so a
    /// straggler still running one occurrence's pass also holds off the next one's.
    /// </summary>
    internal static long ScheduledPassKeyFor(RelationalObjectName sweepRun, Guid tenantId, bool dryRun) =>
        HashKey($"cohort-scheduled-pass:{PostgreSqlIdentifier.Format(sweepRun)}:{tenantId:D}:{(dryRun ? "dry" : "real")}");

    /// <summary>
    /// The key a dispatcher holds while it runs one row-handler status. Status ids are only
    /// unique within one install's <c>sweep_row_handler_status</c> table, so the key is derived
    /// from the schema-qualified table as well, the same way as <see cref="WorkerKeyFor"/>.
    /// </summary>
    internal static long RowHandlerKeyFor(RelationalObjectName statusTable, long statusId) =>
        HashKey($"cohort-row-handler:{PostgreSqlIdentifier.Format(statusTable)}:{statusId}");

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

    /// <summary>
    /// Whether the connection's session holds every key. Asked inside a transaction just
    /// before it commits, it is exact rather than check-then-act: a session keeps its session
    /// locks until it unlocks them or ends, and ending aborts the transaction. A connection EF
    /// silently reopened after its session was lost holds none of them.
    /// </summary>
    internal static async Task<bool> HoldsAllAsync(
        DbConnection connection,
        DbTransaction? transaction,
        IReadOnlyCollection<long> keys,
        CancellationToken ct
    )
    {
        // A bigint advisory key is held as classid (high 32 bits) and objid (low 32 bits).
        await using var command = new SqlParams { ["keys"] = keys.ToArray() }.CreateCommand(
            connection,
            transaction,
            """
            SELECT pg_catalog.count(*) = pg_catalog.cardinality(@keys)
            FROM pg_catalog.pg_locks AS l
            JOIN pg_catalog.unnest(@keys) AS k(key)
              ON l.classid = ((k.key >> 32) & 4294967295)::oid AND l.objid = (k.key & 4294967295)::oid
            WHERE l.locktype = 'advisory' AND l.objsubid = 1 AND l.granted
              AND l.pid = pg_catalog.pg_backend_pid()
            """
        );
        return await command.ExecuteScalarAsync(ct) is true;
    }

    private static long HashKey(string name)
    {
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Encoding.UTF8.GetBytes(name), hash);
        return BinaryPrimitives.ReadInt64BigEndian(hash);
    }

    private static async Task<object?> ExecuteAsync(DbConnection connection, string sql, long key, CancellationToken ct)
    {
        await using var command = new SqlParams { ["key"] = key }.CreateCommand(connection, null, sql);
        return await command.ExecuteScalarAsync(ct);
    }
}
