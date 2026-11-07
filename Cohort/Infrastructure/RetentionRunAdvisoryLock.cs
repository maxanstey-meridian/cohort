using System.Buffers.Binary;
using System.Data.Common;
using System.Security.Cryptography;
using System.Text;

using Cohort.Infrastructure.Sweep;

namespace Cohort.Infrastructure;

/// <summary>
/// Session-level PostgreSQL advisory locks that mark a live run, worker or scheduled pass. They
/// live as long as the session that took them, so they need direct connections or
/// session-mode pooling: a transaction-mode pooler hands the session to someone else, and
/// Npgsql multiplexing runs each command outside a transaction on whichever session is free
/// (readiness refuses a multiplexed connection).
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

    internal static SweepStatement LockStatement(long key) =>
        CohortStatements.Statement("advisory-lock", "SELECT pg_catalog.pg_advisory_lock(@key)", new SqlParams { ["key"] = key });

    internal static SweepStatement TryLockStatement(long key) =>
        CohortStatements.Statement(
            "advisory-try-lock",
            "SELECT pg_catalog.pg_try_advisory_lock(@key)",
            new SqlParams { ["key"] = key }
        );

    internal static SweepStatement UnlockStatement(long key) =>
        CohortStatements.Statement(
            "advisory-unlock",
            "SELECT pg_catalog.pg_advisory_unlock(@key)",
            new SqlParams { ["key"] = key }
        );

    /// <summary>Releases every session advisory lock the connection holds.</summary>
    internal static SweepStatement UnlockAllStatement() =>
        CohortStatements.Statement(
            "advisory-unlock-all",
            "SELECT pg_catalog.pg_advisory_unlock_all()",
            new SqlParams()
        );

    /// <summary>Whether the session holds every key, as <see cref="HoldsAllAsync"/> asks.</summary>
    internal static SweepStatement HoldsAllStatement(IReadOnlyCollection<long> keys) =>
        // A bigint advisory key is held as classid (high 32 bits) and objid (low 32 bits).
        CohortStatements.Statement(
            "advisory-locks-held",
            """
            SELECT pg_catalog.count(*) = pg_catalog.cardinality(@keys)
            FROM pg_catalog.pg_locks AS l
            JOIN pg_catalog.unnest(@keys) AS k(key)
              ON l.classid = ((k.key >> 32) & 4294967295)::oid AND l.objid = (k.key & 4294967295)::oid
            WHERE l.locktype = 'advisory' AND l.objsubid = 1 AND l.granted
              AND l.pid = pg_catalog.pg_backend_pid()
            """,
            new SqlParams { ["keys"] = keys.ToArray() }
        );

    internal static Task AcquireAsync(DbConnection connection, long key, CancellationToken ct) =>
        ExecuteAsync(connection, null, LockStatement(key), ct);

    internal static async Task<bool> TryAcquireAsync(DbConnection connection, long key, CancellationToken ct) =>
        await ExecuteAsync(connection, null, TryLockStatement(key), ct) is true;

    internal static async Task ReleaseAsync(DbConnection connection, long key, CancellationToken ct)
    {
        if (await ExecuteAsync(connection, null, UnlockStatement(key), ct) is not true)
        {
            throw new InvalidOperationException(
                $"PostgreSQL reported that Cohort advisory lock {key} was not owned by this connection."
            );
        }
    }

    /// <summary>Releases every session advisory lock the connection holds.</summary>
    internal static async Task ReleaseAllAsync(DbConnection connection, CancellationToken ct)
    {
        await ExecuteNonQueryAsync(connection, null, UnlockAllStatement(), ct);
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
        return await ExecuteAsync(connection, transaction, HoldsAllStatement(keys), ct) is true;
    }

    private static long HashKey(string name)
    {
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Encoding.UTF8.GetBytes(name), hash);
        return BinaryPrimitives.ReadInt64BigEndian(hash);
    }

    private static async Task<object?> ExecuteAsync(
        DbConnection connection,
        DbTransaction? transaction,
        SweepStatement statement,
        CancellationToken ct
    )
    {
        await using var command = statement.Parameters.CreateCommand(connection, transaction, statement.Sql);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecuteNonQueryAsync(
        DbConnection connection,
        DbTransaction? transaction,
        SweepStatement statement,
        CancellationToken ct
    )
    {
        await using var command = statement.Parameters.CreateCommand(connection, transaction, statement.Sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
