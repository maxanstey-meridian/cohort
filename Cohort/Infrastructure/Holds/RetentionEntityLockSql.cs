using System.Data.Common;

using Cohort.Infrastructure.Sweep;

namespace Cohort.Infrastructure.Holds;

internal static class RetentionEntityLockSql
{
    private const long HashSeed = 4_341_726_887;

    internal readonly record struct Target(
        Guid RetentionEntityId,
        Guid? TenantId,
        string CanonicalRecordId
    );

    internal static Task AcquireAsync(
        DbConnection connection,
        DbTransaction transaction,
        Guid retentionEntityId,
        Guid? tenantId,
        string canonicalRecordId,
        CancellationToken ct
    )
    {
        return AcquireAsync(connection, transaction, retentionEntityId, tenantId, [canonicalRecordId], ct);
    }

    internal static async Task AcquireAsync(
        DbConnection connection,
        DbTransaction transaction,
        Guid retentionEntityId,
        Guid? tenantId,
        IReadOnlyList<string> canonicalRecordIds,
        CancellationToken ct
    )
    {
        if (canonicalRecordIds.Count == 0)
        {
            return;
        }

        await AcquireAsync(
            connection,
            transaction,
            canonicalRecordIds
                .Select(recordId => new Target(retentionEntityId, tenantId, recordId))
                .ToArray(),
            ct
        );
    }

    internal static async Task AcquireAsync(
        DbConnection connection,
        DbTransaction transaction,
        IReadOnlyCollection<Target> targets,
        CancellationToken ct
    )
    {
        if (targets.Count == 0)
        {
            return;
        }

        var lockKeys = targets
            .Select(target => BuildKey(
                target.RetentionEntityId,
                target.TenantId,
                target.CanonicalRecordId
            ))
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();

        var statement = LockStatement(lockKeys);
        await using var command = statement.Parameters.CreateCommand(connection, transaction, statement.Sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Takes the transaction-scoped lock of every key in <paramref name="lockKeys"/> (from
    /// <see cref="BuildKey"/>), in key order so two sessions taking overlapping sets can't
    /// deadlock.
    /// </summary>
    internal static SweepStatement LockStatement(string[] lockKeys) =>
        CohortStatements.Statement(
            "entity-lock",
            """
            SELECT pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended(ordered.lock_key, @hashSeed)
            )
            FROM (
                SELECT lock_key
                FROM pg_catalog.unnest(@lockKeys) AS keys(lock_key)
                ORDER BY lock_key
            ) AS ordered
            """,
            new SqlParams { ["hashSeed"] = HashSeed, ["lockKeys"] = lockKeys }
        );

    private static string BuildKey(Guid retentionEntityId, Guid? tenantId, string canonicalRecordId)
    {
        return $"{retentionEntityId:D}:{tenantId?.ToString("D") ?? "tenantless"}:{canonicalRecordId.Length}:{canonicalRecordId}";
    }
}
