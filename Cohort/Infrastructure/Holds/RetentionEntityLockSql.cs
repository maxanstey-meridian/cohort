using System.Data.Common;

namespace Cohort.Infrastructure.Holds;

internal static class RetentionEntityLockSql
{
    private const long HashSeed = 4_341_726_887;

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

        var lockKeys = canonicalRecordIds
            .Select(recordId => BuildKey(retentionEntityId, tenantId, recordId))
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended(ordered.lock_key, @hashSeed)
            )
            FROM (
                SELECT lock_key
                FROM pg_catalog.unnest(@lockKeys) AS keys(lock_key)
                ORDER BY lock_key
            ) AS ordered
            """;
        command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "hashSeed", HashSeed));
        command.Parameters.Add(RetentionHoldSql.CreateParameter(command, "lockKeys", lockKeys));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string BuildKey(Guid retentionEntityId, Guid? tenantId, string canonicalRecordId)
    {
        return $"{retentionEntityId:D}:{tenantId?.ToString("D") ?? "tenantless"}:{canonicalRecordId.Length}:{canonicalRecordId}";
    }
}
