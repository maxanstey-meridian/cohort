using Microsoft.EntityFrameworkCore;

namespace Cohort.Infrastructure;

/// <summary>
/// Checks the worker's tenant source against the data it exists to retire: every tenant that
/// owns rows in a retained tenanted table must be swept under its one true context, or a
/// durable failure must say why not. Runs on the worker's coordination connection, in a
/// <c>System</c> scope, so row-level security doesn't hide the tenants it is looking for.
/// </summary>
internal static class RetentionTenantCoverage
{
    /// <summary>
    /// The tenants that own rows in a retained tenanted table but aren't in
    /// <paramref name="sourceTenantIds"/>, and the entities with rows under
    /// <see cref="Guid.Empty"/>, which no tenant pass can sweep.
    /// </summary>
    internal static async Task<TenantCoverageGaps> FindGapsAsync(
        DbContext db,
        RetentionRegistry registry,
        IReadOnlyCollection<Guid> sourceTenantIds,
        CancellationToken ct
    )
    {
        var connection = db.Database.GetDbConnection();
        var missing = new SortedSet<Guid>();
        var emptyTenantEntities = new List<Type>();
        foreach (var entry in registry.Scan().Values)
        {
            if (entry.Tenant is not { } tenant)
            {
                continue;
            }

            // One pass over the table, whatever its indexes: the source's tenants are filtered
            // out as rows are read, and only the tenants left are aggregated.
            var table = PostgreSqlIdentifier.Format(entry.Table);
            var column = PostgreSqlIdentifier.Quote(tenant.TenantColumn);
            await using var command = new SqlParams
            {
                ["sourceTenantIds"] = sourceTenantIds.ToArray(),
            }.CreateCommand(
                connection,
                null,
                $"""
                SELECT DISTINCT target.{column} FROM {table} AS target
                WHERE target.{column} IS NOT NULL AND NOT (target.{column} = ANY(@sourceTenantIds))
                """
            );
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var tenantId = reader.GetGuid(0);
                if (tenantId == Guid.Empty)
                {
                    emptyTenantEntities.Add(entry.EntityType);
                }
                else
                {
                    missing.Add(tenantId);
                }
            }
        }

        return new TenantCoverageGaps(missing.ToArray(), emptyTenantEntities);
    }
}

internal sealed record TenantCoverageGaps(IReadOnlyList<Guid> MissingTenantIds, IReadOnlyList<Type> EntitiesWithEmptyTenant);
