using Cohort.Domain;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Infrastructure;

/// <summary>
/// Checks the worker's tenant source against the data it exists to retire: every tenant that
/// owns rows a sweep could still act on must be swept under its one true context, or a
/// durable failure must say why not. Rows every declared strategy has already retired (soft
/// deleted, anonymised, or exempt) need no context, so they don't count. Runs on the worker's
/// coordination connection, in a <c>System</c> scope, so row-level security doesn't hide the
/// tenants it is looking for.
/// </summary>
internal static class RetentionTenantCoverage
{
    /// <summary>
    /// The tenants that own rows a sweep could still act on in a retained tenanted table but
    /// aren't in <paramref name="sourceTenantIds"/>, and the entities with such rows under
    /// <see cref="Guid.Empty"/>, which no tenant pass can sweep. A row counts when any strategy
    /// its category declares in <paramref name="capabilities"/> could act on it; held rows and
    /// rows not yet due count, because the tenant still owns them.
    /// </summary>
    internal static async Task<TenantCoverageGaps> FindGapsAsync(
        DbContext db,
        RetentionRegistry registry,
        IReadOnlyDictionary<string, RetentionCategoryCapabilities> capabilities,
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

            // The missing tenant's own rule can't be resolved without the context it lacks, so
            // every strategy the category may resolve to counts. A category with no validated
            // capabilities counts every row: the conservative answer.
            var actionable = capabilities.TryGetValue(entry.Category, out var declared)
                ? SweepEligibilitySql.AnyPredicate(declared.Strategies, entry)
                : "";
            if (actionable is null)
            {
                continue;
            }

            // One pass over the table, whatever its indexes: the source's tenants and retired
            // rows are filtered out as rows are read, and only the tenants left are aggregated.
            var table = PostgreSqlIdentifier.Format(entry.Table);
            var column = PostgreSqlIdentifier.Quote(tenant.TenantColumn);
            var actionableClause = actionable.Length > 0 ? $"AND ({actionable})" : "";
            await using var command = new SqlParams
            {
                ["sourceTenantIds"] = sourceTenantIds.ToArray(),
            }.CreateCommand(
                connection,
                null,
                $"""
                SELECT DISTINCT target.{column} FROM {table} AS target
                WHERE target.{column} IS NOT NULL AND NOT (target.{column} = ANY(@sourceTenantIds))
                {actionableClause}
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
