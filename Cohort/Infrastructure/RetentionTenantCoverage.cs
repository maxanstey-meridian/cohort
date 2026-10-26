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

            // A loose index scan: one probe per distinct tenant, cheap with an index led by
            // the tenant column and a sequential scan without one.
            var table = PostgreSqlIdentifier.Format(entry.Table);
            var column = PostgreSqlIdentifier.Quote(tenant.TenantColumn);
            await using var command = new SqlParams
            {
                ["sourceTenantIds"] = sourceTenantIds.ToArray(),
            }.CreateCommand(
                connection,
                null,
                $"""
                WITH RECURSIVE tenants(id) AS (
                    (SELECT target.{column} FROM {table} AS target ORDER BY 1 LIMIT 1)
                    UNION ALL
                    SELECT (
                        SELECT target.{column} FROM {table} AS target
                        WHERE target.{column} > tenants.id ORDER BY 1 LIMIT 1
                    )
                    FROM tenants WHERE tenants.id IS NOT NULL
                )
                SELECT id FROM tenants WHERE id IS NOT NULL AND NOT (id = ANY(@sourceTenantIds))
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

/// <summary>Why the worker refused a tenant's pass instead of sweeping it.</summary>
internal enum RetentionPassRefusal
{
    /// <summary>The tenant owns retained rows, but the tenant source didn't return it.</summary>
    NotInTenantSource,

    /// <summary>The tenant source returned the tenant more than once, with different contexts.</summary>
    ConflictingTenantContexts,
}

/// <summary>
/// Settles a refused pass's run as Failed. Its machine code is Cohort-defined and safe to
/// persist; the message, which names the tenant, stays in the logs.
/// </summary>
internal sealed class RetentionPassRefusedException(Guid tenantId, RetentionPassRefusal refusal)
    : Exception(Describe(tenantId, refusal))
{
    public RetentionPassRefusal Refusal { get; } = refusal;

    public string MachineCode => Refusal switch
    {
        RetentionPassRefusal.NotInTenantSource => "cohort:tenant-not-in-source",
        RetentionPassRefusal.ConflictingTenantContexts => "cohort:conflicting-tenant-contexts",
        _ => throw new ArgumentOutOfRangeException(nameof(Refusal)),
    };

    private static string Describe(Guid tenantId, RetentionPassRefusal refusal) => refusal switch
    {
        RetentionPassRefusal.NotInTenantSource =>
            $"Tenant {tenantId} owns rows in retained tables, but IRetentionTenantSource did not return it. Cohort won't sweep a tenant without its jurisdiction and tags; add it to the tenant source.",
        RetentionPassRefusal.ConflictingTenantContexts =>
            $"IRetentionTenantSource returned tenant {tenantId} more than once with different contexts. Cohort won't guess which jurisdiction and tags apply; return one context per tenant.",
        _ => throw new ArgumentOutOfRangeException(nameof(refusal)),
    };
}
