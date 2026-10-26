using System.Data.Common;

using Cohort.Application;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure.Holds;

/// <summary>
/// Proves host work inside a Cohort transaction left held rows alone. Read
/// <see cref="ReadHeldRowVersionsAsync"/> before the work and diff a second read after it, on
/// the same connection and transaction: removal for <see cref="IRetentionDeletion"/>
/// (<see cref="ReadRemovedHeldRowsAsync"/>), any change or removal for a dependents step
/// (<see cref="ReadChangedHeldRowsAsync"/>). The reads see only the holds the caller's scope
/// can read, so a caller that may touch tenantless rows first runs
/// <see cref="EnsureTenantlessHoldsVisibleAsync"/>.
/// </summary>
internal sealed class RetentionHoldGuard(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionRegistry registry,
    IServiceScopeFactory scopeFactory
)
{
    private readonly CohortStoreTables tables = CohortStoreTables.FromModel(db.Model);

    /// <summary>
    /// The hold checks see only the holds the caller's scope can read. A tenant scope whose
    /// holds policy hides NULL-tenant holds would pass a held tenantless row as unheld, so when
    /// the model has a tenantless retained entity the caller must see every active tenantless
    /// hold that system work sees. <paramref name="work"/> names the caller in the refusal.
    /// </summary>
    internal async Task EnsureTenantlessHoldsVisibleAsync(
        DbConnection connection,
        DbTransaction transaction,
        string work,
        CancellationToken ct
    )
    {
        // Any work can reach a tenantless row (a cascade, raw SQL), so the check follows the
        // model, not what the work names.
        if (!registry.Scan().Values.Any(entry => entry.Tenant is null))
        {
            return;
        }

        var hidden = await ReadSystemTenantlessHoldsAsync(ct);
        if (hidden.Count == 0)
        {
            return;
        }

        hidden.ExceptWith(await ReadActiveTenantlessHoldsAsync(connection, transaction, ct));
        if (hidden.Count == 0)
        {
            return;
        }

        // A hold that ended between the two reads was not hidden; one still active now was.
        hidden.IntersectWith(await ReadSystemTenantlessHoldsAsync(ct));
        if (hidden.Count > 0)
        {
            throw new RetentionConfigurationException([
                $"{work}'s scope cannot see {hidden.Count} active hold(s) on tenantless rows that Cohort's System(Readiness) work can, so it cannot tell whether a tenantless row it changes or removes is held. The holds table's row-level security must let every scope read holds whose \"TenantId\" is NULL. Nothing was changed.",
            ]);
        }
    }

    /// <summary>
    /// Every active hold the scope can see, with the physical version of its row (NULL when the
    /// row is gone). Take it before the work, in the transaction the work runs in.
    /// </summary>
    internal Task<HeldRowVersions> ReadHeldRowVersionsAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken ct
    ) => ReadVersionsAsync(connection, transaction, [], ct);

    /// <summary>
    /// The read after the work: the active holds, plus the baseline's holds whether or not they
    /// are still active, so a hold that ended during the work is judged by its row.
    /// </summary>
    private async Task<HeldRowVersions> ReadVersionsAsync(
        DbConnection connection,
        DbTransaction transaction,
        IReadOnlyCollection<Guid> baselineHoldIds,
        CancellationToken ct
    )
    {
        var (sql, parameters) = RetentionHoldSql.BuildActiveHeldRowVersions(
            tables.RetentionHolds,
            registry.Scan().Values.ToList(),
            baselineHoldIds
        );
        await using var command = parameters.CreateCommand(connection, transaction, sql);
        var versions = new Dictionary<Guid, string?>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            // Add, not set: two rows for one hold would mean its record id matched two rows, and
            // keeping either version could hide a change to the other.
            versions.Add(reader.GetGuid(0), reader.IsDBNull(1) ? null : reader.GetString(1));
        }

        return new HeldRowVersions(versions);
    }

    /// <summary>
    /// The dependents step's rule: holds whose row the work changed or removed (a non-NULL
    /// version that differs or became NULL), whether or not the hold is still active, so ending
    /// a hold first doesn't hide a change to its row; plus holds that appeared during the work,
    /// whose rows it may have changed after the hold committed. Sorted; empty when nothing was
    /// touched.
    /// </summary>
    internal async Task<IReadOnlyList<Guid>> ReadChangedHeldRowsAsync(
        HeldRowVersions baseline,
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken ct
    ) => HeldRowVersions.Changed(
        baseline,
        await ReadVersionsAsync(connection, transaction, baseline.ByHold.Keys.ToArray(), ct)
    );

    /// <summary>
    /// <see cref="IRetentionDeletion"/>'s rule: holds whose row is gone now but was not gone
    /// before the work, whether or not the hold is still active, so ending a hold first doesn't
    /// hide the deletion of its row. Holds already orphaned before it are not its doing. Sorted.
    /// </summary>
    internal async Task<IReadOnlyList<Guid>> ReadRemovedHeldRowsAsync(
        HeldRowVersions baseline,
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken ct
    ) => HeldRowVersions.Removed(
        baseline,
        await ReadVersionsAsync(connection, transaction, baseline.ByHold.Keys.ToArray(), ct)
    );

    private Task<HashSet<Guid>> ReadSystemTenantlessHoldsAsync(CancellationToken ct) =>
        scopeFactory.RunInScopeAsync(
            new RetentionExecutionContext.System(RetentionSystemWork.Readiness),
            async services =>
            {
                var systemDb = services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext);
                await systemDb.Database.OpenConnectionAsync(ct);
                try
                {
                    return await ReadActiveTenantlessHoldsAsync(systemDb.Database.GetDbConnection(), null, ct);
                }
                finally
                {
                    await systemDb.Database.CloseConnectionAsync();
                }
            },
            ct
        );

    private async Task<HashSet<Guid>> ReadActiveTenantlessHoldsAsync(
        DbConnection connection,
        DbTransaction? transaction,
        CancellationToken ct
    )
    {
        await using var command = new SqlParams().CreateCommand(
            connection,
            transaction,
            RetentionHoldSql.BuildActiveTenantlessHoldIds(tables.RetentionHolds)
        );
        var holdIds = new HashSet<Guid>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            holdIds.Add(reader.GetGuid(0));
        }

        return holdIds;
    }
}

/// <summary>
/// Hold ids with the physical version (<c>tableoid/ctid/xmin</c>) of the row each covers, or
/// NULL when the row is gone. Only meaningful diffed against another read in the same
/// transaction. The read after the work includes the baseline's holds whether or not they are
/// still active, so a baseline hold missing from it was deleted outright; both rules flag that,
/// since what it covered can no longer be compared.
/// </summary>
internal sealed record HeldRowVersions(IReadOnlyDictionary<Guid, string?> ByHold)
{
    /// <summary>
    /// Holds whose row changed or went (a non-NULL version that differs or became NULL), holds
    /// that appeared, and baseline holds deleted outright.
    /// </summary>
    internal static IReadOnlyList<Guid> Changed(HeldRowVersions before, HeldRowVersions after) =>
        after.ByHold
            .Where(hold => !before.ByHold.TryGetValue(hold.Key, out var version)
                || (version is not null && version != hold.Value))
            .Select(hold => hold.Key)
            .Concat(DeletedOutright(before, after))
            .Order()
            .ToArray();

    /// <summary>
    /// Holds whose row is gone now but was not before (an appeared hold included), and baseline
    /// holds deleted outright.
    /// </summary>
    internal static IReadOnlyList<Guid> Removed(HeldRowVersions before, HeldRowVersions after) =>
        after.ByHold
            .Where(hold => hold.Value is null
                && !(before.ByHold.TryGetValue(hold.Key, out var version) && version is null))
            .Select(hold => hold.Key)
            .Concat(DeletedOutright(before, after))
            .Order()
            .ToArray();

    private static IEnumerable<Guid> DeletedOutright(HeldRowVersions before, HeldRowVersions after) =>
        before.ByHold.Keys.Where(holdId => !after.ByHold.ContainsKey(holdId));
}
