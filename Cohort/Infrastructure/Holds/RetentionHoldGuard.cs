using System.Data.Common;

using Cohort.Application;
using Cohort.Infrastructure.Sweep;

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
    /// Every active hold the scope can see, with the physical version of each row it covers, the
    /// row it names and the rows its covered paths reach (NULL when the row it names is gone).
    /// Take it before the work, in the transaction the work runs in.
    /// </summary>
    internal Task<HeldRowVersions> ReadHeldRowVersionsAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken ct
    ) => ReadVersionsAsync(connection, transaction, null, ct);

    /// <summary>
    /// The read after the work: the active holds, plus the baseline's holds whether or not they
    /// are still active, so a hold that ended during the work is judged by its rows. A baseline
    /// key the covered paths no longer reach (a link was cut, or the row went) is then re-read by
    /// record id, so cutting a link and then changing the row still shows as a change; normally
    /// no key is missing and the second read doesn't run.
    /// </summary>
    private async Task<HeldRowVersions> ReadVersionsAsync(
        DbConnection connection,
        DbTransaction transaction,
        HeldRowVersions? baseline,
        CancellationToken ct
    )
    {
        var entries = registry.Scan().Values.ToList();
        var holdsTable = CohortStoreTables.FromModel(db.Model).RetentionHolds;
        var versions = new Dictionary<HeldRowKey, string?>();
        var entityByHold = new Dictionary<Guid, Guid>();

        // No retained entity means no held row: nothing to read, and nothing to re-read.
        if (RetentionHoldSql.BuildHeldRowVersions(holdsTable, entries, baseline?.HoldIds.ToArray() ?? []) is not { } read)
        {
            return new HeldRowVersions(versions, entityByHold);
        }

        await ReadIntoAsync(connection, transaction, read, versions, entityByHold, ct);

        if (baseline is not null)
        {
            var missing = baseline.ByRow.Keys.Where(key => !versions.ContainsKey(key)).ToArray();
            if (missing.Length > 0)
            {
                var rereads = missing
                    .GroupBy(key => key.RetentionEntityId)
                    .Select(group => new HeldRowReread(
                        group.Key,
                        group.Select(key => key.HoldId).ToArray(),
                        group.Select(key => key.RecordId).ToArray()
                    ))
                    .ToArray();
                if (RetentionHoldSql.BuildHeldRowRereads(holdsTable, entries, rereads) is { } reread)
                {
                    await ReadIntoAsync(connection, transaction, reread, versions, entityByHold, ct);
                }
            }
        }

        return new HeldRowVersions(versions, entityByHold);
    }

    private static async Task ReadIntoAsync(
        DbConnection connection,
        DbTransaction transaction,
        SweepStatement statement,
        Dictionary<HeldRowKey, string?> versions,
        Dictionary<Guid, Guid> entityByHold,
        CancellationToken ct
    )
    {
        await using var command = statement.Parameters.CreateCommand(connection, transaction, statement.Sql);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var holdId = reader.GetGuid(0);
            entityByHold[holdId] = reader.GetGuid(1);
            // Add, not set: after DISTINCT, two versions for one key would mean its record id
            // matched two rows, and keeping either version could hide a change to the other.
            versions.Add(
                new HeldRowKey(holdId, reader.GetGuid(2), reader.GetString(3)),
                reader.IsDBNull(4) ? null : reader.GetString(4)
            );
        }
    }

    /// <summary>
    /// The dependents step's rule: covered rows the work changed or removed (a non-NULL version
    /// that differs or became NULL), whether or not the hold is still active, so ending a hold
    /// first doesn't hide a change to its rows; plus holds and covered rows that appeared during
    /// the work, which it may have changed after the hold committed or a row was linked under
    /// it. Hold ids, sorted; empty when nothing was touched.
    /// </summary>
    internal async Task<IReadOnlyList<Guid>> ReadChangedHeldRowsAsync(
        HeldRowVersions baseline,
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken ct
    ) => HeldRowVersions.Changed(baseline, await ReadVersionsAsync(connection, transaction, baseline, ct));

    /// <summary>
    /// <see cref="IRetentionDeletion"/>'s rule: covered rows gone now that were not gone before
    /// the work, whether or not the hold is still active, so ending a hold first doesn't hide
    /// the deletion of its rows. Rows already orphaned before it are not its doing. A hold that
    /// appeared during the work on an entity that covers other rows is flagged whatever its rows
    /// look like now: hold creation locks the rows it covers, so a deletion of one of them waits
    /// for the hold to commit and then runs, and the read can no longer reach the deleted row from
    /// the new hold. Hold ids, sorted.
    /// </summary>
    internal async Task<IReadOnlyList<Guid>> ReadRemovedHeldRowsAsync(
        HeldRowVersions baseline,
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken ct
    ) => HeldRowVersions.Removed(
        baseline,
        await ReadVersionsAsync(connection, transaction, baseline, ct),
        registry.Scan().Values.Where(entry => entry.CoveredPaths.Count > 0).Select(entry => entry.RetentionEntityId).ToHashSet()
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
            RetentionHoldSql.BuildActiveTenantlessHoldIds(CohortStoreTables.FromModel(db.Model).RetentionHolds)
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

/// <summary>One covered row of one hold: the hold, the covered row's entity, and its canonical record id.</summary>
internal readonly record struct HeldRowKey(Guid HoldId, Guid RetentionEntityId, string RecordId);

/// <summary>
/// The physical version (<c>tableoid/ctid/xmin</c>) of each row each hold covers, or NULL when
/// the row is gone, and the entity each hold is on. Only meaningful diffed against another read
/// in the same transaction. The read after the work includes the baseline's holds whether or not
/// they are still active, so a baseline hold missing from it was deleted outright; both rules
/// flag that, since what it covered can no longer be compared.
/// </summary>
internal sealed record HeldRowVersions(
    IReadOnlyDictionary<HeldRowKey, string?> ByRow,
    IReadOnlyDictionary<Guid, Guid> EntityByHold
)
{
    /// <summary>Every hold the read saw.</summary>
    internal IEnumerable<Guid> HoldIds => EntityByHold.Keys;

    /// <summary>
    /// Holds with a covered row that changed or went (a non-NULL version that differs or became
    /// NULL), holds and covered rows that appeared, and baseline holds deleted outright.
    /// </summary>
    internal static IReadOnlyList<Guid> Changed(HeldRowVersions before, HeldRowVersions after) =>
        after.ByRow
            .Where(row => !before.ByRow.TryGetValue(row.Key, out var version)
                || (version is not null && version != row.Value))
            .Select(row => row.Key.HoldId)
            .Concat(DeletedOutright(before, after))
            .Distinct()
            .Order()
            .ToArray();

    /// <summary>
    /// Holds with a covered row that is gone now but was not before (an appeared hold's gone rows
    /// included), baseline holds deleted outright, and holds that appeared on an entity in
    /// <paramref name="coveringEntities"/>, whatever their rows look like now.
    /// </summary>
    internal static IReadOnlyList<Guid> Removed(
        HeldRowVersions before,
        HeldRowVersions after,
        IReadOnlySet<Guid> coveringEntities
    ) =>
        after.ByRow
            .Where(row => row.Value is null
                && !(before.ByRow.TryGetValue(row.Key, out var version) && version is null))
            .Select(row => row.Key.HoldId)
            .Concat(after.EntityByHold
                .Where(hold => !before.EntityByHold.ContainsKey(hold.Key) && coveringEntities.Contains(hold.Value))
                .Select(hold => hold.Key))
            .Concat(DeletedOutright(before, after))
            .Distinct()
            .Order()
            .ToArray();

    private static IEnumerable<Guid> DeletedOutright(HeldRowVersions before, HeldRowVersions after) =>
        before.HoldIds.Where(holdId => !after.EntityByHold.ContainsKey(holdId));
}
