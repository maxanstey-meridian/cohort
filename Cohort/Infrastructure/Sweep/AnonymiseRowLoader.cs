using System.Data.Common;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure.Sweep;

internal sealed class AnonymiseRowLoader(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    AnonymiseAssignmentResolver assignmentResolver
)
{
    private readonly DbContext modelDb = db ?? throw new ArgumentNullException(nameof(db));
    private readonly AnonymiseAssignmentResolver assignmentResolver =
        assignmentResolver ?? throw new ArgumentNullException(nameof(assignmentResolver));
    internal Microsoft.EntityFrameworkCore.Metadata.IModel Model => modelDb.Model;

    internal async Task<IReadOnlyList<string>> SelectCandidateRecordIdsAsync(
        RetentionEntry entry,
        Guid tenantId,
        DbConnection conn,
        DbTransaction transaction,
        SqlFilter filter,
        int? batchSize,
        IReadOnlyList<string>? excludedRecordIds,
        bool skipLocked,
        CancellationToken ct
    )
    {
        var lockedRecordIds = new List<string>();
        var attemptedRecordIds = excludedRecordIds?.ToList() ?? [];
        var targetCount = batchSize is null ? int.MaxValue : Math.Max(1, batchSize.Value);

        while (lockedRecordIds.Count < targetCount)
        {
            var remaining = batchSize is null ? (int?)null : targetCount - lockedRecordIds.Count;
            var candidateRecordIds = await DiscoverCandidateRecordIdsAsync(
                entry,
                tenantId,
                conn,
                transaction,
                filter,
                remaining,
                attemptedRecordIds,
                ct
            );
            if (candidateRecordIds.Count == 0)
            {
                break;
            }

            await RetentionEntityLockSql.AcquireAsync(
                conn,
                transaction,
                entry.EntityId,
                entry.Tenant is not null ? tenantId : null,
                candidateRecordIds,
                ct
            );
            lockedRecordIds.AddRange(
                await LockCandidateRecordIdsAsync(
                    entry,
                    tenantId,
                    conn,
                    transaction,
                    filter,
                    candidateRecordIds,
                    skipLocked,
                    ct
                )
            );
            attemptedRecordIds.AddRange(candidateRecordIds);
            if (batchSize is null || candidateRecordIds.Count < remaining)
            {
                break;
            }
        }

        return lockedRecordIds;
    }

    private static async Task<List<string>> DiscoverCandidateRecordIdsAsync(
        RetentionEntry entry,
        Guid tenantId,
        DbConnection conn,
        DbTransaction transaction,
        SqlFilter filter,
        int? batchSize,
        IReadOnlyList<string> excludedRecordIds,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = AnonymiseSqlBuilder.BuildCandidateSelectionCommandText(
            entry,
            filter,
            batchSize,
            hasExcludedRecordIds: excludedRecordIds.Count > 0
        );
        AnonymiseDbParameterFactory.AddFilterParameters(command, filter);
        AnonymiseDbParameterFactory.AddTenantParameter(
            command,
            entry.Tenant?.TenantColumn,
            tenantId
        );
        AnonymiseDbParameterFactory.AddHoldParameters(command, entry.EntityId);
        if (excludedRecordIds.Count > 0)
        {
            command.Parameters.Add(
                AnonymiseDbParameterFactory.Create(
                    command,
                    "excludedRecordIds",
                    excludedRecordIds.ToArray()
                )
            );
        }
        if (batchSize is not null)
        {
            command.Parameters.Add(
                AnonymiseDbParameterFactory.Create(command, "batchSize", batchSize.Value)
            );
        }

        return await ReadRecordIdsAsync(command, ct);
    }

    private static async Task<List<string>> LockCandidateRecordIdsAsync(
        RetentionEntry entry,
        Guid tenantId,
        DbConnection conn,
        DbTransaction transaction,
        SqlFilter filter,
        IReadOnlyList<string> candidateRecordIds,
        bool skipLocked,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = AnonymiseSqlBuilder.BuildCandidateLockCommandText(
            entry,
            filter,
            skipLocked
        );
        AnonymiseDbParameterFactory.AddFilterParameters(command, filter);
        AnonymiseDbParameterFactory.AddTenantParameter(
            command,
            entry.Tenant?.TenantColumn,
            tenantId
        );
        AnonymiseDbParameterFactory.AddCandidateIdsParameter(command, candidateRecordIds);
        AnonymiseDbParameterFactory.AddHoldParameters(command, entry.EntityId);
        return await ReadRecordIdsAsync(command, ct);
    }

    private static async Task<List<string>> ReadRecordIdsAsync(
        DbCommand command,
        CancellationToken ct
    )
    {
        var recordIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            recordIds.Add(reader.GetValue(0).ToString()!);
        }

        return recordIds;
    }

    internal async Task<IReadOnlyList<AnonymiseRowSnapshot>> LoadUpdatableRowsAsync(
        RetentionEntry entry,
        TenantContext tenant,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        CancellationToken ct
    )
    {
        var originalValueFields = assignmentResolver.GetOriginalValueFields(entry);

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = AnonymiseSqlBuilder.BuildLoadUpdatableRowsCommandText(
            entry,
            originalValueFields
        );
        AnonymiseDbParameterFactory.AddCandidateIdsParameter(command, candidateRecordIds);
        AnonymiseDbParameterFactory.AddTenantParameter(
            command,
            entry.Tenant?.TenantColumn,
            tenant.Id
        );
        AnonymiseDbParameterFactory.AddHoldParameters(command, entry.EntityId);

        var rows = new List<AnonymiseRowSnapshot>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var originalValues = new Dictionary<string, object?>(StringComparer.Ordinal);
            for (var index = 0; index < originalValueFields.Count; index++)
            {
                var providerValue = reader.IsDBNull(index + 1) ? null : reader.GetValue(index + 1);
                originalValues[originalValueFields[index].MemberName] =
                    assignmentResolver.ConvertOriginalValueFromProvider(
                        entry,
                        originalValueFields[index],
                        providerValue
                    );
            }

            rows.Add(new AnonymiseRowSnapshot(reader.GetString(0), originalValues));
        }

        return rows;
    }

    internal Task<List<TEntity>> LoadHandlerRowsAsync<TEntity>(
        RetentionEntry entry,
        TenantContext tenant,
        DbConnection conn,
        IReadOnlyList<string> candidateRecordIds,
        CancellationToken ct
    )
        where TEntity : class
    {
        var sql = AnonymiseSqlBuilder.BuildLoadHandlerRowsCommandText(entry);
        var parameters = new List<object>
        {
            AnonymiseDbParameterFactory.CreateProviderParameter(
                conn,
                "candidateIds",
                candidateRecordIds.ToArray()
            ),
            AnonymiseDbParameterFactory.CreateProviderParameter(
                conn,
                "retentionEntityId",
                entry.EntityId
            ),
        };
        if (entry.Tenant is not null)
        {
            parameters.Add(
                AnonymiseDbParameterFactory.CreateProviderParameter(conn, "tenantId", tenant.Id)
            );
        }

        return modelDb
            .Set<TEntity>()
            .FromSqlRaw(sql, parameters.ToArray())
            .IgnoreQueryFilters()
            .AsNoTracking()
            .ToListAsync(ct);
    }
}

internal sealed record AnonymiseRowSnapshot(
    string RecordId,
    IReadOnlyDictionary<string, object?> OriginalValues
);
