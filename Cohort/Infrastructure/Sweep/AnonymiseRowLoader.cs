using System.Data.Common;

using Cohort.Domain;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Infrastructure.Sweep;

internal sealed class AnonymiseRowLoader(
    DbContext db,
    AnonymiseAssignmentResolver assignmentResolver
)
{
    private readonly DbContext modelDb = db ?? throw new ArgumentNullException(nameof(db));
    private readonly AnonymiseAssignmentResolver assignmentResolver =
        assignmentResolver ?? throw new ArgumentNullException(nameof(assignmentResolver));

    internal async Task<IReadOnlyList<string>> SelectCandidateRecordIdsAsync(
        RetentionEntry entry,
        Guid tenantId,
        DbConnection conn,
        DbTransaction transaction,
        SqlFilter filter,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = AnonymiseSqlBuilder.BuildCandidateSelectionCommandText(entry, filter);
        AnonymiseDbParameterFactory.AddFilterParameters(command, filter);
        AnonymiseDbParameterFactory.AddTenantParameter(command, entry.Tenant?.TenantColumn, tenantId);

        var candidateRecordIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            candidateRecordIds.Add(reader.GetValue(0).ToString()!);
        }

        return candidateRecordIds;
    }

    internal async Task<IReadOnlyList<AnonymiseRowSnapshot>> LoadUpdatableRowsAsync(
        RetentionEntry entry,
        TenantContext tenant,
        DateTimeOffset now,
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
        AnonymiseDbParameterFactory.AddTenantParameter(command, entry.Tenant?.TenantColumn, tenant.Id);
        AnonymiseDbParameterFactory.AddHoldParameters(command, entry.TableName, now);

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
        DateTimeOffset now,
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
            AnonymiseDbParameterFactory.CreateProviderParameter(conn, "holdTableName", entry.TableName),
            AnonymiseDbParameterFactory.CreateProviderParameter(conn, "holdAsOf", now),
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
            .AsNoTracking()
            .ToListAsync(ct);
    }
}

internal sealed record AnonymiseRowSnapshot(
    string RecordId,
    IReadOnlyDictionary<string, object?> OriginalValues
);
