using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Infrastructure.Sweep;

internal sealed class AnonymiseMutationExecutor(
    AnonymiseAssignmentResolver assignmentResolver,
    AnonymiseRowLoader rowLoader
)
{
    private readonly AnonymiseAssignmentResolver assignmentResolver =
        assignmentResolver ?? throw new ArgumentNullException(nameof(assignmentResolver));
    private readonly AnonymiseRowLoader rowLoader =
        rowLoader ?? throw new ArgumentNullException(nameof(rowLoader));

    internal async Task<SweepExecutionResult> ExecutePerRowMutationAsync(
        RetentionEntry entry,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        SqlFilter filter,
        CancellationToken ct
    )
    {
        var updatableRows = await rowLoader.LoadUpdatableRowsAsync(
            entry,
            tenant,
            now,
            conn,
            transaction,
            candidateRecordIds,
            ct
        );
        if (updatableRows.Count == 0)
        {
            return new SweepExecutionResult([], candidateRecordIds.Count);
        }

        var staticAssignments = assignmentResolver.CreateStaticAssignments(entry, tenant.Id, now);
        var affectedRecordIds = new List<string>(updatableRows.Count);

        foreach (var row in updatableRows)
        {
            var affectedRecordId = await TryUpdateRowAsync(
                entry,
                tenant,
                now,
                conn,
                transaction,
                row.RecordId,
                row.OriginalValues,
                staticAssignments,
                filter,
                ct
            );
            if (affectedRecordId is not null)
            {
                affectedRecordIds.Add(affectedRecordId);
            }
        }

        return new SweepExecutionResult(
            affectedRecordIds,
            candidateRecordIds.Count - affectedRecordIds.Count
        );
    }

    internal async Task<SweepExecutionResult> ExecuteSetBasedMutationAsync(
        RetentionEntry entry,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        SqlFilter filter,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = AnonymiseSqlBuilder.BuildSetBasedCommandText(entry, filter);
        AddAssignmentParameters(
            command,
            assignmentResolver.CreateSetBasedAssignmentValues(entry, tenant.Id, now)
        );
        AnonymiseDbParameterFactory.AddFilterParameters(command, filter);
        AnonymiseDbParameterFactory.AddTenantParameter(command, entry.Tenant?.TenantColumn, tenant.Id);
        AnonymiseDbParameterFactory.AddCandidateIdsParameter(command, candidateRecordIds);
        AnonymiseDbParameterFactory.AddHoldParameters(command, entry.TableName, now);

        var affectedRecordIds = await ReadAffectedRecordIdsAsync(command, ct);
        return new SweepExecutionResult(
            affectedRecordIds,
            candidateRecordIds.Count - affectedRecordIds.Count
        );
    }

    internal async Task<string?> TryUpdateRowAsync(
        RetentionEntry entry,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        string recordId,
        IReadOnlyDictionary<string, object?> originalValues,
        IReadOnlyDictionary<string, object?> staticAssignments,
        SqlFilter filter,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = AnonymiseSqlBuilder.BuildPerRowCommandText(entry, filter);
        AddAssignmentParameters(
            command,
            assignmentResolver.CreatePerRowAssignmentValues(
                entry,
                tenant,
                now,
                originalValues,
                staticAssignments
            )
        );
        command.Parameters.Add(AnonymiseDbParameterFactory.Create(command, "recordId", recordId));
        AnonymiseDbParameterFactory.AddFilterParameters(command, filter);
        AnonymiseDbParameterFactory.AddTenantParameter(command, entry.Tenant?.TenantColumn, tenant.Id);
        AnonymiseDbParameterFactory.AddHoldParameters(command, entry.TableName, now);

        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? reader.GetValue(0).ToString() : null;
    }

    private static async Task<List<string>> ReadAffectedRecordIdsAsync(
        DbCommand command,
        CancellationToken ct
    )
    {
        var affectedRecordIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            affectedRecordIds.Add(reader.GetValue(0).ToString()!);
        }

        return affectedRecordIds;
    }

    private static void AddAssignmentParameters(
        DbCommand command,
        IReadOnlyList<object?> assignmentValues
    )
    {
        for (var index = 0; index < assignmentValues.Count; index++)
        {
            command.Parameters.Add(
                AnonymiseDbParameterFactory.Create(command, $"value{index}", assignmentValues[index])
            );
        }
    }
}
