using System.Data.Common;
using System.Reflection;

using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Infrastructure.Sweep;

internal sealed class AnonymiseHandlerAwareMutationExecutor(
    AnonymiseAssignmentResolver assignmentResolver,
    AnonymiseRowLoader rowLoader,
    AnonymiseMutationExecutor mutationExecutor
)
{
    private static readonly MethodInfo ExecuteCoreMethod =
        typeof(AnonymiseHandlerAwareMutationExecutor).GetMethod(
            nameof(ExecuteCoreAsync),
            BindingFlags.Instance | BindingFlags.NonPublic
        )!;
    private readonly AnonymiseAssignmentResolver assignmentResolver =
        assignmentResolver ?? throw new ArgumentNullException(nameof(assignmentResolver));
    private readonly AnonymiseRowLoader rowLoader =
        rowLoader ?? throw new ArgumentNullException(nameof(rowLoader));
    private readonly AnonymiseMutationExecutor mutationExecutor =
        mutationExecutor ?? throw new ArgumentNullException(nameof(mutationExecutor));

    internal Task<SweepExecutionResult> ExecuteAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        SqlFilter filter,
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        SweepMutationContext execution,
        CancellationToken ct
    )
    {
        return (Task<SweepExecutionResult>)ExecuteCoreMethod
            .MakeGenericMethod(entry.EntityType)
            .Invoke(
                this,
                [entry, rule, ctx, conn, transaction, candidateRecordIds, filter, handlers, execution, ct]
            )!;
    }

    private async Task<SweepExecutionResult> ExecuteCoreAsync<TEntity>(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        SqlFilter filter,
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        SweepMutationContext execution,
        CancellationToken ct
    )
        where TEntity : class
    {
        var rows = await rowLoader.LoadHandlerRowsAsync<TEntity>(
            entry,
            ctx.Tenant,
            ctx.Now,
            conn,
            candidateRecordIds,
            ct
        );
        var recordIdProperty =
            typeof(TEntity).GetProperty(entry.RecordId.RecordIdMember)
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} references missing record-id member '{entry.RecordId.RecordIdMember}'."
            );
        var candidateOrder = candidateRecordIds
            .Select((recordId, index) => new KeyValuePair<string, int>(recordId, index))
            .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
        var orderedRows = rows
            .OrderBy(
                row =>
                {
                    var recordId = recordIdProperty.GetValue(row)?.ToString();
                    return recordId is not null && candidateOrder.TryGetValue(recordId, out var index)
                        ? index
                        : int.MaxValue;
                }
            )
            .ToArray();
        var staticAssignments = assignmentResolver.CreateStaticAssignments(entry, ctx.Tenant.Id, ctx.Now);
        var affectedRecordIds = new List<string>();
        var heldCount = candidateRecordIds.Count - rows.Count;
        var skippedCount = 0;

        foreach (var row in orderedRows)
        {
            var recordId = recordIdProperty.GetValue(row)?.ToString();
            if (string.IsNullOrWhiteSpace(recordId))
            {
                throw new InvalidOperationException(
                    $"Retention row for {entry.EntityType.FullName} produced an empty record id for member '{entry.RecordId.RecordIdMember}'."
                );
            }

            var beforeContext = new RetentionBeforeContext(
                execution.SweepId,
                entry.Category,
                rule.Strategy,
                ctx.Tenant.Id,
                execution.At
            );

            var beforeResult = await RetentionHandlerSupport.InvokeOnBeforeAsync(
                handlers,
                row,
                beforeContext,
                ct
            );
            if (!beforeResult.Succeeded)
            {
                skippedCount++;
                await RetentionHandlerSupport.PersistBeforeFailureAsync(
                    conn,
                    transaction,
                    execution,
                    entry,
                    rule.Strategy,
                    ctx.Tenant.Id,
                    recordId,
                    new Dictionary<string, object?>(beforeContext.Snapshot, StringComparer.Ordinal),
                    beforeResult.FailedHandler!,
                    beforeResult.Failure!,
                    ct
                );
                continue;
            }

            var originalValues = assignmentResolver.CreateOriginalValuesFromEntity(entry, row);
            if (
                await mutationExecutor.TryUpdateRowAsync(
                    entry,
                    ctx.Tenant,
                    ctx.Now,
                    conn,
                    transaction,
                    recordId,
                    originalValues,
                    staticAssignments,
                    filter,
                    ct
                ) is null
            )
            {
                heldCount++;
                continue;
            }

            await RetentionHandlerSupport.PersistCapturedRowAsync(
                conn,
                transaction,
                execution,
                entry,
                rule.Strategy,
                ctx.Tenant.Id,
                recordId,
                new Dictionary<string, object?>(beforeContext.Snapshot, StringComparer.Ordinal),
                handlers,
                ct
            );
            affectedRecordIds.Add(recordId);
        }

        return new SweepExecutionResult(
            affectedRecordIds,
            heldCount,
            RowDetailsPersisted: true,
            SkippedCount: skippedCount
        );
    }
}
