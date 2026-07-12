using System.Data.Common;
using System.Reflection;
using Cohort.Domain;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Sweep;

internal sealed class AnonymiseHandlerAwareMutationExecutor(
    AnonymiseAssignmentResolver assignmentResolver,
    AnonymiseRowLoader rowLoader,
    AnonymiseMutationExecutor mutationExecutor,
    ILogger? logger = null
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
        return (Task<SweepExecutionResult>)
            ExecuteCoreMethod
                .MakeGenericMethod(entry.EntityType)
                .Invoke(
                    this,
                    [
                        entry,
                        rule,
                        ctx,
                        conn,
                        transaction,
                        candidateRecordIds,
                        filter,
                        handlers,
                        execution,
                        ct,
                    ]
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
            conn,
            candidateRecordIds,
            filter,
            ct
        );
        var recordIdProperty =
            ReflectionMemberResolver.FindPropertyByName(
                typeof(TEntity),
                entry.RecordId.RecordIdMember
            )
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} references missing record-id member '{entry.RecordId.RecordIdMember}'."
            );
        var canonicalRows = new List<(TEntity Row, string RecordId)>(rows.Count);
        var recordIdConverter = rowLoader
            .Model.FindEntityType(typeof(TEntity))
            ?.FindProperty(entry.RecordId.RecordIdMember)
            ?.GetTypeMapping()
            .Converter;
        foreach (var row in rows)
        {
            var recordIdValue = recordIdProperty.GetValue(row);
            if (recordIdValue is null)
            {
                throw new InvalidOperationException(
                    $"Retention row for {entry.EntityType.FullName} produced an empty record id for member '{entry.RecordId.RecordIdMember}'."
                );
            }
            canonicalRows.Add(
                (
                    row,
                    await RecordIdSql.CanonicalizeAsync(
                        conn,
                        transaction,
                        entry.RecordId,
                        recordIdConverter?.ConvertToProvider(recordIdValue) ?? recordIdValue,
                        ct
                    )
                )
            );
        }
        var candidateOrder = candidateRecordIds
            .Select((recordId, index) => new KeyValuePair<string, int>(recordId, index))
            .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
        var orderedRows = canonicalRows.OrderBy(item =>
            {
                return candidateOrder.TryGetValue(item.RecordId, out var index)
                    ? index
                    : int.MaxValue;
            })
            .ToArray();
        var staticAssignments = assignmentResolver.CreateStaticAssignments(
            entry,
            ctx.Tenant.Id,
            ctx.Now
        );
        var affectedRecordIds = new List<string>();
        var heldCount = candidateRecordIds.Count - rows.Count;
        var skippedRecordIds = new List<string>();

        foreach (var (row, recordId) in orderedRows)
        {
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
                skippedRecordIds.Add(recordId);
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
                    logger,
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
                )
                is null
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
            SkippedCount: skippedRecordIds.Count,
            CandidateCount: candidateRecordIds.Count,
            SkippedRecordIds: skippedRecordIds
        );
    }
}
