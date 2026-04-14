using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;

using Cohort.Application;
using Cohort.Domain;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Infrastructure.Sweep;

public sealed class AnonymiseSweepStrategy : IRetentionSweepStrategy
{
    private static readonly MethodInfo ExecuteHandlerAwareSweepCoreMethod =
        typeof(AnonymiseSweepStrategy).GetMethod(
            nameof(ExecuteHandlerAwareSweepCoreAsync),
            BindingFlags.Instance | BindingFlags.NonPublic
        )!;
    private readonly AnonymiseAssignmentResolver assignmentResolver;
    private readonly AnonymiseMutationExecutor mutationExecutor;
    private readonly AnonymiseRowLoader rowLoader;
    private readonly IServiceProvider? services;

    public AnonymiseSweepStrategy(
        DbContext db,
        IEnumerable<IAnonymiseValueFactory>? anonymiseValueFactories = null,
        IServiceProvider? services = null
    )
    {
        ArgumentNullException.ThrowIfNull(db);

        assignmentResolver = new AnonymiseAssignmentResolver(db, anonymiseValueFactories);
        rowLoader = new AnonymiseRowLoader(db, assignmentResolver);
        mutationExecutor = new AnonymiseMutationExecutor(assignmentResolver, rowLoader);
        this.services = services;
    }

    public Strategy HandlesStrategy => Strategy.Anonymise;

    public async Task<int> PreviewAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(conn);

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);
        return await PreviewMutationCountAsync(
            entry,
            rule,
            AnonymiseFilterBuilder.CreateCutoffFilter(entry.AnchorColumn, cutoff),
            ctx.Tenant,
            ctx.Now,
            conn,
            ct,
            "preview"
        );
    }

    public async Task<SweepExecutionResult> SweepAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        CancellationToken ct,
        SweepMutationContext? execution = null
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(conn);
        ArgumentNullException.ThrowIfNull(transaction);

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);
        return await ExecuteMutationAsync(
            entry,
            rule,
            ctx,
            AnonymiseFilterBuilder.CreateCutoffFilter(entry.AnchorColumn, cutoff),
            conn,
            transaction,
            execution,
            ct,
            "sweeps"
        );
    }

    public async Task<int> PreviewEraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(conn);

        var cutoff = CutoffCalculator.Compute(now, rule.Period, rule.LegalMin);
        return await PreviewMutationCountAsync(
            entry,
            rule,
            AnonymiseFilterBuilder.Combine(
                AnonymiseFilterBuilder.CreateSubjectFilter(predicate),
                AnonymiseFilterBuilder.CreateCutoffFilter(entry.AnchorColumn, cutoff)
            ),
            tenant,
            now,
            conn,
            ct,
            "erasure previews"
        );
    }

    public async Task<SweepExecutionResult> EraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        CancellationToken ct,
        SweepMutationContext? execution = null
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(conn);
        ArgumentNullException.ThrowIfNull(transaction);

        var cutoff = CutoffCalculator.Compute(now, rule.Period, rule.LegalMin);
        return await ExecuteMutationAsync(
            entry,
            rule,
            new RetentionResolutionContext(entry.Category, tenant, now, []),
            AnonymiseFilterBuilder.Combine(
                AnonymiseFilterBuilder.CreateSubjectFilter(predicate),
                AnonymiseFilterBuilder.CreateCutoffFilter(entry.AnchorColumn, cutoff)
            ),
            conn,
            transaction,
            execution,
            ct,
            "erasure"
        );
    }

    private async Task<int> PreviewMutationCountAsync(
        RetentionEntry entry,
        RetentionRule rule,
        SqlFilter filter,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        CancellationToken ct,
        string operation
    )
    {
        ValidateEntry(entry, rule, operation);
        await EnsureConnectionOpenAsync(conn, ct);

        await using var command = conn.CreateCommand();
        command.CommandText = AnonymiseSqlBuilder.BuildPreviewCountCommandText(entry, filter);
        AnonymiseDbParameterFactory.AddFilterParameters(command, filter);
        AnonymiseDbParameterFactory.AddTenantParameter(command, entry.Tenant?.TenantColumn, tenant.Id);
        AnonymiseDbParameterFactory.AddHoldParameters(command, entry.TableName, now);

        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private async Task<SweepExecutionResult> ExecuteMutationAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        SqlFilter filter,
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext? execution,
        CancellationToken ct,
        string operation
    )
    {
        ValidateEntry(entry, rule, operation);
        await EnsureConnectionOpenAsync(conn, ct);

        var candidateRecordIds = await rowLoader.SelectCandidateRecordIdsAsync(
            entry,
            ctx.Tenant.Id,
            conn,
            transaction,
            filter,
            ct
        );

        if (candidateRecordIds.Count == 0)
        {
            return new SweepExecutionResult([], 0);
        }

        var handlers = RetentionHandlerSupport.ResolveHandlers(services, entry.EntityType);
        if (execution is not null && handlers.Count > 0)
        {
            return await ExecuteHandlerAwareSweepAsync(
                entry,
                rule,
                ctx,
                conn,
                transaction,
                candidateRecordIds,
                filter,
                handlers,
                execution,
                ct
            );
        }

        return assignmentResolver.RequiresPerRowExecution(entry)
            ? await mutationExecutor.ExecutePerRowMutationAsync(
                entry,
                ctx.Tenant,
                ctx.Now,
                conn,
                transaction,
                candidateRecordIds,
                filter,
                ct
            )
            : await mutationExecutor.ExecuteSetBasedMutationAsync(
                entry,
                ctx.Tenant,
                ctx.Now,
                conn,
                transaction,
                candidateRecordIds,
                filter,
                ct
            );
    }

    private Task<SweepExecutionResult> ExecuteHandlerAwareSweepAsync(
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
        return (Task<SweepExecutionResult>)ExecuteHandlerAwareSweepCoreMethod
            .MakeGenericMethod(entry.EntityType)
            .Invoke(
                this,
                [entry, rule, ctx, conn, transaction, candidateRecordIds, filter, handlers, execution, ct]
            )!;
    }

    private async Task<SweepExecutionResult> ExecuteHandlerAwareSweepCoreAsync<TEntity>(
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
        var staticAssignments = assignmentResolver.CreateStaticAssignments(entry, ctx.Tenant.Id, ctx.Now);
        var affectedRecordIds = new List<string>();
        var heldCount = candidateRecordIds.Count - rows.Count;
        var skippedCount = 0;

        foreach (var row in rows)
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

    private static void ValidateEntry(
        RetentionEntry entry,
        RetentionRule rule,
        string operation
    )
    {
        if (rule.Strategy != Strategy.Anonymise)
        {
            throw new InvalidOperationException(
                $"AnonymiseSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        if (entry.AnonymiseFields.Count == 0)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose anonymise metadata for anonymise {operation}."
            );
        }
    }

    private static async Task EnsureConnectionOpenAsync(DbConnection conn, CancellationToken ct)
    {
        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }
    }
}

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
