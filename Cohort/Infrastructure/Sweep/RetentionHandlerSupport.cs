using System.Collections;
using System.Data.Common;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Sweep;

internal static class RetentionHandlerSupport
{
    public static IReadOnlyList<ResolvedRetentionHandler> ResolveHandlers(
        IServiceProvider services,
        Type entityType
    )
    {
        var handlerInterface = typeof(IRetentionHandler<>).MakeGenericType(entityType);
        var enumerableType = typeof(IEnumerable<>).MakeGenericType(handlerInterface);
        var registeredHandlers = services.GetService(enumerableType) as IEnumerable;
        if (registeredHandlers is null)
        {
            return [];
        }

        var metadata = services
            .GetService<IEnumerable<IRetentionHandlerRegistration>>()
            ?.Where(registration => registration.EntityType == entityType)
            .ToDictionary(registration => registration.HandlerType, registration => registration);

        return registeredHandlers
            .Cast<object>()
            .Select(handler =>
            {
                var registration =
                    metadata is not null && metadata.TryGetValue(handler.GetType(), out var matched)
                        ? matched
                        : null;
                return new ResolvedRetentionHandler(
                    handler,
                    handlerInterface,
                    registration?.DispatchPhase ?? RowHandlerDispatchPhase.Immediate,
                    registration?.Identity
                );
            })
            .OrderBy(handler => RowHandlerPriority.Get(handler.HandlerType))
            .ThenBy(handler => handler.HandlerType.FullName, StringComparer.Ordinal)
            .ToArray();
    }

    /// <summary>
    /// Runs every handler's OnBefore and captures the snapshot after each one, so a value
    /// that cannot round-trip to OnAfter fails the handler that left it, before the row
    /// is mutated.
    /// </summary>
    public static async Task<OnBeforeInvocationResult> InvokeOnBeforeAsync(
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        Type entityType,
        object row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        var handlerAssemblies = handlers.Select(handler => handler.HandlerType.Assembly).ToArray();
        string? capturedPayload = null;
        foreach (var handler in handlers)
        {
            try
            {
                var invocation = handler.OnBeforeMethod.Invoke(handler.Instance, [row, ctx, ct]);
                await (Task)invocation!;
                capturedPayload = RetentionSnapshotSerializer.Capture(
                    ctx.Snapshot,
                    entityType,
                    handlerAssemblies
                );
            }
            catch (System.Reflection.TargetInvocationException ex)
                when (ex.InnerException is OperationCanceledException cancellation)
            {
                throw cancellation;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (System.Reflection.TargetInvocationException ex)
                when (ex.InnerException is not null)
            {
                return new OnBeforeInvocationResult(handler, ex.InnerException);
            }
            catch (Exception ex)
            {
                return new OnBeforeInvocationResult(handler, ex);
            }
        }

        return OnBeforeInvocationResult.Captured(
            capturedPayload
                ?? RetentionSnapshotSerializer.Capture(ctx.Snapshot, entityType, handlerAssemblies)
        );
    }

    /// <summary>
    /// Records a mutated row with its captured snapshot payload and queues one pending status per
    /// handler, in the mutation's transaction.
    /// </summary>
    public static async Task PersistCapturedRowAsync(
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext execution,
        RetentionEntry entry,
        Strategy strategy,
        Guid tenantId,
        string recordId,
        string capturedPayload,
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        CancellationToken ct
    )
    {
        var rowDetailId = await InsertRowDetailAsync(
            conn,
            transaction,
            execution,
            entry,
            strategy,
            tenantId,
            recordId,
            capturedPayload,
            ct
        );
        foreach (var handler in handlers)
        {
            await InsertHandlerStatusAsync(conn, transaction, entry.CohortTables, rowDetailId, handler, execution.At, failure: null, ct);
        }
    }

    /// <summary>
    /// Records a row left unmutated because an OnBefore handler failed, with that handler's
    /// status dead-lettered. No snapshot is kept: nothing will run after it.
    /// </summary>
    public static async Task PersistBeforeFailureAsync(
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext execution,
        RetentionEntry entry,
        Strategy strategy,
        Guid tenantId,
        string recordId,
        ResolvedRetentionHandler failedHandler,
        Exception failure,
        ILogger logger,
        CancellationToken ct
    )
    {
        var rowDetailId = await InsertRowDetailAsync(
            conn,
            transaction,
            execution,
            entry,
            strategy,
            tenantId,
            recordId,
            capturedPayload: null,
            ct
        );

        var diagnostic = RetentionFailureDiagnostic.Create(failure);
        logger.LogError(
            failure,
            "Cohort row handler failed before mutation for sweep {SweepId} and entity {EntityType}. Diagnostic {DiagnosticId}.",
            execution.SweepId,
            entry.EntityType.FullName,
            diagnostic.DiagnosticIdText
        );
        await InsertHandlerStatusAsync(
            conn,
            transaction,
            entry.CohortTables,
            rowDetailId,
            failedHandler,
            execution.At,
            diagnostic.ToString(),
            ct
        );
    }

    private static async Task<long> InsertRowDetailAsync(
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext execution,
        RetentionEntry entry,
        Strategy strategy,
        Guid tenantId,
        string recordId,
        string? capturedPayload,
        CancellationToken ct
    )
    {
        await using var command = new SqlParams
        {
            ["sweepId"] = execution.SweepId,
            ["at"] = execution.At,
            ["entityType"] = entry.EntityType.FullName ?? entry.EntityType.Name,
            ["retentionEntityId"] = entry.RetentionEntityId,
            ["recordId"] = recordId,
            ["category"] = entry.Category,
            ["strategy"] = (int)strategy,
            ["tenantId"] = tenantId,
            ["capturedPayload"] = capturedPayload,
        }.CreateCommand(
            conn,
            transaction,
            $"""
            INSERT INTO {PostgreSqlIdentifier.Format(entry.CohortTables.SweepRunRowDetail)}
                ("SweepId", "At", "EntityType", "RetentionEntityId", "RecordId", "Category", "Strategy", "TenantId", "CapturedPayload")
            VALUES (@sweepId, @at, @entityType, @retentionEntityId, @recordId, @category, @strategy, @tenantId, @capturedPayload)
            RETURNING "Id"
            """
        );
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>
    /// Queues a pending status, or with <paramref name="failure"/> records one already
    /// dead-lettered on its first attempt.
    /// </summary>
    private static async Task InsertHandlerStatusAsync(
        DbConnection conn,
        DbTransaction transaction,
        CohortStoreTables tables,
        long rowDetailId,
        ResolvedRetentionHandler handler,
        DateTimeOffset at,
        string? failure,
        CancellationToken ct
    )
    {
        await using var command = new SqlParams
        {
            ["rowDetailId"] = rowDetailId,
            ["handlerType"] = handler.HandlerIdentity,
            ["dispatchPhase"] = (int)handler.DispatchPhase,
            ["state"] = (int)(failure is null ? SweepRowHandlerDispatchState.Pending : SweepRowHandlerDispatchState.DeadLettered),
            ["attempt"] = failure is null ? 0 : 1,
            ["at"] = at,
            ["completedAt"] = failure is null ? null : at,
            ["lastError"] = failure,
        }.CreateCommand(
            conn,
            transaction,
            $"""
            INSERT INTO {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)}
                ("SweepRunRowDetailId", "HandlerType", "DispatchPhase", "State", "Attempt", "QueuedAt", "NextAttemptAt", "ClaimedAt", "CompletedAt", "LastError")
            VALUES (@rowDetailId, @handlerType, @dispatchPhase, @state, @attempt, @at, @at, NULL, @completedAt, @lastError)
            """
        );
        await command.ExecuteNonQueryAsync(ct);
    }
}

internal sealed class ResolvedRetentionHandler(
    object instance,
    Type handlerInterface,
    RowHandlerDispatchPhase dispatchPhase,
    Guid? identity = null
)
{
    public object Instance { get; } = instance;

    public Type HandlerType { get; } = instance.GetType();

    public string HandlerTypeName { get; } =
        RetentionTypeIdentity.GetPersistedName(instance.GetType());

    /// <summary>
    /// What queued work is persisted and matched under: the registration's explicit
    /// UUID when given, otherwise the CLR type name. Explicit identities survive class
    /// renames; type-name identities do not.
    /// </summary>
    public string HandlerIdentity { get; } =
        identity?.ToString("D") ?? RetentionTypeIdentity.GetPersistedName(instance.GetType());

    public RowHandlerDispatchPhase DispatchPhase { get; } = dispatchPhase;

    public System.Reflection.MethodInfo OnBeforeMethod { get; } =
        handlerInterface.GetMethod(nameof(IRetentionHandler<object>.OnBeforeAsync))
        ?? throw new InvalidOperationException(
            $"Could not resolve {nameof(IRetentionHandler<object>.OnBeforeAsync)} for handler interface {handlerInterface.FullName}."
        );
}

internal sealed record OnBeforeInvocationResult(
    ResolvedRetentionHandler? FailedHandler,
    Exception? Failure,
    string? CapturedPayload = null
)
{
    public static OnBeforeInvocationResult Captured(string capturedPayload) =>
        new(null, null, capturedPayload);

    public bool Succeeded => FailedHandler is null;
}
