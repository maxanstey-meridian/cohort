using System.Collections;
using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Handlers;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Sweep;

internal static class RetentionHandlerSupport
{
    /// <summary>
    /// Whether any handler is registered for <paramref name="entityType"/>, read from the
    /// registrations alone so no handler is constructed to find out.
    /// </summary>
    public static bool HasHandlers(IEnumerable<IRetentionHandlerRegistration> registrations, Type entityType) =>
        registrations.Any(registration => registration.EntityType == entityType);

    /// <summary>
    /// The handlers registered with <c>AddRowHandler</c> for <paramref name="entityType"/>,
    /// in priority order. A handler registered only as a service, with no registration, is
    /// not a row handler: <see cref="HasHandlers"/> never sees it.
    /// </summary>
    public static IReadOnlyList<ResolvedRetentionHandler> ResolveHandlers(
        IServiceProvider services,
        Type entityType
    )
    {
        var registrations = services
            .GetServices<IRetentionHandlerRegistration>()
            .Where(registration => registration.EntityType == entityType)
            .ToDictionary(registration => registration.HandlerType);
        if (registrations.Count == 0)
        {
            return [];
        }

        var handlerInterface = typeof(IRetentionHandler<>).MakeGenericType(entityType);
        var registeredHandlers = (IEnumerable)services.GetRequiredService(
            typeof(IEnumerable<>).MakeGenericType(handlerInterface)
        );
        return registeredHandlers
            .Cast<object>()
            .Where(handler => registrations.ContainsKey(handler.GetType()))
            .Select(handler =>
            {
                var registration = registrations[handler.GetType()];
                return new ResolvedRetentionHandler(
                    handler,
                    handlerInterface,
                    registration.DispatchPhase,
                    registration.Identity
                );
            })
            .OrderBy(handler => RowHandlerPriority.Get(handler.HandlerType))
            .ThenBy(handler => handler.HandlerType.FullName, StringComparer.Ordinal)
            .ToArray();
    }

    /// <summary>
    /// Runs every handler's OnBefore and captures the snapshot after each one, so a value
    /// that cannot round-trip to OnAfter fails the handler that left it, before the row
    /// is mutated. A handler that leaves unsaved changes on <paramref name="db"/>, the
    /// handlers' own context, fails too: Cohort discards them, so the handler's work would
    /// silently not happen.
    /// </summary>
    public static async Task<OnBeforeInvocationResult> InvokeOnBeforeAsync(
        DbContext db,
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
                if (HasUnsavedChanges(db))
                {
                    return OnBeforeInvocationResult.LeftUnsavedChanges(handler);
                }

                capturedPayload = RetentionSnapshotSerializer.Capture(
                    ctx.Snapshot,
                    entityType,
                    handlerAssemblies
                );
            }
            // Only the run's own cancellation stops the run. A handler's own (an HttpClient
            // timeout is a TaskCanceledException) is that handler failing its row.
            catch (System.Reflection.TargetInvocationException ex)
                when (ex.InnerException is OperationCanceledException cancellation && ct.IsCancellationRequested)
            {
                throw cancellation;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
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
    /// Detects explicitly, so a context with automatic detection turned off is still
    /// checked. Detection that throws (a handler changed a tracked entity's key) is the
    /// handler's unsaved change too.
    /// </summary>
    public static bool HasUnsavedChanges(DbContext db)
    {
        try
        {
            db.ChangeTracker.DetectChanges();
            return db.ChangeTracker.HasChanges();
        }
        catch (Exception)
        {
            return true;
        }
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
    /// status dead-lettered. No snapshot is kept: nothing will run after it. An exception is
    /// recorded as the sanitized diagnostic envelope; a failure Cohort found itself as its
    /// plain reason.
    /// </summary>
    public static async Task PersistBeforeFailureAsync(
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext execution,
        RetentionEntry entry,
        Strategy strategy,
        Guid tenantId,
        string recordId,
        OnBeforeInvocationResult failed,
        ILogger logger,
        CancellationToken ct
    )
    {
        var failedHandler = failed.FailedHandler
            ?? throw new ArgumentException("Only a failed OnBefore leaves a row unmutated.", nameof(failed));
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

        string lastError;
        if (failed.Failure is { } failure)
        {
            var diagnostic = RetentionFailureDiagnostic.Create(failure);
            lastError = diagnostic.ToString();
            logger.LogError(
                failure,
                "Cohort row handler failed before mutation for sweep {SweepId} and entity {EntityType}. Diagnostic {DiagnosticId}.",
                execution.SweepId,
                entry.EntityType.FullName,
                diagnostic.DiagnosticIdText
            );
        }
        else
        {
            lastError = $"reason={failed.FailureReason}";
            logger.LogError(
                "Cohort row handler {HandlerType} failed before mutation for sweep {SweepId} and entity {EntityType} ({Reason}).",
                failedHandler.HandlerTypeName,
                execution.SweepId,
                entry.EntityType.FullName,
                failed.FailureReason
            );
        }

        await InsertHandlerStatusAsync(
            conn,
            transaction,
            entry.CohortTables,
            rowDetailId,
            failedHandler,
            execution.At,
            lastError,
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

/// <summary>
/// One batch's OnBefore handlers and the context they were resolved with: a scope of their
/// own whose context is on the run's connection and transaction, never the run's context.
/// </summary>
internal sealed record RetentionHandlerScope(DbContext Db, IReadOnlyList<ResolvedRetentionHandler> Handlers);

internal sealed record OnBeforeInvocationResult(
    ResolvedRetentionHandler? FailedHandler,
    Exception? Failure,
    string? CapturedPayload = null,
    string? FailureReason = null
)
{
    /// <summary>
    /// The plain reason recorded for a handler that returned with unsaved changes on its
    /// DbContext. Cohort-defined, so it carries no handler or row data.
    /// </summary>
    public const string UnsavedChangesReason = "on-before-unsaved-changes";

    public static OnBeforeInvocationResult Captured(string capturedPayload) =>
        new(null, null, capturedPayload);

    public static OnBeforeInvocationResult LeftUnsavedChanges(ResolvedRetentionHandler handler) =>
        new(handler, null, FailureReason: UnsavedChangesReason);

    public bool Succeeded => FailedHandler is null;
}
