using System.Collections;
using System.Data.Common;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Handlers;
using Cohort.Infrastructure.Migrations;

namespace Cohort.Infrastructure.Sweep;

internal static class RetentionHandlerSupport
{
    public static IReadOnlyList<ResolvedRetentionHandler> ResolveHandlers(
        IServiceProvider? services,
        Type entityType
    )
    {
        ArgumentNullException.ThrowIfNull(entityType);

        if (services is null)
        {
            return [];
        }

        var handlerInterface = typeof(IRetentionHandler<>).MakeGenericType(entityType);
        var enumerableType = typeof(IEnumerable<>).MakeGenericType(handlerInterface);
        var registeredHandlers = services.GetService(enumerableType) as IEnumerable;
        if (registeredHandlers is null)
        {
            return [];
        }

        return registeredHandlers
            .Cast<object>()
            .Select(handler => new ResolvedRetentionHandler(handler, handlerInterface))
            .OrderBy(handler => RowHandlerPriorityAttribute.GetPriority(handler.HandlerType))
            .ThenBy(handler => handler.HandlerType.FullName, StringComparer.Ordinal)
            .ToArray();
    }

    public static async Task<OnBeforeInvocationResult> InvokeOnBeforeAsync(
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        object row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(handlers);
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(ctx);

        foreach (var handler in handlers)
        {
            try
            {
                var invocation = handler.OnBeforeMethod.Invoke(handler.Instance, [row, ctx, ct]);
                await (Task)invocation!;
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
            catch (System.Reflection.TargetInvocationException ex) when (ex.InnerException is not null)
            {
                return new OnBeforeInvocationResult(handler, ex.InnerException);
            }
            catch (Exception ex)
            {
                return new OnBeforeInvocationResult(handler, ex);
            }
        }

        return OnBeforeInvocationResult.Success;
    }

    public static async Task PersistCapturedRowAsync(
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext execution,
        RetentionEntry entry,
        Strategy strategy,
        Guid tenantId,
        string entityId,
        IReadOnlyDictionary<string, object?> snapshot,
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(conn);
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(execution);
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentException.ThrowIfNullOrWhiteSpace(entityId);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(handlers);

        var rowDetailId = await InsertRowDetailAsync(
            conn,
            transaction,
            execution,
            entry,
            strategy,
            tenantId,
            entityId,
            snapshot,
            ct
        );

        foreach (var handler in handlers)
        {
            await InsertPendingHandlerStatusAsync(
                conn,
                transaction,
                rowDetailId,
                handler,
                execution.At,
                ct
            );
        }
    }

    public static async Task PersistBeforeFailureAsync(
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext execution,
        RetentionEntry entry,
        Strategy strategy,
        Guid tenantId,
        string entityId,
        IReadOnlyDictionary<string, object?> snapshot,
        ResolvedRetentionHandler failedHandler,
        Exception failure,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(conn);
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(execution);
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentException.ThrowIfNullOrWhiteSpace(entityId);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(failedHandler);
        ArgumentNullException.ThrowIfNull(failure);

        var rowDetailId = await InsertRowDetailAsync(
            conn,
            transaction,
            execution,
            entry,
            strategy,
            tenantId,
            entityId,
            snapshot,
            ct
        );

        await InsertDeadLetteredHandlerStatusAsync(
            conn,
            transaction,
            rowDetailId,
            failedHandler,
            execution.At,
            failure,
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
        string entityId,
        IReadOnlyDictionary<string, object?> snapshot,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            INSERT INTO {QuoteIdentifier(CohortTableNames.SweepRunRowDetail)} (
                "SweepId",
                "At",
                "EntityType",
                "EntityId",
                "Category",
                "Strategy",
                "TenantId",
                "CapturedPayload"
            )
            VALUES (
                @sweepId,
                @at,
                @entityType,
                @entityId,
                @category,
                @strategy,
                @tenantId,
                @capturedPayload
            )
            RETURNING "Id"
            """;
        command.Parameters.Add(CreateParameter(command, "sweepId", execution.SweepId));
        command.Parameters.Add(CreateParameter(command, "at", execution.At));
        command.Parameters.Add(
            CreateParameter(command, "entityType", entry.EntityType.FullName ?? entry.EntityType.Name)
        );
        command.Parameters.Add(CreateParameter(command, "entityId", entityId));
        command.Parameters.Add(CreateParameter(command, "category", entry.Category));
        command.Parameters.Add(CreateParameter(command, "strategy", (int)strategy));
        command.Parameters.Add(CreateParameter(command, "tenantId", tenantId));
        command.Parameters.Add(
            CreateParameter(command, "capturedPayload", RetentionSnapshotSerializer.Serialize(snapshot))
        );

        var insertedId = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt64(insertedId, System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task InsertPendingHandlerStatusAsync(
        DbConnection conn,
        DbTransaction transaction,
        long rowDetailId,
        ResolvedRetentionHandler handler,
        DateTimeOffset queuedAt,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            INSERT INTO {QuoteIdentifier(CohortTableNames.SweepRowHandlerStatus)} (
                "SweepRunRowDetailId",
                "HandlerType",
                "State",
                "Attempt",
                "QueuedAt",
                "NextAttemptAt",
                "ClaimedAt",
                "CompletedAt",
                "LastError"
            )
            VALUES (
                @rowDetailId,
                @handlerType,
                @state,
                @attempt,
                @queuedAt,
                @nextAttemptAt,
                NULL,
                NULL,
                NULL
            )
            """;
        command.Parameters.Add(CreateParameter(command, "rowDetailId", rowDetailId));
        command.Parameters.Add(CreateParameter(command, "handlerType", handler.HandlerTypeName));
        command.Parameters.Add(
            CreateParameter(command, "state", (int)SweepRowHandlerDispatchState.Pending)
        );
        command.Parameters.Add(CreateParameter(command, "attempt", 0));
        command.Parameters.Add(CreateParameter(command, "queuedAt", queuedAt));
        command.Parameters.Add(CreateParameter(command, "nextAttemptAt", queuedAt));

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertDeadLetteredHandlerStatusAsync(
        DbConnection conn,
        DbTransaction transaction,
        long rowDetailId,
        ResolvedRetentionHandler handler,
        DateTimeOffset failedAt,
        Exception failure,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            INSERT INTO {QuoteIdentifier(CohortTableNames.SweepRowHandlerStatus)} (
                "SweepRunRowDetailId",
                "HandlerType",
                "State",
                "Attempt",
                "QueuedAt",
                "NextAttemptAt",
                "ClaimedAt",
                "CompletedAt",
                "LastError"
            )
            VALUES (
                @rowDetailId,
                @handlerType,
                @state,
                @attempt,
                @queuedAt,
                @nextAttemptAt,
                NULL,
                @completedAt,
                @lastError
            )
            """;
        command.Parameters.Add(CreateParameter(command, "rowDetailId", rowDetailId));
        command.Parameters.Add(CreateParameter(command, "handlerType", handler.HandlerTypeName));
        command.Parameters.Add(
            CreateParameter(command, "state", (int)SweepRowHandlerDispatchState.DeadLettered)
        );
        command.Parameters.Add(CreateParameter(command, "attempt", 1));
        command.Parameters.Add(CreateParameter(command, "queuedAt", failedAt));
        command.Parameters.Add(CreateParameter(command, "nextAttemptAt", failedAt));
        command.Parameters.Add(CreateParameter(command, "completedAt", failedAt));
        command.Parameters.Add(CreateParameter(command, "lastError", failure.ToString()));

        await command.ExecuteNonQueryAsync(ct);
    }

    private static DbParameter CreateParameter(DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        return parameter;
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }
}

internal sealed class ResolvedRetentionHandler(object instance, Type handlerInterface)
{
    public object Instance { get; } = instance;

    public Type HandlerType { get; } = instance.GetType();

    public string HandlerTypeName { get; } =
        instance.GetType().AssemblyQualifiedName
        ?? instance.GetType().FullName
        ?? instance.GetType().Name;

    public System.Reflection.MethodInfo OnBeforeMethod { get; } =
        handlerInterface.GetMethod(nameof(IRetentionHandler<object>.OnBeforeAsync))
        ?? throw new InvalidOperationException(
            $"Could not resolve {nameof(IRetentionHandler<object>.OnBeforeAsync)} for handler interface {handlerInterface.FullName}."
        );
}

internal sealed record OnBeforeInvocationResult(
    ResolvedRetentionHandler? FailedHandler,
    Exception? Failure
)
{
    public static OnBeforeInvocationResult Success { get; } = new(null, null);

    public bool Succeeded => FailedHandler is null;
}
