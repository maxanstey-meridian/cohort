using System.Data;
using System.Data.Common;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Cohort.Infrastructure.Handlers;

public sealed class RetentionRowDispatcher(
    IServiceScopeFactory scopeFactory,
    IOptionsMonitor<CohortOptions> options,
    ILogger<RetentionRowDispatcher> logger
) : BackgroundService, IRetentionRowDispatcher
{
    public Task FlushAsync(CancellationToken ct = default)
    {
        return DrainQueueAsync(DateTimeOffset.MaxValue, ct);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await DrainQueueAsync(DateTimeOffset.UtcNow, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Cohort row handler dispatcher iteration failed.");
            }

            var pollInterval = options.CurrentValue.RowHandlerDispatch.PollInterval;
            if (pollInterval < TimeSpan.Zero)
            {
                pollInterval = TimeSpan.Zero;
            }

            await Task.Delay(pollInterval, stoppingToken);
        }
    }

    private async Task DrainQueueAsync(DateTimeOffset dueCutoff, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var claimed = await ClaimBatchAsync(dueCutoff, ct);
            if (claimed.Count == 0)
            {
                return;
            }

            var maxParallelism = Math.Max(1, options.CurrentValue.RowHandlerDispatch.MaxParallelism);
            try
            {
                await Parallel.ForEachAsync(
                    claimed,
                    new ParallelOptions
                    {
                        CancellationToken = ct,
                        MaxDegreeOfParallelism = maxParallelism,
                    },
                    ProcessClaimedRowAsync
                );
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                await RequeueCancelledClaimsAsync(claimed.Select(row => row.StatusId).ToArray());
                throw;
            }
        }
    }

    private async ValueTask ProcessClaimedRowAsync(ClaimedHandlerRow claimed, CancellationToken ct)
    {
        var currentAttempt = claimed.Attempt + 1;
        var handlerCompleted = false;

        try
        {
            await using var scope = scopeFactory.CreateAsyncScope();
            var entityType = ResolveEntityType(claimed.EntityType);
            var handlers = RetentionHandlerSupport.ResolveHandlers(scope.ServiceProvider, entityType);
            var claimedHandlerIdentity = RetentionTypeIdentity.Normalize(claimed.HandlerType);
            var handler =
                handlers.FirstOrDefault(candidate =>
                    string.Equals(
                        candidate.HandlerTypeName,
                        claimedHandlerIdentity,
                        StringComparison.Ordinal
                    )
                )
                ?? throw new InvalidOperationException(
                    $"Retention row handler '{claimed.HandlerType}' is not registered for entity {claimed.EntityType}."
                );

            var snapshot = RetentionSnapshotSerializer.Deserialize(claimed.CapturedPayload);
            var context = CreateAfterContext(entityType, claimed, currentAttempt, snapshot);
            await InvokeOnAfterAsync(entityType, handler.Instance, context, ct);
            handlerCompleted = true;
            await MarkSucceededAsync(
                claimed.StatusId,
                currentAttempt,
                DateTimeOffset.UtcNow,
                CancellationToken.None
            );
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested && !handlerCompleted)
        {
            throw;
        }
        catch (Exception ex)
        {
            await MarkFailureAsync(claimed, currentAttempt, ex, DateTimeOffset.UtcNow, ct);
        }
    }

    private async Task<IReadOnlyList<ClaimedHandlerRow>> ClaimBatchAsync(
        DateTimeOffset dueCutoff,
        CancellationToken ct
    )
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DbContext>();
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            await using var transaction = await db.Database.BeginTransactionAsync(ct);
            var claimedAt = DateTimeOffset.UtcNow;
            var claimedIds = await ClaimBatchIdsAsync(
                connection,
                transaction.GetDbTransaction(),
                claimedAt,
                dueCutoff,
                ct
            );
            if (claimedIds.Count == 0)
            {
                await transaction.CommitAsync(ct);
                return [];
            }

            var claimedRows = await LoadClaimedRowsAsync(
                connection,
                transaction.GetDbTransaction(),
                claimedIds,
                ct
            );
            if (claimedRows.Count != claimedIds.Count)
            {
                throw new InvalidOperationException(
                    "Retention row dispatcher claimed status rows that could not be reloaded."
                );
            }

            await transaction.CommitAsync(ct);
            return claimedRows;
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }
    }

    private async Task<IReadOnlyList<long>> ClaimBatchIdsAsync(
        DbConnection connection,
        DbTransaction transaction,
        DateTimeOffset claimedAt,
        DateTimeOffset dueCutoff,
        CancellationToken ct
    )
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            WITH due AS (
                SELECT status."Id"
                FROM {QuoteIdentifier(CohortTableNames.SweepRowHandlerStatus)} AS status
                INNER JOIN {QuoteIdentifier(CohortTableNames.SweepRunRowDetail)} AS detail
                    ON detail."Id" = status."SweepRunRowDetailId"
                INNER JOIN {QuoteIdentifier(CohortTableNames.SweepRun)} AS run
                    ON run."SweepId" = detail."SweepId"
                WHERE status."State" = @pending
                  AND status."NextAttemptAt" <= @dueCutoff
                  AND (
                      status."DispatchPhase" = @immediatePhase
                      OR (
                          status."DispatchPhase" = @afterSweepSettledPhase
                          AND run."CompletedAt" IS NOT NULL
                      )
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {QuoteIdentifier(CohortTableNames.SweepRowHandlerStatus)} AS blocker
                      WHERE blocker."SweepRunRowDetailId" = status."SweepRunRowDetailId"
                        AND blocker."Id" < status."Id"
                        AND blocker."State" IN (@pending, @inFlight)
                  )
                ORDER BY status."NextAttemptAt", status."Id"
                FOR UPDATE SKIP LOCKED
                LIMIT @batchSize
            )
            UPDATE {QuoteIdentifier(CohortTableNames.SweepRowHandlerStatus)} AS status
            SET "State" = @inFlight,
                "ClaimedAt" = @claimedAt
            FROM due
            WHERE status."Id" = due."Id"
            RETURNING status."Id"
            """;
        command.Parameters.Add(CreateParameter(command, "pending", (int)SweepRowHandlerDispatchState.Pending));
        command.Parameters.Add(
            CreateParameter(command, "dueCutoff", ClampDateTimeOffset(dueCutoff))
        );
        command.Parameters.Add(
            CreateParameter(command, "immediatePhase", (int)RowHandlerDispatchPhase.Immediate)
        );
        command.Parameters.Add(
            CreateParameter(
                command,
                "afterSweepSettledPhase",
                (int)RowHandlerDispatchPhase.AfterSweepSettled
            )
        );
        command.Parameters.Add(
            CreateParameter(command, "batchSize", Math.Max(1, options.CurrentValue.RowHandlerDispatch.BatchSize))
        );
        command.Parameters.Add(
            CreateParameter(command, "inFlight", (int)SweepRowHandlerDispatchState.InFlight)
        );
        command.Parameters.Add(CreateParameter(command, "claimedAt", claimedAt));

        var claimedIds = new List<long>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            claimedIds.Add(reader.GetInt64(0));
        }

        return claimedIds;
    }

    private static async Task<IReadOnlyList<ClaimedHandlerRow>> LoadClaimedRowsAsync(
        DbConnection connection,
        DbTransaction transaction,
        IReadOnlyList<long> claimedIds,
        CancellationToken ct
    )
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            SELECT
                status."Id",
                detail."Id",
                status."HandlerType",
                status."Attempt",
                detail."SweepId",
                detail."At",
                detail."EntityType",
                detail."EntityId",
                detail."Category",
                detail."Strategy",
                detail."TenantId",
                detail."CapturedPayload"
            FROM {QuoteIdentifier(CohortTableNames.SweepRowHandlerStatus)} AS status
            INNER JOIN {QuoteIdentifier(CohortTableNames.SweepRunRowDetail)} AS detail
                ON detail."Id" = status."SweepRunRowDetailId"
            WHERE status."Id" = ANY(@claimedIds)
            ORDER BY status."Id"
            """;
        command.Parameters.Add(CreateParameter(command, "claimedIds", claimedIds.ToArray()));

        var rows = new List<ClaimedHandlerRow>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(
                new ClaimedHandlerRow(
                    reader.GetInt64(0),
                    reader.GetInt64(1),
                    reader.GetString(2),
                    reader.GetInt32(3),
                    reader.GetGuid(4),
                    reader.GetFieldValue<DateTimeOffset>(5),
                    reader.GetString(6),
                    reader.GetString(7),
                    reader.GetString(8),
                    (Strategy)reader.GetInt32(9),
                    reader.GetGuid(10),
                    reader.IsDBNull(11) ? null : reader.GetString(11)
                )
            );
        }

        return rows;
    }

    private async Task MarkSucceededAsync(
        long statusId,
        int attempt,
        DateTimeOffset completedAt,
        CancellationToken ct
    )
    {
        await ExecuteStatusUpdateAsync(
            statusId,
            """
            "State" = @state,
            "Attempt" = @attempt,
            "CompletedAt" = @completedAt,
            "LastError" = NULL
            """,
            parameters =>
            {
                parameters.Add(("state", (int)SweepRowHandlerDispatchState.Succeeded));
                parameters.Add(("attempt", attempt));
                parameters.Add(("completedAt", completedAt));
            },
            ct
        );
    }

    private async Task MarkFailureAsync(
        ClaimedHandlerRow claimed,
        int attempt,
        Exception ex,
        DateTimeOffset now,
        CancellationToken ct
    )
    {
        var optionsSnapshot = options.CurrentValue.RowHandlerDispatch;
        var maxAttempts = Math.Max(1, optionsSnapshot.MaxAttempts);
        var lastError = ex.ToString();

        if (attempt >= maxAttempts)
        {
            await MarkDeadLetteredAsync(claimed, attempt, lastError, now, ct);
            return;
        }

        await ExecuteStatusUpdateAsync(
            claimed.StatusId,
            """
            "State" = @state,
            "Attempt" = @attempt,
            "NextAttemptAt" = @nextAttemptAt,
            "ClaimedAt" = NULL,
            "CompletedAt" = NULL,
            "LastError" = @lastError
            """,
            parameters =>
            {
                parameters.Add(("state", (int)SweepRowHandlerDispatchState.Pending));
                parameters.Add(("attempt", attempt));
                parameters.Add(("nextAttemptAt", now + CalculateBackoff(optionsSnapshot.BaseBackoff, attempt)));
                parameters.Add(("lastError", lastError));
            },
            ct
        );
    }

    private async Task MarkDeadLetteredAsync(
        ClaimedHandlerRow claimed,
        int attempt,
        string lastError,
        DateTimeOffset now,
        CancellationToken ct
    )
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DbContext>();
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            await using var transaction = await db.Database.BeginTransactionAsync(ct);

            await using (var currentCommand = connection.CreateCommand())
            {
                currentCommand.Transaction = transaction.GetDbTransaction();
                currentCommand.CommandText =
                    $"""
                    UPDATE {QuoteIdentifier(CohortTableNames.SweepRowHandlerStatus)}
                    SET "State" = @state,
                        "Attempt" = @attempt,
                        "CompletedAt" = @completedAt,
                        "LastError" = @lastError
                    WHERE "Id" = @statusId
                      AND "State" = @expectedState
                    """;
                currentCommand.Parameters.Add(CreateParameter(currentCommand, "state", (int)SweepRowHandlerDispatchState.DeadLettered));
                currentCommand.Parameters.Add(CreateParameter(currentCommand, "attempt", attempt));
                currentCommand.Parameters.Add(CreateParameter(currentCommand, "completedAt", now));
                currentCommand.Parameters.Add(CreateParameter(currentCommand, "lastError", lastError));
                currentCommand.Parameters.Add(CreateParameter(currentCommand, "statusId", claimed.StatusId));
                currentCommand.Parameters.Add(
                    CreateParameter(currentCommand, "expectedState", (int)SweepRowHandlerDispatchState.InFlight)
                );

                var affected = await currentCommand.ExecuteNonQueryAsync(ct);
                if (affected != 1)
                {
                    throw new InvalidOperationException(
                        $"Retention row dispatcher could not update status row {claimed.StatusId} from InFlight state."
                    );
                }
            }

            await using (var dependentCommand = connection.CreateCommand())
            {
                dependentCommand.Transaction = transaction.GetDbTransaction();
                dependentCommand.CommandText =
                    $"""
                    UPDATE {QuoteIdentifier(CohortTableNames.SweepRowHandlerStatus)}
                    SET "State" = @state,
                        "ClaimedAt" = NULL,
                        "CompletedAt" = @completedAt,
                        "LastError" = @lastError
                    WHERE "SweepRunRowDetailId" = @rowDetailId
                      AND "Id" > @statusId
                      AND "State" IN (@pending, @inFlight)
                    """;
                dependentCommand.Parameters.Add(
                    CreateParameter(dependentCommand, "state", (int)SweepRowHandlerDispatchState.DeadLettered)
                );
                dependentCommand.Parameters.Add(CreateParameter(dependentCommand, "completedAt", now));
                dependentCommand.Parameters.Add(
                    CreateParameter(
                        dependentCommand,
                        "lastError",
                        $"Skipped because an earlier handler for the same row dead-lettered: {claimed.HandlerType}"
                    )
                );
                dependentCommand.Parameters.Add(CreateParameter(dependentCommand, "rowDetailId", claimed.RowDetailId));
                dependentCommand.Parameters.Add(CreateParameter(dependentCommand, "statusId", claimed.StatusId));
                dependentCommand.Parameters.Add(
                    CreateParameter(dependentCommand, "pending", (int)SweepRowHandlerDispatchState.Pending)
                );
                dependentCommand.Parameters.Add(
                    CreateParameter(dependentCommand, "inFlight", (int)SweepRowHandlerDispatchState.InFlight)
                );

                await dependentCommand.ExecuteNonQueryAsync(ct);
            }

            await transaction.CommitAsync(ct);
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }
    }

    private async Task RequeueCancelledClaimsAsync(IReadOnlyList<long> statusIds)
    {
        if (statusIds.Count == 0)
        {
            return;
        }

        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DbContext>();
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(CancellationToken.None);
        }

        try
        {
            await using var transaction = await db.Database.BeginTransactionAsync(CancellationToken.None);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction.GetDbTransaction();
            command.CommandText =
                $"""
                UPDATE {QuoteIdentifier(CohortTableNames.SweepRowHandlerStatus)}
                SET "State" = @pending,
                    "ClaimedAt" = NULL
                WHERE "Id" = ANY(@statusIds)
                  AND "State" = @inFlight
                """;
            command.Parameters.Add(CreateParameter(command, "pending", (int)SweepRowHandlerDispatchState.Pending));
            command.Parameters.Add(
                CreateParameter(command, "statusIds", statusIds.ToArray())
            );
            command.Parameters.Add(
                CreateParameter(command, "inFlight", (int)SweepRowHandlerDispatchState.InFlight)
            );

            await command.ExecuteNonQueryAsync(CancellationToken.None);
            await transaction.CommitAsync(CancellationToken.None);
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }
    }

    private async Task ExecuteStatusUpdateAsync(
        long statusId,
        string setClause,
        Action<List<(string Name, object? Value)>> configureParameters,
        CancellationToken ct
    )
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DbContext>();
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            await using var transaction = await db.Database.BeginTransactionAsync(ct);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction.GetDbTransaction();
            command.CommandText =
                $"""
                UPDATE {QuoteIdentifier(CohortTableNames.SweepRowHandlerStatus)}
                SET {setClause}
                WHERE "Id" = @statusId
                  AND "State" = @expectedState
                """;
            command.Parameters.Add(CreateParameter(command, "statusId", statusId));
            command.Parameters.Add(
                CreateParameter(command, "expectedState", (int)SweepRowHandlerDispatchState.InFlight)
            );

            var parameters = new List<(string Name, object? Value)>();
            configureParameters(parameters);
            foreach (var (name, value) in parameters)
            {
                command.Parameters.Add(CreateParameter(command, name, value));
            }

            var affected = await command.ExecuteNonQueryAsync(ct);
            if (affected != 1)
            {
                throw new InvalidOperationException(
                    $"Retention row dispatcher could not update status row {statusId} from InFlight state."
                );
            }

            await transaction.CommitAsync(ct);
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }
    }

    private static object CreateAfterContext(
        Type entityType,
        ClaimedHandlerRow claimed,
        int attempt,
        IReadOnlyDictionary<string, object?> snapshot
    )
    {
        var contextType = typeof(RetentionAfterContext<>).MakeGenericType(entityType);
        return Activator.CreateInstance(
            contextType,
            claimed.SweepId,
            claimed.EntityId,
            claimed.Category,
            claimed.Strategy,
            claimed.TenantId,
            claimed.At,
            attempt,
            snapshot
        ) ?? throw new InvalidOperationException(
            $"Could not construct RetentionAfterContext for entity type {entityType.FullName}."
        );
    }

    private static async Task InvokeOnAfterAsync(
        Type entityType,
        object handler,
        object context,
        CancellationToken ct
    )
    {
        var handlerInterface = typeof(IRetentionHandler<>).MakeGenericType(entityType);
        var onAfterMethod =
            handlerInterface.GetMethod(nameof(IRetentionHandler<object>.OnAfterAsync))
            ?? throw new InvalidOperationException(
                $"Could not resolve {nameof(IRetentionHandler<object>.OnAfterAsync)} for {handlerInterface.FullName}."
            );

        var invocation = onAfterMethod.Invoke(handler, [context, ct]);
        await (Task)invocation!;
    }

    private static TimeSpan CalculateBackoff(TimeSpan baseBackoff, int attempt)
    {
        if (baseBackoff < TimeSpan.Zero)
        {
            baseBackoff = TimeSpan.Zero;
        }

        if (attempt <= 0 || baseBackoff == TimeSpan.Zero)
        {
            return baseBackoff;
        }

        var multiplier = Math.Pow(2, attempt - 1);
        var backoffTicks = baseBackoff.Ticks * multiplier;
        if (backoffTicks >= TimeSpan.MaxValue.Ticks)
        {
            return TimeSpan.MaxValue;
        }

        return TimeSpan.FromTicks((long)backoffTicks);
    }

    private static Type ResolveEntityType(string entityType)
    {
        var resolved =
            Type.GetType(entityType, throwOnError: false, ignoreCase: false)
            ?? AppDomain.CurrentDomain
                .GetAssemblies()
                .Select(assembly => assembly.GetType(entityType, throwOnError: false, ignoreCase: false))
                .FirstOrDefault(type => type is not null);

        return resolved
            ?? throw new InvalidOperationException(
                $"Could not resolve retention handler entity type '{entityType}'."
            );
    }

    private static DateTimeOffset ClampDateTimeOffset(DateTimeOffset value)
    {
        return value > DateTimeOffset.MaxValue.AddYears(-1)
            ? DateTimeOffset.MaxValue.AddYears(-1)
            : value;
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

    private sealed record ClaimedHandlerRow(
        long StatusId,
        long RowDetailId,
        string HandlerType,
        int Attempt,
        Guid SweepId,
        DateTimeOffset At,
        string EntityType,
        string EntityId,
        string Category,
        Strategy Strategy,
        Guid TenantId,
        string? CapturedPayload
    );
}
