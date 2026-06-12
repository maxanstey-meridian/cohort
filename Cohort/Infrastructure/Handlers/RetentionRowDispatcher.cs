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
    public async Task<RowDispatcherFlushResult> FlushAsync(CancellationToken ct = default)
    {
        await ScrubExpiredPayloadsAsync(ct);
        await DrainQueueAsync(DateTimeOffset.MaxValue, ct);
        return await CountRemainingWorkAsync(ct);
    }

    /// <summary>
    /// Counts non-terminal handler rows left after a drain. Any pending row remaining
    /// here was not dispatchable by the flush (deferred phase with an unsettled sweep,
    /// or queued behind an in-flight sibling); any in-flight row is held by another
    /// claimer under a live lease.
    /// </summary>
    private Task<RowDispatcherFlushResult> CountRemainingWorkAsync(CancellationToken ct)
    {
        return WithScopedConnectionAsync(
            async (_, connection) =>
            {
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    SELECT
                        COUNT(*) FILTER (WHERE "State" = @inFlight),
                        COUNT(*) FILTER (WHERE "State" = @pending)
                    FROM {QuoteIdentifier(CohortTableNames.SweepRowHandlerStatus)}
                    """;
                command.Parameters.Add(
                    CreateParameter(command, "inFlight", (int)SweepRowHandlerDispatchState.InFlight)
                );
                command.Parameters.Add(
                    CreateParameter(command, "pending", (int)SweepRowHandlerDispatchState.Pending)
                );

                await using var reader = await command.ExecuteReaderAsync(ct);
                await reader.ReadAsync(ct);

                return new RowDispatcherFlushResult(
                    reader.GetInt64(0),
                    reader.GetInt64(1)
                );
            },
            ct
        );
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var now = DateTimeOffset.UtcNow;
                if (now - lastPayloadScrubAt >= PayloadScrubInterval)
                {
                    lastPayloadScrubAt = now;
                    await ScrubExpiredPayloadsAsync(stoppingToken);
                }

                await DrainQueueAsync(now, stoppingToken);
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

    private static readonly TimeSpan PayloadScrubInterval = TimeSpan.FromHours(1);
    private DateTimeOffset lastPayloadScrubAt = DateTimeOffset.MinValue;

    /// <summary>
    /// Clears a row detail's captured snapshot once every handler for that row has
    /// reached a terminal state. Snapshots exist solely to feed OnAfterAsync; keeping
    /// them longer retains exactly the personal data the sweep was meant to remove.
    /// </summary>
    private Task ClearSettledPayloadAsync(long rowDetailId)
    {
        return WithScopedConnectionAsync(
            async (_, connection) =>
            {
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    UPDATE {QuoteIdentifier(CohortTableNames.SweepRunRowDetail)} AS detail
                    SET "CapturedPayload" = NULL
                    WHERE detail."Id" = @rowDetailId
                      AND detail."CapturedPayload" IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {QuoteIdentifier(CohortTableNames.SweepRowHandlerStatus)} AS status
                          WHERE status."SweepRunRowDetailId" = detail."Id"
                            AND status."State" IN (@pending, @inFlight)
                      )
                    """;
                command.Parameters.Add(CreateParameter(command, "rowDetailId", rowDetailId));
                command.Parameters.Add(
                    CreateParameter(command, "pending", (int)SweepRowHandlerDispatchState.Pending)
                );
                command.Parameters.Add(
                    CreateParameter(command, "inFlight", (int)SweepRowHandlerDispatchState.InFlight)
                );

                await command.ExecuteNonQueryAsync(CancellationToken.None);
            },
            CancellationToken.None
        );
    }

    private Task ScrubExpiredPayloadsAsync(CancellationToken ct)
    {
        var retention = options.CurrentValue.RowHandlerDispatch.PayloadRetention;
        if (retention < TimeSpan.FromHours(1))
        {
            retention = TimeSpan.FromHours(1);
        }

        return WithScopedConnectionAsync(
            async (_, connection) =>
            {
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    UPDATE {QuoteIdentifier(CohortTableNames.SweepRunRowDetail)}
                    SET "CapturedPayload" = NULL
                    WHERE "CapturedPayload" IS NOT NULL
                      AND "At" < @payloadCutoff
                    """;
                command.Parameters.Add(
                    CreateParameter(command, "payloadCutoff", DateTimeOffset.UtcNow - retention)
                );

                await command.ExecuteNonQueryAsync(ct);
            },
            ct
        );
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
            if (string.IsNullOrWhiteSpace(claimed.CapturedPayload))
            {
                // The payload-retention backstop scrubs captured snapshots that outlive
                // RowHandlerDispatch:PayloadRetention even while handler work is still
                // queued — the personal data must not outlive its retention. Without the
                // snapshot the handler can never succeed, so dead-letter immediately
                // with the real reason instead of burning the retry budget on a
                // misleading deserialisation error.
                await MarkDeadLetteredAsync(
                    claimed,
                    currentAttempt,
                    "Captured row snapshot was scrubbed by the payload-retention backstop (RowHandlerDispatch:PayloadRetention) before this handler ran; the work can no longer complete. Increase PayloadRetention or drain handler work sooner.",
                    DateTimeOffset.UtcNow,
                    ct
                );
                return;
            }

            await using var scope = scopeFactory.CreateAsyncScope();
            var registry = scope.ServiceProvider.GetRequiredService<RetentionRegistry>();
            var entityType = ResolveEntityType(claimed.EntityType, registry);
            var handlers = RetentionHandlerSupport.ResolveHandlers(scope.ServiceProvider, entityType);
            var claimedHandlerIdentity = RetentionTypeIdentity.Normalize(claimed.HandlerType);
            // Identity first; type-name fallback so rows queued before a handler gained
            // an explicit identity still resolve.
            var handler =
                handlers.FirstOrDefault(candidate =>
                    string.Equals(
                        candidate.HandlerIdentity,
                        claimedHandlerIdentity,
                        StringComparison.Ordinal
                    )
                )
                ?? handlers.FirstOrDefault(candidate =>
                    string.Equals(
                        candidate.HandlerTypeName,
                        claimedHandlerIdentity,
                        StringComparison.Ordinal
                    )
                );

            if (handler is null)
            {
                // A persisted identity with no registered match cannot heal by retrying —
                // the handler was renamed without an explicit identity, or unregistered.
                // Dead-letter immediately instead of burning the retry budget.
                await MarkDeadLetteredAsync(
                    claimed,
                    currentAttempt,
                    $"Retention row handler '{claimed.HandlerType}' is not registered for entity {claimed.EntityType}. If the handler class was renamed, register it with an explicit identity (AddRowHandler<TEntity, THandler>(identity: ...)) so queued work survives renames.",
                    DateTimeOffset.UtcNow,
                    ct
                );
                await ClearSettledPayloadAsync(claimed.RowDetailId);
                return;
            }

            var snapshot = RetentionSnapshotSerializer.Deserialize(
                claimed.CapturedPayload,
                entityType,
                handlers.Select(resolved => resolved.Instance.GetType().Assembly)
            );
            var context = CreateAfterContext(entityType, claimed, currentAttempt, snapshot);
            await InvokeOnAfterAsync(entityType, handler.Instance, context, ct);
            handlerCompleted = true;
            await MarkSucceededAsync(
                claimed.StatusId,
                currentAttempt,
                DateTimeOffset.UtcNow,
                CancellationToken.None
            );
            await ClearSettledPayloadAsync(claimed.RowDetailId);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested && !handlerCompleted)
        {
            throw;
        }
        catch (Exception ex)
        {
            await MarkFailureAsync(claimed, currentAttempt, ex, DateTimeOffset.UtcNow, ct);
            // No-op while any sibling handler is still pending/in-flight (including a
            // requeue of this one); clears the snapshot once the row is fully settled.
            await ClearSettledPayloadAsync(claimed.RowDetailId);
        }
    }

    private Task<IReadOnlyList<ClaimedHandlerRow>> ClaimBatchAsync(
        DateTimeOffset dueCutoff,
        CancellationToken ct
    )
    {
        return WithScopedConnectionAsync<IReadOnlyList<ClaimedHandlerRow>>(
            async (db, connection) =>
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
            },
            ct
        );
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
                WHERE (
                      (status."State" = @pending AND status."NextAttemptAt" <= @dueCutoff)
                      OR (
                          status."State" = @inFlight
                          AND status."ClaimedAt" IS NOT NULL
                          AND status."ClaimedAt" <= @leaseCutoff
                      )
                  )
                  AND (
                      status."DispatchPhase" = @immediatePhase
                      OR (
                          status."DispatchPhase" = @afterSweepSettledPhase
                          AND (
                              run."CompletedAt" IS NOT NULL
                              OR run."FailedAt" IS NOT NULL
                              OR run."StartedAt" <= @sweepSettledCutoff
                          )
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
                FOR UPDATE OF status SKIP LOCKED
                LIMIT @batchSize
            )
            UPDATE {QuoteIdentifier(CohortTableNames.SweepRowHandlerStatus)} AS status
            SET "State" = @inFlight,
                "ClaimedAt" = @claimedAt,
                "Attempt" = status."Attempt"
                    + CASE WHEN status."State" = @inFlight THEN 1 ELSE 0 END
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
        var claimTimeout = options.CurrentValue.RowHandlerDispatch.ClaimTimeout;
        if (claimTimeout < TimeSpan.FromSeconds(30))
        {
            claimTimeout = TimeSpan.FromSeconds(30);
        }

        command.Parameters.Add(
            CreateParameter(command, "leaseCutoff", claimedAt - claimTimeout)
        );

        // A run that crashed mid-sweep never gets CompletedAt; its committed deferred
        // work is released after the settle timeout instead of being stuck forever.
        var settleTimeout = options.CurrentValue.RowHandlerDispatch.SweepSettleTimeout;
        if (settleTimeout < TimeSpan.FromMinutes(1))
        {
            settleTimeout = TimeSpan.FromMinutes(1);
        }

        command.Parameters.Add(
            CreateParameter(command, "sweepSettledCutoff", claimedAt - settleTimeout)
        );

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
        var lastError = SanitizeError(ex);

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

    private Task MarkDeadLetteredAsync(
        ClaimedHandlerRow claimed,
        int attempt,
        string lastError,
        DateTimeOffset now,
        CancellationToken ct
    )
    {
        return WithScopedConnectionAsync(
            async (db, connection) =>
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
            },
            ct
        );
    }

    private Task RequeueCancelledClaimsAsync(IReadOnlyList<long> statusIds)
    {
        if (statusIds.Count == 0)
        {
            return Task.CompletedTask;
        }

        return WithScopedConnectionAsync(
            async (db, connection) =>
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
            },
            CancellationToken.None
        );
    }

    private Task ExecuteStatusUpdateAsync(
        long statusId,
        string setClause,
        Action<List<(string Name, object? Value)>> configureParameters,
        CancellationToken ct
    )
    {
        return WithScopedConnectionAsync(
            async (db, connection) =>
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
            },
            ct
        );
    }

    /// <summary>
    /// Runs <paramref name="action"/> against a fresh scope's DbContext connection,
    /// opening it when closed and restoring its state afterwards.
    /// </summary>
    private async Task<TResult> WithScopedConnectionAsync<TResult>(
        Func<DbContext, DbConnection, Task<TResult>> action,
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
            return await action(db, connection);
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }
    }

    private async Task WithScopedConnectionAsync(
        Func<DbContext, DbConnection, Task> action,
        CancellationToken ct
    )
    {
        await WithScopedConnectionAsync<bool>(
            async (db, connection) =>
            {
                await action(db, connection);
                return true;
            },
            ct
        );
    }

    private static object CreateAfterContext(
        Type entityType,
        ClaimedHandlerRow claimed,
        int attempt,
        IReadOnlyDictionary<string, object?> snapshot
    )
    {
        var contextType = GetDispatchReflection(entityType).ContextType;
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

    // MakeGenericType/GetMethod run on every dispatched row otherwise; the entity set
    // is small and fixed, so the closed types are cached for the process lifetime.
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<
        Type,
        (Type ContextType, System.Reflection.MethodInfo OnAfterMethod)
    > DispatchReflectionCache = new();

    private static (Type ContextType, System.Reflection.MethodInfo OnAfterMethod) GetDispatchReflection(
        Type entityType
    )
    {
        return DispatchReflectionCache.GetOrAdd(
            entityType,
            static entityType =>
            {
                var handlerInterface = typeof(IRetentionHandler<>).MakeGenericType(entityType);
                var onAfterMethod =
                    handlerInterface.GetMethod(nameof(IRetentionHandler<object>.OnAfterAsync))
                    ?? throw new InvalidOperationException(
                        $"Could not resolve {nameof(IRetentionHandler<object>.OnAfterAsync)} for {handlerInterface.FullName}."
                    );

                return (typeof(RetentionAfterContext<>).MakeGenericType(entityType), onAfterMethod);
            }
        );
    }

    private static async Task InvokeOnAfterAsync(
        Type entityType,
        object handler,
        object context,
        CancellationToken ct
    )
    {
        var invocation = GetDispatchReflection(entityType).OnAfterMethod.Invoke(handler, [context, ct]);
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

    private static Type ResolveEntityType(string entityType, RetentionRegistry registry)
    {
        // Persisted names resolve only against the registered retention model, never
        // AppDomain-wide: a tampered row must not be able to materialise arbitrary types.
        var resolved = registry
            .Scan()
            .Values.Select(entry => entry.EntityType)
            .FirstOrDefault(type => string.Equals(type.FullName, entityType, StringComparison.Ordinal));

        return resolved
            ?? throw new InvalidOperationException(
                $"Could not resolve retention handler entity type '{entityType}': it is not a registered retained entity in the current model."
            );
    }

    private static DateTimeOffset ClampDateTimeOffset(DateTimeOffset value)
    {
        return value > DateTimeOffset.MaxValue.AddYears(-1)
            ? DateTimeOffset.MaxValue.AddYears(-1)
            : value;
    }

    private static string SanitizeError(Exception ex)
    {
        // Root-cause type + message only: full ToString() includes stack frames that can
        // echo row data into a long-lived audit table, and reflection wrappers like
        // TargetInvocationException would otherwise hide the handler's actual failure.
        var root = ex.GetBaseException();
        var error = $"{root.GetType().FullName}: {root.Message}";
        return error.Length <= 2000 ? error : error[..2000];
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
