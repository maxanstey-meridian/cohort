using System.Data;
using System.Data.Common;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Handlers;

internal sealed class RetentionRowDispatcher(
    IServiceScopeFactory scopeFactory,
    IRetentionExecutionSettings options,
    ILogger<RetentionRowDispatcher> logger
) : BackgroundService, IRetentionRowDispatcher
{
    private static readonly TimeSpan ClaimCleanupTimeout = TimeSpan.FromSeconds(30);

    // Keep persisted retry timestamps inside both DateTimeOffset and PostgreSQL
    // timestamptz ranges, with headroom for provider conversions at the boundary.
    internal static readonly DateTimeOffset RetryScheduleUpperBound =
        DateTimeOffset.MaxValue.AddYears(-1);
    private readonly TaskCompletionSource pollDelayEntered = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    internal Task PollDelayEntered => pollDelayEntered.Task;

    public async Task<RowDispatcherFlushResult> FlushAsync(CancellationToken ct = default)
    {
        await RecoverAbandonedRunsAsync(DateTimeOffset.UtcNow, ct);
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
            async (_, connection, tables) =>
            {
                await using var command = connection.CreateCommand();
                command.CommandText = $"""
                    SELECT
                        pg_catalog.count(*) FILTER (WHERE "State" = @inFlight),
                        pg_catalog.count(*) FILTER (WHERE "State" = @pending)
                    FROM {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)}
                    """;
                command.Parameters.Add(
                    CreateParameter(command, "inFlight", (int)SweepRowHandlerDispatchState.InFlight)
                );
                command.Parameters.Add(
                    CreateParameter(command, "pending", (int)SweepRowHandlerDispatchState.Pending)
                );

                await using var reader = await command.ExecuteReaderAsync(ct);
                await reader.ReadAsync(ct);

                return new RowDispatcherFlushResult(reader.GetInt64(0), reader.GetInt64(1));
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
                await RecoverAbandonedRunsAsync(now, stoppingToken);
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

            var pollInterval = options.RowHandlerDispatch.PollInterval;
            if (pollInterval < TimeSpan.Zero)
            {
                pollInterval = TimeSpan.Zero;
            }

            try
            {
                pollDelayEntered.TrySetResult();
                await OperationalTime.DelayAsync(pollInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
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
            async (_, connection, tables) =>
            {
                await using var command = connection.CreateCommand();
                command.CommandText = $"""
                    UPDATE {PostgreSqlIdentifier.Format(tables.SweepRunRowDetail)} AS detail
                    SET "CapturedPayload" = NULL
                    WHERE detail."Id" = @rowDetailId
                      AND detail."CapturedPayload" IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)} AS status
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
        var retention = options.RowHandlerDispatch.PayloadRetention;
        if (retention < TimeSpan.FromHours(1))
        {
            retention = TimeSpan.FromHours(1);
        }

        return WithScopedConnectionAsync(
            async (_, connection, tables) =>
            {
                await using var command = connection.CreateCommand();
                command.CommandText = $"""
                    UPDATE {PostgreSqlIdentifier.Format(tables.SweepRunRowDetail)} AS detail
                    SET "CapturedPayload" = NULL
                    WHERE detail."CapturedPayload" IS NOT NULL
                      AND detail."At" < @payloadCutoff
                    """;
                command.Parameters.Add(
                    CreateParameter(
                        command,
                        "payloadCutoff",
                        OperationalTime.SubtractSaturating(DateTimeOffset.UtcNow, retention)
                    )
                );

                await command.ExecuteNonQueryAsync(ct);
            },
            ct
        );
    }

    private Task RecoverAbandonedRunsAsync(DateTimeOffset now, CancellationToken ct)
    {
        var settleTimeout = options.RowHandlerDispatch.SweepSettleTimeout;
        if (settleTimeout < TimeSpan.FromMinutes(1))
        {
            settleTimeout = TimeSpan.FromMinutes(1);
        }

        return WithScopedConnectionAsync(
            async (_, connection, tables) =>
            {
                var staleSweepIds = new List<Guid>();
                await using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"""
                        SELECT "SweepId"
                        FROM {PostgreSqlIdentifier.Format(tables.SweepRun)}
                        WHERE "Status" = @started
                          AND "StartedAt" <= @cutoff
                        ORDER BY "StartedAt"
                        """;
                    command.Parameters.Add(
                        CreateParameter(command, "started", (int)SweepRunStatus.Started)
                    );
                    command.Parameters.Add(
                        CreateParameter(
                            command,
                            "cutoff",
                            OperationalTime.SubtractSaturating(now, settleTimeout)
                        )
                    );
                    await using var reader = await command.ExecuteReaderAsync(ct);
                    while (await reader.ReadAsync(ct))
                    {
                        staleSweepIds.Add(reader.GetGuid(0));
                    }
                }

                foreach (var sweepId in staleSweepIds)
                {
                    if (!await RetentionRunAdvisoryLock.TryAcquireAsync(connection, sweepId, ct))
                    {
                        continue;
                    }

                    Exception? primaryException = null;
                    try
                    {
                        await using var command = connection.CreateCommand();
                        command.CommandText = $"""
                            UPDATE {PostgreSqlIdentifier.Format(tables.SweepRun)}
                            SET "Status" = @failed,
                                "SettledAt" = @settledAt,
                                "Duration" = @settledAt - "StartedAt",
                                "Error" = @error
                            WHERE "SweepId" = @sweepId
                              AND "Status" = @started
                            """;
                        command.Parameters.Add(
                            CreateParameter(command, "failed", (int)SweepRunStatus.Failed)
                        );
                        command.Parameters.Add(CreateParameter(command, "settledAt", now));
                        command.Parameters.Add(
                            CreateParameter(
                                command,
                                "error",
                                "Run owner exited before writing a terminal audit event."
                            )
                        );
                        command.Parameters.Add(CreateParameter(command, "sweepId", sweepId));
                        command.Parameters.Add(
                            CreateParameter(command, "started", (int)SweepRunStatus.Started)
                        );
                        if (await command.ExecuteNonQueryAsync(ct) > 0)
                        {
                            logger.LogWarning(
                                "Cohort recovered abandoned retention run {SweepId} as failed.",
                                sweepId
                            );
                        }
                    }
                    catch (Exception ex)
                    {
                        primaryException = ex;
                        throw;
                    }
                    finally
                    {
                        await OperationalConnectionCleanup.RunAsync(
                            cleanupToken =>
                                RetentionRunAdvisoryLock.ReleaseAsync(
                                    connection,
                                    sweepId,
                                    cleanupToken
                                ),
                            close: null,
                            primaryException,
                            logger
                        );
                    }
                }
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

            var maxParallelism = Math.Max(1, options.RowHandlerDispatch.MaxParallelism);
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
            catch (Exception)
            {
                try
                {
                    using var cleanup = new CancellationTokenSource(ClaimCleanupTimeout);
                    await RequeueOwnedClaimsAsync(claimed, cleanup.Token);
                }
                catch (Exception cleanupException)
                {
                    logger.LogWarning(
                        cleanupException,
                        "Cohort could not requeue owned row-handler claims after dispatch failed; claim leases will recover them."
                    );
                }
                throw;
            }
        }
    }

    private async ValueTask ProcessClaimedRowAsync(ClaimedHandlerRow claimed, CancellationToken ct)
    {
        var currentAttempt = claimed.Attempt;
        var handlerCompleted = false;
        using var handlerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var heartbeat = HeartbeatClaimAsync(claimed, handlerCts, heartbeatCts.Token);

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
            var entityType = ResolveEntityType(
                claimed.RetentionEntityId,
                claimed.EntityType,
                registry
            );
            var handlers = RetentionHandlerSupport.ResolveHandlers(
                scope.ServiceProvider,
                entityType
            );
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
                    "The queued retention row handler is not registered. If it was renamed, register it with an explicit identity so queued work survives renames.",
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
            await InvokeOnAfterAsync(entityType, handler.Instance, context, handlerCts.Token);
            handlerCompleted = true;
            heartbeatCts.Cancel();
            await ObserveStoppedHeartbeatAsync(heartbeat, heartbeatCts.Token);
            await MarkSucceededAsync(
                claimed,
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
        catch (OperationCanceledException)
            when (handlerCts.IsCancellationRequested
                && !ct.IsCancellationRequested
                && !handlerCompleted
            )
        {
            await heartbeat;
            throw;
        }
        catch (RetentionRowDispatchClaimLostException)
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
        finally
        {
            heartbeatCts.Cancel();
            try
            {
                await heartbeat;
            }
            catch (OperationCanceledException) when (heartbeatCts.IsCancellationRequested) { }
        }
    }

    private static async Task ObserveStoppedHeartbeatAsync(
        Task heartbeat,
        CancellationToken heartbeatToken
    )
    {
        try
        {
            await heartbeat;
        }
        catch (OperationCanceledException) when (heartbeatToken.IsCancellationRequested) { }
    }

    private Task<IReadOnlyList<ClaimedHandlerRow>> ClaimBatchAsync(
        DateTimeOffset dueCutoff,
        CancellationToken ct
    )
    {
        return WithScopedConnectionAsync<IReadOnlyList<ClaimedHandlerRow>>(
            async (db, connection, tables) =>
            {
                await using var transaction = await db.Database.BeginTransactionAsync(ct);
                var claimedAt = DateTimeOffset.UtcNow;
                var claimToken = Guid.NewGuid();
                await DeadLetterExhaustedExpiredClaimsAsync(
                    connection,
                    transaction.GetDbTransaction(),
                    tables,
                    claimedAt,
                    ct
                );
                var claimedIds = await ClaimBatchIdsAsync(
                    connection,
                    transaction.GetDbTransaction(),
                    tables,
                    claimedAt,
                    claimToken,
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
                    tables,
                    claimedIds,
                    claimToken,
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

    private async Task DeadLetterExhaustedExpiredClaimsAsync(
        DbConnection connection,
        DbTransaction transaction,
        CohortStoreTables tables,
        DateTimeOffset now,
        CancellationToken ct
    )
    {
        var claimTimeout = options.RowHandlerDispatch.ClaimTimeout;
        if (claimTimeout < TimeSpan.FromSeconds(30))
        {
            claimTimeout = TimeSpan.FromSeconds(30);
        }

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            WITH exhausted AS (
                SELECT status."Id", status."SweepRunRowDetailId", status."HandlerType"
                FROM {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)} AS status
                WHERE status."State" = @inFlight
                  AND status."Attempt" >= @maxAttempts
                  AND status."ClaimedAt" <= @leaseCutoff
                FOR UPDATE SKIP LOCKED
            ),
            dead_lettered AS (
                UPDATE {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)} AS status
                SET "State" = @deadLettered,
                    "ClaimedAt" = NULL,
                    "ClaimToken" = NULL,
                    "CompletedAt" = @completedAt,
                    "LastError" = @lastError
                FROM exhausted
                WHERE status."Id" = exhausted."Id"
                RETURNING status."Id", status."SweepRunRowDetailId", status."HandlerType"
            ),
            dependent_dead_lettered AS (
                UPDATE {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)} AS dependent
                SET "State" = @deadLettered,
                    "ClaimedAt" = NULL,
                    "ClaimToken" = NULL,
                    "CompletedAt" = @completedAt,
                    "LastError" = 'Skipped because an earlier handler for the same row exhausted its claim attempts.'
                FROM dead_lettered
                WHERE dependent."SweepRunRowDetailId" = dead_lettered."SweepRunRowDetailId"
                  AND dependent."Id" > dead_lettered."Id"
                  AND dependent."State" IN (@pending, @inFlight)
                RETURNING dependent."Id", dependent."SweepRunRowDetailId"
            ),
            affected_statuses AS (
                SELECT "Id", "SweepRunRowDetailId" FROM dead_lettered
                UNION
                SELECT "Id", "SweepRunRowDetailId" FROM dependent_dead_lettered
            )
            UPDATE {PostgreSqlIdentifier.Format(tables.SweepRunRowDetail)} AS detail
            SET "CapturedPayload" = NULL
            WHERE detail."Id" IN (
                  SELECT "SweepRunRowDetailId" FROM affected_statuses
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)} AS unsettled
                  WHERE unsettled."SweepRunRowDetailId" = detail."Id"
                    AND unsettled."State" IN (@pending, @inFlight)
                    AND unsettled."Id" NOT IN (SELECT "Id" FROM affected_statuses)
              )
            """;
        command.Parameters.Add(
            CreateParameter(command, "inFlight", (int)SweepRowHandlerDispatchState.InFlight)
        );
        command.Parameters.Add(
            CreateParameter(command, "pending", (int)SweepRowHandlerDispatchState.Pending)
        );
        command.Parameters.Add(
            CreateParameter(command, "deadLettered", (int)SweepRowHandlerDispatchState.DeadLettered)
        );
        command.Parameters.Add(
            CreateParameter(
                command,
                "maxAttempts",
                Math.Max(1, options.RowHandlerDispatch.MaxAttempts)
            )
        );
        command.Parameters.Add(
            CreateParameter(
                command,
                "leaseCutoff",
                OperationalTime.SubtractSaturating(now, claimTimeout)
            )
        );
        command.Parameters.Add(CreateParameter(command, "completedAt", now));
        command.Parameters.Add(
            CreateParameter(
                command,
                "lastError",
                "Handler claim expired after reaching RowHandlerDispatch:MaxAttempts."
            )
        );
        await command.ExecuteNonQueryAsync(ct);
    }

    private async Task<IReadOnlyList<long>> ClaimBatchIdsAsync(
        DbConnection connection,
        DbTransaction transaction,
        CohortStoreTables tables,
        DateTimeOffset claimedAt,
        Guid claimToken,
        DateTimeOffset dueCutoff,
        CancellationToken ct
    )
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            WITH due AS (
                SELECT status."Id"
                FROM {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)} AS status
                INNER JOIN {PostgreSqlIdentifier.Format(tables.SweepRunRowDetail)} AS detail
                    ON detail."Id" = status."SweepRunRowDetailId"
                INNER JOIN {PostgreSqlIdentifier.Format(tables.SweepRun)} AS run
                    ON run."SweepId" = detail."SweepId"
                WHERE (
                      (status."State" = @pending AND status."NextAttemptAt" <= @dueCutoff)
                      OR (
                          status."State" = @inFlight
                          AND status."ClaimedAt" IS NOT NULL
                          AND status."ClaimedAt" <= @leaseCutoff
                          AND status."Attempt" < @maxAttempts
                      )
                  )
                  AND (
                      status."DispatchPhase" = @immediatePhase
                      OR (
                           status."DispatchPhase" = @afterSweepSettledPhase
                            AND run."Status" IN (
                                @succeededStatus,
                                @partiallyFailedStatus,
                                @failedStatus,
                                @cancelledStatus
                            )
                            AND run."SettledAt" IS NOT NULL
                      )
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)} AS blocker
                      WHERE blocker."SweepRunRowDetailId" = status."SweepRunRowDetailId"
                        AND blocker."Id" < status."Id"
                        AND blocker."State" IN (@pending, @inFlight)
                  )
                ORDER BY status."NextAttemptAt", status."Id"
                FOR UPDATE OF status SKIP LOCKED
                LIMIT @batchSize
            )
            UPDATE {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)} AS status
            SET "State" = @inFlight,
                "ClaimedAt" = @claimedAt,
                "ClaimToken" = @claimToken,
                "Attempt" = status."Attempt" + 1
            FROM due
            WHERE status."Id" = due."Id"
            RETURNING status."Id"
            """;
        command.Parameters.Add(
            CreateParameter(command, "pending", (int)SweepRowHandlerDispatchState.Pending)
        );
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
            CreateParameter(command, "batchSize", Math.Max(1, options.RowHandlerDispatch.BatchSize))
        );
        command.Parameters.Add(
            CreateParameter(command, "inFlight", (int)SweepRowHandlerDispatchState.InFlight)
        );
        command.Parameters.Add(
            CreateParameter(command, "succeededStatus", (int)SweepRunStatus.Succeeded)
        );
        command.Parameters.Add(
            CreateParameter(command, "partiallyFailedStatus", (int)SweepRunStatus.PartiallyFailed)
        );
        command.Parameters.Add(
            CreateParameter(command, "failedStatus", (int)SweepRunStatus.Failed)
        );
        command.Parameters.Add(
            CreateParameter(command, "cancelledStatus", (int)SweepRunStatus.Cancelled)
        );
        command.Parameters.Add(
            CreateParameter(
                command,
                "maxAttempts",
                Math.Max(1, options.RowHandlerDispatch.MaxAttempts)
            )
        );
        command.Parameters.Add(CreateParameter(command, "claimedAt", claimedAt));
        command.Parameters.Add(CreateParameter(command, "claimToken", claimToken));
        var claimTimeout = options.RowHandlerDispatch.ClaimTimeout;
        if (claimTimeout < TimeSpan.FromSeconds(30))
        {
            claimTimeout = TimeSpan.FromSeconds(30);
        }

        command.Parameters.Add(
            CreateParameter(
                command,
                "leaseCutoff",
                OperationalTime.SubtractSaturating(claimedAt, claimTimeout)
            )
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
        CohortStoreTables tables,
        IReadOnlyList<long> claimedIds,
        Guid claimToken,
        CancellationToken ct
    )
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            SELECT
                status."Id",
                detail."Id",
                status."HandlerType",
                status."Attempt",
                status."ClaimToken",
                detail."SweepId",
                detail."At",
                detail."EntityType",
                detail."RetentionEntityId",
                detail."RecordId",
                detail."Category",
                detail."Strategy",
                detail."TenantId",
                detail."CapturedPayload"
            FROM {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)} AS status
            INNER JOIN {PostgreSqlIdentifier.Format(tables.SweepRunRowDetail)} AS detail
                ON detail."Id" = status."SweepRunRowDetailId"
            WHERE status."Id" = ANY(@claimedIds)
              AND status."ClaimToken" = @claimToken
            ORDER BY status."Id"
            """;
        command.Parameters.Add(CreateParameter(command, "claimedIds", claimedIds.ToArray()));
        command.Parameters.Add(CreateParameter(command, "claimToken", claimToken));

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
                    reader.GetGuid(5),
                    reader.GetFieldValue<DateTimeOffset>(6),
                    reader.GetString(7),
                    reader.GetGuid(8),
                    reader.GetString(9),
                    reader.GetString(10),
                    (Strategy)reader.GetInt32(11),
                    reader.GetGuid(12),
                    reader.IsDBNull(13) ? null : reader.GetString(13)
                )
            );
        }

        return rows;
    }

    private async Task MarkSucceededAsync(
        ClaimedHandlerRow claimed,
        int attempt,
        DateTimeOffset completedAt,
        CancellationToken ct
    )
    {
        await ExecuteStatusUpdateAsync(
            claimed,
            """
            "State" = @state,
            "Attempt" = @attempt,
            "CompletedAt" = @completedAt,
            "ClaimedAt" = NULL,
            "ClaimToken" = NULL,
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
        var optionsSnapshot = options.RowHandlerDispatch;
        var maxAttempts = Math.Max(1, optionsSnapshot.MaxAttempts);
        var diagnostic = RetentionFailureDiagnostic.Create(ex);
        var lastError = diagnostic.ToString();
        logger.LogError(
            ex.GetBaseException(),
            "Cohort row handler failed for sweep {SweepId}. Diagnostic {DiagnosticId}.",
            claimed.SweepId,
            diagnostic.DiagnosticIdText
        );

        if (attempt >= maxAttempts)
        {
            await MarkDeadLetteredAsync(claimed, attempt, lastError, now, ct);
            return;
        }

        await ExecuteStatusUpdateAsync(
            claimed,
            """
            "State" = @state,
            "Attempt" = @attempt,
            "NextAttemptAt" = @nextAttemptAt,
            "ClaimedAt" = NULL,
            "ClaimToken" = NULL,
            "CompletedAt" = NULL,
            "LastError" = @lastError
            """,
            parameters =>
            {
                parameters.Add(("state", (int)SweepRowHandlerDispatchState.Pending));
                parameters.Add(("attempt", attempt));
                parameters.Add(
                    (
                        "nextAttemptAt",
                        CalculateNextAttemptAt(now, optionsSnapshot.BaseBackoff, attempt)
                    )
                );
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
            async (db, connection, tables) =>
            {
                await using var transaction = await db.Database.BeginTransactionAsync(ct);

                await using (var currentCommand = connection.CreateCommand())
                {
                    currentCommand.Transaction = transaction.GetDbTransaction();
                    currentCommand.CommandText = $"""
                        UPDATE {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)}
                        SET "State" = @state,
                            "Attempt" = @attempt,
                            "ClaimedAt" = NULL,
                            "ClaimToken" = NULL,
                            "CompletedAt" = @completedAt,
                            "LastError" = @lastError
                        WHERE "Id" = @statusId
                          AND "State" = @expectedState
                          AND "ClaimToken" = @claimToken
                        """;
                    currentCommand.Parameters.Add(
                        CreateParameter(
                            currentCommand,
                            "state",
                            (int)SweepRowHandlerDispatchState.DeadLettered
                        )
                    );
                    currentCommand.Parameters.Add(
                        CreateParameter(currentCommand, "attempt", attempt)
                    );
                    currentCommand.Parameters.Add(
                        CreateParameter(currentCommand, "completedAt", now)
                    );
                    currentCommand.Parameters.Add(
                        CreateParameter(currentCommand, "lastError", lastError)
                    );
                    currentCommand.Parameters.Add(
                        CreateParameter(currentCommand, "statusId", claimed.StatusId)
                    );
                    currentCommand.Parameters.Add(
                        CreateParameter(currentCommand, "claimToken", claimed.ClaimToken)
                    );
                    currentCommand.Parameters.Add(
                        CreateParameter(
                            currentCommand,
                            "expectedState",
                            (int)SweepRowHandlerDispatchState.InFlight
                        )
                    );

                    var affected = await currentCommand.ExecuteNonQueryAsync(ct);
                    if (affected != 1)
                    {
                        await transaction.RollbackAsync(ct);
                        throw new RetentionRowDispatchClaimLostException(claimed.StatusId);
                    }
                }

                await using (var dependentCommand = connection.CreateCommand())
                {
                    dependentCommand.Transaction = transaction.GetDbTransaction();
                    dependentCommand.CommandText = $"""
                        UPDATE {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)}
                        SET "State" = @state,
                            "ClaimedAt" = NULL,
                            "ClaimToken" = NULL,
                            "CompletedAt" = @completedAt,
                            "LastError" = @lastError
                        WHERE "SweepRunRowDetailId" = @rowDetailId
                          AND "Id" > @statusId
                          AND "State" IN (@pending, @inFlight)
                        """;
                    dependentCommand.Parameters.Add(
                        CreateParameter(
                            dependentCommand,
                            "state",
                            (int)SweepRowHandlerDispatchState.DeadLettered
                        )
                    );
                    dependentCommand.Parameters.Add(
                        CreateParameter(dependentCommand, "completedAt", now)
                    );
                    dependentCommand.Parameters.Add(
                        CreateParameter(
                            dependentCommand,
                            "lastError",
                            "Skipped because an earlier handler for the same row dead-lettered."
                        )
                    );
                    dependentCommand.Parameters.Add(
                        CreateParameter(dependentCommand, "rowDetailId", claimed.RowDetailId)
                    );
                    dependentCommand.Parameters.Add(
                        CreateParameter(dependentCommand, "statusId", claimed.StatusId)
                    );
                    dependentCommand.Parameters.Add(
                        CreateParameter(
                            dependentCommand,
                            "pending",
                            (int)SweepRowHandlerDispatchState.Pending
                        )
                    );
                    dependentCommand.Parameters.Add(
                        CreateParameter(
                            dependentCommand,
                            "inFlight",
                            (int)SweepRowHandlerDispatchState.InFlight
                        )
                    );

                    await dependentCommand.ExecuteNonQueryAsync(ct);
                }

                await transaction.CommitAsync(ct);
            },
            ct
        );
    }

    private Task RequeueOwnedClaimsAsync(
        IReadOnlyList<ClaimedHandlerRow> claims,
        CancellationToken ct
    )
    {
        if (claims.Count == 0)
        {
            return Task.CompletedTask;
        }

        return WithScopedConnectionAsync(
            async (db, connection, tables) =>
            {
                await using var transaction = await db.Database.BeginTransactionAsync(ct);
                foreach (var claim in claims)
                {
                    await using var command = connection.CreateCommand();
                    command.Transaction = transaction.GetDbTransaction();
                    command.CommandText = $"""
                        UPDATE {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)}
                        SET "State" = @pending,
                            "ClaimedAt" = NULL,
                            "ClaimToken" = NULL
                        WHERE "Id" = @statusId
                          AND "State" = @inFlight
                          AND "ClaimToken" = @claimToken
                        """;
                    command.Parameters.Add(
                        CreateParameter(
                            command,
                            "pending",
                            (int)SweepRowHandlerDispatchState.Pending
                        )
                    );
                    command.Parameters.Add(CreateParameter(command, "statusId", claim.StatusId));
                    command.Parameters.Add(
                        CreateParameter(
                            command,
                            "inFlight",
                            (int)SweepRowHandlerDispatchState.InFlight
                        )
                    );
                    command.Parameters.Add(
                        CreateParameter(command, "claimToken", claim.ClaimToken)
                    );
                    await command.ExecuteNonQueryAsync(ct);
                }
                await transaction.CommitAsync(ct);
            },
            ct
        );
    }

    private Task ExecuteStatusUpdateAsync(
        ClaimedHandlerRow claimed,
        string setClause,
        Action<List<(string Name, object? Value)>> configureParameters,
        CancellationToken ct
    )
    {
        return WithScopedConnectionAsync(
            async (db, connection, tables) =>
            {
                await using var transaction = await db.Database.BeginTransactionAsync(ct);
                await using var command = connection.CreateCommand();
                command.Transaction = transaction.GetDbTransaction();
                command.CommandText = $"""
                    UPDATE {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)}
                    SET {setClause}
                    WHERE "Id" = @statusId
                      AND "State" = @expectedState
                      AND "ClaimToken" = @claimToken
                    """;
                command.Parameters.Add(CreateParameter(command, "statusId", claimed.StatusId));
                command.Parameters.Add(CreateParameter(command, "claimToken", claimed.ClaimToken));
                command.Parameters.Add(
                    CreateParameter(
                        command,
                        "expectedState",
                        (int)SweepRowHandlerDispatchState.InFlight
                    )
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
                    await transaction.RollbackAsync(ct);
                    throw new RetentionRowDispatchClaimLostException(claimed.StatusId);
                }

                await transaction.CommitAsync(ct);
            },
            ct
        );
    }

    private async Task HeartbeatClaimAsync(
        ClaimedHandlerRow claimed,
        CancellationTokenSource handlerCts,
        CancellationToken ct
    )
    {
        var claimTimeout = options.RowHandlerDispatch.ClaimTimeout;
        var interval = TimeSpan.FromTicks(
            Math.Max(TimeSpan.FromMilliseconds(100).Ticks, claimTimeout.Ticks / 3)
        );

        try
        {
            while (true)
            {
                await OperationalTime.DelayAsync(interval, ct);
                await WithScopedConnectionAsync(
                    async (_, connection, tables) =>
                    {
                        await using var command = connection.CreateCommand();
                        command.CommandText = $"""
                            UPDATE {PostgreSqlIdentifier.Format(tables.SweepRowHandlerStatus)}
                            SET "ClaimedAt" = @claimedAt
                            WHERE "Id" = @statusId
                              AND "State" = @inFlight
                              AND "ClaimToken" = @claimToken
                            """;
                        command.Parameters.Add(
                            CreateParameter(command, "claimedAt", DateTimeOffset.UtcNow)
                        );
                        command.Parameters.Add(
                            CreateParameter(command, "statusId", claimed.StatusId)
                        );
                        command.Parameters.Add(
                            CreateParameter(
                                command,
                                "inFlight",
                                (int)SweepRowHandlerDispatchState.InFlight
                            )
                        );
                        command.Parameters.Add(
                            CreateParameter(command, "claimToken", claimed.ClaimToken)
                        );
                        var affected = await command.ExecuteNonQueryAsync(ct);
                        if (affected != 1)
                        {
                            throw new RetentionRowDispatchClaimLostException(claimed.StatusId);
                        }
                    },
                    ct
                );
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            handlerCts.Cancel();
            if (ex is RetentionRowDispatchClaimLostException)
            {
                throw;
            }

            throw new RetentionRowDispatchClaimLostException(claimed.StatusId, ex);
        }
    }

    /// <summary>
    /// Runs <paramref name="action"/> against a fresh scope's DbContext connection,
    /// opening it when closed and restoring its state afterwards.
    /// </summary>
    private async Task<TResult> WithScopedConnectionAsync<TResult>(
        Func<DbContext, DbConnection, CohortStoreTables, Task<TResult>> action,
        CancellationToken ct
    )
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        await scope.ServiceProvider
            .GetRequiredService<RetentionRuntimeReadinessValidator>()
            .ValidateAsync(ct);
        var db = scope.ServiceProvider.GetRequiredKeyedService<DbContext>(
            CohortServiceKeys.DbContext
        );
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        Exception? primaryException = null;
        try
        {
            return await action(db, connection, CohortStoreTables.FromModel(db.Model));
        }
        catch (Exception ex)
        {
            primaryException = ex;
            throw;
        }
        finally
        {
            await OperationalConnectionCleanup.RunAsync(
                unlock: null,
                shouldCloseConnection
                    ? cleanupToken => db.Database.CloseConnectionAsync().WaitAsync(cleanupToken)
                    : null,
                primaryException,
                logger
            );
        }
    }

    private async Task WithScopedConnectionAsync(
        Func<DbContext, DbConnection, CohortStoreTables, Task> action,
        CancellationToken ct
    )
    {
        await WithScopedConnectionAsync<bool>(
            async (db, connection, tables) =>
            {
                await action(db, connection, tables);
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
                claimed.RecordId,
                claimed.Category,
                claimed.Strategy,
                claimed.TenantId,
                claimed.At,
                attempt,
                snapshot
            )
            ?? throw new InvalidOperationException(
                $"Could not construct RetentionAfterContext for entity type {entityType.FullName}."
            );
    }

    // MakeGenericType/GetMethod run on every dispatched row otherwise; the entity set
    // is small and fixed, so the closed types are cached for the process lifetime.
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<
        Type,
        (Type ContextType, System.Reflection.MethodInfo OnAfterMethod)
    > DispatchReflectionCache = new();

    private static (
        Type ContextType,
        System.Reflection.MethodInfo OnAfterMethod
    ) GetDispatchReflection(Type entityType)
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
        var invocation = GetDispatchReflection(entityType)
            .OnAfterMethod.Invoke(handler, [context, ct]);
        await (Task)invocation!;
    }

    internal static DateTimeOffset CalculateNextAttemptAt(
        DateTimeOffset now,
        TimeSpan baseBackoff,
        int attempt
    )
    {
        if (baseBackoff < TimeSpan.Zero)
        {
            baseBackoff = TimeSpan.Zero;
        }

        if (attempt <= 0 || baseBackoff == TimeSpan.Zero)
        {
            return now > RetryScheduleUpperBound ? RetryScheduleUpperBound : now;
        }

        if (now >= RetryScheduleUpperBound)
        {
            return RetryScheduleUpperBound;
        }

        var availableTicks = (RetryScheduleUpperBound - now).Ticks;
        var shifts = attempt - 1;
        if (shifts >= 63 || baseBackoff.Ticks > (availableTicks >> shifts))
        {
            return RetryScheduleUpperBound;
        }

        return now.AddTicks(baseBackoff.Ticks << shifts);
    }

    private Type ResolveEntityType(
        Guid retentionEntityId,
        string entityType,
        RetentionRegistry registry
    )
    {
        var entries = registry.Scan().Values;
        var resolved = entries
            .FirstOrDefault(entry => entry.RetentionEntityId == retentionEntityId)
            ?.EntityType;

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

    private static DbParameter CreateParameter(DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        return parameter;
    }


    private sealed record ClaimedHandlerRow(
        long StatusId,
        long RowDetailId,
        string HandlerType,
        int Attempt,
        Guid ClaimToken,
        Guid SweepId,
        DateTimeOffset At,
        string EntityType,
        Guid RetentionEntityId,
        string RecordId,
        string Category,
        Strategy Strategy,
        Guid TenantId,
        string? CapturedPayload
    );
}
