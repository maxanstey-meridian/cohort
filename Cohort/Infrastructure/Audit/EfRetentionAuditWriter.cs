using System.Data;
using System.Data.Common;
using Cohort.Application;
using Cohort.Infrastructure.Migrations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure.Audit;

internal sealed class EfRetentionAuditWriter(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db
) : IRetentionAuditWriter
{
    private const string TerminalTransitionError =
        "Sweep run does not exist or is no longer in the Started state.";

    public Task WriteAsync(SweepEvent evt, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(evt);

        return evt switch
        {
            SweepEvent.Started started => WriteStartedAsync(started, ct),
            SweepEvent.EntityProgress progress => WriteEntityProgressAsync(progress, ct),
            SweepEvent.EntitySummary summary => WriteEntitySummaryAsync(summary, ct),
            SweepEvent.RowDetail rowDetail => WriteRowDetailAsync(rowDetail, ct),
            SweepEvent.Completed completed => WriteCompletedAsync(completed, ct),
            SweepEvent.PartiallyFailed partiallyFailed => WritePartiallyFailedAsync(
                partiallyFailed,
                ct
            ),
            SweepEvent.Failed failed => WriteFailedAsync(failed, ct),
            SweepEvent.Cancelled cancelled => WriteCancelledAsync(cancelled, ct),
            _ => throw new InvalidOperationException(
                $"Unsupported sweep event type '{evt.GetType().FullName}'."
            ),
        };
    }

    private Task WriteStartedAsync(SweepEvent.Started started, CancellationToken ct)
    {
        return ExecuteAsync(
            $"""
            INSERT INTO {QuoteIdentifier(CohortTableNames.SweepRun)} (
                "SweepId",
                "StartedAt",
                "Status",
                "SettledAt",
                "Duration",
                "TriggerKind",
                "DryRun",
                "TenantId",
                "TotalAffected"
            )
            VALUES (
                @sweepId,
                @startedAt,
                @status,
                NULL,
                NULL,
                @triggerKind,
                @dryRun,
                @tenantId,
                0
            )
            """,
            command =>
            {
                command.Parameters.Add(CreateParameter(command, "sweepId", started.SweepId));
                command.Parameters.Add(CreateParameter(command, "startedAt", started.At));
                command.Parameters.Add(
                    CreateParameter(command, "status", (int)SweepRunStatus.Started)
                );
                command.Parameters.Add(
                    CreateParameter(command, "triggerKind", (int)started.Trigger)
                );
                command.Parameters.Add(CreateParameter(command, "dryRun", started.DryRun));
                command.Parameters.Add(CreateParameter(command, "tenantId", started.TenantId));
            },
            ct
        );
    }

    private Task WriteEntitySummaryAsync(SweepEvent.EntitySummary summary, CancellationToken ct)
    {
        return ExecuteAsync(
            $"""
            INSERT INTO {QuoteIdentifier(CohortTableNames.SweepRunEntitySummary)} (
                "SweepId",
                "At",
                "EntityType",
                "RetentionEntityId",
                "Category",
                "TenantId",
                "Strategy",
                "ResolvedPeriod",
                "Affected",
                "HeldCount",
                "SkippedCount",
                "NullAnchorCount",
                "RuleSource",
                "RuleReason"
            )
            VALUES (
                @sweepId,
                @at,
                @entityType,
                @retentionEntityId,
                @category,
                @tenantId,
                @strategy,
                @resolvedPeriod,
                @affected,
                @heldCount,
                @skippedCount,
                @nullAnchorCount,
                @ruleSource,
                @ruleReason
            )
            ON CONFLICT ("SweepId", "RetentionEntityId", "Category", "TenantId", "Strategy")
            DO UPDATE SET
                "EntityType" = EXCLUDED."EntityType",
                "At" = EXCLUDED."At",
                "ResolvedPeriod" = EXCLUDED."ResolvedPeriod",
                "HeldCount" = EXCLUDED."HeldCount",
                "NullAnchorCount" = EXCLUDED."NullAnchorCount",
                "RuleSource" = EXCLUDED."RuleSource",
                "RuleReason" = EXCLUDED."RuleReason"
            """,
            command =>
            {
                command.Parameters.Add(CreateParameter(command, "sweepId", summary.SweepId));
                command.Parameters.Add(CreateParameter(command, "at", summary.At));
                command.Parameters.Add(
                    CreateParameter(command, "entityType", GetEntityTypeName(summary.EntityType))
                );
                command.Parameters.Add(
                    CreateParameter(command, "retentionEntityId", summary.RetentionEntityId)
                );
                command.Parameters.Add(CreateParameter(command, "category", summary.Category));
                command.Parameters.Add(CreateParameter(command, "tenantId", summary.TenantId));
                command.Parameters.Add(CreateParameter(command, "strategy", (int)summary.Strategy));
                command.Parameters.Add(
                    CreateParameter(command, "resolvedPeriod", summary.ResolvedPeriod)
                );
                command.Parameters.Add(CreateParameter(command, "affected", summary.Affected));
                command.Parameters.Add(CreateParameter(command, "heldCount", summary.HeldCount));
                command.Parameters.Add(
                    CreateParameter(command, "skippedCount", summary.SkippedCount)
                );
                command.Parameters.Add(
                    CreateParameter(command, "nullAnchorCount", summary.NullAnchorCount)
                );
                command.Parameters.Add(
                    CreateParameter(command, "ruleSource", summary.Provenance?.Source)
                );
                command.Parameters.Add(
                    CreateParameter(command, "ruleReason", summary.Provenance?.Reason)
                );
            },
            ct
        );
    }

    private Task WriteEntityProgressAsync(SweepEvent.EntityProgress progress, CancellationToken ct)
    {
        return ExecuteAsync(
            $"""
            WITH upserted_summary AS (
            INSERT INTO {QuoteIdentifier(CohortTableNames.SweepRunEntitySummary)} (
                "SweepId", "At", "EntityType", "RetentionEntityId", "Category", "TenantId", "Strategy",
                "ResolvedPeriod", "Affected", "HeldCount", "SkippedCount", "NullAnchorCount",
                "RuleSource", "RuleReason"
            ) VALUES (
                @sweepId, @at, @entityType, @retentionEntityId, @category, @tenantId, @strategy,
                @resolvedPeriod, @affected, 0, @skippedCount, 0, @ruleSource, @ruleReason
            )
            ON CONFLICT ("SweepId", "RetentionEntityId", "Category", "TenantId", "Strategy")
            DO UPDATE SET
                "EntityType" = EXCLUDED."EntityType",
                "Affected" = {QuoteIdentifier(
                CohortTableNames.SweepRunEntitySummary
            )}."Affected" + EXCLUDED."Affected",
                "SkippedCount" = {QuoteIdentifier(
                CohortTableNames.SweepRunEntitySummary
            )}."SkippedCount" + EXCLUDED."SkippedCount"
            RETURNING 1
            )
            UPDATE {QuoteIdentifier(CohortTableNames.SweepRun)}
            SET "TotalAffected" = "TotalAffected" + @affected
            WHERE "SweepId" = @sweepId
              AND EXISTS (SELECT 1 FROM upserted_summary)
            """,
            command =>
            {
                command.Parameters.Add(CreateParameter(command, "sweepId", progress.SweepId));
                command.Parameters.Add(CreateParameter(command, "at", progress.At));
                command.Parameters.Add(
                    CreateParameter(command, "entityType", GetEntityTypeName(progress.EntityType))
                );
                command.Parameters.Add(
                    CreateParameter(command, "retentionEntityId", progress.RetentionEntityId)
                );
                command.Parameters.Add(CreateParameter(command, "category", progress.Category));
                command.Parameters.Add(CreateParameter(command, "tenantId", progress.TenantId));
                command.Parameters.Add(
                    CreateParameter(command, "strategy", (int)progress.Strategy)
                );
                command.Parameters.Add(
                    CreateParameter(command, "resolvedPeriod", progress.ResolvedPeriod)
                );
                command.Parameters.Add(CreateParameter(command, "affected", progress.Affected));
                command.Parameters.Add(
                    CreateParameter(command, "skippedCount", progress.SkippedCount)
                );
                command.Parameters.Add(
                    CreateParameter(command, "ruleSource", progress.Provenance?.Source)
                );
                command.Parameters.Add(
                    CreateParameter(command, "ruleReason", progress.Provenance?.Reason)
                );
            },
            ct
        );
    }

    private Task WriteRowDetailAsync(SweepEvent.RowDetail rowDetail, CancellationToken ct)
    {
        return ExecuteAsync(
            $"""
            INSERT INTO {QuoteIdentifier(CohortTableNames.SweepRunRowDetail)} (
                "SweepId",
                "At",
                "EntityType",
                "RetentionEntityId",
                "EntityId",
                "Category",
                "Strategy",
                "TenantId"
            )
            VALUES (
                @sweepId,
                @at,
                @entityType,
                @retentionEntityId,
                @entityId,
                @category,
                @strategy,
                @tenantId
            )
            """,
            command =>
            {
                command.Parameters.Add(CreateParameter(command, "sweepId", rowDetail.SweepId));
                command.Parameters.Add(CreateParameter(command, "at", rowDetail.At));
                command.Parameters.Add(
                    CreateParameter(command, "entityType", GetEntityTypeName(rowDetail.EntityType))
                );
                command.Parameters.Add(
                    CreateParameter(command, "retentionEntityId", rowDetail.RetentionEntityId)
                );
                command.Parameters.Add(CreateParameter(command, "entityId", rowDetail.EntityId));
                command.Parameters.Add(CreateParameter(command, "category", rowDetail.Category));
                command.Parameters.Add(
                    CreateParameter(command, "strategy", (int)rowDetail.Strategy)
                );
                command.Parameters.Add(CreateParameter(command, "tenantId", rowDetail.TenantId));
            },
            ct
        );
    }

    private Task WriteCompletedAsync(SweepEvent.Completed completed, CancellationToken ct)
    {
        return ExecuteAsync(
            $"""
            UPDATE {QuoteIdentifier(CohortTableNames.SweepRun)}
            SET "Status" = @status,
                "SettledAt" = @completedAt,
                "Duration" = @duration,
                "TotalAffected" = CASE WHEN "DryRun" THEN @totalAffected ELSE "TotalAffected" END
            WHERE "SweepId" = @sweepId
              AND "Status" = @startedStatus
            """,
            command =>
            {
                command.Parameters.Add(CreateParameter(command, "sweepId", completed.SweepId));
                command.Parameters.Add(
                    CreateParameter(command, "status", (int)SweepRunStatus.Succeeded)
                );
                command.Parameters.Add(
                    CreateParameter(command, "startedStatus", (int)SweepRunStatus.Started)
                );
                command.Parameters.Add(CreateParameter(command, "completedAt", completed.At));
                command.Parameters.Add(CreateParameter(command, "duration", completed.Duration));
                command.Parameters.Add(
                    CreateParameter(command, "totalAffected", completed.TotalAffected)
                );
            },
            ct,
            TerminalTransitionError
        );
    }

    private Task WritePartiallyFailedAsync(SweepEvent.PartiallyFailed failed, CancellationToken ct)
    {
        return WriteTerminalAsync(
            failed.SweepId,
            SweepRunStatus.PartiallyFailed,
            failed.At,
            failed.Duration,
            failed.TotalAffected,
            failed.Error,
            ct
        );
    }

    private Task WriteFailedAsync(SweepEvent.Failed failed, CancellationToken ct)
    {
        return ExecuteAsync(
            $"""
            UPDATE {QuoteIdentifier(CohortTableNames.SweepRun)}
            SET "Status" = @status,
                "SettledAt" = @failedAt,
                "Duration" = @duration,
                "TotalAffected" = CASE
                    WHEN "DryRun" THEN COALESCE(@totalAffected, "TotalAffected")
                    ELSE "TotalAffected"
                END,
                "Error" = @error
            WHERE "SweepId" = @sweepId
              AND "Status" = @startedStatus
            """,
            command =>
            {
                command.Parameters.Add(CreateParameter(command, "sweepId", failed.SweepId));
                command.Parameters.Add(
                    CreateParameter(command, "status", (int)SweepRunStatus.Failed)
                );
                command.Parameters.Add(
                    CreateParameter(command, "startedStatus", (int)SweepRunStatus.Started)
                );
                command.Parameters.Add(CreateParameter(command, "failedAt", failed.At));
                command.Parameters.Add(CreateParameter(command, "duration", failed.Duration));
                command.Parameters.Add(
                    CreateParameter(command, "totalAffected", failed.TotalAffected)
                );
                command.Parameters.Add(CreateParameter(command, "error", failed.Error));
            },
            ct,
            TerminalTransitionError
        );
    }

    private Task WriteCancelledAsync(SweepEvent.Cancelled cancelled, CancellationToken ct)
    {
        return WriteTerminalAsync(
            cancelled.SweepId,
            SweepRunStatus.Cancelled,
            cancelled.At,
            cancelled.Duration,
            cancelled.TotalAffected,
            cancelled.Error,
            ct
        );
    }

    private Task WriteTerminalAsync(
        Guid sweepId,
        SweepRunStatus status,
        DateTimeOffset settledAt,
        TimeSpan duration,
        long totalAffected,
        string error,
        CancellationToken ct
    )
    {
        return ExecuteAsync(
            $"""
            UPDATE {QuoteIdentifier(CohortTableNames.SweepRun)}
            SET "Status" = @status,
                "SettledAt" = @settledAt,
                "Duration" = @duration,
                "TotalAffected" = CASE WHEN "DryRun" THEN @totalAffected ELSE "TotalAffected" END,
                "Error" = @error
            WHERE "SweepId" = @sweepId
              AND "Status" = @startedStatus
            """,
            command =>
            {
                command.Parameters.Add(CreateParameter(command, "sweepId", sweepId));
                command.Parameters.Add(CreateParameter(command, "status", (int)status));
                command.Parameters.Add(
                    CreateParameter(command, "startedStatus", (int)SweepRunStatus.Started)
                );
                command.Parameters.Add(CreateParameter(command, "settledAt", settledAt));
                command.Parameters.Add(CreateParameter(command, "duration", duration));
                command.Parameters.Add(CreateParameter(command, "totalAffected", totalAffected));
                command.Parameters.Add(CreateParameter(command, "error", error));
            },
            ct,
            TerminalTransitionError
        );
    }

    private async Task ExecuteAsync(
        string commandText,
        Action<DbCommand> configure,
        CancellationToken ct,
        string? zeroRowsError = null
    )
    {
        var connection = db.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != ConnectionState.Open;

        if (shouldCloseConnection)
        {
            await db.Database.OpenConnectionAsync(ct);
        }

        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Transaction = db.Database.CurrentTransaction?.GetDbTransaction();
            configure(command);
            var affected = await command.ExecuteNonQueryAsync(ct);
            if (affected != 1 && zeroRowsError is not null)
            {
                throw new InvalidOperationException(zeroRowsError);
            }
        }
        finally
        {
            if (shouldCloseConnection)
            {
                await db.Database.CloseConnectionAsync();
            }
        }
    }

    private static DbParameter CreateParameter(DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        return parameter;
    }

    private static string GetEntityTypeName(Type entityType)
    {
        return entityType.FullName ?? entityType.Name;
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }
}
