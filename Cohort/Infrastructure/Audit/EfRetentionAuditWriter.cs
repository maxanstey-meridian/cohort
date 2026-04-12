using System.Data;
using System.Data.Common;

using Cohort.Application;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Cohort.Infrastructure.Audit;

public sealed class EfRetentionAuditWriter(DbContext db) : IRetentionAuditWriter
{
    public Task WriteAsync(SweepEvent evt, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(evt);

        return evt switch
        {
            SweepEvent.Started started => WriteStartedAsync(started, ct),
            SweepEvent.EntitySummary summary => WriteEntitySummaryAsync(summary, ct),
            SweepEvent.RowDetail rowDetail => WriteRowDetailAsync(rowDetail, ct),
            SweepEvent.Completed completed => WriteCompletedAsync(completed, ct),
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
                "CompletedAt",
                "Duration",
                "TriggerKind",
                "DryRun",
                "TenantId",
                "TotalAffected"
            )
            VALUES (
                @sweepId,
                @startedAt,
                NULL,
                NULL,
                @triggerKind,
                @dryRun,
                @tenantId,
                NULL
            )
            """,
            command =>
            {
                command.Parameters.Add(CreateParameter(command, "sweepId", started.SweepId));
                command.Parameters.Add(CreateParameter(command, "startedAt", started.At));
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
                "Category",
                "TenantId",
                "Strategy",
                "ResolvedPeriod",
                "Affected",
                "HeldCount"
            )
            VALUES (
                @sweepId,
                @at,
                @entityType,
                @category,
                @tenantId,
                @strategy,
                @resolvedPeriod,
                @affected,
                @heldCount
            )
            """,
            command =>
            {
                command.Parameters.Add(CreateParameter(command, "sweepId", summary.SweepId));
                command.Parameters.Add(CreateParameter(command, "at", summary.At));
                command.Parameters.Add(
                    CreateParameter(command, "entityType", GetEntityTypeName(summary.EntityType))
                );
                command.Parameters.Add(CreateParameter(command, "category", summary.Category));
                command.Parameters.Add(CreateParameter(command, "tenantId", summary.TenantId));
                command.Parameters.Add(CreateParameter(command, "strategy", (int)summary.Strategy));
                command.Parameters.Add(
                    CreateParameter(command, "resolvedPeriod", summary.ResolvedPeriod)
                );
                command.Parameters.Add(CreateParameter(command, "affected", summary.Affected));
                command.Parameters.Add(CreateParameter(command, "heldCount", summary.HeldCount));
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
                "EntityId",
                "Category",
                "Strategy",
                "TenantId"
            )
            VALUES (
                @sweepId,
                @at,
                @entityType,
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
            SET "CompletedAt" = @completedAt,
                "Duration" = @duration,
                "TotalAffected" = @totalAffected
            WHERE "SweepId" = @sweepId
            """,
            command =>
            {
                command.Parameters.Add(CreateParameter(command, "sweepId", completed.SweepId));
                command.Parameters.Add(CreateParameter(command, "completedAt", completed.At));
                command.Parameters.Add(CreateParameter(command, "duration", completed.Duration));
                command.Parameters.Add(
                    CreateParameter(command, "totalAffected", completed.TotalAffected)
                );
            },
            ct
        );
    }

    private async Task ExecuteAsync(
        string commandText,
        Action<DbCommand> configure,
        CancellationToken ct
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
            await command.ExecuteNonQueryAsync(ct);
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
