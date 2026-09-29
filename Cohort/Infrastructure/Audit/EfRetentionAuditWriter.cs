using Cohort.Application;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure.Audit;

// Cohort's run tables may be adopted by host entity types, so they are written with raw SQL
// rather than typed LINQ. Interpolations are table names quoted from the EF model only.
#pragma warning disable EF1002

/// <summary>
/// Writes the audit ledger. Statements run on the scoped DbContext, so they join the
/// caller's transaction when there is one.
/// </summary>
internal sealed class EfRetentionAuditWriter(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db
)
{
    private readonly CohortStoreTables tables = CohortStoreTables.FromModel(db.Model);

    public Task WriteAsync(SweepEvent evt, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(evt);

        return evt switch
        {
            SweepEvent.Started started => WriteStartedAsync(started, ct),
            SweepEvent.EntityProgress progress => WriteEntityProgressAsync(progress, ct),
            SweepEvent.EntitySummary summary => WriteEntitySummaryAsync(summary, ct),
            SweepEvent.RowDetail rowDetail => WriteRowDetailAsync(rowDetail, ct),
            SweepEvent.Completed e => WriteTerminalAsync(e.SweepId, SweepRunStatus.Succeeded, e.At, e.Duration, e.TotalAffected, null, ct),
            SweepEvent.PartiallyFailed e => WriteTerminalAsync(e.SweepId, SweepRunStatus.PartiallyFailed, e.At, e.Duration, e.TotalAffected, e.Error, ct),
            SweepEvent.Failed e => WriteTerminalAsync(e.SweepId, SweepRunStatus.Failed, e.At, e.Duration, e.TotalAffected, e.Error, ct),
            SweepEvent.Cancelled e => WriteTerminalAsync(e.SweepId, SweepRunStatus.Cancelled, e.At, e.Duration, e.TotalAffected, e.Error, ct),
            _ => throw new InvalidOperationException(
                $"Unsupported sweep event type '{evt.GetType().FullName}'."
            ),
        };
    }

    private Task WriteStartedAsync(SweepEvent.Started started, CancellationToken ct)
    {
        return ExecuteAsync(
            $"""
            INSERT INTO {PostgreSqlIdentifier.Format(tables.SweepRun)}
                ("SweepId", "StartedAt", "Status", "SettledAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected")
            VALUES (@sweepId, @startedAt, @status, NULL, NULL, @triggerKind, @dryRun, @tenantId, 0)
            """,
            new SqlParams
            {
                ["sweepId"] = started.SweepId,
                ["startedAt"] = started.At,
                ["status"] = (int)SweepRunStatus.Started,
                ["triggerKind"] = (int)started.Trigger,
                ["dryRun"] = started.DryRun,
                ["tenantId"] = started.TenantId,
            },
            ct
        );
    }

    private Task WriteEntitySummaryAsync(SweepEvent.EntitySummary summary, CancellationToken ct)
    {
        return ExecuteAsync(
            $"""
            INSERT INTO {PostgreSqlIdentifier.Format(tables.SweepRunEntitySummary)} AS current_summary
                ("SweepId", "At", "EntityType", "RetentionEntityId", "Category", "TenantId", "Strategy",
                 "ResolvedPeriod", "Affected", "HeldCount", "SkippedCount", "NullAnchorCount", "RuleSource", "RuleReason")
            VALUES (@sweepId, @at, @entityType, @retentionEntityId, @category, @tenantId, @strategy,
                    @resolvedPeriod, @affected, @heldCount, @skippedCount, @nullAnchorCount, @ruleSource, @ruleReason)
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
            new SqlParams
            {
                ["sweepId"] = summary.SweepId,
                ["at"] = summary.At,
                ["entityType"] = EntityTypeName(summary.EntityType),
                ["retentionEntityId"] = summary.RetentionEntityId,
                ["category"] = summary.Category,
                ["tenantId"] = summary.TenantId,
                ["strategy"] = (int)summary.Strategy,
                ["resolvedPeriod"] = summary.ResolvedPeriod,
                ["affected"] = summary.Affected,
                ["heldCount"] = summary.HeldCount,
                ["skippedCount"] = summary.SkippedCount,
                ["nullAnchorCount"] = summary.NullAnchorCount,
                ["ruleSource"] = summary.Provenance?.Source,
                ["ruleReason"] = summary.Provenance?.Reason,
            },
            ct
        );
    }

    private Task WriteEntityProgressAsync(SweepEvent.EntityProgress progress, CancellationToken ct)
    {
        return ExecuteAsync(
            $"""
            WITH upserted_summary AS (
                INSERT INTO {PostgreSqlIdentifier.Format(tables.SweepRunEntitySummary)} AS current_summary
                    ("SweepId", "At", "EntityType", "RetentionEntityId", "Category", "TenantId", "Strategy",
                     "ResolvedPeriod", "Affected", "HeldCount", "SkippedCount", "NullAnchorCount", "RuleSource", "RuleReason")
                VALUES (@sweepId, @at, @entityType, @retentionEntityId, @category, @tenantId, @strategy,
                        @resolvedPeriod, @affected, 0, @skippedCount, 0, @ruleSource, @ruleReason)
                ON CONFLICT ("SweepId", "RetentionEntityId", "Category", "TenantId", "Strategy")
                DO UPDATE SET
                    "EntityType" = EXCLUDED."EntityType",
                    "Affected" = current_summary."Affected" + EXCLUDED."Affected",
                    "SkippedCount" = current_summary."SkippedCount" + EXCLUDED."SkippedCount"
                RETURNING 1
            )
            UPDATE {PostgreSqlIdentifier.Format(tables.SweepRun)}
            SET "TotalAffected" = "TotalAffected" + @affected
            WHERE "SweepId" = @sweepId
              AND EXISTS (SELECT 1 FROM upserted_summary)
            """,
            new SqlParams
            {
                ["sweepId"] = progress.SweepId,
                ["at"] = progress.At,
                ["entityType"] = EntityTypeName(progress.EntityType),
                ["retentionEntityId"] = progress.RetentionEntityId,
                ["category"] = progress.Category,
                ["tenantId"] = progress.TenantId,
                ["strategy"] = (int)progress.Strategy,
                ["resolvedPeriod"] = progress.ResolvedPeriod,
                ["affected"] = progress.Affected,
                ["skippedCount"] = progress.SkippedCount,
                ["ruleSource"] = progress.Provenance?.Source,
                ["ruleReason"] = progress.Provenance?.Reason,
            },
            ct
        );
    }

    private Task WriteRowDetailAsync(SweepEvent.RowDetail rowDetail, CancellationToken ct)
    {
        return ExecuteAsync(
            $"""
            INSERT INTO {PostgreSqlIdentifier.Format(tables.SweepRunRowDetail)}
                ("SweepId", "At", "EntityType", "RetentionEntityId", "RecordId", "Category", "Strategy", "TenantId")
            VALUES (@sweepId, @at, @entityType, @retentionEntityId, @recordId, @category, @strategy, @tenantId)
            """,
            new SqlParams
            {
                ["sweepId"] = rowDetail.SweepId,
                ["at"] = rowDetail.At,
                ["entityType"] = EntityTypeName(rowDetail.EntityType),
                ["retentionEntityId"] = rowDetail.RetentionEntityId,
                ["recordId"] = rowDetail.RecordId,
                ["category"] = rowDetail.Category,
                ["strategy"] = (int)rowDetail.Strategy,
                ["tenantId"] = rowDetail.TenantId,
            },
            ct
        );
    }

    /// <summary>
    /// Settles a Started run. A real run's total is the sum of its committed progress, so
    /// only a dry run takes the event's total.
    /// </summary>
    private async Task WriteTerminalAsync(
        Guid sweepId,
        SweepRunStatus status,
        DateTimeOffset settledAt,
        TimeSpan? duration,
        long? totalAffected,
        string? error,
        CancellationToken ct
    )
    {
        var settled = await ExecuteAsync(
            $"""
            UPDATE {PostgreSqlIdentifier.Format(tables.SweepRun)}
            SET "Status" = @status,
                "SettledAt" = @settledAt,
                "Duration" = @duration,
                "TotalAffected" = CASE WHEN "DryRun" THEN COALESCE(@totalAffected, "TotalAffected") ELSE "TotalAffected" END,
                "Error" = @error
            WHERE "SweepId" = @sweepId
              AND "Status" = @startedStatus
            """,
            new SqlParams
            {
                ["sweepId"] = sweepId,
                ["status"] = (int)status,
                ["startedStatus"] = (int)SweepRunStatus.Started,
                ["settledAt"] = settledAt,
                ["duration"] = duration,
                ["totalAffected"] = totalAffected,
                ["error"] = error,
            },
            ct
        );
        if (settled != 1)
        {
            throw new InvalidOperationException(
                "Sweep run does not exist or is no longer in the Started state."
            );
        }
    }

    private Task<int> ExecuteAsync(string sql, SqlParams parameters, CancellationToken ct) =>
        db.Database.ExecuteSqlRawAsync(sql, parameters.ToDbParameters(db.Database.GetDbConnection()), ct);

    private static string EntityTypeName(Type entityType) => entityType.FullName ?? entityType.Name;
}
