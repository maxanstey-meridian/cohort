using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure.Audit;

/// <summary>
/// Writes the audit ledger. Statements run on the scoped DbContext, so they join the
/// caller's transaction when there is one. Each statement is built by a static builder
/// (<see cref="StartedStatement"/> and its siblings).
/// </summary>
internal sealed class EfRetentionAuditWriter(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db
)
{
    private CohortStoreTables? tables;

    // Read lazily, after readiness, so a model with no Cohort mapping reaches readiness's refusal.
    private CohortStoreTables Tables => tables ??= CohortStoreTables.FromModel(db.Model);

    public Task WriteAsync(SweepEvent evt, CancellationToken ct)
    {
        return evt switch
        {
            SweepEvent.Started started => ExecuteAsync(StartedStatement(Tables, started), ct),
            SweepEvent.EntityProgress progress => ExecuteAsync(EntityProgressStatement(Tables, progress), ct),
            SweepEvent.EntitySummary summary => ExecuteAsync(EntitySummaryStatement(Tables, summary), ct),
            SweepEvent.RowDetail rowDetail => ExecuteAsync(
                RowDetailStatement(
                    Tables,
                    rowDetail.SweepId,
                    rowDetail.At,
                    rowDetail.EntityType,
                    rowDetail.RetentionEntityId,
                    rowDetail.RecordId,
                    rowDetail.Category,
                    rowDetail.Strategy,
                    rowDetail.TenantId,
                    failure: null
                ),
                ct
            ),
            SweepEvent.Completed e => WriteTerminalAsync(e.SweepId, SweepRunStatus.Succeeded, e.At, e.Duration, e.TotalAffected, null, ct),
            SweepEvent.PartiallyFailed e => WriteTerminalAsync(e.SweepId, SweepRunStatus.PartiallyFailed, e.At, e.Duration, e.TotalAffected, e.Error, ct),
            SweepEvent.Failed e => WriteTerminalAsync(e.SweepId, SweepRunStatus.Failed, e.At, e.Duration, e.TotalAffected, e.Error, ct),
            SweepEvent.Cancelled e => WriteTerminalAsync(e.SweepId, SweepRunStatus.Cancelled, e.At, e.Duration, e.TotalAffected, e.Error, ct),
            _ => throw new InvalidOperationException(
                $"Unsupported sweep event type '{evt.GetType().FullName}'."
            ),
        };
    }

    /// <summary>Records a run as Started.</summary>
    internal static SweepStatement StartedStatement(CohortStoreTables tables, SweepEvent.Started started)
    {
        var sweepRun = PostgreSqlIdentifier.Format(tables.SweepRun);
        return CohortStatements.Statement(
            "run-started",
            $"""
            INSERT INTO {sweepRun}
                ("SweepId", "StartedAt", "Status", "SettledAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected", "ErasureSubjectKind")
            VALUES (@sweepId, @startedAt, @status, NULL, NULL, @triggerKind, @dryRun, @tenantId, 0, @erasureSubjectKind)
            """,
            new SqlParams
            {
                ["sweepId"] = started.SweepId,
                ["startedAt"] = started.At,
                ["status"] = (int)SweepRunStatus.Started,
                ["triggerKind"] = (int)started.Trigger,
                ["dryRun"] = started.DryRun,
                ["tenantId"] = started.TenantId,
                ["erasureSubjectKind"] = started.ErasureSubjectKind,
            },
            tables.SweepRun
        );
    }

    /// <summary>Records an entity's summary, replacing the progress rows written so far.</summary>
    internal static SweepStatement EntitySummaryStatement(CohortStoreTables tables, SweepEvent.EntitySummary summary)
    {
        var entitySummary = PostgreSqlIdentifier.Format(tables.SweepRunEntitySummary);
        return CohortStatements.Statement(
            "entity-summary",
            $"""
            INSERT INTO {entitySummary} AS current_summary
                ("SweepId", "At", "EntityType", "RetentionEntityId", "Category", "TenantId", "Strategy",
                 "ResolvedMonths", "ResolvedDays", "Affected", "HeldCount", "SkippedCount", "NullAnchorCount",
                 "RetainedCount", "DeferredCount", "BlockedCount", "RuleSource", "RuleReason")
            VALUES (@sweepId, @at, @entityType, @retentionEntityId, @category, @tenantId, @strategy,
                    @resolvedMonths, @resolvedDays, @affected, @heldCount, @skippedCount, @nullAnchorCount,
                    @retainedCount, @deferredCount, @blockedCount, @ruleSource, @ruleReason)
            ON CONFLICT ("SweepId", "RetentionEntityId", "Category", "TenantId", "Strategy")
            DO UPDATE SET
                "EntityType" = EXCLUDED."EntityType",
                "At" = EXCLUDED."At",
                "ResolvedMonths" = EXCLUDED."ResolvedMonths",
                "ResolvedDays" = EXCLUDED."ResolvedDays",
                "HeldCount" = EXCLUDED."HeldCount",
                "NullAnchorCount" = EXCLUDED."NullAnchorCount",
                "RetainedCount" = EXCLUDED."RetainedCount",
                "DeferredCount" = EXCLUDED."DeferredCount",
                "BlockedCount" = EXCLUDED."BlockedCount",
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
                ["resolvedMonths"] = summary.ResolvedPeriod.TotalMonths,
                ["resolvedDays"] = summary.ResolvedPeriod.Days,
                ["affected"] = summary.Affected,
                ["heldCount"] = summary.HeldCount,
                ["skippedCount"] = summary.SkippedCount,
                ["nullAnchorCount"] = summary.NullAnchorCount,
                ["retainedCount"] = summary.RetainedCount,
                ["deferredCount"] = summary.DeferredCount,
                ["blockedCount"] = summary.BlockedCount,
                ["ruleSource"] = summary.Provenance?.Source,
                ["ruleReason"] = summary.Provenance?.Reason,
            },
            tables.SweepRunEntitySummary
        );
    }

    /// <summary>
    /// Adds a batch to an entity's summary and to the run's total, in one statement.
    /// </summary>
    internal static SweepStatement EntityProgressStatement(CohortStoreTables tables, SweepEvent.EntityProgress progress)
    {
        var entitySummary = PostgreSqlIdentifier.Format(tables.SweepRunEntitySummary);
        var sweepRun = PostgreSqlIdentifier.Format(tables.SweepRun);
        return CohortStatements.Statement(
            "entity-progress",
            $"""
            WITH upserted_summary AS (
                INSERT INTO {entitySummary} AS current_summary
                    ("SweepId", "At", "EntityType", "RetentionEntityId", "Category", "TenantId", "Strategy",
                     "ResolvedMonths", "ResolvedDays", "Affected", "HeldCount", "SkippedCount", "NullAnchorCount",
                     "RetainedCount", "DeferredCount", "BlockedCount", "RuleSource", "RuleReason")
                VALUES (@sweepId, @at, @entityType, @retentionEntityId, @category, @tenantId, @strategy,
                        @resolvedMonths, @resolvedDays, @affected, 0, @skippedCount, 0,
                        0, 0, @blockedCount, @ruleSource, @ruleReason)
                ON CONFLICT ("SweepId", "RetentionEntityId", "Category", "TenantId", "Strategy")
                DO UPDATE SET
                    "EntityType" = EXCLUDED."EntityType",
                    "Affected" = current_summary."Affected" + EXCLUDED."Affected",
                    "SkippedCount" = current_summary."SkippedCount" + EXCLUDED."SkippedCount",
                    "BlockedCount" = current_summary."BlockedCount" + EXCLUDED."BlockedCount"
                RETURNING 1
            )
            UPDATE {sweepRun}
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
                ["resolvedMonths"] = progress.ResolvedPeriod.TotalMonths,
                ["resolvedDays"] = progress.ResolvedPeriod.Days,
                ["affected"] = progress.Affected,
                ["skippedCount"] = progress.SkippedCount,
                ["blockedCount"] = progress.BlockedCount,
                ["ruleSource"] = progress.Provenance?.Source,
                ["ruleReason"] = progress.Provenance?.Reason,
            },
            tables.SweepRunEntitySummary,
            tables.SweepRun
        );
    }

    /// <summary>
    /// Records one row. <paramref name="recordId"/> is the row's canonical text.
    /// <paramref name="failure"/> is null for a mutated row; for a row a failing dependents
    /// step left unmutated, it says why.
    /// </summary>
    internal static SweepStatement RowDetailStatement(
        CohortStoreTables tables,
        Guid sweepId,
        DateTimeOffset at,
        Type entityType,
        Guid retentionEntityId,
        string recordId,
        string category,
        Strategy strategy,
        Guid tenantId,
        string? failure
    )
    {
        var rowDetail = PostgreSqlIdentifier.Format(tables.SweepRunRowDetail);
        return CohortStatements.Statement(
            "row-detail",
            $"""
            INSERT INTO {rowDetail}
                ("SweepId", "At", "EntityType", "RetentionEntityId", "RecordId", "Category", "Strategy", "TenantId", "Failure")
            VALUES (@sweepId, @at, @entityType, @retentionEntityId, @recordId, @category, @strategy, @tenantId, @failure)
            """,
            new SqlParams
            {
                ["sweepId"] = sweepId,
                ["at"] = at,
                ["entityType"] = EntityTypeName(entityType),
                ["retentionEntityId"] = retentionEntityId,
                ["recordId"] = recordId,
                ["category"] = category,
                ["strategy"] = (int)strategy,
                ["tenantId"] = tenantId,
                ["failure"] = failure,
            },
            tables.SweepRunRowDetail
        );
    }

    /// <summary>
    /// Settles a Started run. A real run's total is the sum of its committed progress, so
    /// only a dry run takes <paramref name="totalAffected"/>.
    /// </summary>
    internal static SweepStatement TerminalStatement(
        CohortStoreTables tables,
        Guid sweepId,
        SweepRunStatus status,
        DateTimeOffset settledAt,
        TimeSpan? duration,
        long? totalAffected,
        string? error
    )
    {
        var sweepRun = PostgreSqlIdentifier.Format(tables.SweepRun);
        return CohortStatements.Statement(
            "run-settled",
            $"""
            UPDATE {sweepRun}
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
            tables.SweepRun
        );
    }

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
            TerminalStatement(Tables, sweepId, status, settledAt, duration, totalAffected, error),
            ct
        );
        if (settled != 1)
        {
            throw new InvalidOperationException(
                "Sweep run does not exist or is no longer in the Started state."
            );
        }
    }

    private Task<int> ExecuteAsync(SweepStatement statement, CancellationToken ct) =>
        db.Database.ExecuteSqlRawAsync(
            statement.Sql,
            statement.Parameters.ToDbParameters(db.Database.GetDbConnection()),
            ct
        );

    private static string EntityTypeName(Type entityType) => entityType.FullName ?? entityType.Name;
}
