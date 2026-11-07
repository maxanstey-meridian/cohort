using System.Data.Common;

using Cohort.Application;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure;

// The only interpolations in this file's raw SQL are Cohort's own table names, quoted from
// the EF model; every value is a named parameter.

/// <summary>
/// The <c>sweep_run</c> ledger reads and writes the scheduled worker and abandoned-run recovery
/// own.
/// </summary>
internal static class RetentionRunLedgerSql
{
    private const string AbandonedRunError = "Run owner exited before writing a terminal audit event.";

    /// <summary>
    /// Marks runs left <c>Started</c> at or before <paramref name="cutoff"/> as failed, but
    /// only after taking their run lock proves no live process still owns them. Each recovery
    /// is logged as it commits, so a later run's failure loses none of them.
    /// </summary>
    internal static async Task RecoverAbandonedAsync(
        DbContext db,
        DateTimeOffset cutoff,
        DateTimeOffset now,
        ILogger logger,
        CancellationToken ct
    )
    {
        var tables = CohortStoreTables.FromModel(db.Model);
        var abandoned = AbandonedRunsStatement(tables, cutoff);
        var staleSweepIds = await db.Database
            .SqlQueryRaw<Guid>(abandoned.Sql, abandoned.Parameters.ToDbParameters(db.Database.GetDbConnection()))
            .ToListAsync(ct);
        if (staleSweepIds.Count == 0)
        {
            return;
        }

        // The run lock is session-scoped, so hold one connection throughout.
        await db.Database.OpenConnectionAsync(ct);
        try
        {
            var connection = db.Database.GetDbConnection();
            foreach (var sweepId in staleSweepIds)
            {
                var key = RetentionRunAdvisoryLock.KeyFor(sweepId);
                if (!await RetentionRunAdvisoryLock.TryAcquireAsync(connection, key, ct))
                {
                    continue;
                }

                try
                {
                    var failed = AbandonedRunFailedStatement(tables, sweepId, now, AbandonedRunError);
                    var updated = await db.Database.ExecuteSqlRawAsync(
                        failed.Sql,
                        failed.Parameters.ToDbParameters(connection),
                        ct
                    );
                    if (updated > 0)
                    {
                        logger.LogWarning("Cohort recovered abandoned retention run {SweepId} as failed.", sweepId);
                    }
                }
                finally
                {
                    await OperationalConnectionCleanup.ReleaseLockAsync(connection, key, "run", sweepId, logger);
                }
            }
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }
    }

    /// <summary>The runs left <c>Started</c> at or before <paramref name="cutoff"/>, oldest first.</summary>
    internal static SweepStatement AbandonedRunsStatement(CohortStoreTables tables, DateTimeOffset cutoff) =>
        CohortStatements.Statement(
            "abandoned-runs",
            $"""
            SELECT "SweepId" AS "Value" FROM {PostgreSqlIdentifier.Format(tables.SweepRun)}
            WHERE "Status" = @started AND "StartedAt" <= @cutoff
            ORDER BY "StartedAt"
            """,
            new SqlParams { ["started"] = (int)SweepRunStatus.Started, ["cutoff"] = cutoff },
            tables.SweepRun
        );

    /// <summary>
    /// Settles an abandoned run as failed, at <paramref name="now"/> (the settled time, and
    /// the start of its duration), with <paramref name="error"/> as its error.
    /// </summary>
    internal static SweepStatement AbandonedRunFailedStatement(
        CohortStoreTables tables,
        Guid sweepId,
        DateTimeOffset now,
        string error
    ) =>
        CohortStatements.Statement(
            "abandoned-run-failed",
            $"""
            UPDATE {PostgreSqlIdentifier.Format(tables.SweepRun)}
            SET "Status" = @failed, "SettledAt" = @now, "Duration" = @now - "StartedAt", "Error" = @error
            WHERE "SweepId" = @sweepId AND "Status" = @started
            """,
            new SqlParams
            {
                ["failed"] = (int)SweepRunStatus.Failed,
                ["now"] = now,
                ["error"] = error,
                ["sweepId"] = sweepId,
                ["started"] = (int)SweepRunStatus.Started,
            },
            tables.SweepRun
        );

    /// <summary>
    /// The scheduled runs of one kind since <paramref name="occurrence"/>, per pass, or for
    /// only <paramref name="tenantId"/>'s pass when one is given (the clause and its parameter
    /// exist only then).
    /// </summary>
    internal static SweepStatement OccurrenceStatement(
        RelationalObjectName sweepRun,
        DateTimeOffset occurrence,
        bool dryRun,
        Guid? tenantId
    )
    {
        // A pass is keyed by its tenant (Guid.Empty for the tenantless pass), and only runs of
        // the same kind count: a replica still configured to dry run must not stand in for the
        // real sweep. A run still Started counts as an attempt; read under its pass lock, it
        // has lost its owner, which would otherwise hold that lock. Cancelled runs (the kill
        // switch, or a shutdown) use no attempt.
        var parameters = new SqlParams
        {
            ["scheduled"] = (int)SweepTriggerKind.Scheduled,
            ["occurrence"] = occurrence,
            ["dryRun"] = dryRun,
            ["succeeded"] = (int)SweepRunStatus.Succeeded,
            ["cancelled"] = (int)SweepRunStatus.Cancelled,
        };
        if (tenantId is { } tenant)
        {
            parameters["tenantId"] = tenant;
        }

        return CohortStatements.Statement(
            "occurrence-ledger",
            $"""
            SELECT "TenantId",
                   pg_catalog.bool_or("Status" = @succeeded) AS completed,
                   pg_catalog.count(*) FILTER (WHERE "Status" <> @cancelled) AS attempts
            FROM {PostgreSqlIdentifier.Format(sweepRun)}
            WHERE "TriggerKind" = @scheduled AND "DryRun" = @dryRun AND "StartedAt" >= @occurrence
            {(tenantId is null ? "" : "AND \"TenantId\" = @tenantId")}
            GROUP BY "TenantId"
            """,
            parameters,
            sweepRun
        );
    }

    /// <summary>
    /// What the ledger holds for one occurrence's scheduled runs of one kind, per pass, or for
    /// only <paramref name="tenantId"/>'s pass when one is given.
    /// </summary>
    internal static async Task<OccurrenceLedger> LoadOccurrenceAsync(
        DbConnection connection,
        RelationalObjectName sweepRun,
        DateTimeOffset occurrence,
        bool dryRun,
        Guid? tenantId,
        CancellationToken ct
    )
    {
        var statement = OccurrenceStatement(sweepRun, occurrence, dryRun, tenantId);
        await using var command = statement.Parameters.CreateCommand(connection, null, statement.Sql);
        var passes = new Dictionary<Guid, (bool Completed, long Attempts)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            passes[reader.GetGuid(0)] = (reader.GetBoolean(1), reader.GetInt64(2));
        }

        return new OccurrenceLedger(passes);
    }
}

/// <summary>One occurrence's scheduled runs of one kind, per pass (keyed by tenant).</summary>
internal sealed class OccurrenceLedger(IReadOnlyDictionary<Guid, (bool Completed, long Attempts)> passes)
{
    public bool IsCompleted(Guid tenantId) => passes.TryGetValue(tenantId, out var pass) && pass.Completed;

    /// <summary>Neither succeeded nor out of attempts.</summary>
    public bool IsPending(Guid tenantId, int maxAttempts) =>
        !passes.TryGetValue(tenantId, out var pass) || (!pass.Completed && pass.Attempts < maxAttempts);
}
