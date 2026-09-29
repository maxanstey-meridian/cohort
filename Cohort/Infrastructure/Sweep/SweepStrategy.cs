using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Reflection;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Sweep;

// Raw SQL here interpolates only identifiers quoted from the EF model and validated store
// types; every value is a named parameter.
#pragma warning disable EF1002

/// <summary>
/// The one SQL pipeline behind every strategy. A <see cref="SweepScope"/> says which rows
/// are in play; the strategy says which of those it has not yet handled
/// (<see cref="EligibilitySql"/>) and how it changes one (<see cref="CreateMutation"/>).
/// Candidates are discovered, entity-locked and row-locked, then mutated in one statement,
/// or row by row when handlers run or the new values depend on each row.
/// </summary>
internal abstract class SweepStrategy(DbContext db, IServiceProvider services, ILogger logger)
{
    private static readonly MethodInfo RowByRowMethod = typeof(SweepStrategy).GetMethod(
        nameof(ExecuteRowByRowAsync),
        BindingFlags.Instance | BindingFlags.NonPublic
    )!;
    private static readonly ConcurrentDictionary<Type, MethodInfo> RowByRowMethods = new();

    public abstract Strategy HandlesStrategy { get; }

    /// <summary>
    /// Predicate over <c>target</c> for rows this strategy has not already handled, or ""
    /// when every row in scope qualifies.
    /// </summary>
    protected abstract string EligibilitySql(RetentionEntry entry);

    protected abstract SweepMutation CreateMutation(SweepScope scope);

    /// <summary>Rejects entries this strategy cannot act on.</summary>
    protected virtual void Validate(RetentionEntry entry) { }

    public async Task<long> CountAsync(
        SweepScope scope,
        SweepCount count,
        DbConnection conn,
        CancellationToken ct
    )
    {
        EnsureHandles(scope);
        await EnsureOpenAsync(conn, ct);

        var parameters = new SqlParams();
        var where = count switch
        {
            SweepCount.Eligible => $"{ScopeSql(scope, parameters)} AND {HoldExclusion(scope, parameters)}",
            SweepCount.Held => $"{ScopeSql(scope, parameters)} AND NOT {HoldExclusion(scope, parameters)}",
            // No hold exclusion: a held NULL-anchor row is just as invisible to retention.
            SweepCount.NullAnchor => ScopeSql(scope, parameters, nullAnchors: true),
            _ => throw new ArgumentOutOfRangeException(nameof(count), count, null),
        };
        await using var command = parameters.CreateCommand(
            conn,
            null,
            $"SELECT pg_catalog.count(*) FROM {PostgreSqlIdentifier.Format(scope.Entry.Table)} AS target WHERE {where}"
        );
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    public async Task<SweepExecutionResult> ExecuteAsync(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext? execution,
        CancellationToken ct
    )
    {
        EnsureHandles(scope);
        await EnsureOpenAsync(conn, ct);

        var candidates = await LockCandidatesAsync(scope, conn, transaction, execution, ct);
        if (candidates.Count == 0)
        {
            return new SweepExecutionResult([], 0);
        }

        var entry = scope.Entry;
        var mutation = CreateMutation(scope);
        var handlers = execution is null
            ? []
            : RetentionHandlerSupport.ResolveHandlers(services, entry.EntityType);
        if (handlers.Count == 0 && !mutation.RowByRow)
        {
            var parameters = new SqlParams { ["candidateIds"] = candidates.ToArray() };
            mutation.AddParameters(parameters, null);
            await using var command = parameters.CreateCommand(
                conn,
                transaction,
                $"""
                {mutation.HeadSql}
                WHERE {ScopeSql(scope, parameters)}
                  AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
                  AND {HoldExclusion(scope, parameters)}
                RETURNING {RecordIdSql.TextExpression("target", entry.RecordId)}
                """
            );
            var affected = await ReadRecordIdsAsync(command, ct);
            return new SweepExecutionResult(
                affected,
                candidates.Count - affected.Count,
                CandidateCount: candidates.Count
            );
        }

        var rowByRow = RowByRowMethods.GetOrAdd(
            entry.EntityType,
            static entityType => RowByRowMethod.MakeGenericMethod(entityType)
        );
        return await (Task<SweepExecutionResult>)
            rowByRow.Invoke(
                this,
                [scope, conn, transaction, candidates, mutation, handlers, execution, ct]
            )!;
    }

    private async Task<SweepExecutionResult> ExecuteRowByRowAsync<TEntity>(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidates,
        SweepMutation mutation,
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        SweepMutationContext? execution,
        CancellationToken ct
    )
        where TEntity : class
    {
        var entry = scope.Entry;
        var load = new SqlParams { ["candidateIds"] = candidates.ToArray() };
        var rows = await db.Set<TEntity>()
            .FromSqlRaw(
                $"""
                SELECT {string.Join(", ", entry.MaterializationColumns.Select(column => $"target.{PostgreSqlIdentifier.Quote(column)}"))}
                FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
                WHERE {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
                  AND {ScopeSql(scope, load)}
                  AND {HoldExclusion(scope, load)}
                ORDER BY {OrderBySql(entry)}
                """,
                load.ToDbParameters(conn)
            )
            .IgnoreQueryFilters()
            .AsNoTracking()
            .ToListAsync(ct);

        var recordIdProperty =
            ReflectionMemberResolver.FindPropertyByName(typeof(TEntity), entry.RecordId.RecordIdMember)
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} references missing record-id member '{entry.RecordId.RecordIdMember}'."
            );
        var recordIdConverter = db.Model.FindEntityType(typeof(TEntity))
            ?.FindProperty(entry.RecordId.RecordIdMember)
            ?.GetTypeMapping()
            .Converter;

        var affected = new List<string>();
        var skipped = new List<string>();
        var heldCount = candidates.Count - rows.Count;
        foreach (var row in rows)
        {
            var recordIdValue =
                recordIdProperty.GetValue(row)
                ?? throw new InvalidOperationException(
                    $"Retention row for {entry.EntityType.FullName} produced an empty record id for member '{entry.RecordId.RecordIdMember}'."
                );
            var recordKey = recordIdConverter?.ConvertToProvider(recordIdValue) ?? recordIdValue;

            RetentionBeforeContext? before = null;
            if (handlers.Count > 0)
            {
                before = new RetentionBeforeContext(
                    execution!.SweepId,
                    entry.Category,
                    scope.Rule.Strategy,
                    scope.Tenant.Id,
                    execution.At
                );
                var result = await RetentionHandlerSupport.InvokeOnBeforeAsync(handlers, row, before, ct);
                if (!result.Succeeded)
                {
                    var skippedId = await RecordIdSql.CanonicalizeAsync(
                        conn,
                        transaction,
                        entry.RecordId,
                        recordKey,
                        ct
                    );
                    skipped.Add(skippedId);
                    await RetentionHandlerSupport.PersistBeforeFailureAsync(
                        conn,
                        transaction,
                        execution,
                        entry,
                        scope.Rule.Strategy,
                        scope.Tenant.Id,
                        skippedId,
                        new Dictionary<string, object?>(before.Snapshot, StringComparer.Ordinal),
                        result.FailedHandler!,
                        result.Failure!,
                        logger,
                        ct
                    );
                    continue;
                }
            }

            var update = new SqlParams { ["recordKey"] = recordKey };
            mutation.AddParameters(update, row);
            await using var command = update.CreateCommand(
                conn,
                transaction,
                $"""
                {mutation.HeadSql}
                WHERE {RecordIdSql.EqualsParameter("target", entry.RecordId, "recordKey")}
                  AND {ScopeSql(scope, update)}
                  AND {HoldExclusion(scope, update)}
                RETURNING {RecordIdSql.TextExpression("target", entry.RecordId)}
                """
            );
            // No row back: a hold landed after the row was loaded.
            if (await command.ExecuteScalarAsync(ct) is not string recordId)
            {
                heldCount++;
                continue;
            }

            if (before is not null)
            {
                await RetentionHandlerSupport.PersistCapturedRowAsync(
                    conn,
                    transaction,
                    execution!,
                    entry,
                    scope.Rule.Strategy,
                    scope.Tenant.Id,
                    recordId,
                    new Dictionary<string, object?>(before.Snapshot, StringComparer.Ordinal),
                    handlers,
                    ct
                );
            }
            affected.Add(recordId);
        }

        return new SweepExecutionResult(
            affected,
            heldCount,
            RowDetailsPersisted: handlers.Count > 0,
            SkippedCount: skipped.Count,
            CandidateCount: candidates.Count,
            SkippedRecordIds: skipped
        );
    }

    /// <summary>
    /// Selects up to one batch of candidates and locks them: the entity lock serialises
    /// against hold creation, the row lock against other runs. Held rows are excluded up
    /// front so they are neither locked nor reselected by every batch; rows an earlier
    /// batch of this run already recorded (e.g. skipped by a failing OnBefore) are excluded
    /// too, since they stay eligible and would otherwise be re-failed forever.
    /// </summary>
    private async Task<IReadOnlyList<string>> LockCandidatesAsync(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext? execution,
        CancellationToken ct
    )
    {
        var entry = scope.Entry;
        var locked = new List<string>();
        var attempted = new List<string>();
        var target = execution?.BatchSize ?? int.MaxValue;
        while (locked.Count < target)
        {
            int? limit = execution?.BatchSize is null ? null : target - locked.Count;
            var discovered = await DiscoverAsync(scope, conn, transaction, execution, attempted, limit, ct);
            if (discovered.Count == 0)
            {
                break;
            }

            await RetentionEntityLockSql.AcquireAsync(
                conn,
                transaction,
                entry.RetentionEntityId,
                entry.Tenant is not null ? scope.Tenant.Id : null,
                discovered,
                ct
            );
            var parameters = new SqlParams { ["candidateIds"] = discovered.ToArray() };
            await using var command = parameters.CreateCommand(
                conn,
                transaction,
                $"""
                SELECT {RecordIdSql.TextExpression("target", entry.RecordId)}
                FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
                WHERE {ScopeSql(scope, parameters)}
                  AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
                  AND {HoldExclusion(scope, parameters)}
                ORDER BY {OrderBySql(entry)}
                FOR UPDATE{(scope.SkipLocked ? " SKIP LOCKED" : "")}
                """
            );
            locked.AddRange(await ReadRecordIdsAsync(command, ct));
            attempted.AddRange(discovered);
            if (limit is null || discovered.Count < limit)
            {
                break;
            }
        }

        return locked;
    }

    private async Task<List<string>> DiscoverAsync(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext? execution,
        IReadOnlyList<string> attempted,
        int? limit,
        CancellationToken ct
    )
    {
        var entry = scope.Entry;
        var parameters = new SqlParams();
        var exclusions = "";
        if (attempted.Count > 0)
        {
            parameters["attemptedRecordIds"] = attempted.ToArray();
            exclusions += $"\n  AND NOT ({RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "attemptedRecordIds")})";
        }
        if (execution is not null)
        {
            parameters["excludedSweepId"] = execution.SweepId;
            parameters["excludedRetentionEntityId"] = entry.RetentionEntityId;
            parameters["excludedCategory"] = entry.Category;
            parameters["excludedStrategy"] = (int)HandlesStrategy;
            parameters["excludedTenantId"] = scope.Tenant.Id;
            exclusions += $"""

                  AND NOT EXISTS (
                      SELECT 1
                      FROM {PostgreSqlIdentifier.Format(entry.CohortTables.SweepRunRowDetail)} AS prior_detail
                      WHERE prior_detail."SweepId" = @excludedSweepId
                        AND prior_detail."RetentionEntityId" = @excludedRetentionEntityId
                        AND prior_detail."RecordId" = {RecordIdSql.TextExpression("target", entry.RecordId)}
                        AND prior_detail."Category" = @excludedCategory
                        AND prior_detail."Strategy" = @excludedStrategy
                        AND prior_detail."TenantId" = @excludedTenantId
                  )
                """;
        }
        if (limit is not null)
        {
            parameters["batchSize"] = limit.Value;
        }

        await using var command = parameters.CreateCommand(
            conn,
            transaction,
            $"""
            SELECT {RecordIdSql.TextExpression("target", entry.RecordId)}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {ScopeSql(scope, parameters)}{exclusions}
              AND {HoldExclusion(scope, parameters)}
            ORDER BY {OrderBySql(entry)}
            {(limit is null ? "" : "LIMIT @batchSize")}
            """
        );
        return await ReadRecordIdsAsync(command, ct);
    }

    /// <summary>
    /// The scope's rows, restricted to the tenant and to rows this strategy has not handled.
    /// With <paramref name="nullAnchors"/>, rows whose anchor is NULL replace the cutoff.
    /// </summary>
    private string ScopeSql(SweepScope scope, SqlParams parameters, bool nullAnchors = false)
    {
        var entry = scope.Entry;
        var anchor = $"target.{PostgreSqlIdentifier.Quote(entry.AnchorColumn)}";
        var clauses = new List<string>();
        if (scope.Subject is { } subject)
        {
            var matches = new List<string>();
            for (var index = 0; index < subject.Matches.Count; index++)
            {
                parameters[$"subjectValue{index}"] = subject.Matches[index].SubjectValue;
                matches.Add(subject.Matches[index].EqualsParameterSql("target", $"subjectValue{index}"));
            }
            clauses.Add($"({string.Join(" OR ", matches)})");
        }
        if (nullAnchors)
        {
            clauses.Add($"{anchor} IS NULL");
        }
        else if (scope.Cutoff is { } cutoff)
        {
            parameters["cutoff"] = cutoff;
            clauses.Add($"{anchor} < @cutoff");
        }
        if (entry.Tenant is { } tenant)
        {
            parameters["tenantId"] = scope.Tenant.Id;
            clauses.Add($"target.{PostgreSqlIdentifier.Quote(tenant.TenantColumn)} = @tenantId");
        }
        if (EligibilitySql(entry) is { Length: > 0 } eligibility)
        {
            clauses.Add(eligibility);
        }

        return clauses.Count == 0 ? "TRUE" : string.Join(" AND ", clauses);
    }

    private static string HoldExclusion(SweepScope scope, SqlParams parameters)
    {
        var entry = scope.Entry;
        parameters["retentionEntityId"] = entry.RetentionEntityId;
        if (entry.Tenant is not null)
        {
            parameters["tenantId"] = scope.Tenant.Id;
        }

        return RetentionHoldSql.BuildActiveHoldExclusion(
            entry.CohortTables.RetentionHolds,
            "target",
            entry.RecordId.RecordIdColumn,
            entry.Tenant?.TenantColumn
        );
    }

    private static string OrderBySql(RetentionEntry entry) =>
        $"target.{PostgreSqlIdentifier.Quote(entry.AnchorColumn)} ASC, {RecordIdSql.TextExpression("target", entry.RecordId)} ASC";

    private void EnsureHandles(SweepScope scope)
    {
        if (scope.Rule.Strategy != HandlesStrategy)
        {
            throw new InvalidOperationException(
                $"{GetType().Name} cannot execute {scope.Rule.Strategy} rules."
            );
        }

        Validate(scope.Entry);
    }

    private static async Task EnsureOpenAsync(DbConnection conn, CancellationToken ct)
    {
        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }
    }

    private static async Task<List<string>> ReadRecordIdsAsync(DbCommand command, CancellationToken ct)
    {
        var recordIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            recordIds.Add(reader.GetString(0));
        }

        return recordIds;
    }
}

/// <summary>
/// How a strategy changes rows: a statement head aliased as <c>target</c>, the values it
/// binds (for one loaded row, or for every row when <c>row</c> is null), and whether the
/// values depend on the row so rows must be loaded and changed one at a time.
/// </summary>
internal sealed record SweepMutation(
    string HeadSql,
    Action<SqlParams, object?> AddParameters,
    bool RowByRow = false
);
