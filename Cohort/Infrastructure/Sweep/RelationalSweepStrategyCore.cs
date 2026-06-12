using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Infrastructure.Sweep;

/// <summary>
/// Shared SQL execution core for the row-level sweep strategies (purge and
/// soft-delete). The owning strategy supplies the pieces that differ: an extra
/// eligibility clause (e.g. excluding already soft-deleted rows), the mutation
/// statement head (DELETE vs UPDATE ... SET), and any mutation-only parameters.
/// </summary>
internal sealed class RelationalSweepStrategyCore(
    Strategy strategy,
    string strategyName,
    DbContext? db,
    IServiceProvider? services,
    Func<RetentionEntry, string> eligibilityClause,
    Func<RetentionEntry, string> mutationHead,
    Action<DbCommand, RetentionEntry, DateTimeOffset> addMutationParameters
)
{
    private static readonly MethodInfo ExecuteHandlerAwareSweepCoreMethod =
        typeof(RelationalSweepStrategyCore).GetMethod(
            nameof(ExecuteHandlerAwareSweepCoreAsync),
            BindingFlags.Instance | BindingFlags.NonPublic
        )!;

    public async Task<int> PreviewAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(conn);
        EnsureStrategy(rule);
        await EnsureConnectionOpenAsync(conn, ct);

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);

        await using var command = conn.CreateCommand();
        command.CommandText =
            $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND {HoldExclusion(entry)}
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        AddTenantParameter(command, entry, ctx.Tenant.Id);
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));

        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    public async Task<int> CountHeldAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(conn);
        EnsureStrategy(rule);
        await EnsureConnectionOpenAsync(conn, ct);

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);

        await using var command = conn.CreateCommand();
        command.CommandText =
            $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND NOT {HoldExclusion(entry)}
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        AddTenantParameter(command, entry, ctx.Tenant.Id);
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));

        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    public async Task<SweepExecutionResult> SweepAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        CancellationToken ct,
        SweepMutationContext? execution
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(conn);
        ArgumentNullException.ThrowIfNull(transaction);
        EnsureStrategy(rule);
        await EnsureConnectionOpenAsync(conn, ct);

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);
        var candidateRecordIds = await SelectCandidateRecordIdsAsync(
            entry,
            ctx,
            conn,
            transaction,
            cutoff,
            execution?.BatchSize,
            ct
        );

        if (candidateRecordIds.Count == 0)
        {
            return new SweepExecutionResult([], 0);
        }

        var handlers = RetentionHandlerSupport.ResolveHandlers(services, entry.EntityType);
        if (execution is not null && handlers.Count > 0)
        {
            return await ExecuteHandlerAwareSweepAsync(
                entry,
                rule,
                ctx,
                conn,
                transaction,
                candidateRecordIds,
                cutoff,
                handlers,
                execution,
                ct
            );
        }

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            {mutationHead(entry)}
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {HoldExclusion(entry)}
            RETURNING target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        AddTenantParameter(command, entry, ctx.Tenant.Id);
        command.Parameters.Add(CreateParameter(command, "candidateIds", candidateRecordIds.ToArray()));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        addMutationParameters(command, entry, ctx.Now);

        var affectedRecordIds = await ReadRecordIdsAsync(command, ct);

        return new SweepExecutionResult(
            affectedRecordIds,
            candidateRecordIds.Count - affectedRecordIds.Count,
            CandidateCount: candidateRecordIds.Count
        );
    }

    public async Task<int> PreviewEraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(conn);
        EnsureStrategy(rule);
        await EnsureConnectionOpenAsync(conn, ct);

        var cutoff = CutoffCalculator.Compute(now, rule.Period, rule.LegalMin);
        var subjectPredicateSql = BuildSubjectPredicateSql(predicate);
        var candidateRecordIds = await SelectErasureCandidateRecordIdsAsync(
            entry,
            predicate,
            tenant,
            cutoff,
            conn,
            transaction: null,
            batchSize: null,
            ct
        );

        if (candidateRecordIds.Count == 0)
        {
            return 0;
        }

        await using var command = conn.CreateCommand();
        command.CommandText =
            $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE {subjectPredicateSql}
              AND target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {HoldExclusion(entry)}
            """;
        AddTenantParameter(command, entry, tenant.Id);
        AddSubjectParameters(command, predicate);
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        command.Parameters.Add(CreateParameter(command, "candidateIds", candidateRecordIds.ToArray()));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));

        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    public async Task<int> CountHeldForEraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(conn);
        EnsureStrategy(rule);
        await EnsureConnectionOpenAsync(conn, ct);

        var cutoff = CutoffCalculator.Compute(now, rule.Period, rule.LegalMin);

        await using var command = conn.CreateCommand();
        command.CommandText =
            $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE {BuildSubjectPredicateSql(predicate)}
              AND target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND NOT {HoldExclusion(entry)}
            """;
        AddTenantParameter(command, entry, tenant.Id);
        AddSubjectParameters(command, predicate);
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));

        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    public async Task<SweepExecutionResult> EraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        CancellationToken ct,
        SweepMutationContext? execution
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(conn);
        ArgumentNullException.ThrowIfNull(transaction);
        EnsureStrategy(rule);
        await EnsureConnectionOpenAsync(conn, ct);

        var cutoff = CutoffCalculator.Compute(now, rule.Period, rule.LegalMin);
        var subjectPredicateSql = BuildSubjectPredicateSql(predicate);
        var candidateRecordIds = await SelectErasureCandidateRecordIdsAsync(
            entry,
            predicate,
            tenant,
            cutoff,
            conn,
            transaction,
            execution?.BatchSize,
            ct
        );

        if (candidateRecordIds.Count == 0)
        {
            return new SweepExecutionResult([], 0);
        }

        var handlers = RetentionHandlerSupport.ResolveHandlers(services, entry.EntityType);
        if (execution is not null && handlers.Count > 0)
        {
            return await ExecuteHandlerAwareSweepAsync(
                entry,
                rule,
                new RetentionResolutionContext(entry.Category, tenant, now, []),
                conn,
                transaction,
                candidateRecordIds,
                cutoff,
                handlers,
                execution,
                ct
            );
        }

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            {mutationHead(entry)}
            WHERE {subjectPredicateSql}
              AND target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {HoldExclusion(entry)}
            RETURNING target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            """;
        AddTenantParameter(command, entry, tenant.Id);
        AddSubjectParameters(command, predicate);
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        command.Parameters.Add(CreateParameter(command, "candidateIds", candidateRecordIds.ToArray()));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        addMutationParameters(command, entry, now);

        var affectedRecordIds = await ReadRecordIdsAsync(command, ct);

        return new SweepExecutionResult(
            affectedRecordIds,
            candidateRecordIds.Count - affectedRecordIds.Count,
            CandidateCount: candidateRecordIds.Count
        );
    }

    private Task<SweepExecutionResult> ExecuteHandlerAwareSweepAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        DateTimeOffset cutoff,
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        SweepMutationContext execution,
        CancellationToken ct
    )
    {
        return (Task<SweepExecutionResult>)ExecuteHandlerAwareSweepCoreMethod
            .MakeGenericMethod(entry.EntityType)
            .Invoke(
                this,
                [entry, rule, ctx, conn, transaction, candidateRecordIds, cutoff, handlers, execution, ct]
            )!;
    }

    private async Task<SweepExecutionResult> ExecuteHandlerAwareSweepCoreAsync<TEntity>(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        DateTimeOffset cutoff,
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        SweepMutationContext execution,
        CancellationToken ct
    )
        where TEntity : class
    {
        var runtimeDb = db
            ?? throw new InvalidOperationException(
                $"Handler-aware {strategyName} execution for {entry.EntityType.FullName} requires a DbContext-backed strategy instance."
            );
        var rows = await LoadHandlerRowsAsync<TEntity>(
            runtimeDb,
            entry,
            ctx.Tenant,
            conn,
            candidateRecordIds,
            ct
        );
        var recordIdProperty =
            typeof(TEntity).GetProperty(entry.RecordId.RecordIdMember)
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} references missing record-id member '{entry.RecordId.RecordIdMember}'."
            );
        var affectedRecordIds = new List<string>();
        var heldCount = candidateRecordIds.Count - rows.Count;
        var skippedCount = 0;

        foreach (var row in rows)
        {
            var recordId = recordIdProperty.GetValue(row)?.ToString();
            if (string.IsNullOrWhiteSpace(recordId))
            {
                throw new InvalidOperationException(
                    $"Retention row for {entry.EntityType.FullName} produced an empty record id for member '{entry.RecordId.RecordIdMember}'."
                );
            }

            var beforeContext = new RetentionBeforeContext(
                execution.SweepId,
                entry.Category,
                rule.Strategy,
                ctx.Tenant.Id,
                execution.At
            );

            var beforeResult = await RetentionHandlerSupport.InvokeOnBeforeAsync(
                handlers,
                row,
                beforeContext,
                ct
            );
            if (!beforeResult.Succeeded)
            {
                skippedCount++;
                await RetentionHandlerSupport.PersistBeforeFailureAsync(
                    conn,
                    transaction,
                    execution,
                    entry,
                    rule.Strategy,
                    ctx.Tenant.Id,
                    recordId,
                    new Dictionary<string, object?>(beforeContext.Snapshot, StringComparer.Ordinal),
                    beforeResult.FailedHandler!,
                    beforeResult.Failure!,
                    ct
                );
                continue;
            }

            if (!await MutateCapturedRowAsync(entry, ctx, conn, transaction, recordId, cutoff, ct))
            {
                heldCount++;
                continue;
            }

            await RetentionHandlerSupport.PersistCapturedRowAsync(
                conn,
                transaction,
                execution,
                entry,
                rule.Strategy,
                ctx.Tenant.Id,
                recordId,
                new Dictionary<string, object?>(beforeContext.Snapshot, StringComparer.Ordinal),
                handlers,
                ct
            );
            affectedRecordIds.Add(recordId);
        }

        return new SweepExecutionResult(
            affectedRecordIds,
            heldCount,
            RowDetailsPersisted: true,
            SkippedCount: skippedCount,
            CandidateCount: candidateRecordIds.Count
        );
    }

    private async Task<List<TEntity>> LoadHandlerRowsAsync<TEntity>(
        DbContext runtimeDb,
        RetentionEntry entry,
        TenantContext tenant,
        DbConnection conn,
        IReadOnlyList<string> candidateRecordIds,
        CancellationToken ct
    )
        where TEntity : class
    {
        var sql =
            $"""
            SELECT *
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND {HoldExclusion(entry)}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) ASC
            """;
        var parameters = new List<object>
        {
            CreateProviderParameter(conn, "candidateIds", candidateRecordIds.ToArray()),
            CreateProviderParameter(conn, "holdTableName", entry.TableName),
        };
        if (entry.Tenant is not null)
        {
            parameters.Add(CreateProviderParameter(conn, "tenantId", tenant.Id));
        }

        return await runtimeDb.Set<TEntity>()
            .FromSqlRaw(sql, parameters.ToArray())
            .IgnoreQueryFilters()
            .AsNoTracking()
            .ToListAsync(ct);
    }

    private async Task<bool> MutateCapturedRowAsync(
        RetentionEntry entry,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        string recordId,
        DateTimeOffset cutoff,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            {mutationHead(entry)}
            WHERE {RecordIdSql.EqualsParameter("target", entry.RecordId, "recordId")}
              AND target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND {HoldExclusion(entry)}
            RETURNING target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            """;
        command.Parameters.Add(CreateParameter(command, "recordId", recordId));
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        AddTenantParameter(command, entry, ctx.Tenant.Id);
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        addMutationParameters(command, entry, ctx.Now);

        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct);
    }

    private async Task<IReadOnlyList<string>> SelectCandidateRecordIdsAsync(
        RetentionEntry entry,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        DateTimeOffset cutoff,
        int? batchSize,
        CancellationToken ct
    )
    {
        var limitClause = batchSize is not null ? "\nLIMIT @batchSize" : "";

        // Held rows are excluded up front so they are neither locked nor re-selected by
        // every batch; the engine measures them separately for the audit summary.
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            SELECT target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND {HoldExclusion(entry)}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) ASC
            FOR UPDATE SKIP LOCKED{limitClause}
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        AddTenantParameter(command, entry, ctx.Tenant.Id);
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        if (batchSize is not null)
        {
            command.Parameters.Add(
                CreateParameter(command, "batchSize", Math.Max(1, batchSize.Value))
            );
        }

        return await ReadRecordIdsAsync(command, ct);
    }

    private async Task<IReadOnlyList<string>> SelectErasureCandidateRecordIdsAsync(
        RetentionEntry entry,
        ErasureSubjectPredicate predicate,
        TenantContext erasureTenant,
        DateTimeOffset cutoff,
        DbConnection conn,
        DbTransaction? transaction,
        int? batchSize,
        CancellationToken ct
    )
    {
        var limitClause = batchSize is not null ? "\nLIMIT @batchSize" : "";
        // Erasure waits for locked rows (plain FOR UPDATE) instead of skipping them: an
        // erasure request must not silently miss a row a concurrent transaction holds.
        // Previews take no locks at all.
        var lockClause = transaction is not null ? $"\nFOR UPDATE{limitClause}" : limitClause;

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            SELECT target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE {BuildSubjectPredicateSql(predicate)}
              AND target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND {HoldExclusion(entry)}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) ASC{lockClause}
            """;
        AddTenantParameter(command, entry, erasureTenant.Id);
        AddSubjectParameters(command, predicate);
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        if (batchSize is not null)
        {
            command.Parameters.Add(
                CreateParameter(command, "batchSize", Math.Max(1, batchSize.Value))
            );
        }

        return await ReadRecordIdsAsync(command, ct);
    }

    private void EnsureStrategy(RetentionRule rule)
    {
        if (rule.Strategy != strategy)
        {
            throw new InvalidOperationException(
                $"{strategyName} cannot execute {rule.Strategy} rules."
            );
        }
    }

    private static async Task EnsureConnectionOpenAsync(DbConnection conn, CancellationToken ct)
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
            recordIds.Add(reader.GetValue(0).ToString()!);
        }

        return recordIds;
    }

    private static string TenantClause(RetentionEntry entry)
    {
        return entry.Tenant is not null
            ? $"AND target.{QuoteIdentifier(entry.Tenant.TenantColumn)} = @tenantId"
            : "";
    }

    private static string HoldExclusion(RetentionEntry entry)
    {
        return RetentionHoldSql.BuildActiveHoldExclusion(
            "target",
            entry.RecordId.RecordIdColumn,
            entry.Tenant?.TenantColumn
        );
    }

    private static void AddTenantParameter(DbCommand command, RetentionEntry entry, Guid tenantId)
    {
        if (entry.Tenant is not null)
        {
            command.Parameters.Add(CreateParameter(command, "tenantId", tenantId));
        }
    }

    internal static DbParameter CreateParameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        return parameter;
    }

    private static DbParameter CreateProviderParameter(
        DbConnection conn,
        string name,
        object value
    )
    {
        using var command = conn.CreateCommand();
        return CreateParameter(command, name, value);
    }

    internal static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    private static string BuildSubjectPredicateSql(ErasureSubjectPredicate predicate)
    {
        return "("
            + string.Join(
                " OR ",
                predicate.Matches.Select((match, index) =>
                    $"target.{QuoteIdentifier(match.SubjectColumn)} = @subjectValue{index}"
                )
            )
            + ")";
    }

    private static void AddSubjectParameters(DbCommand command, ErasureSubjectPredicate predicate)
    {
        for (var index = 0; index < predicate.Matches.Count; index++)
        {
            command.Parameters.Add(
                CreateParameter(command, $"subjectValue{index}", predicate.Matches[index].SubjectValue)
            );
        }
    }
}
