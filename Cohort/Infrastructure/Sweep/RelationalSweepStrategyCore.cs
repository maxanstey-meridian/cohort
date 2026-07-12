using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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
    private readonly ILogger? logger = services
        ?.GetService<ILoggerFactory>()
        ?.CreateLogger(typeof(RetentionHandlerSupport).FullName!);
    private static readonly MethodInfo ExecuteHandlerAwareSweepCoreMethod =
        typeof(RelationalSweepStrategyCore).GetMethod(
            nameof(ExecuteHandlerAwareSweepCoreAsync),
            BindingFlags.Instance | BindingFlags.NonPublic
        )!;

    // MakeGenericMethod is allocation-heavy and the entity set is small and fixed;
    // closed methods are cached per entity type for the process lifetime.
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<
        Type,
        MethodInfo
    > HandlerAwareSweepMethods = new();

    public async Task<long> PreviewAsync(
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
        command.CommandText = $"""
            SELECT pg_catalog.count(*)
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND {HoldExclusion(entry)}
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        AddTenantParameter(command, entry, ctx.Tenant.Id);
        command.Parameters.Add(CreateParameter(command, "retentionEntityId", entry.RetentionEntityId));

        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    public async Task<long> CountHeldAsync(
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
        command.CommandText = $"""
            SELECT pg_catalog.count(*)
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND NOT {HoldExclusion(entry)}
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        AddTenantParameter(command, entry, ctx.Tenant.Id);
        command.Parameters.Add(CreateParameter(command, "retentionEntityId", entry.RetentionEntityId));

        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    public async Task<long> CountNullAnchorsAsync(
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

        // No cutoff and no hold exclusion: a NULL anchor never matches any cutoff, and
        // a held NULL-anchor row is just as invisible to retention either way.
        await using var command = conn.CreateCommand();
        command.CommandText = $"""
            SELECT pg_catalog.count(*)
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} IS NULL
              {TenantClause(entry)}
              {eligibilityClause(entry)}
            """;
        AddTenantParameter(command, entry, ctx.Tenant.Id);

        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
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
            execution,
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
                subjectPredicate: null,
                ct
            );
        }

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            {mutationHead(entry)}
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {HoldExclusion(entry)}
            RETURNING {RecordIdSql.TextExpression("target", entry.RecordId)}
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        AddTenantParameter(command, entry, ctx.Tenant.Id);
        command.Parameters.Add(
            CreateParameter(command, "candidateIds", candidateRecordIds.ToArray())
        );
        command.Parameters.Add(CreateParameter(command, "retentionEntityId", entry.RetentionEntityId));
        addMutationParameters(command, entry, ctx.Now);

        var affectedRecordIds = await ReadRecordIdsAsync(command, ct);

        return new SweepExecutionResult(
            affectedRecordIds,
            candidateRecordIds.Count - affectedRecordIds.Count,
            CandidateCount: candidateRecordIds.Count
        );
    }

    public async Task<long> PreviewEraseAsync(
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

        var cutoff = CutoffCalculator.ComputeErasureCutoff(now, rule.LegalMin);
        var subjectPredicateSql = BuildSubjectPredicateSql(predicate);
        var candidateRecordIds = await SelectErasureCandidateRecordIdsAsync(
            entry,
            predicate,
            tenant,
            cutoff,
            conn,
            transaction: null,
            execution: null,
            ct
        );

        if (candidateRecordIds.Count == 0)
        {
            return 0;
        }

        await using var command = conn.CreateCommand();
        command.CommandText = $"""
            SELECT pg_catalog.count(*)
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {subjectPredicateSql}
              {ErasureAnchorClause(entry, cutoff)}
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {HoldExclusion(entry)}
            """;
        AddTenantParameter(command, entry, tenant.Id);
        AddSubjectParameters(command, predicate);
        AddErasureCutoffParameter(command, cutoff);
        command.Parameters.Add(
            CreateParameter(command, "candidateIds", candidateRecordIds.ToArray())
        );
        command.Parameters.Add(CreateParameter(command, "retentionEntityId", entry.RetentionEntityId));

        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    public async Task<long> CountHeldForEraseAsync(
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

        var cutoff = CutoffCalculator.ComputeErasureCutoff(now, rule.LegalMin);

        await using var command = conn.CreateCommand();
        command.CommandText = $"""
            SELECT pg_catalog.count(*)
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {BuildSubjectPredicateSql(predicate)}
              {ErasureAnchorClause(entry, cutoff)}
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND NOT {HoldExclusion(entry)}
            """;
        AddTenantParameter(command, entry, tenant.Id);
        AddSubjectParameters(command, predicate);
        AddErasureCutoffParameter(command, cutoff);
        command.Parameters.Add(CreateParameter(command, "retentionEntityId", entry.RetentionEntityId));

        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    public async Task<long> CountNullAnchorsForEraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
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

        await using var command = conn.CreateCommand();
        command.CommandText = $"""
            SELECT pg_catalog.count(*)
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {BuildSubjectPredicateSql(predicate)}
              AND target.{QuoteIdentifier(entry.AnchorColumn)} IS NULL
              {TenantClause(entry)}
              {eligibilityClause(entry)}
            """;
        AddTenantParameter(command, entry, tenant.Id);
        AddSubjectParameters(command, predicate);

        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
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

        var cutoff = CutoffCalculator.ComputeErasureCutoff(now, rule.LegalMin);
        var subjectPredicateSql = BuildSubjectPredicateSql(predicate);
        var candidateRecordIds = await SelectErasureCandidateRecordIdsAsync(
            entry,
            predicate,
            tenant,
            cutoff,
            conn,
            transaction,
            execution,
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
                predicate,
                ct
            );
        }

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            {mutationHead(entry)}
            WHERE {subjectPredicateSql}
              {ErasureAnchorClause(entry, cutoff)}
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {HoldExclusion(entry)}
            RETURNING {RecordIdSql.TextExpression("target", entry.RecordId)}
            """;
        AddTenantParameter(command, entry, tenant.Id);
        AddSubjectParameters(command, predicate);
        AddErasureCutoffParameter(command, cutoff);
        command.Parameters.Add(
            CreateParameter(command, "candidateIds", candidateRecordIds.ToArray())
        );
        command.Parameters.Add(CreateParameter(command, "retentionEntityId", entry.RetentionEntityId));
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
        DateTimeOffset? cutoff,
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        SweepMutationContext execution,
        ErasureSubjectPredicate? subjectPredicate,
        CancellationToken ct
    )
    {
        var closedMethod = HandlerAwareSweepMethods.GetOrAdd(
            entry.EntityType,
            static entityType => ExecuteHandlerAwareSweepCoreMethod.MakeGenericMethod(entityType)
        );

        return (Task<SweepExecutionResult>)
            closedMethod.Invoke(
                this,
                [
                    entry,
                    rule,
                    ctx,
                    conn,
                    transaction,
                    candidateRecordIds,
                    cutoff,
                    handlers,
                    execution,
                    subjectPredicate,
                    ct,
                ]
            )!;
    }

    private async Task<SweepExecutionResult> ExecuteHandlerAwareSweepCoreAsync<TEntity>(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        DateTimeOffset? cutoff,
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        SweepMutationContext execution,
        ErasureSubjectPredicate? subjectPredicate,
        CancellationToken ct
    )
        where TEntity : class
    {
        var runtimeDb =
            db
            ?? throw new InvalidOperationException(
                $"Handler-aware {strategyName} execution for {entry.EntityType.FullName} requires a DbContext-backed strategy instance."
            );
        var rows = await LoadHandlerRowsAsync<TEntity>(
            runtimeDb,
            entry,
            ctx.Tenant,
            conn,
            candidateRecordIds,
            cutoff,
            subjectPredicate,
            ct
        );
        var recordIdProperty =
            ReflectionMemberResolver.FindPropertyByName(
                typeof(TEntity),
                entry.RecordId.RecordIdMember
            )
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} references missing record-id member '{entry.RecordId.RecordIdMember}'."
            );
        var recordIdConverter = runtimeDb
            .Model.FindEntityType(typeof(TEntity))
            ?.FindProperty(entry.RecordId.RecordIdMember)
            ?.GetTypeMapping()
            .Converter;
        var affectedRecordIds = new List<string>();
        var heldCount = candidateRecordIds.Count - rows.Count;
        var skippedRecordIds = new List<string>();

        foreach (var row in rows)
        {
            var recordIdValue = recordIdProperty.GetValue(row);
            if (recordIdValue is null)
            {
                throw new InvalidOperationException(
                    $"Retention row for {entry.EntityType.FullName} produced an empty record id for member '{entry.RecordId.RecordIdMember}'."
                );
            }
            var recordId = await RecordIdSql.CanonicalizeAsync(
                conn,
                transaction,
                entry.RecordId,
                recordIdConverter?.ConvertToProvider(recordIdValue) ?? recordIdValue,
                ct
            );

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
                skippedRecordIds.Add(recordId);
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
                    logger,
                    ct
                );
                continue;
            }

            if (
                !await MutateCapturedRowAsync(
                    entry,
                    ctx,
                    conn,
                    transaction,
                    recordId,
                    cutoff,
                    subjectPredicate,
                    ct
                )
            )
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
            SkippedCount: skippedRecordIds.Count,
            CandidateCount: candidateRecordIds.Count,
            SkippedRecordIds: skippedRecordIds
        );
    }

    private async Task<List<TEntity>> LoadHandlerRowsAsync<TEntity>(
        DbContext runtimeDb,
        RetentionEntry entry,
        TenantContext tenant,
        DbConnection conn,
        IReadOnlyList<string> candidateRecordIds,
        DateTimeOffset? cutoff,
        ErasureSubjectPredicate? subjectPredicate,
        CancellationToken ct
    )
        where TEntity : class
    {
        var sql = $"""
            SELECT *
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              {SubjectPredicateClause(subjectPredicate)}
              {ErasureAnchorClause(entry, cutoff)}
              AND {HoldExclusion(entry)}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(
                entry.RecordId.RecordIdColumn
            )} AS text) ASC
            """;
        var parameters = new List<object>
        {
            CreateProviderParameter(conn, "candidateIds", candidateRecordIds.ToArray()),
            CreateProviderParameter(conn, "retentionEntityId", entry.RetentionEntityId),
        };
        if (entry.Tenant is not null)
        {
            parameters.Add(CreateProviderParameter(conn, "tenantId", tenant.Id));
        }
        if (subjectPredicate is not null)
        {
            AddSubjectParameters(parameters, conn, subjectPredicate);
        }
        if (cutoff is not null)
        {
            parameters.Add(CreateProviderParameter(conn, "cutoff", cutoff.Value));
        }

        return await runtimeDb
            .Set<TEntity>()
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
        DateTimeOffset? cutoff,
        ErasureSubjectPredicate? subjectPredicate,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            {mutationHead(entry)}
            WHERE {RecordIdSql.EqualsParameter("target", entry.RecordId, "recordId")}
              {ErasureAnchorClause(entry, cutoff)}
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              {SubjectPredicateClause(subjectPredicate)}
              AND {HoldExclusion(entry)}
            RETURNING {RecordIdSql.TextExpression("target", entry.RecordId)}
            """;
        command.Parameters.Add(CreateParameter(command, "recordId", recordId));
        AddErasureCutoffParameter(command, cutoff);
        AddTenantParameter(command, entry, ctx.Tenant.Id);
        if (subjectPredicate is not null)
        {
            AddSubjectParameters(command, subjectPredicate);
        }
        command.Parameters.Add(CreateParameter(command, "retentionEntityId", entry.RetentionEntityId));
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
        SweepMutationContext? execution,
        CancellationToken ct
    )
    {
        // Held rows are excluded up front so they are neither locked nor re-selected by
        // every batch; the engine measures them separately for the audit summary. Rows
        // already skipped by an earlier batch of this run are excluded too — they stay
        // eligible, so reselecting them would re-fail them forever.
        var selectedRecordIds = new List<string>();
        var attemptedRecordIds = new List<string>();
        var targetCount = execution?.BatchSize is null
            ? int.MaxValue
            : Math.Max(1, execution.BatchSize.Value);

        while (selectedRecordIds.Count < targetCount)
        {
            var remaining = execution?.BatchSize is null
                ? (int?)null
                : targetCount - selectedRecordIds.Count;
            var discoveredRecordIds = await DiscoverSweepCandidateRecordIdsAsync(
                entry,
                ctx,
                conn,
                transaction,
                cutoff,
                remaining,
                attemptedRecordIds,
                execution,
                ct
            );
            if (discoveredRecordIds.Count == 0)
            {
                break;
            }

            await RetentionEntityLockSql.AcquireAsync(
                conn,
                transaction,
                entry.RetentionEntityId,
                entry.Tenant is not null ? ctx.Tenant.Id : null,
                discoveredRecordIds,
                ct
            );
            selectedRecordIds.AddRange(
                await LockSweepCandidatesAsync(
                    entry,
                    ctx,
                    conn,
                    transaction,
                    cutoff,
                    discoveredRecordIds,
                    ct
                )
            );
            attemptedRecordIds.AddRange(discoveredRecordIds);
            if (execution?.BatchSize is null || discoveredRecordIds.Count < remaining)
            {
                break;
            }
        }

        return selectedRecordIds;
    }

    private async Task<IReadOnlyList<string>> SelectErasureCandidateRecordIdsAsync(
        RetentionEntry entry,
        ErasureSubjectPredicate predicate,
        TenantContext erasureTenant,
        DateTimeOffset? cutoff,
        DbConnection conn,
        DbTransaction? transaction,
        SweepMutationContext? execution,
        CancellationToken ct
    )
    {
        var discoveredRecordIds = await DiscoverErasureCandidateRecordIdsAsync(
            entry,
            predicate,
            erasureTenant,
            cutoff,
            conn,
            transaction,
            execution?.BatchSize,
            [],
            execution,
            ct
        );
        if (transaction is null)
        {
            return discoveredRecordIds;
        }

        var selectedRecordIds = new List<string>();
        var attemptedRecordIds = new List<string>();
        var targetCount = execution?.BatchSize is null
            ? int.MaxValue
            : Math.Max(1, execution.BatchSize.Value);
        while (discoveredRecordIds.Count > 0)
        {
            await RetentionEntityLockSql.AcquireAsync(
                conn,
                transaction,
                entry.RetentionEntityId,
                entry.Tenant is not null ? erasureTenant.Id : null,
                discoveredRecordIds,
                ct
            );
            selectedRecordIds.AddRange(
                await LockErasureCandidatesAsync(
                    entry,
                    predicate,
                    erasureTenant,
                    cutoff,
                    conn,
                    transaction,
                    discoveredRecordIds,
                    ct
                )
            );
            attemptedRecordIds.AddRange(discoveredRecordIds);
            if (execution?.BatchSize is null || selectedRecordIds.Count >= targetCount)
            {
                break;
            }

            discoveredRecordIds = await DiscoverErasureCandidateRecordIdsAsync(
                entry,
                predicate,
                erasureTenant,
                cutoff,
                conn,
                transaction,
                targetCount - selectedRecordIds.Count,
                attemptedRecordIds,
                execution,
                ct
            );
        }

        return selectedRecordIds;
    }

    private async Task<IReadOnlyList<string>> DiscoverSweepCandidateRecordIdsAsync(
        RetentionEntry entry,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        DateTimeOffset cutoff,
        int? limit,
        IReadOnlyList<string> attemptedRecordIds,
        SweepMutationContext? execution,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            SELECT {RecordIdSql.TextExpression("target", entry.RecordId)}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              {AttemptedRecordIdsClause(entry, attemptedRecordIds)}
              {CommittedRowDetailExclusion(entry, execution)}
              AND {HoldExclusion(entry)}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) ASC
            {(limit is null ? "" : "LIMIT @batchSize")}
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        AddTenantParameter(command, entry, ctx.Tenant.Id);
        command.Parameters.Add(CreateParameter(command, "retentionEntityId", entry.RetentionEntityId));
        AddAttemptedRecordIdsParameter(command, attemptedRecordIds);
        AddCommittedRowDetailParameters(command, entry, ctx.Tenant.Id, execution);
        if (limit is not null)
        {
            command.Parameters.Add(CreateParameter(command, "batchSize", limit.Value));
        }
        return await ReadRecordIdsAsync(command, ct);
    }

    private async Task<IReadOnlyList<string>> DiscoverErasureCandidateRecordIdsAsync(
        RetentionEntry entry,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset? cutoff,
        DbConnection conn,
        DbTransaction? transaction,
        int? limit,
        IReadOnlyList<string> attemptedRecordIds,
        SweepMutationContext? execution,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            SELECT {RecordIdSql.TextExpression("target", entry.RecordId)}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {BuildSubjectPredicateSql(predicate)}
              {ErasureAnchorClause(entry, cutoff)}
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              {AttemptedRecordIdsClause(entry, attemptedRecordIds)}
              {CommittedRowDetailExclusion(entry, execution)}
              AND {HoldExclusion(entry)}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) ASC
            {(limit is null ? "" : "LIMIT @batchSize")}
            """;
        AddTenantParameter(command, entry, tenant.Id);
        AddSubjectParameters(command, predicate);
        AddErasureCutoffParameter(command, cutoff);
        command.Parameters.Add(CreateParameter(command, "retentionEntityId", entry.RetentionEntityId));
        AddAttemptedRecordIdsParameter(command, attemptedRecordIds);
        AddCommittedRowDetailParameters(command, entry, tenant.Id, execution);
        if (limit is not null)
        {
            command.Parameters.Add(CreateParameter(command, "batchSize", Math.Max(1, limit.Value)));
        }
        return await ReadRecordIdsAsync(command, ct);
    }

    private async Task<IReadOnlyList<string>> LockSweepCandidatesAsync(
        RetentionEntry entry,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        DateTimeOffset cutoff,
        IReadOnlyList<string> discoveredRecordIds,
        CancellationToken ct
    )
    {
        if (discoveredRecordIds.Count == 0)
        {
            return discoveredRecordIds;
        }

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            SELECT {RecordIdSql.TextExpression("target", entry.RecordId)}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {HoldExclusion(entry)}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(
                entry.RecordId.RecordIdColumn
            )} AS text) ASC
            FOR UPDATE SKIP LOCKED
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        AddTenantParameter(command, entry, ctx.Tenant.Id);
        command.Parameters.Add(
            CreateParameter(command, "candidateIds", discoveredRecordIds.ToArray())
        );
        command.Parameters.Add(CreateParameter(command, "retentionEntityId", entry.RetentionEntityId));
        return await ReadRecordIdsAsync(command, ct);
    }

    private async Task<IReadOnlyList<string>> LockErasureCandidatesAsync(
        RetentionEntry entry,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset? cutoff,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> discoveredRecordIds,
        CancellationToken ct
    )
    {
        if (discoveredRecordIds.Count == 0)
        {
            return discoveredRecordIds;
        }

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            SELECT {RecordIdSql.TextExpression("target", entry.RecordId)}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {BuildSubjectPredicateSql(predicate)}
              {ErasureAnchorClause(entry, cutoff)}
              {TenantClause(entry)}
              {eligibilityClause(entry)}
              AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {HoldExclusion(entry)}
            ORDER BY target.{QuoteIdentifier(entry.AnchorColumn)} ASC, CAST(target.{QuoteIdentifier(
                entry.RecordId.RecordIdColumn
            )} AS text) ASC
            FOR UPDATE
            """;
        AddTenantParameter(command, entry, tenant.Id);
        AddSubjectParameters(command, predicate);
        AddErasureCutoffParameter(command, cutoff);
        command.Parameters.Add(
            CreateParameter(command, "candidateIds", discoveredRecordIds.ToArray())
        );
        command.Parameters.Add(CreateParameter(command, "retentionEntityId", entry.RetentionEntityId));
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

    private static async Task<List<string>> ReadRecordIdsAsync(
        DbCommand command,
        CancellationToken ct
    )
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
            entry.CohortTables.RetentionHolds,
            "target",
            entry.RecordId.RecordIdColumn,
            entry.Tenant?.TenantColumn
        );
    }

    private static string AttemptedRecordIdsClause(
        RetentionEntry entry,
        IReadOnlyList<string> attemptedRecordIds
    )
    {
        return attemptedRecordIds.Count > 0
            ? $"AND NOT ({RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "attemptedRecordIds")})"
            : "";
    }

    private static void AddAttemptedRecordIdsParameter(
        DbCommand command,
        IReadOnlyList<string> attemptedRecordIds
    )
    {
        if (attemptedRecordIds.Count > 0)
        {
            command.Parameters.Add(
                CreateParameter(command, "attemptedRecordIds", attemptedRecordIds.ToArray())
            );
        }
    }

    private string CommittedRowDetailExclusion(
        RetentionEntry entry,
        SweepMutationContext? execution
    )
    {
        return execution is null
            ? ""
            : $"""
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

    private void AddCommittedRowDetailParameters(
        DbCommand command,
        RetentionEntry entry,
        Guid tenantId,
        SweepMutationContext? execution
    )
    {
        if (execution is null)
        {
            return;
        }

        command.Parameters.Add(CreateParameter(command, "excludedSweepId", execution.SweepId));
        command.Parameters.Add(
            CreateParameter(command, "excludedRetentionEntityId", entry.RetentionEntityId)
        );
        command.Parameters.Add(CreateParameter(command, "excludedCategory", entry.Category));
        command.Parameters.Add(CreateParameter(command, "excludedStrategy", (int)strategy));
        command.Parameters.Add(CreateParameter(command, "excludedTenantId", tenantId));
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

    private static DbParameter CreateProviderParameter(DbConnection conn, string name, object value)
    {
        using var command = conn.CreateCommand();
        return CreateParameter(command, name, value);
    }

    internal static string QuoteIdentifier(string identifier)
    {
        return PostgreSqlIdentifier.Quote(identifier);
    }

    private static string BuildSubjectPredicateSql(ErasureSubjectPredicate predicate)
    {
        return "("
            + string.Join(
                " OR ",
                predicate.Matches.Select(
                    (match, index) =>
                        $"target.{QuoteIdentifier(match.SubjectColumn)} = @subjectValue{index}"
                )
            )
            + ")";
    }

    private static string SubjectPredicateClause(ErasureSubjectPredicate? predicate)
    {
        return predicate is null ? "" : $"AND {BuildSubjectPredicateSql(predicate)}";
    }

    private static string ErasureAnchorClause(RetentionEntry entry, DateTimeOffset? cutoff)
    {
        return cutoff is null
            ? ""
            : $"AND target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff";
    }

    private static void AddErasureCutoffParameter(DbCommand command, DateTimeOffset? cutoff)
    {
        if (cutoff is not null)
        {
            command.Parameters.Add(CreateParameter(command, "cutoff", cutoff.Value));
        }
    }

    private static void AddSubjectParameters(DbCommand command, ErasureSubjectPredicate predicate)
    {
        for (var index = 0; index < predicate.Matches.Count; index++)
        {
            command.Parameters.Add(
                CreateParameter(
                    command,
                    $"subjectValue{index}",
                    predicate.Matches[index].SubjectValue
                )
            );
        }
    }

    private static void AddSubjectParameters(
        ICollection<object> parameters,
        DbConnection connection,
        ErasureSubjectPredicate predicate
    )
    {
        for (var index = 0; index < predicate.Matches.Count; index++)
        {
            parameters.Add(
                CreateProviderParameter(
                    connection,
                    $"subjectValue{index}",
                    predicate.Matches[index].SubjectValue
                )
            );
        }
    }
}
