using System.Data;
using System.Data.Common;
using System.Runtime.ExceptionServices;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Audit;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure;

/// <summary>
/// One audited retention run (sweep, dry run or erasure): owns the run lock, the audit
/// trail from Started to a terminal event, per-entity execution, and the result counts.
/// With <paramref name="callerCommit"/> the run joins the transaction open on
/// <paramref name="db"/> instead: it commits nothing, and observers hear of its events only
/// once the caller commits.
/// </summary>
internal sealed class RetentionRun(
    DbContext db,
    EfRetentionAuditWriter auditWriter,
    RetentionAuditNotifier auditNotifier,
    ILogger logger,
    SweepEvent.Started started,
    int batchSize,
    CohortCommitInterceptor? callerCommit = null
)
{
    private const string EntitySavepoint = "cohort_entity";
    private static readonly TimeSpan AuditSettlementTimeout = TimeSpan.FromSeconds(30);

    private readonly List<EntitySweepCount> counts = [];
    private readonly List<string> failures = [];
    // Caller mode only: events written into the caller's transaction, told to observers
    // once it commits.
    private readonly List<SweepEvent> deferredEvents = [];
    private readonly string operation = started.Trigger == SweepTriggerKind.Erasure
        ? "erasure"
        : started.DryRun ? "dry run" : "sweep";
    private DateTimeOffset? settledAt;

    public Guid SweepId => started.SweepId;

    /// <summary>
    /// Takes the run lock, commits Started, runs <paramref name="body"/>, then settles the
    /// run: Completed or PartiallyFailed when the body returns, Cancelled or Failed when it
    /// throws. The lock is taken first so recovery never sees a Started run without a live
    /// owner. In the caller's transaction there is no lock and nothing commits: Started and
    /// the settlement are written into that transaction, and commit or roll back with it.
    /// </summary>
    public async Task ExecuteAsync(Func<Task> body, CancellationToken ct)
    {
        if (callerCommit is not null)
        {
            // No run lock: Started stays invisible to recovery until the caller commits, and
            // by then the run has settled. A session lock would also have to be released
            // inside a transaction that may already be aborted. The ledger table is resolved
            // now, so the post-commit check touches no model metadata.
            var sweepRun = PostgreSqlIdentifier.Format(CohortStoreTables.FromModel(db.Model).SweepRun);
            callerCommit.AfterCommit(
                db.Database.CurrentTransaction!,
                connection => IsCommittedAsync(connection, sweepRun),
                NotifyDeferredAsync,
                committedWithoutRows => logger.LogInformation(
                    committedWithoutRows
                        ? "Cohort {Operation} {SweepId} has no committed ledger row the caller's connection can see: it rolled back with one of the caller's savepoints or an aborted transaction, or the connection's session can't read sweep_run. Its observer events are dropped."
                        : "Cohort {Operation} {SweepId} rolled back with the caller's transaction; no ledger row of the attempt remains, and nothing it changed was kept.",
                    operation,
                    SweepId
                ),
                exception => logger.LogWarning(
                    exception,
                    "Cohort could not confirm that the caller's commit kept {Operation} {SweepId}; dropping its observer events rather than reporting a run that may not have happened.",
                    operation,
                    SweepId
                )
            );
            await RunAndSettleAsync(body, ct);
            return;
        }

        await db.Database.OpenConnectionAsync(ct);
        var connection = db.Database.GetDbConnection();
        var locked = false;
        Exception? primaryException = null;
        try
        {
            await RetentionRunAdvisoryLock.AcquireAsync(connection, RetentionRunAdvisoryLock.KeyFor(SweepId), ct);
            locked = true;
            await RunAndSettleAsync(body, ct);
        }
        catch (Exception ex)
        {
            primaryException = ex;
            throw;
        }
        finally
        {
            await OperationalConnectionCleanup.RunAsync(
                connection,
                locked ? cleanupToken => RetentionRunAdvisoryLock.ReleaseAsync(connection, RetentionRunAdvisoryLock.KeyFor(SweepId), cleanupToken) : null,
                _ => db.Database.CloseConnectionAsync(),
                primaryException,
                logger
            );
        }
    }

    private async Task RunAndSettleAsync(Func<Task> body, CancellationToken ct)
    {
        var startedPersisted = false;
        try
        {
            // In an owned run Started commits on its own, so a run that later fails still
            // leaves evidence. In the caller's transaction it commits or rolls back with the rest.
            await WriteDurableAsync(started, CancellationToken.None);
            startedPersisted = true;

            await body();

            ct.ThrowIfCancellationRequested();
            await SettleAsync(at => failures.Count > 0
                ? new SweepEvent.PartiallyFailed(SweepId, at, at - started.At, AffectedTotal, TruncateError(string.Join("\n", failures)))
                : new SweepEvent.Completed(SweepId, at, at - started.At, AffectedTotal));
        }
        catch (OperationCanceledException ex) when (startedPersisted && ct.IsCancellationRequested)
        {
            var diagnostic = RetentionFailureDiagnostic.Create(ex);
            logger.LogWarning(ex, "Cohort {Operation} {SweepId} was cancelled. Diagnostic {DiagnosticId}.", operation, SweepId, diagnostic.DiagnosticIdText);
            await TrySettleAsync(at => new SweepEvent.Cancelled(SweepId, at, diagnostic.ToString(), at - started.At, AffectedTotal));
            throw;
        }
        catch (Exception ex) when (startedPersisted)
        {
            var diagnostic = RetentionFailureDiagnostic.Create(ex);
            logger.LogError(ex, "Cohort {Operation} {SweepId} failed. Diagnostic {DiagnosticId}.", operation, SweepId, diagnostic.DiagnosticIdText);
            await TrySettleAsync(at => new SweepEvent.Failed(SweepId, at, diagnostic.ToString(), at - started.At, AffectedTotal));
            throw;
        }
    }

    /// <summary>
    /// Runs each planned entity in dependency order. One entity's failure is recorded and
    /// the rest still run.
    /// </summary>
    public async Task RunEntitiesAsync(
        IReadOnlyList<SweepScope> plan,
        IReadOnlyDictionary<Strategy, SweepStrategy> strategies,
        CancellationToken ct
    )
    {
        plan = await ReferencingForeignKeySql.AttachAsync(db.Database.GetDbConnection(), plan, ct);
        foreach (var scope in RetentionExecutionPlanOrderer.Order(db, plan, scope => scope.Entry, logger))
        {
            // In the caller's transaction a failed statement would abort everything after it,
            // so each entity runs under a savepoint and a failure undoes only that entity, as
            // a failed batch transaction does in an owned run.
            var callerTransaction = callerCommit is null ? null : db.Database.CurrentTransaction!.GetDbTransaction();
            var deferredMark = deferredEvents.Count;
            if (callerTransaction is not null)
            {
                // Not cancellable: a cancellation here would abort the caller's transaction
                // before the savepoint that recovers it exists.
                await callerTransaction.SaveAsync(EntitySavepoint, CancellationToken.None);
            }

            try
            {
                await RunEntityAsync(
                    scope.Rule.Strategy == Strategy.Exempt ? null : strategies[scope.Rule.Strategy],
                    scope,
                    ct
                );
                if (callerTransaction is not null)
                {
                    // Not cancellable: a cancelled RELEASE that took effect would leave the
                    // catch rolling back to a savepoint that no longer exists.
                    await callerTransaction.ReleaseAsync(EntitySavepoint, CancellationToken.None);
                }
            }
            catch (Exception ex)
            {
                if (callerTransaction is not null)
                {
                    // A cancelled statement aborts the caller's transaction as a failed one
                    // does; rolling back to the savepoint lets the run still settle in it.
                    await callerTransaction.RollbackAsync(EntitySavepoint, CancellationToken.None);
                    counts.RemoveAll(count => count.EntityType == scope.Entry.EntityType);
                    deferredEvents.RemoveRange(deferredMark, deferredEvents.Count - deferredMark);
                }

                // Only the run's own cancellation stops the run; any other is this entity's
                // failure. So is a handler's change Cohort couldn't undo on the caller's
                // context: an entity failure would leave the caller free to commit.
                if (ex is OperationCanceledException && ct.IsCancellationRequested
                    || ex is OnBeforeChangesNotUndoneException)
                {
                    throw;
                }

                RecordEntityFailure(scope.Entry.EntityType, ex, "failed for");
            }
        }
    }

    public void RecordEntityFailure(Type entityType, Exception exception, string what)
    {
        var diagnostic = RetentionFailureDiagnostic.Create(exception);
        failures.Add(diagnostic.ToString());
        logger.LogError(
            exception,
            "Cohort {Operation} {SweepId} {What} entity {EntityType}; continuing with remaining entities. Diagnostic {DiagnosticId}.",
            operation,
            SweepId,
            what,
            entityType.FullName,
            diagnostic.DiagnosticIdText
        );
    }

    public RetentionSweepResult CreateSweepResult() =>
        new(SweepId, started.At, settledAt!.Value, counts, failures);

    public ErasureResult CreateErasureResult(ErasureScope scope) =>
        new(SweepId, started.At, settledAt!.Value, scope, counts, started.DryRun, failures);

    private long AffectedTotal => counts.Sum(count => count.Affected);

    private async Task RunEntityAsync(SweepStrategy? strategy, SweepScope scope, CancellationToken ct)
    {
        var entry = scope.Entry;
        var rule = scope.Rule;
        var eventAt = await DatabaseClock.NowAsync(db, ct);
        var connection = db.Database.GetDbConnection();
        long affected = 0, skipped = 0, held = 0, nullAnchors = 0, retained = 0, deferred = 0, blocked = 0;

        if (strategy is not null)
        {
            if (started.DryRun)
            {
                affected = await strategy.CountAsync(scope, SweepCount.Eligible, connection, ct);
                blocked = await strategy.CountAsync(scope, SweepCount.Blocked, connection, ct);
            }
            else
            {
                (affected, skipped, var refused) = await ExecuteBatchesAsync(strategy, scope, eventAt, ct);
                // Measured once the loop is done, like held rows: what is still due and was
                // neither changed, skipped nor refused by this run, and what a reference
                // still blocks besides the rows a constraint refused.
                deferred = await strategy.CountDeferredAsync(scope, SweepId, refused, connection, ct);
                var referenced = await strategy.CountBlockedAsync(scope, refused, connection, ct);
                LogReferenced(scope, referenced);
                blocked = refused.Count + referenced;
            }

            // Held rows are measured directly (in scope, eligible, actively held) rather
            // than inferred from candidate arithmetic. Without a cutoff (an erasure with no
            // legal minimum) a NULL anchor excludes nothing, so there is nothing to report.
            held = await strategy.CountAsync(scope, SweepCount.Held, connection, ct);
            if (scope.Cutoff is not null)
            {
                nullAnchors = await strategy.CountAsync(scope, SweepCount.NullAnchor, connection, ct);
                // What an erasure's legal minimum keeps; a sweep's younger rows are simply
                // not due yet.
                if (scope.Subject is not null)
                {
                    retained = await strategy.CountAsync(scope, SweepCount.RetainedByRule, connection, ct);
                }
            }
        }
        else if (scope.Subject is not null)
        {
            // An Exempt rule keeps every one of the subject's rows from the erasure.
            retained = await SweepStrategy.CountSubjectRowsAsync(scope, connection, ct);
        }

        var count = new EntitySweepCount(entry.EntityType, entry.Category, scope.Tenant.Id, rule.Strategy, affected, held, skipped, nullAnchors, retained, deferred, blocked);
        // Mutated rows count even if the summary write fails; a dry run's prediction
        // counts only once its summary is recorded.
        if (!started.DryRun)
        {
            ReplaceCount(count);
        }
        await WriteDurableAsync(
            new SweepEvent.EntitySummary(
                SweepId,
                eventAt,
                entry.EntityType,
                entry.RetentionEntityId,
                entry.Category,
                scope.Tenant.Id,
                rule.Strategy,
                scope.ResolvedPeriod,
                affected,
                held,
                skipped,
                nullAnchors,
                retained,
                deferred,
                blocked,
                rule.Provenance
            ),
            ct
        );
        if (started.DryRun)
        {
            ReplaceCount(count);
        }

        RecordRowsLeft(count);
    }

    /// <summary>
    /// One warning for rows left out because a foreign key still references them, naming the
    /// keys that can (schema metadata) but no row. Rows a constraint refused when tried are
    /// logged by the strategy, per constraint.
    /// </summary>
    private void LogReferenced(SweepScope scope, long referenced)
    {
        if (referenced == 0)
        {
            return;
        }

        logger.LogWarning(
            "Cohort {Operation} {SweepId} left {Count} row(s) of entity {EntityType} unchanged: a row still references them through one of {ConstraintNames}. They count as blocked and are retried by the next run.",
            operation,
            SweepId,
            referenced,
            scope.Entry.EntityType.FullName,
            string.Join(", ", scope.BlockingReferences.Select(reference => reference.ConstraintName))
        );
    }

    /// <summary>
    /// Turns rows the run should have changed but didn't into run failures, so the run
    /// settles PartiallyFailed and <c>EntityFailures</c> is empty exactly when the ledger says
    /// Succeeded. Skipped rows always count; deferred and blocked rows count only for a real
    /// erasure, since a sweep's next run retires them by design and a dry run changes
    /// nothing (its <c>Complete</c> says what the real run would leave). The reasons are
    /// Cohort-defined plain codes and carry no row data.
    /// </summary>
    private void RecordRowsLeft(EntitySweepCount count)
    {
        var erasure = started.Trigger == SweepTriggerKind.Erasure && !started.DryRun;
        RecordRowsLeft(count.EntityType, "rows-skipped", count.SkippedCount);
        if (erasure)
        {
            RecordRowsLeft(count.EntityType, "rows-missed", count.DeferredCount);
            RecordRowsLeft(count.EntityType, "rows-blocked", count.BlockedCount);
        }
    }

    private void RecordRowsLeft(Type entityType, string reason, long rows)
    {
        if (rows == 0)
        {
            return;
        }

        failures.Add($"reason={reason};entityType={entityType.FullName};count={rows}");
        logger.LogWarning(
            "Cohort {Operation} {SweepId} left {Count} row(s) of entity {EntityType} in scope ({Reason}).",
            operation,
            SweepId,
            rows,
            entityType.FullName,
            reason
        );
    }

    /// <summary>
    /// Mutates in batches, each selecting, locking and changing at most one batch of rows in
    /// its own transaction, so a large backlog retires incrementally and a failure loses only
    /// the current batch. In the caller's transaction the batches still bound each statement,
    /// but none commits on its own: the entity's savepoint and the caller's commit decide.
    /// Rows a constraint refuses are blocked and left out of later batches; a full batch in
    /// which every row is refused fails the entity, since the cause is then the rule or the
    /// schema rather than the rows, and bisecting the rest of the table would find the same.
    /// </summary>
    private async Task<(long Affected, long Skipped, IReadOnlyCollection<string> Blocked)> ExecuteBatchesAsync(
        SweepStrategy strategy,
        SweepScope scope,
        DateTimeOffset entityAt,
        CancellationToken ct
    )
    {
        var entry = scope.Entry;
        var rule = scope.Rule;
        var perRowAudit =
            (entry.AuditRowDetail == AuditRowDetail.Inherit ? rule.AuditRowDetail : entry.AuditRowDetail)
            == AuditRowDetail.PerRow;
        long affected = 0, skipped = 0;
        var blocked = new HashSet<string>(StringComparer.Ordinal);
        DateTimeOffset? nextBatchAt = entityAt;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            SweepExecutionResult execution;
            List<SweepEvent> committed = [];
            // READ COMMITTED: the lock query and mutation must see a hold committed after
            // discovery. In the caller's transaction (READ COMMITTED, checked on entry) a batch
            // neither begins nor commits; it commits with the caller.
            await using (var batchTransaction = callerCommit is null
                ? await db.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct)
                : null)
            {
                if (batchTransaction is not null)
                {
                    // Deferred constraints fail inside the statement that broke them, where the
                    // row can be blocked, rather than failing the whole batch at COMMIT. Only
                    // in Cohort's own transaction: in the caller's it would also check the
                    // caller's pending work early and change the mode of everything after.
                    await using var immediate = new SqlParams().CreateCommand(
                        db.Database.GetDbConnection(),
                        batchTransaction.GetDbTransaction(),
                        "SET CONSTRAINTS ALL IMMEDIATE"
                    );
                    await immediate.ExecuteNonQueryAsync(ct);
                }

                // The batch's rows and events carry the database's time; the first batch
                // reuses the time the entity just read.
                var eventAt = nextBatchAt ?? await DatabaseClock.NowAsync(db, ct);
                nextBatchAt = null;
                execution = await strategy.ExecuteAsync(
                    scope,
                    db.Database.GetDbConnection(),
                    (batchTransaction ?? db.Database.CurrentTransaction!).GetDbTransaction(),
                    new SweepMutationContext(SweepId, eventAt, batchSize, blocked, OnCallersContext: callerCommit is not null),
                    ct
                );
                if (
                    execution.CandidateCount == batchSize
                    && execution.BlockedRecordIds.Count == execution.CandidateCount
                    && execution.BlockFailure is { } refusal
                )
                {
                    ExceptionDispatchInfo.Throw(refusal);
                }

                // Row-detail audit commits with the batch whose mutations it documents: a
                // crash later in the run cannot lose evidence for rows already changed, and
                // affected ids never accumulate in memory.
                if (perRowAudit)
                {
                    foreach (var recordId in execution.AffectedRecordIds)
                    {
                        var rowDetail = new SweepEvent.RowDetail(SweepId, eventAt, entry.EntityType, entry.RetentionEntityId, recordId, entry.Category, rule.Strategy, scope.Tenant.Id);
                        if (!execution.RowDetailsPersisted)
                        {
                            await auditWriter.WriteAsync(rowDetail, ct);
                        }
                        committed.Add(rowDetail);
                    }
                }

                var progress = new SweepEvent.EntityProgress(
                    SweepId,
                    eventAt,
                    entry.EntityType,
                    entry.RetentionEntityId,
                    entry.Category,
                    scope.Tenant.Id,
                    rule.Strategy,
                    scope.ResolvedPeriod,
                    execution.AffectedRecordIds.Count,
                    execution.SkippedCount,
                    execution.BlockedRecordIds.Count,
                    rule.Provenance
                );
                await auditWriter.WriteAsync(progress, ct);
                committed.Add(progress);

                if (batchTransaction is not null)
                {
                    await batchTransaction.CommitAsync(ct);
                }
            }

            foreach (var evt in committed)
            {
                await NotifyAsync(evt);
            }

            affected += execution.AffectedRecordIds.Count;
            skipped += execution.SkippedCount;
            blocked.UnionWith(execution.BlockedRecordIds);
            ReplaceCount(new EntitySweepCount(entry.EntityType, entry.Category, scope.Tenant.Id, rule.Strategy, affected, 0, skipped, 0, 0, 0, blocked.Count));

            // A short batch means the scope is exhausted. A full batch that neither mutated,
            // skipped nor blocked anything would reselect the same rows forever; the
            // remainder is left for the next run.
            if (
                execution.CandidateCount < batchSize
                || (execution.AffectedRecordIds.Count == 0 && execution.SkippedRecordIds.Count == 0 && execution.BlockedRecordIds.Count == 0)
            )
            {
                return (affected, skipped, blocked);
            }
        }
    }

    private void ReplaceCount(EntitySweepCount count)
    {
        var index = counts.FindIndex(existing =>
            existing.EntityType == count.EntityType
            && existing.Category == count.Category
            && existing.TenantId == count.TenantId
            && existing.Strategy == count.Strategy
        );
        if (index < 0)
        {
            counts.Add(count);
        }
        else
        {
            counts[index] = count;
        }
    }

    private async Task WriteDurableAsync(SweepEvent evt, CancellationToken ct)
    {
        using var timeout = ct.CanBeCanceled ? null : new CancellationTokenSource(AuditSettlementTimeout);
        await auditWriter.WriteAsync(evt, timeout?.Token ?? ct);
        await NotifyAsync(evt);
    }

    private async Task NotifyAsync(SweepEvent evt)
    {
        if (callerCommit is null)
        {
            await auditNotifier.NotifyCommittedAsync(evt);
        }
        else
        {
            deferredEvents.Add(evt);
        }
    }

    /// <summary>
    /// Asked on the caller's connection once its transaction has committed, outside any
    /// transaction: the run's ledger row exists only if the commit kept the run, rather than
    /// a rollback to a savepoint taken before it, or an aborted transaction, discarding it.
    /// </summary>
    private async Task<bool> IsCommittedAsync(DbConnection connection, string sweepRun)
    {
        await using var command = new SqlParams { ["sweepId"] = SweepId }.CreateCommand(
            connection,
            null,
            $"SELECT EXISTS (SELECT 1 FROM {sweepRun} WHERE \"SweepId\" = @sweepId)"
        );
        return await command.ExecuteScalarAsync(CancellationToken.None) is true;
    }

    private async Task NotifyDeferredAsync()
    {
        foreach (var evt in deferredEvents)
        {
            await auditNotifier.NotifyCommittedAsync(evt);
        }
    }

    /// <summary>
    /// Writes the terminal event, stamped with the database's time like every other event of
    /// the run. Bounded by the settlement timeout rather than the run's token, so a cancelled
    /// run still settles.
    /// </summary>
    private async Task SettleAsync(Func<DateTimeOffset, SweepEvent> terminal)
    {
        using var timeout = new CancellationTokenSource(AuditSettlementTimeout);
        var at = await DatabaseClock.NowAsync(db, timeout.Token);
        var evt = terminal(at);
        await auditWriter.WriteAsync(evt, timeout.Token);
        await NotifyAsync(evt);
        settledAt = at;
    }

    private async Task TrySettleAsync(Func<DateTimeOffset, SweepEvent> terminal)
    {
        try
        {
            await SettleAsync(terminal);
        }
        catch (Exception settlementException)
        {
            logger.LogError(settlementException, "Cohort could not mark {Operation} {SweepId} as terminal.", operation, SweepId);
        }
    }

    internal static string TruncateError(string value)
    {
        const int maxLength = 4000;
        if (value.Length <= maxLength)
        {
            return value;
        }

        var lastCompleteDiagnostic = value.LastIndexOf('\n', maxLength - 1, maxLength);
        return lastCompleteDiagnostic > 0 ? value[..lastCompleteDiagnostic] : value[..maxLength];
    }
}
