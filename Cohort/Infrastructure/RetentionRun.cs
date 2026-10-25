using System.Data;
using System.Data.Common;

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
            // now, so the commit-time check touches no model metadata.
            var sweepRun = PostgreSqlIdentifier.Format(CohortStoreTables.FromModel(db.Model).SweepRun);
            callerCommit.AfterCommit(
                db.Database.CurrentTransaction!,
                transaction => IsStillWrittenAsync(transaction, sweepRun),
                NotifyDeferredAsync,
                () => logger.LogInformation(
                    "Cohort {Operation} {SweepId} rolled back with the caller's transaction or one of its savepoints; no ledger row of the attempt remains, and nothing it changed was kept.",
                    operation,
                    SweepId
                ),
                exception => logger.LogWarning(
                    exception,
                    "Cohort could not confirm, before the caller's transaction committed, that {Operation} {SweepId} was still part of it; its observers are told of it if the transaction commits.",
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

                if (ex is OperationCanceledException)
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
        long affected = 0, skipped = 0, held = 0, nullAnchors = 0;

        if (strategy is not null)
        {
            if (started.DryRun)
            {
                affected = await strategy.CountAsync(scope, SweepCount.Eligible, connection, ct);
            }
            else
            {
                (affected, skipped) = await ExecuteBatchesAsync(strategy, scope, eventAt, ct);
            }

            // Held rows are measured directly (in scope, eligible, actively held) rather
            // than inferred from candidate arithmetic. Without a cutoff (an erasure with no
            // legal minimum) a NULL anchor excludes nothing, so there is nothing to report.
            held = await strategy.CountAsync(scope, SweepCount.Held, connection, ct);
            if (scope.Cutoff is not null)
            {
                nullAnchors = await strategy.CountAsync(scope, SweepCount.NullAnchor, connection, ct);
            }
        }

        var count = new EntitySweepCount(entry.EntityType, entry.Category, scope.Tenant.Id, rule.Strategy, affected, held, skipped, nullAnchors);
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
                rule.Provenance
            ),
            ct
        );
        if (started.DryRun)
        {
            ReplaceCount(count);
        }
    }

    /// <summary>
    /// Mutates in batches, each selecting, locking and changing at most one batch of rows in
    /// its own transaction, so a large backlog retires incrementally and a failure loses only
    /// the current batch. In the caller's transaction the batches still bound each statement,
    /// but none commits on its own: the entity's savepoint and the caller's commit decide.
    /// </summary>
    private async Task<(long Affected, long Skipped)> ExecuteBatchesAsync(
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
                // The batch's rows and events carry the database's time; the first batch
                // reuses the time the entity just read.
                var eventAt = nextBatchAt ?? await DatabaseClock.NowAsync(db, ct);
                nextBatchAt = null;
                execution = await strategy.ExecuteAsync(
                    scope,
                    db.Database.GetDbConnection(),
                    (batchTransaction ?? db.Database.CurrentTransaction!).GetDbTransaction(),
                    new SweepMutationContext(SweepId, eventAt, batchSize),
                    ct
                );

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
            ReplaceCount(new EntitySweepCount(entry.EntityType, entry.Category, scope.Tenant.Id, rule.Strategy, affected, 0, skipped, 0));

            // A short batch means the scope is exhausted. A full batch that neither mutated
            // nor skipped anything would reselect the same rows forever; the remainder is
            // left for the next run.
            if (
                execution.CandidateCount < batchSize
                || (execution.AffectedRecordIds.Count == 0 && execution.SkippedRecordIds.Count == 0)
            )
            {
                return (affected, skipped);
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
    /// Asked inside the caller's transaction just before it commits: whether the run's ledger
    /// row is still there, or a rollback to a savepoint taken before the run removed it.
    /// </summary>
    private async Task<bool> IsStillWrittenAsync(DbTransaction transaction, string sweepRun)
    {
        await using var command = new SqlParams { ["sweepId"] = SweepId }.CreateCommand(
            transaction.Connection!,
            transaction,
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
