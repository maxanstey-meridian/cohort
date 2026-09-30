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
/// </summary>
internal sealed class RetentionRun(
    DbContext db,
    EfRetentionAuditWriter auditWriter,
    RetentionAuditNotifier auditNotifier,
    ILogger logger,
    SweepEvent.Started started,
    int batchSize
)
{
    private static readonly TimeSpan AuditSettlementTimeout = TimeSpan.FromSeconds(30);

    private readonly List<EntitySweepCount> counts = [];
    private readonly List<string> failures = [];
    private readonly string operation = started.Trigger == SweepTriggerKind.Erasure
        ? "erasure"
        : started.DryRun ? "dry run" : "sweep";
    private DateTimeOffset? settledAt;

    public Guid SweepId => started.SweepId;

    /// <summary>
    /// Takes the run lock, commits Started, runs <paramref name="body"/>, then settles the
    /// run: Completed or PartiallyFailed when the body returns, Cancelled or Failed when it
    /// throws. The lock is taken first so recovery never sees a Started run without a live
    /// owner.
    /// </summary>
    public async Task ExecuteAsync(Func<Task> body, CancellationToken ct)
    {
        await db.Database.OpenConnectionAsync(ct);
        var connection = db.Database.GetDbConnection();
        var locked = false;
        var startedPersisted = false;
        Exception? primaryException = null;
        try
        {
            await RetentionRunAdvisoryLock.AcquireAsync(connection, RetentionRunAdvisoryLock.KeyFor(SweepId), ct);
            locked = true;
            // Started commits on its own, so a run that later fails still leaves evidence.
            await WriteDurableAsync(started, CancellationToken.None);
            startedPersisted = true;

            await body();

            ct.ThrowIfCancellationRequested();
            var at = DateTimeOffset.UtcNow;
            await WriteDurableAsync(
                failures.Count > 0
                    ? new SweepEvent.PartiallyFailed(SweepId, at, at - started.At, AffectedTotal, TruncateError(string.Join("\n", failures)))
                    : new SweepEvent.Completed(SweepId, at, at - started.At, AffectedTotal),
                CancellationToken.None
            );
            settledAt = at;
        }
        catch (OperationCanceledException ex) when (startedPersisted && ct.IsCancellationRequested)
        {
            primaryException = ex;
            var diagnostic = RetentionFailureDiagnostic.Create(ex);
            logger.LogWarning(ex, "Cohort {Operation} {SweepId} was cancelled. Diagnostic {DiagnosticId}.", operation, SweepId, diagnostic.DiagnosticIdText);
            var at = DateTimeOffset.UtcNow;
            await TrySettleAsync(new SweepEvent.Cancelled(SweepId, at, diagnostic.ToString(), at - started.At, AffectedTotal));
            throw;
        }
        catch (Exception ex) when (startedPersisted)
        {
            primaryException = ex;
            var diagnostic = RetentionFailureDiagnostic.Create(ex);
            logger.LogError(ex, "Cohort {Operation} {SweepId} failed. Diagnostic {DiagnosticId}.", operation, SweepId, diagnostic.DiagnosticIdText);
            var at = DateTimeOffset.UtcNow;
            await TrySettleAsync(new SweepEvent.Failed(SweepId, at, diagnostic.ToString(), at - started.At, AffectedTotal));
            throw;
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
            try
            {
                await RunEntityAsync(
                    scope.Rule.Strategy == Strategy.Exempt ? null : strategies[scope.Rule.Strategy],
                    scope,
                    ct
                );
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
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
        var eventAt = DateTimeOffset.UtcNow;
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
    /// the current batch.
    /// </summary>
    private async Task<(long Affected, long Skipped)> ExecuteBatchesAsync(
        SweepStrategy strategy,
        SweepScope scope,
        DateTimeOffset eventAt,
        CancellationToken ct
    )
    {
        var entry = scope.Entry;
        var rule = scope.Rule;
        var perRowAudit =
            (entry.AuditRowDetail == AuditRowDetail.Inherit ? rule.AuditRowDetail : entry.AuditRowDetail)
            == AuditRowDetail.PerRow;
        long affected = 0, skipped = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            SweepExecutionResult execution;
            List<SweepEvent> committed = [];
            await using (var transaction = await db.Database.BeginTransactionAsync(ct))
            {
                execution = await strategy.ExecuteAsync(
                    scope,
                    db.Database.GetDbConnection(),
                    transaction.GetDbTransaction(),
                    new SweepMutationContext(SweepId, DateTimeOffset.UtcNow, batchSize),
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

                await transaction.CommitAsync(ct);
            }

            foreach (var evt in committed)
            {
                await auditNotifier.NotifyCommittedAsync(evt);
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
        await auditNotifier.NotifyCommittedAsync(evt);
    }

    private async Task TrySettleAsync(SweepEvent terminal)
    {
        try
        {
            await WriteDurableAsync(terminal, CancellationToken.None);
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
