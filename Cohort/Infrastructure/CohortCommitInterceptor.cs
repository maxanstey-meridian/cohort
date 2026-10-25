using System.Data.Common;
using System.Runtime.CompilerServices;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure;

/// <summary>
/// Runs work Cohort deferred until a caller's transaction commits (an erasure joined with
/// <c>ErasureTransaction.Caller</c> tells its audit observers only then), and drops it when
/// the transaction rolls back or fails, or when the work's own rows are gone by the time it
/// commits (the caller rolled back to a savepoint taken before it). EF has no other commit
/// hook. <c>AddCohort</c> adds one instance to the host's DbContext options.
/// </summary>
internal sealed class CohortCommitInterceptor(ILogger<CohortCommitInterceptor> logger)
    : DbTransactionInterceptor
{
    // Npgsql recycles transaction objects, so each entry also names the EF transaction it
    // belongs to: work deferred on a transaction that ended unseen (disposed without commit
    // or rollback) never runs on a later transaction that reuses the object.
    private readonly ConditionalWeakTable<DbTransaction, Deferred> pending = new();

    // The Action strings EF's RelationalTransaction passes to TransactionFailed for a failed
    // Commit or Rollback; its savepoint operations pass others ("CreateSavepoint", ...).
    private const string CommitAction = "Commit";
    private const string RollbackAction = "Rollback";

    // Taken around the commit check, so a check that fails can't abort the caller's
    // transaction and turn its COMMIT into a silent rollback.
    private const string CheckSavepoint = "cohort_commit_check";

    /// <summary>Whether this interceptor is part of <paramref name="db"/>'s options.</summary>
    internal bool IsRegisteredOn(DbContext db) =>
        db.GetService<IDbContextOptions>()
            .FindExtension<CoreOptionsExtension>()
            ?.Interceptors?.Contains(this) == true;

    /// <summary>
    /// Runs <paramref name="committed"/> once <paramref name="transaction"/> commits, or
    /// <paramref name="dropped"/> if it rolls back or fails instead. Just before the commit,
    /// <paramref name="stillWritten"/> asks, inside the transaction, whether the work's rows
    /// survived: a rollback to a savepoint taken before them leaves the transaction
    /// committable without them. EF tells interceptors of a savepoint rollback but not which
    /// savepoint, and a raw <c>ROLLBACK TO SAVEPOINT</c> isn't seen at all, so the rows are
    /// the only reliable answer. The check runs under its own savepoint and is not
    /// cancellable; when it fails the work is kept and <paramref name="unconfirmed"/> is told.
    /// </summary>
    internal void AfterCommit(
        IDbContextTransaction transaction,
        Func<DbTransaction, Task<bool>> stillWritten,
        Func<Task> committed,
        Action dropped,
        Action<Exception> unconfirmed
    )
    {
        var dbTransaction = transaction.GetDbTransaction();
        lock (pending)
        {
            if (!pending.TryGetValue(dbTransaction, out var deferred) || deferred.TransactionId != transaction.TransactionId)
            {
                deferred = new Deferred(transaction.TransactionId);
                pending.AddOrUpdate(dbTransaction, deferred);
            }

            deferred.Work.Add(new DeferredWork(stillWritten, committed, dropped, unconfirmed));
        }
    }

    // Deliberately blocking: EF's synchronous commit has no async hook. The wait is bounded by
    // the stillWritten query and AuditObservers:Timeout; CommitAsync is the supported path.
    public override InterceptionResult TransactionCommitting(
        DbTransaction transaction,
        TransactionEventData eventData,
        InterceptionResult result
    )
    {
        DropUnwrittenAsync(transaction, eventData.TransactionId).GetAwaiter().GetResult();
        return result;
    }

    // The commit's token is deliberately not passed on: a cancelled check would still have
    // to be rolled back, and the commit itself decides whether the caller's work survives.
    public override async ValueTask<InterceptionResult> TransactionCommittingAsync(
        DbTransaction transaction,
        TransactionEventData eventData,
        InterceptionResult result,
        CancellationToken cancellationToken = default
    )
    {
        await DropUnwrittenAsync(transaction, eventData.TransactionId);
        return result;
    }

    // Deliberately blocking, as for TransactionCommitting: delivery is bounded by
    // AuditObservers:Timeout, and CommitAsync is the supported path.
    public override void TransactionCommitted(DbTransaction transaction, TransactionEndEventData eventData) =>
        RunAsync(transaction, eventData.TransactionId).GetAwaiter().GetResult();

    public override Task TransactionCommittedAsync(
        DbTransaction transaction,
        TransactionEndEventData eventData,
        CancellationToken cancellationToken = default
    ) => RunAsync(transaction, eventData.TransactionId);

    public override void TransactionRolledBack(DbTransaction transaction, TransactionEndEventData eventData) =>
        Drop(transaction, eventData.TransactionId);

    public override Task TransactionRolledBackAsync(
        DbTransaction transaction,
        TransactionEndEventData eventData,
        CancellationToken cancellationToken = default
    )
    {
        Drop(transaction, eventData.TransactionId);
        return Task.CompletedTask;
    }

    // A failed commit may not have committed; deferred work is best effort, so it is dropped.
    // A failed savepoint operation leaves the transaction open and possibly still committable,
    // so its work waits for the commit's check.
    public override void TransactionFailed(DbTransaction transaction, TransactionErrorEventData eventData)
    {
        if (EndsTransaction(eventData))
        {
            Drop(transaction, eventData.TransactionId);
        }
    }

    public override Task TransactionFailedAsync(
        DbTransaction transaction,
        TransactionErrorEventData eventData,
        CancellationToken cancellationToken = default
    )
    {
        TransactionFailed(transaction, eventData);
        return Task.CompletedTask;
    }

    private static bool EndsTransaction(TransactionErrorEventData eventData) =>
        eventData.Action is CommitAction or RollbackAction;

    private async Task DropUnwrittenAsync(DbTransaction transaction, Guid transactionId)
    {
        List<DeferredWork> work;
        lock (pending)
        {
            if (!pending.TryGetValue(transaction, out var deferred) || deferred.TransactionId != transactionId)
            {
                return;
            }

            work = [.. deferred.Work];
        }

        foreach (var item in work)
        {
            if (!await IsStillWrittenAsync(transaction, item))
            {
                lock (pending)
                {
                    if (pending.TryGetValue(transaction, out var deferred))
                    {
                        deferred.Work.Remove(item);
                    }
                }

                Report(item);
            }
        }
    }

    /// <summary>
    /// Runs <paramref name="item"/>'s check under <see cref="CheckSavepoint"/>, so the
    /// caller's commit neither fails nor silently rolls back because of it. A transaction
    /// that can't take the savepoint is already aborted and will not commit, so its work is
    /// dropped. A check that fails after the savepoint can't say whether the work's rows
    /// survived; the work is kept, since its observers are best effort and the ledger the
    /// transaction commits is the record.
    /// </summary>
    private async Task<bool> IsStillWrittenAsync(DbTransaction transaction, DeferredWork item)
    {
        try
        {
            await transaction.SaveAsync(CheckSavepoint, CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Cohort could not check whether work deferred until a caller's transaction committed was still part of it; the transaction cannot commit, so the work is dropped.");
            return false;
        }

        try
        {
            var written = await item.StillWritten(transaction);
            await transaction.ReleaseAsync(CheckSavepoint, CancellationToken.None);
            return written;
        }
        catch (Exception checkException)
        {
            try
            {
                await transaction.RollbackAsync(CheckSavepoint, CancellationToken.None);
                await transaction.ReleaseAsync(CheckSavepoint, CancellationToken.None);
            }
            catch (Exception rollbackException)
            {
                logger.LogError(rollbackException, "Cohort could not undo its failed check inside a caller's transaction; the transaction cannot commit, so the deferred work is dropped.");
                return false;
            }

            Unconfirmed(item, checkException);
            return true;
        }
    }

    private void Unconfirmed(DeferredWork work, Exception exception)
    {
        try
        {
            work.Unconfirmed(exception);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Cohort could not report work it could not confirm was part of a caller's transaction.");
        }
    }

    private async Task RunAsync(DbTransaction transaction, Guid transactionId)
    {
        foreach (var work in Take(transaction, transactionId))
        {
            // The caller's commit has happened; nothing deferred may turn it into a failure.
            try
            {
                await work.Committed();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Cohort work deferred until a caller's transaction committed failed.");
            }
        }
    }

    private void Drop(DbTransaction transaction, Guid transactionId)
    {
        foreach (var work in Take(transaction, transactionId))
        {
            Report(work);
        }
    }

    private void Report(DeferredWork work)
    {
        try
        {
            work.Dropped();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Cohort could not report work dropped with a caller's transaction.");
        }
    }

    private List<DeferredWork> Take(DbTransaction transaction, Guid transactionId)
    {
        lock (pending)
        {
            if (!pending.TryGetValue(transaction, out var deferred))
            {
                return [];
            }

            pending.Remove(transaction);
            return deferred.TransactionId == transactionId ? deferred.Work : [];
        }
    }

    private sealed record Deferred(Guid TransactionId)
    {
        public List<DeferredWork> Work { get; } = [];
    }

    // A class, not a record: work is removed by identity.
    private sealed class DeferredWork(
        Func<DbTransaction, Task<bool>> stillWritten,
        Func<Task> committed,
        Action dropped,
        Action<Exception> unconfirmed
    )
    {
        public Func<DbTransaction, Task<bool>> StillWritten { get; } = stillWritten;
        public Func<Task> Committed { get; } = committed;
        public Action Dropped { get; } = dropped;
        public Action<Exception> Unconfirmed { get; } = unconfirmed;
    }
}
