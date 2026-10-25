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
/// the transaction rolls back or fails, or when the work's own rows did not commit (the
/// caller rolled back to a savepoint taken before it, or committed an aborted transaction).
/// EF has no other commit hook. <c>AddCohort</c> adds one instance to the host's DbContext
/// options.
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

    /// <summary>Whether this interceptor is part of <paramref name="db"/>'s options.</summary>
    internal bool IsRegisteredOn(DbContext db) =>
        db.GetService<IDbContextOptions>()
            .FindExtension<CoreOptionsExtension>()
            ?.Interceptors?.Contains(this) == true;

    /// <summary>
    /// Runs <paramref name="committed"/> once <paramref name="transaction"/> commits with the
    /// work's rows, or <paramref name="dropped"/> if it rolls back or fails (false) or commits
    /// without them (true). A rollback to a savepoint taken before the work, or a COMMIT of an aborted
    /// transaction, ends in a successful commit that kept nothing; EF doesn't say which
    /// savepoint was rolled back, and a raw <c>ROLLBACK TO SAVEPOINT</c> isn't seen at all, so
    /// the committed rows are the only reliable answer. After the commit,
    /// <paramref name="committedRows"/> asks, on the context's connection and outside any
    /// transaction, whether they exist. When it can't answer, the work is dropped and
    /// <paramref name="unconfirmed"/> is told: deferred work is best effort, and running it
    /// for a commit that may not have kept its rows would report something that didn't happen.
    /// </summary>
    internal void AfterCommit(
        IDbContextTransaction transaction,
        Func<DbConnection, Task<bool>> committedRows,
        Func<Task> committed,
        Action<bool> dropped,
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

            deferred.Work.Add(new DeferredWork(committedRows, committed, dropped, unconfirmed));
        }
    }

    // Deliberately blocking: EF's synchronous commit has no async hook. The wait is bounded by
    // the committed-rows query and AuditObservers:Timeout; CommitAsync is the supported path.
    public override void TransactionCommitted(DbTransaction transaction, TransactionEndEventData eventData) =>
        RunAsync(transaction, eventData).GetAwaiter().GetResult();

    // The commit's token is deliberately not passed on: the commit has happened, and whether
    // its work is reported must not depend on the caller's cancellation.
    public override Task TransactionCommittedAsync(
        DbTransaction transaction,
        TransactionEndEventData eventData,
        CancellationToken cancellationToken = default
    ) => RunAsync(transaction, eventData);

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
    // so its work waits for the commit.
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

    /// <summary>
    /// EF raises TransactionCommitted before it releases the connection, so a connection it
    /// opened for the transaction is still open and idle here, with the transaction over.
    /// </summary>
    private async Task RunAsync(DbTransaction transaction, TransactionEndEventData eventData)
    {
        foreach (var work in Take(transaction, eventData.TransactionId))
        {
            // The caller's commit has happened; nothing deferred may turn it into a failure.
            bool rowsCommitted;
            try
            {
                var connection = eventData.Context?.Database.GetDbConnection()
                    ?? throw new InvalidOperationException("The committed transaction has no DbContext to check its rows through.");
                rowsCommitted = await work.CommittedRows(connection);
            }
            catch (Exception ex)
            {
                Unconfirmed(work, ex);
                continue;
            }

            if (!rowsCommitted)
            {
                Report(work, committedWithoutRows: true);
                continue;
            }

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
            Report(work, committedWithoutRows: false);
        }
    }

    private void Report(DeferredWork work, bool committedWithoutRows)
    {
        try
        {
            work.Dropped(committedWithoutRows);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Cohort could not report work dropped with a caller's transaction.");
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
            logger.LogError(ex, "Cohort could not report work it could not confirm a caller's transaction committed.");
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

    private sealed record DeferredWork(
        Func<DbConnection, Task<bool>> CommittedRows,
        Func<Task> Committed,
        Action<bool> Dropped,
        Action<Exception> Unconfirmed
    );
}
