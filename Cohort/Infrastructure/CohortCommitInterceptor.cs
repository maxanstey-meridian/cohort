using System.Data.Common;
using System.Runtime.CompilerServices;

using Cohort.Application;
using Cohort.Infrastructure.Audit;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure;

/// <summary>
/// Runs work Cohort deferred until a caller's transaction commits (an erasure joined with
/// <c>ErasureTransaction.Caller</c> tells its audit observers only then), and drops it when
/// the transaction rolls back or fails, or when the work's own rows did not commit (the
/// caller rolled back to a savepoint taken before it, or committed an aborted transaction).
/// A transaction begun through EF is watched through EF's transaction events, which a
/// System.Transactions commit never raises; for one of those the work waits on
/// <see cref="System.Transactions.Transaction.TransactionCompleted"/> instead. <c>AddCohort</c>
/// adds one instance to the host's DbContext options, which only the EF case needs.
/// </summary>
internal sealed class CohortCommitInterceptor(
    ILogger<CohortCommitInterceptor> logger,
    IServiceScopeFactory scopeFactory
) : DbTransactionInterceptor
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
    /// <paramref name="committedRows"/> asks, outside any transaction, whether they exist. When
    /// it can't answer, the work is dropped and <paramref name="unconfirmed"/> is told: deferred
    /// work is best effort, and running it for a commit that may not have kept its rows would
    /// report something that didn't happen.
    /// For a transaction begun through EF the question is asked on the caller's context's
    /// connection and <paramref name="committed"/> is given <paramref name="callerNotifier"/>:
    /// that context ends the transaction when it is disposed, so it is alive at the commit. For a
    /// System.Transactions transaction the question is asked, and the observers are told, in a
    /// scope Cohort opens in <paramref name="context"/>: the caller's scope may be gone by then.
    /// </summary>
    internal void AfterCommit(
        ContextTransaction transaction,
        RetentionAuditNotifier callerNotifier,
        RetentionExecutionContext context,
        Func<DbConnection, Task<bool>> committedRows,
        Func<RetentionAuditNotifier, Task> committed,
        Action<bool> dropped,
        Action<Exception> unconfirmed
    )
    {
        var work = new DeferredWork(callerNotifier, context, committedRows, committed, dropped, unconfirmed);
        switch (transaction)
        {
            case ContextTransaction.Ef ef:
                DeferUntilEfCommit(ef.Transaction, work);
                break;
            case ContextTransaction.Enlisted enlisted:
                enlisted.Transaction.TransactionCompleted += (_, completed) =>
                    RunCompleted(work, completed.Transaction?.TransactionInformation.Status);
                break;
            default:
                throw new InvalidOperationException("ErasureTransaction.Caller needs an open transaction on the scoped DbContext.");
        }
    }

    private void DeferUntilEfCommit(IDbContextTransaction transaction, DeferredWork work)
    {
        var dbTransaction = transaction.GetDbTransaction();
        lock (pending)
        {
            if (!pending.TryGetValue(dbTransaction, out var deferred) || deferred.TransactionId != transaction.TransactionId)
            {
                deferred = new Deferred(transaction.TransactionId);
                pending.AddOrUpdate(dbTransaction, deferred);
            }

            deferred.Work.Add(work);
        }
    }

    // Raised synchronously while the caller's scope is disposed or its transaction committed.
    // Deliberately blocking, as the EF path below: the wait is bounded by the committed-rows
    // query and AuditObservers:Timeout. Nothing here throws: the work's own failures are caught
    // and logged. An exception out of this handler would stop the host's own TransactionCompleted
    // handlers subscribed after Cohort's, and would leave a BeginCommit commit never completing;
    // on a synchronous commit Npgsql swallows it, so the commit looks unharmed and proves nothing.
    // A scope's disposal pops it before it commits, so a scope nested in another one (a
    // RequiresNew inside the host's outer scope) runs this with the outer transaction ambient:
    // RunInScopeAsync suppresses it, or the scope's connection and the observers' own writes
    // would enlist in a transaction the erasure never belonged to.
    private void RunCompleted(DeferredWork work, System.Transactions.TransactionStatus? status)
    {
        if (status != System.Transactions.TransactionStatus.Committed)
        {
            Report(work, committedWithoutRows: false);
            return;
        }

        BlockOffTheCallersContext(() => ConfirmInOwnScopeAsync(work));
    }

    // Nothing of the caller's is used: a System.Transactions transaction can outlive the DI scope
    // its context came from, and that scope's context, notifier, and scoped observers die with it.
    // The question and the telling both run in a scope of Cohort's own. Until the question is
    // answered, a failure (the hook threw or never ran the work, the connection would not open)
    // leaves the commit unconfirmed; once it is answered, the events were told or dropped, and
    // a failure is only logged.
    private async Task ConfirmInOwnScopeAsync(DeferredWork work)
    {
        var answered = false;
        try
        {
            await scopeFactory.RunInScopeAsync(
                work.Context,
                async services =>
                {
                    var db = services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext);
                    var notifier = services.GetRequiredService<RetentionAuditNotifier>();
                    await db.Database.OpenConnectionAsync(CancellationToken.None);
                    try
                    {
                        await ConfirmAndRunAsync(work, () => work.CommittedRows(db.Database.GetDbConnection()), notifier);
                        answered = true;
                    }
                    finally
                    {
                        await db.Database.CloseConnectionAsync();
                    }
                },
                CancellationToken.None
            );
        }
        catch (Exception ex) when (!answered)
        {
            Unconfirmed(work, ex);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Cohort's scope for work deferred until a caller's transaction committed failed after the work was answered.");
        }
    }

    // Deliberately blocking: EF's synchronous commit has no async hook. The wait is bounded by
    // the committed-rows query and AuditObservers:Timeout; CommitAsync is the supported path.
    public override void TransactionCommitted(DbTransaction transaction, TransactionEndEventData eventData) =>
        BlockOffTheCallersContext(() => RunAsync(transaction, eventData));

    // The calling thread stays blocked, so the caller's context is still used by one thread at
    // a time, but the work runs on the thread pool: its awaits would otherwise resume on the
    // caller's SynchronizationContext (Blazor Server, WPF, WinForms), whose thread is waiting here.
    private static void BlockOffTheCallersContext(Func<Task> work) => Task.Run(work).GetAwaiter().GetResult();

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
            await ConfirmAndRunAsync(
                work,
                () =>
                {
                    var connection = eventData.Context?.Database.GetDbConnection()
                        ?? throw new InvalidOperationException("The committed transaction has no DbContext to check its rows through.");
                    return work.CommittedRows(connection);
                },
                work.CallerNotifier
            );
        }
    }

    /// <summary>
    /// After a commit: asks whether the work's rows were kept, then runs it, drops it, or reports
    /// it unconfirmed. The caller's commit has happened; nothing deferred may turn it into a failure.
    /// </summary>
    private async Task ConfirmAndRunAsync(
        DeferredWork work,
        Func<Task<bool>> rowsCommitted,
        RetentionAuditNotifier notifier
    )
    {
        bool committedRows;
        try
        {
            committedRows = await rowsCommitted();
        }
        catch (Exception ex)
        {
            Unconfirmed(work, ex);
            return;
        }

        if (!committedRows)
        {
            Report(work, committedWithoutRows: true);
            return;
        }

        try
        {
            await work.Committed(notifier);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Cohort work deferred until a caller's transaction committed failed.");
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

    // CallerNotifier is for a transaction begun through EF only, whose context is alive when it
    // commits; a System.Transactions transaction is told through a scope of Cohort's own.
    private sealed record DeferredWork(
        RetentionAuditNotifier CallerNotifier,
        RetentionExecutionContext Context,
        Func<DbConnection, Task<bool>> CommittedRows,
        Func<RetentionAuditNotifier, Task> Committed,
        Action<bool> Dropped,
        Action<Exception> Unconfirmed
    );
}
