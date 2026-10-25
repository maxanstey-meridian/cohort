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
/// the transaction rolls back or fails. EF has no other commit hook. <c>AddCohort</c> adds
/// one instance to the host's DbContext options.
/// </summary>
internal sealed class CohortCommitInterceptor(ILogger<CohortCommitInterceptor> logger)
    : DbTransactionInterceptor
{
    // Npgsql recycles transaction objects, so each entry also names the EF transaction it
    // belongs to: work deferred on a transaction that ended unseen (disposed without commit
    // or rollback) never runs on a later transaction that reuses the object.
    private readonly ConditionalWeakTable<DbTransaction, Deferred> pending = new();

    /// <summary>Whether this interceptor is part of <paramref name="db"/>'s options.</summary>
    internal bool IsRegisteredOn(DbContext db) =>
        db.GetService<IDbContextOptions>()
            .FindExtension<CoreOptionsExtension>()
            ?.Interceptors?.Contains(this) == true;

    /// <summary>
    /// Runs <paramref name="committed"/> once <paramref name="transaction"/> commits, or
    /// <paramref name="dropped"/> if it rolls back or fails instead.
    /// </summary>
    internal void AfterCommit(IDbContextTransaction transaction, Func<Task> committed, Action dropped)
    {
        var dbTransaction = transaction.GetDbTransaction();
        lock (pending)
        {
            if (!pending.TryGetValue(dbTransaction, out var deferred) || deferred.TransactionId != transaction.TransactionId)
            {
                deferred = new Deferred(transaction.TransactionId);
                pending.AddOrUpdate(dbTransaction, deferred);
            }

            deferred.Work.Add(new DeferredWork(committed, dropped));
        }
    }

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
    public override void TransactionFailed(DbTransaction transaction, TransactionErrorEventData eventData) =>
        Drop(transaction, eventData.TransactionId);

    public override Task TransactionFailedAsync(
        DbTransaction transaction,
        TransactionErrorEventData eventData,
        CancellationToken cancellationToken = default
    )
    {
        Drop(transaction, eventData.TransactionId);
        return Task.CompletedTask;
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
            try
            {
                work.Dropped();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Cohort could not report work dropped with a caller's transaction.");
            }
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

    private sealed record DeferredWork(Func<Task> Committed, Action Dropped);
}
