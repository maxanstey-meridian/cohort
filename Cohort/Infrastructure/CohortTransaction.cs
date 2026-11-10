using System.Data;
using System.Runtime.ExceptionServices;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Cohort.Infrastructure;

/// <summary>
/// The one way Cohort begins a transaction on a <see cref="DbContext"/>. A context with a
/// retrying execution strategy (<c>EnableRetryOnFailure()</c>) refuses EF work in a transaction
/// opened outside the strategy, so the transaction is begun and the unit runs inside the
/// context's strategy: a transient failure before the commit disposes the failed attempt's
/// transaction and runs the unit again in a fresh one. The commit runs after the strategy
/// returns, never inside it, so a commit with an unknown outcome is not retried. On a context
/// that doesn't retry this is begin, work, commit, with no strategy: Npgsql's default strategy
/// is not a pass-through, it rethrows a transient failure (a deadlock, a lost connection) as an
/// <see cref="InvalidOperationException"/> around the original, which a host that doesn't retry
/// never saw from Cohort's own transactions.
/// </summary>
internal static class CohortTransaction
{
    /// <summary>
    /// Runs <paramref name="unit"/> in a transaction, then <paramref name="beforeCommit"/> on the
    /// attempt that succeeded, then commits. The unit can run more than once, so it keeps no
    /// state outside what it returns. When the strategy gives up, the failure it gave up on is
    /// thrown, not EF's <see cref="RetryLimitExceededException"/> around it.
    /// </summary>
    internal static async Task<TResult> RunAsync<TResult>(
        DbContext db,
        IsolationLevel isolation,
        Func<IDbContextTransaction, CancellationToken, Task<TResult>> unit,
        Func<IDbContextTransaction, TResult, CancellationToken, Task>? beforeCommit,
        CancellationToken ct
    )
    {
        var strategy = db.Database.CreateExecutionStrategy();
        if (!strategy.RetriesOnFailure)
        {
            await using var owned = await db.Database.BeginTransactionAsync(isolation, ct);
            var result = await unit(owned, ct);
            if (beforeCommit is not null)
            {
                await beforeCommit(owned, result, ct);
            }

            await owned.CommitAsync(ct);
            return result;
        }

        (IDbContextTransaction Transaction, TResult Result) attempt;
        try
        {
            attempt = await strategy.ExecuteAsync(
                async token =>
                {
                    var transaction = await db.Database.BeginTransactionAsync(isolation, token);
                    try
                    {
                        return (transaction, await unit(transaction, token));
                    }
                    catch
                    {
                        // Before the strategy sees the failure, so a retry finds no open transaction.
                        await transaction.DisposeAsync();
                        throw;
                    }
                },
                ct
            );
        }
        catch (RetryLimitExceededException exhausted) when (exhausted.InnerException is { } lastFailure)
        {
            ExceptionDispatchInfo.Throw(lastFailure);
            throw;
        }

        await using (attempt.Transaction)
        {
            if (beforeCommit is not null)
            {
                await beforeCommit(attempt.Transaction, attempt.Result, ct);
            }

            await attempt.Transaction.CommitAsync(ct);
        }

        return attempt.Result;
    }

    /// <summary>
    /// Runs <paramref name="unit"/> in a transaction and commits, for a unit that runs host code
    /// Cohort can't know is safe to run twice. The unit still runs inside the context's strategy,
    /// so its EF work passes on a retrying context, but it runs once: its failure leaves the
    /// strategy as a result, which the strategy doesn't retry, and is thrown before the commit,
    /// so the transaction rolls back and the caller sees the unit's own exception.
    /// </summary>
    internal static async Task<TResult> RunOnceAsync<TResult>(
        DbContext db,
        IsolationLevel isolation,
        Func<IDbContextTransaction, CancellationToken, Task<TResult>> unit,
        CancellationToken ct
    )
    {
        var outcome = await RunAsync<(TResult Result, ExceptionDispatchInfo? Failure)>(
            db,
            isolation,
            async (transaction, token) =>
            {
                try
                {
                    return (await unit(transaction, token), null);
                }
                catch (Exception failure)
                {
                    return (default!, ExceptionDispatchInfo.Capture(failure));
                }
            },
            beforeCommit: (_, attempt, _) =>
            {
                attempt.Failure?.Throw();
                return Task.CompletedTask;
            },
            ct
        );
        return outcome.Result;
    }

    /// <summary><see cref="RunAsync{TResult}"/> for a unit with nothing to return.</summary>
    internal static Task RunAsync(
        DbContext db,
        IsolationLevel isolation,
        Func<IDbContextTransaction, CancellationToken, Task> unit,
        Func<IDbContextTransaction, CancellationToken, Task>? beforeCommit,
        CancellationToken ct
    ) =>
        RunAsync(
            db,
            isolation,
            async (transaction, token) =>
            {
                await unit(transaction, token);
                return true;
            },
            beforeCommit is null ? null : (transaction, _, token) => beforeCommit(transaction, token),
            ct
        );
}
