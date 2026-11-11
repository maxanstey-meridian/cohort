using System.Data.Common;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace Cohort.Infrastructure;

/// <summary>
/// Whether a <see cref="DbContext"/> is in a transaction, and which kind: one begun through EF
/// (<see cref="Ef"/>), or a System.Transactions transaction the context's connection is enlisted
/// in or will enlist in on its next open (<see cref="Enlisted"/>). Every site that branches on
/// "is the context in a transaction", or begins its own when it isn't, reads <see cref="Of"/>.
/// </summary>
internal abstract record ContextTransaction
{
    private ContextTransaction() { }

    /// <summary>The transaction's <see cref="DbTransaction"/> for a command, or null when there is none to give.</summary>
    internal DbTransaction? Commands => this is Ef ef ? ef.Transaction.GetDbTransaction() : null;

    /// <summary>
    /// Asks EF, not <see cref="System.Transactions.Transaction.Current"/>, so Cohort agrees with
    /// the one that decides whether the connection enlists: Npgsql reports no ambient transaction
    /// when the connection string says <c>Enlist=false</c>, and a transaction passed to
    /// <c>EnlistTransaction</c> is not the ambient one.
    /// </summary>
    internal static ContextTransaction Of(DatabaseFacade database)
    {
        if (database.CurrentTransaction is { } transaction)
        {
            return new Ef(transaction);
        }

        // Only a relational provider enlists; any other answers None, so a port reaches
        // readiness's provider refusal as it did before this type existed.
        if (database.GetService<IDbContextTransactionManager>() is not ITransactionEnlistmentManager enlistment)
        {
            return new None();
        }

        var enlisted = enlistment.EnlistedTransaction ?? enlistment.CurrentAmbientTransaction;
        return enlisted is null ? new None() : new Enlisted(enlisted);
    }

    internal sealed record None : ContextTransaction;

    internal sealed record Ef(IDbContextTransaction Transaction) : ContextTransaction;

    /// <summary>
    /// The transaction passed to <c>EnlistTransaction</c>, else the ambient one. EF and Npgsql give
    /// no <see cref="DbTransaction"/> for an enlisted connection, so a command carries none, a step's
    /// context joins by running inside a <see cref="System.Transactions.TransactionScope"/> of it,
    /// and its completion is the commit event.
    /// </summary>
    internal sealed record Enlisted(System.Transactions.Transaction Transaction) : ContextTransaction;
}
