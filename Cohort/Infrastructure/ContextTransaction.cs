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

        return enlistment.CurrentAmbientTransaction is not null || enlistment.EnlistedTransaction is not null
            ? new Enlisted()
            : new None();
    }

    internal sealed record None : ContextTransaction;

    internal sealed record Ef(IDbContextTransaction Transaction) : ContextTransaction;

    // Carries nothing: no site reads a System.Transactions transaction itself, and EF and Npgsql
    // give no transaction object for an enlisted connection.
    internal sealed record Enlisted : ContextTransaction;
}
