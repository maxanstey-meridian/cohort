using System.Data.Common;

namespace Cohort.Infrastructure;

/// <summary>
/// Cohort's savepoints, as plain SQL on the connection. An enlisted System.Transactions
/// connection has no <see cref="DbTransaction"/> to call <c>SaveAsync</c> on, and a host's connection
/// wrapper may not implement it, so one path serves a transaction begun through EF and an enlisted
/// one alike. Each method takes the command's transaction, null for an enlisted connection. The
/// names are Cohort's own constants.
/// </summary>
internal static class SavepointSql
{
    internal static Task SaveAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string name,
        CancellationToken ct
    ) => ExecuteAsync(connection, transaction, $"SAVEPOINT {name}", ct);

    /// <summary>
    /// ROLLBACK TO keeps the savepoint open, so release it too: otherwise each later savepoint of
    /// the name nests one level deeper for the rest of the transaction. Ignores cancellation, like
    /// every Cohort savepoint: a cancelled rollback would leave the transaction aborted.
    /// </summary>
    internal static async Task RollBackAsync(DbConnection connection, DbTransaction? transaction, string name)
    {
        await ExecuteAsync(connection, transaction, $"ROLLBACK TO SAVEPOINT {name}", CancellationToken.None);
        await ExecuteAsync(connection, transaction, $"RELEASE SAVEPOINT {name}", CancellationToken.None);
    }

    internal static Task ReleaseAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string name,
        CancellationToken ct
    ) => ExecuteAsync(connection, transaction, $"RELEASE SAVEPOINT {name}", ct);

    private static async Task ExecuteAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string sql,
        CancellationToken ct
    )
    {
        await using var command = new SqlParams().CreateCommand(connection, transaction, sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
