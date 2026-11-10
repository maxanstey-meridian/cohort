using System.Data;
using System.Data.Common;

using Npgsql;

namespace Cohort.Sample.Tests;

/// <summary>
/// A wrapper that forwards to an <see cref="NpgsqlConnection"/> and is not one itself, as a
/// profiler's or a per-scope unit-of-work wrapper is. <paramref name="connectionStringSuffix"/>
/// is appended to the string it reports, as a wrapper's own settings are. Disposing it disposes
/// the inner connection unless <paramref name="ownsInner"/> is false, as when several wrappers
/// share one connection. <paramref name="rewriteCommandText"/>, when given, rewrites the text of
/// every command made on it before it reaches the server, for a test that has to change a
/// statement Cohort sends on raw ADO.NET, below EF's command interceptors.
/// <paramref name="beforeCommit"/>, when given, runs before each of its transactions commits and
/// can throw, for a test that has to fail a commit.
/// </summary>
internal sealed class PassThroughConnection(
    NpgsqlConnection inner,
    string connectionStringSuffix = "",
    bool ownsInner = true,
    Func<string, string>? rewriteCommandText = null,
    Action? beforeCommit = null
) : DbConnection
{
    [System.Diagnostics.CodeAnalysis.AllowNull]
    public override string ConnectionString
    {
        get => inner.ConnectionString + connectionStringSuffix;
        set => inner.ConnectionString = value;
    }

    public override string Database => inner.Database;

    public override string DataSource => inner.DataSource;

    public override string ServerVersion => inner.ServerVersion;

    public override ConnectionState State => inner.State;

    public override void ChangeDatabase(string databaseName) => inner.ChangeDatabase(databaseName);

    public override void Open() => inner.Open();

    public override Task OpenAsync(CancellationToken cancellationToken) => inner.OpenAsync(cancellationToken);

    public override void Close() => inner.Close();

    protected override DbCommand CreateDbCommand() =>
        new PassThroughCommand(this, inner.CreateCommand(), rewriteCommandText);

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
        new PassThroughTransaction(this, inner.BeginTransaction(isolationLevel), beforeCommit);

    public override ValueTask DisposeAsync() => ownsInner ? inner.DisposeAsync() : ValueTask.CompletedTask;

    protected override void Dispose(bool disposing)
    {
        if (disposing && ownsInner)
        {
            inner.Dispose();
        }

        base.Dispose(disposing);
    }
}

// The transaction and command of that wrapper, as a profiler's are: neither is an Npgsql type,
// so a caller's transaction on the wrapper is not an NpgsqlTransaction and readiness explains
// its statements one at a time, in that transaction.
internal sealed class PassThroughTransaction(
    PassThroughConnection connection,
    NpgsqlTransaction inner,
    Action? beforeCommit = null
) : DbTransaction
{
    internal NpgsqlTransaction Inner => inner;

    protected override DbConnection DbConnection => connection;

    public override IsolationLevel IsolationLevel => inner.IsolationLevel;

    public override void Commit()
    {
        beforeCommit?.Invoke();
        inner.Commit();
    }

    public override void Rollback() => inner.Rollback();

    // Cohort's row-by-row path and its entity savepoints run on the transaction's own savepoints.
    public override bool SupportsSavepoints => inner.SupportsSavepoints;

    public override void Save(string savepointName) => inner.Save(savepointName);

    public override Task SaveAsync(string savepointName, CancellationToken cancellationToken = default) =>
        inner.SaveAsync(savepointName, cancellationToken);

    public override void Release(string savepointName) => inner.Release(savepointName);

    public override Task ReleaseAsync(string savepointName, CancellationToken cancellationToken = default) =>
        inner.ReleaseAsync(savepointName, cancellationToken);

    public override void Rollback(string savepointName) => inner.Rollback(savepointName);

    public override Task RollbackAsync(string savepointName, CancellationToken cancellationToken = default) =>
        inner.RollbackAsync(savepointName, cancellationToken);

    public override Task CommitAsync(CancellationToken cancellationToken = default)
    {
        beforeCommit?.Invoke();
        return inner.CommitAsync(cancellationToken);
    }

    public override Task RollbackAsync(CancellationToken cancellationToken = default) =>
        inner.RollbackAsync(cancellationToken);

    public override ValueTask DisposeAsync() => inner.DisposeAsync();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            inner.Dispose();
        }

        base.Dispose(disposing);
    }
}

internal sealed class PassThroughCommand(
    PassThroughConnection connection,
    NpgsqlCommand inner,
    Func<string, string>? rewriteCommandText
) : DbCommand
{
    private PassThroughTransaction? transaction;

    [System.Diagnostics.CodeAnalysis.AllowNull]
    public override string CommandText
    {
        get => inner.CommandText;
        set => inner.CommandText = value is not null && rewriteCommandText is not null
            ? rewriteCommandText(value)
            : value;
    }

    public override int CommandTimeout
    {
        get => inner.CommandTimeout;
        set => inner.CommandTimeout = value;
    }

    public override CommandType CommandType
    {
        get => inner.CommandType;
        set => inner.CommandType = value;
    }

    public override bool DesignTimeVisible
    {
        get => inner.DesignTimeVisible;
        set => inner.DesignTimeVisible = value;
    }

    public override UpdateRowSource UpdatedRowSource
    {
        get => inner.UpdatedRowSource;
        set => inner.UpdatedRowSource = value;
    }

    protected override DbConnection? DbConnection
    {
        get => connection;
        set { }
    }

    protected override DbParameterCollection DbParameterCollection => inner.Parameters;

    protected override DbTransaction? DbTransaction
    {
        get => transaction;
        set
        {
            transaction = (PassThroughTransaction?)value;
            inner.Transaction = transaction?.Inner;
        }
    }

    public override void Cancel() => inner.Cancel();

    public override int ExecuteNonQuery() => inner.ExecuteNonQuery();

    public override object? ExecuteScalar() => inner.ExecuteScalar();

    public override void Prepare() => inner.Prepare();

    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) =>
        inner.ExecuteNonQueryAsync(cancellationToken);

    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken) =>
        inner.ExecuteScalarAsync(cancellationToken);

    protected override DbParameter CreateDbParameter() => inner.CreateParameter();

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
        inner.ExecuteReader(behavior);

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior,
        CancellationToken cancellationToken
    ) => await inner.ExecuteReaderAsync(behavior, cancellationToken);

    public override ValueTask DisposeAsync() => inner.DisposeAsync();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            inner.Dispose();
        }

        base.Dispose(disposing);
    }
}
