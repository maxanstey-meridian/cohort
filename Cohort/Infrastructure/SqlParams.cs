using System.Data.Common;

namespace Cohort.Infrastructure;

/// <summary>
/// Named values for one raw SQL statement. A null value binds as SQL NULL.
/// </summary>
internal sealed class SqlParams() : Dictionary<string, object?>(StringComparer.Ordinal)
{
    public DbCommand CreateCommand(DbConnection connection, DbTransaction? transaction, string sql)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        foreach (var (name, value) in this)
        {
            command.Parameters.Add(Create(command, name, value));
        }

        return command;
    }

    /// <summary>Provider parameters for <c>FromSqlRaw</c>.</summary>
    public object[] ToDbParameters(DbConnection connection)
    {
        using var command = connection.CreateCommand();
        return this.Select(pair => (object)Create(command, pair.Key, pair.Value)).ToArray();
    }

    private static DbParameter Create(DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        return parameter;
    }
}
