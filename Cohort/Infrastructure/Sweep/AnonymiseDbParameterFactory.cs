using System.Data.Common;

namespace Cohort.Infrastructure.Sweep;

internal static class AnonymiseDbParameterFactory
{
    internal static DbParameter Create(DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        return parameter;
    }

    internal static DbParameter CreateProviderParameter(DbConnection conn, string name, object? value)
    {
        using var command = conn.CreateCommand();
        return Create(command, name, value);
    }

    internal static void AddFilterParameters(DbCommand command, SqlFilter filter)
    {
        foreach (var parameter in filter.Parameters)
        {
            command.Parameters.Add(Create(command, parameter.Name, parameter.Value));
        }
    }

    internal static void AddTenantParameter(DbCommand command, string? tenantColumn, Guid tenantId)
    {
        if (tenantColumn is not null)
        {
            command.Parameters.Add(Create(command, "tenantId", tenantId));
        }
    }

    internal static void AddHoldParameters(DbCommand command, string tableName, DateTimeOffset now)
    {
        command.Parameters.Add(Create(command, "holdTableName", tableName));
        command.Parameters.Add(Create(command, "holdAsOf", now));
    }

    internal static void AddCandidateIdsParameter(
        DbCommand command,
        IReadOnlyList<string> candidateRecordIds
    )
    {
        command.Parameters.Add(Create(command, "candidateIds", candidateRecordIds.ToArray()));
    }
}
