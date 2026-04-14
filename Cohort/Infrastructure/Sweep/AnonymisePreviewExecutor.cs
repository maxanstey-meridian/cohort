using System.Data.Common;
using System.Globalization;

using Cohort.Domain;

namespace Cohort.Infrastructure.Sweep;

internal sealed class AnonymisePreviewExecutor
{
    internal async Task<int> ExecuteAsync(
        RetentionEntry entry,
        SqlFilter filter,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.CommandText = AnonymiseSqlBuilder.BuildPreviewCountCommandText(entry, filter);
        AnonymiseDbParameterFactory.AddFilterParameters(command, filter);
        AnonymiseDbParameterFactory.AddTenantParameter(command, entry.Tenant?.TenantColumn, tenant.Id);
        AnonymiseDbParameterFactory.AddHoldParameters(command, entry.TableName, now);

        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }
}
