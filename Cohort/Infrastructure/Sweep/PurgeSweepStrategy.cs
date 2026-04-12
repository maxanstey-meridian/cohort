using System.Data;
using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Infrastructure.Sweep;

public sealed class PurgeSweepStrategy : IRetentionSweepStrategy
{
    public Strategy HandlesStrategy => Strategy.Purge;

    public async Task<int> SweepAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(conn);
        ArgumentNullException.ThrowIfNull(transaction);

        if (rule.Strategy != Strategy.Purge)
        {
            throw new InvalidOperationException(
                $"PurgeSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        var tenant = entry.Tenant
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose tenant metadata for purge sweeps."
            );

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            DELETE FROM {QuoteIdentifier(entry.TableName)}
            WHERE {QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              AND {QuoteIdentifier(tenant.TenantColumn)} = @tenantId
            """;

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));

        return await command.ExecuteNonQueryAsync(ct);
    }

    private static DbParameter CreateParameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        return parameter;
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }
}
