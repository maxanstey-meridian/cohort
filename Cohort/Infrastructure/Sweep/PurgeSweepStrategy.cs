using System.Data;
using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

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

        var recordIdColumn = RetentionHoldSql.GetRecordIdColumn(entry);

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            DELETE FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              AND target.{QuoteIdentifier(tenant.TenantColumn)} = @tenantId
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", recordIdColumn)}
            """;

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", ctx.Now));

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
