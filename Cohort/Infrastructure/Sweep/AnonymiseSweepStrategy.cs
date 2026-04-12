using System.Data;
using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

namespace Cohort.Infrastructure.Sweep;

public sealed class AnonymiseSweepStrategy : IRetentionSweepStrategy
{
    public Strategy HandlesStrategy => Strategy.Anonymise;

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

        if (rule.Strategy != Strategy.Anonymise)
        {
            throw new InvalidOperationException(
                $"AnonymiseSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        var tenant = entry.Tenant
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose tenant metadata for anonymise sweeps."
            );

        if (entry.AnonymiseFields.Count == 0)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose anonymise metadata for anonymise sweeps."
            );
        }

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildCommandText(entry, tenant, command, entry.RecordId.RecordIdColumn);

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", ctx.Now));

        return await command.ExecuteNonQueryAsync(ct);
    }

    private static string BuildCommandText(
        RetentionEntry entry,
        TenantConvention tenant,
        DbCommand command,
        string recordIdColumn
    )
    {
        var assignments = new List<string>(entry.AnonymiseFields.Count);

        for (var index = 0; index < entry.AnonymiseFields.Count; index++)
        {
            var field = entry.AnonymiseFields[index];
            var parameterName = $"value{index}";
            assignments.Add($"{QuoteIdentifier(field.ColumnName)} = @{parameterName}");
            command.Parameters.Add(
                CreateParameter(command, parameterName, CreateAssignmentValue(field))
            );
        }

        return
            $"""
            UPDATE {QuoteIdentifier(entry.TableName)} AS target
            SET {string.Join(", ", assignments)}
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              AND target.{QuoteIdentifier(tenant.TenantColumn)} = @tenantId
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", recordIdColumn)}
            """;
    }

    private static object CreateAssignmentValue(AnonymiseField field)
    {
        return field.Method switch
        {
            AnonymiseMethod.Null => DBNull.Value,
            AnonymiseMethod.EmptyString => string.Empty,
            AnonymiseMethod.FixedLiteral => field.Literal
                ?? throw new InvalidOperationException(
                    $"Anonymise field '{field.MemberName}' requires a literal value."
                ),
            _ => throw new InvalidOperationException(
                $"Anonymise method '{field.Method}' is not supported."
            ),
        };
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
