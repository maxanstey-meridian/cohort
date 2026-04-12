using System.Data;
using System.Data.Common;
using System.Reflection;

using Cohort.Application;
using Cohort.Domain;

namespace Cohort.Infrastructure.Sweep;

public sealed class SoftDeleteSweepStrategy : IRetentionSweepStrategy
{
    public Strategy HandlesStrategy => Strategy.SoftDelete;

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

        if (rule.Strategy != Strategy.SoftDelete)
        {
            throw new InvalidOperationException(
                $"SoftDeleteSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        var tenant = entry.Tenant
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose tenant metadata for soft-delete sweeps."
            );
        var softDelete = entry.SoftDelete
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose soft-delete metadata for soft-delete sweeps."
            );

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildCommandText(entry, tenant, softDelete);

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));

        if (softDelete.DeletedAtColumn is not null)
        {
            command.Parameters.Add(
                CreateParameter(command, "deletedAt", CreateDeletedAtValue(entry, softDelete, ctx.Now))
            );
        }

        return await command.ExecuteNonQueryAsync(ct);
    }

    private static string BuildCommandText(
        RetentionEntry entry,
        TenantConvention tenant,
        SoftDeleteConvention softDelete
    )
    {
        var deletedAtAssignment = softDelete.DeletedAtColumn is null
            ? ""
            : $", {QuoteIdentifier(softDelete.DeletedAtColumn)} = @deletedAt";

        return
            $"""
            UPDATE {QuoteIdentifier(entry.TableName)}
            SET {QuoteIdentifier(softDelete.IsDeletedColumn)} = TRUE{deletedAtAssignment}
            WHERE {QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              AND {QuoteIdentifier(tenant.TenantColumn)} = @tenantId
              AND {QuoteIdentifier(softDelete.IsDeletedColumn)} = FALSE
            """;
    }

    private static object CreateDeletedAtValue(
        RetentionEntry entry,
        SoftDeleteConvention softDelete,
        DateTimeOffset now
    )
    {
        if (softDelete.DeletedAtMember is null)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} does not define DeletedAt metadata."
            );
        }

        var deletedAtProperty = entry.EntityType.GetProperty(
            softDelete.DeletedAtMember,
            BindingFlags.Public | BindingFlags.Instance
        );
        if (deletedAtProperty is null)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} could not find DeletedAt member '{softDelete.DeletedAtMember}'."
            );
        }

        return deletedAtProperty.PropertyType switch
        {
            var type when type == typeof(DateTime) || type == typeof(DateTime?) => now.UtcDateTime,
            var type when type == typeof(DateTimeOffset) || type == typeof(DateTimeOffset?) => now,
            _ => throw new InvalidOperationException(
                $"Soft-delete DeletedAt member '{deletedAtProperty.Name}' on {entry.EntityType.FullName} must be DateTime or DateTimeOffset (nullable allowed), got {deletedAtProperty.PropertyType.Name}."
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
