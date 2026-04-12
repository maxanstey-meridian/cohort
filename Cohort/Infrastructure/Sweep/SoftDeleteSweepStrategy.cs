using System.Data;
using System.Data.Common;
using System.Reflection;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

namespace Cohort.Infrastructure.Sweep;

public sealed class SoftDeleteSweepStrategy : IRetentionSweepStrategy
{
    public Strategy HandlesStrategy => Strategy.SoftDelete;

    public async Task<SweepExecutionResult> SweepAsync(
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

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);
        var eligibleCount = await CountEligibleRowsAsync(
            entry,
            tenant,
            softDelete,
            ctx,
            conn,
            transaction,
            cutoff,
            ct
        );

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildCommandText(entry, tenant, softDelete, entry.RecordId.RecordIdColumn);
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", ctx.Now));

        if (softDelete.DeletedAtColumn is not null)
        {
            command.Parameters.Add(
                CreateParameter(command, "deletedAt", CreateDeletedAtValue(entry, softDelete, ctx.Now))
            );
        }

        var affectedRecordIds = new List<Guid>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            affectedRecordIds.Add(reader.GetGuid(0));
        }

        return new SweepExecutionResult(affectedRecordIds, eligibleCount - affectedRecordIds.Count);
    }

    public async Task<SweepExecutionResult> EraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectMatch match,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(match);
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(conn);
        ArgumentNullException.ThrowIfNull(transaction);

        if (rule.Strategy != Strategy.SoftDelete)
        {
            throw new InvalidOperationException(
                $"SoftDeleteSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        var tenantConvention = entry.Tenant
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose tenant metadata for soft-delete erasure."
            );
        var softDelete = entry.SoftDelete
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose soft-delete metadata for soft-delete erasure."
            );

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }

        var eligibleCount = await CountEligibleErasureRowsAsync(
            entry,
            tenantConvention,
            softDelete,
            match,
            tenant,
            conn,
            transaction,
            ct
        );

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildErasureCommandText(
            entry,
            tenantConvention,
            softDelete,
            match,
            entry.RecordId.RecordIdColumn
        );
        command.Parameters.Add(CreateParameter(command, "tenantId", tenant.Id));
        command.Parameters.Add(CreateParameter(command, "subjectValue", match.SubjectValue));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", now));

        if (softDelete.DeletedAtColumn is not null)
        {
            command.Parameters.Add(
                CreateParameter(command, "deletedAt", CreateDeletedAtValue(entry, softDelete, now))
            );
        }

        var affectedRecordIds = new List<Guid>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            affectedRecordIds.Add(reader.GetGuid(0));
        }

        return new SweepExecutionResult(affectedRecordIds, eligibleCount - affectedRecordIds.Count);
    }

    private static string BuildCommandText(
        RetentionEntry entry,
        TenantConvention tenant,
        SoftDeleteConvention softDelete,
        string recordIdColumn
    )
    {
        var deletedAtAssignment = softDelete.DeletedAtColumn is null
            ? ""
            : $", {QuoteIdentifier(softDelete.DeletedAtColumn)} = @deletedAt";

        return
            $"""
            UPDATE {QuoteIdentifier(entry.TableName)} AS target
            SET {QuoteIdentifier(softDelete.IsDeletedColumn)} = TRUE{deletedAtAssignment}
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              AND target.{QuoteIdentifier(tenant.TenantColumn)} = @tenantId
              AND target.{QuoteIdentifier(softDelete.IsDeletedColumn)} = FALSE
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", recordIdColumn)}
            RETURNING target.{QuoteIdentifier(recordIdColumn)}
            """;
    }

    private static string BuildErasureCommandText(
        RetentionEntry entry,
        TenantConvention tenant,
        SoftDeleteConvention softDelete,
        ErasureSubjectMatch match,
        string recordIdColumn
    )
    {
        var deletedAtAssignment = softDelete.DeletedAtColumn is null
            ? ""
            : $", {QuoteIdentifier(softDelete.DeletedAtColumn)} = @deletedAt";

        return
            $"""
            UPDATE {QuoteIdentifier(entry.TableName)} AS target
            SET {QuoteIdentifier(softDelete.IsDeletedColumn)} = TRUE{deletedAtAssignment}
            WHERE target.{QuoteIdentifier(tenant.TenantColumn)} = @tenantId
              AND target.{QuoteIdentifier(match.SubjectColumn)} = @subjectValue
              AND target.{QuoteIdentifier(softDelete.IsDeletedColumn)} = FALSE
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", recordIdColumn)}
            RETURNING target.{QuoteIdentifier(recordIdColumn)}
            """;
    }

    private static async Task<int> CountEligibleRowsAsync(
        RetentionEntry entry,
        TenantConvention tenant,
        SoftDeleteConvention softDelete,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        DateTimeOffset cutoff,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              AND target.{QuoteIdentifier(tenant.TenantColumn)} = @tenantId
              AND target.{QuoteIdentifier(softDelete.IsDeletedColumn)} = FALSE
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));

        var result = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt32(result);
    }

    private static async Task<int> CountEligibleErasureRowsAsync(
        RetentionEntry entry,
        TenantConvention tenant,
        SoftDeleteConvention softDelete,
        ErasureSubjectMatch match,
        TenantContext erasureTenant,
        DbConnection conn,
        DbTransaction transaction,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(tenant.TenantColumn)} = @tenantId
              AND target.{QuoteIdentifier(match.SubjectColumn)} = @subjectValue
              AND target.{QuoteIdentifier(softDelete.IsDeletedColumn)} = FALSE
            """;
        command.Parameters.Add(CreateParameter(command, "tenantId", erasureTenant.Id));
        command.Parameters.Add(CreateParameter(command, "subjectValue", match.SubjectValue));

        var result = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt32(result);
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
