using System.Data;
using System.Data.Common;
using System.Globalization;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

namespace Cohort.Infrastructure.Sweep;

public sealed class PurgeSweepStrategy : IRetentionSweepStrategy
{
    public Strategy HandlesStrategy => Strategy.Purge;

    public async Task<int> PreviewAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(conn);

        if (rule.Strategy != Strategy.Purge)
        {
            throw new InvalidOperationException(
                $"PurgeSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);
        var tenantClause = entry.Tenant is not null
            ? $"AND target.{QuoteIdentifier(entry.Tenant.TenantColumn)} = @tenantId"
            : "";

        await using var command = conn.CreateCommand();
        command.CommandText =
            $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        if (entry.Tenant is not null)
        {
            command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));
        }
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", ctx.Now));

        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

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

        if (rule.Strategy != Strategy.Purge)
        {
            throw new InvalidOperationException(
                $"PurgeSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);
        var candidateRecordIds = await SelectCandidateRecordIdsAsync(
            entry,
            entry.Tenant?.TenantColumn,
            ctx,
            conn,
            transaction,
            cutoff,
            ct
        );

        if (candidateRecordIds.Count == 0)
        {
            return new SweepExecutionResult([], 0);
        }

        var tenantClause = entry.Tenant is not null
            ? $"AND target.{QuoteIdentifier(entry.Tenant.TenantColumn)} = @tenantId"
            : "";

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            DELETE FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {tenantClause}
              AND CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) = ANY(@candidateIds)
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            RETURNING target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        if (entry.Tenant is not null)
        {
            command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));
        }
        command.Parameters.Add(CreateParameter(command, "candidateIds", candidateRecordIds.ToArray()));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", ctx.Now));

        var affectedRecordIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            affectedRecordIds.Add(reader.GetValue(0).ToString()!);
        }

        return new SweepExecutionResult(
            affectedRecordIds,
            candidateRecordIds.Count - affectedRecordIds.Count
        );
    }

    public async Task<int> PreviewEraseAsync(
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

        if (rule.Strategy != Strategy.Purge)
        {
            throw new InvalidOperationException(
                $"PurgeSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }

        var candidateRecordIds = await SelectErasureCandidateRecordIdsAsync(
            entry,
            entry.Tenant?.TenantColumn,
            match,
            tenant,
            conn,
            transaction,
            ct
        );

        if (candidateRecordIds.Count == 0)
        {
            return 0;
        }

        var tenantClause = entry.Tenant is not null
            ? $"AND target.{QuoteIdentifier(entry.Tenant.TenantColumn)} = @tenantId"
            : "";

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(match.SubjectColumn)} = @subjectValue
              {tenantClause}
              AND CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) = ANY(@candidateIds)
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            """;
        if (entry.Tenant is not null)
        {
            command.Parameters.Add(CreateParameter(command, "tenantId", tenant.Id));
        }
        command.Parameters.Add(CreateParameter(command, "subjectValue", match.SubjectValue));
        command.Parameters.Add(CreateParameter(command, "candidateIds", candidateRecordIds.ToArray()));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", now));

        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
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

        if (rule.Strategy != Strategy.Purge)
        {
            throw new InvalidOperationException(
                $"PurgeSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }

        var candidateRecordIds = await SelectErasureCandidateRecordIdsAsync(
            entry,
            entry.Tenant?.TenantColumn,
            match,
            tenant,
            conn,
            transaction,
            ct
        );

        if (candidateRecordIds.Count == 0)
        {
            return new SweepExecutionResult([], 0);
        }

        var tenantClause = entry.Tenant is not null
            ? $"AND target.{QuoteIdentifier(entry.Tenant.TenantColumn)} = @tenantId"
            : "";

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            DELETE FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(match.SubjectColumn)} = @subjectValue
              {tenantClause}
              AND CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) = ANY(@candidateIds)
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            RETURNING target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            """;
        if (entry.Tenant is not null)
        {
            command.Parameters.Add(CreateParameter(command, "tenantId", tenant.Id));
        }
        command.Parameters.Add(CreateParameter(command, "subjectValue", match.SubjectValue));
        command.Parameters.Add(CreateParameter(command, "candidateIds", candidateRecordIds.ToArray()));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", now));

        var affectedRecordIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            affectedRecordIds.Add(reader.GetValue(0).ToString()!);
        }

        return new SweepExecutionResult(
            affectedRecordIds,
            candidateRecordIds.Count - affectedRecordIds.Count
        );
    }

    private static async Task<IReadOnlyList<string>> SelectCandidateRecordIdsAsync(
        RetentionEntry entry,
        string? tenantColumn,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        DateTimeOffset cutoff,
        CancellationToken ct
    )
    {
        var tenantClause = tenantColumn is not null
            ? $"AND target.{QuoteIdentifier(tenantColumn)} = @tenantId"
            : "";

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            SELECT target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {tenantClause}
            FOR UPDATE
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        if (tenantColumn is not null)
        {
            command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));
        }

        var candidateRecordIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            candidateRecordIds.Add(reader.GetValue(0).ToString()!);
        }

        return candidateRecordIds;
    }

    private static async Task<IReadOnlyList<string>> SelectErasureCandidateRecordIdsAsync(
        RetentionEntry entry,
        string? tenantColumn,
        ErasureSubjectMatch match,
        TenantContext erasureTenant,
        DbConnection conn,
        DbTransaction transaction,
        CancellationToken ct
    )
    {
        var tenantClause = tenantColumn is not null
            ? $"AND target.{QuoteIdentifier(tenantColumn)} = @tenantId"
            : "";

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            SELECT target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(match.SubjectColumn)} = @subjectValue
              {tenantClause}
            FOR UPDATE
            """;
        if (tenantColumn is not null)
        {
            command.Parameters.Add(CreateParameter(command, "tenantId", erasureTenant.Id));
        }
        command.Parameters.Add(CreateParameter(command, "subjectValue", match.SubjectValue));

        var candidateRecordIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            candidateRecordIds.Add(reader.GetValue(0).ToString()!);
        }

        return candidateRecordIds;
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
