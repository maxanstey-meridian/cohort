using System.Data;
using System.Data.Common;
using System.Globalization;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

namespace Cohort.Infrastructure.Sweep;

public sealed class AnonymiseSweepStrategy : IRetentionSweepStrategy
{
    public Strategy HandlesStrategy => Strategy.Anonymise;

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

        if (rule.Strategy != Strategy.Anonymise)
        {
            throw new InvalidOperationException(
                $"AnonymiseSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        var tenant = entry.Tenant
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose tenant metadata for anonymise previews."
            );

        if (entry.AnonymiseFields.Count == 0)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose anonymise metadata for anonymise previews."
            );
        }

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);

        await using var command = conn.CreateCommand();
        command.CommandText =
            $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              AND target.{QuoteIdentifier(tenant.TenantColumn)} = @tenantId
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn)}
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));
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

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);
        var candidateRecordIds = await SelectCandidateRecordIdsAsync(
            entry,
            tenant,
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

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildCommandText(entry, tenant, command, entry.RecordId.RecordIdColumn);
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));
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

        if (rule.Strategy != Strategy.Anonymise)
        {
            throw new InvalidOperationException(
                $"AnonymiseSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        var tenantConvention = entry.Tenant
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose tenant metadata for anonymise erasure."
            );

        if (entry.AnonymiseFields.Count == 0)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose anonymise metadata for anonymise erasure."
            );
        }

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }

        var candidateRecordIds = await SelectErasureCandidateRecordIdsAsync(
            entry,
            tenantConvention,
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

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildErasureCommandText(
            entry,
            tenantConvention,
            match,
            command,
            entry.RecordId.RecordIdColumn
        );
        command.Parameters.Add(CreateParameter(command, "tenantId", tenant.Id));
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
              AND CAST(target.{QuoteIdentifier(recordIdColumn)} AS text) = ANY(@candidateIds)
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", recordIdColumn)}
            RETURNING target.{QuoteIdentifier(recordIdColumn)}
            """;
    }

    private static string BuildErasureCommandText(
        RetentionEntry entry,
        TenantConvention tenant,
        ErasureSubjectMatch match,
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
            WHERE target.{QuoteIdentifier(tenant.TenantColumn)} = @tenantId
              AND target.{QuoteIdentifier(match.SubjectColumn)} = @subjectValue
              AND CAST(target.{QuoteIdentifier(recordIdColumn)} AS text) = ANY(@candidateIds)
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", recordIdColumn)}
            RETURNING target.{QuoteIdentifier(recordIdColumn)}
            """;
    }

    private static async Task<IReadOnlyList<string>> SelectCandidateRecordIdsAsync(
        RetentionEntry entry,
        TenantConvention tenant,
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
            SELECT target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              AND target.{QuoteIdentifier(tenant.TenantColumn)} = @tenantId
            FOR UPDATE
            """;
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));

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
        TenantConvention tenant,
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
            SELECT target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(tenant.TenantColumn)} = @tenantId
              AND target.{QuoteIdentifier(match.SubjectColumn)} = @subjectValue
            FOR UPDATE
            """;
        command.Parameters.Add(CreateParameter(command, "tenantId", erasureTenant.Id));
        command.Parameters.Add(CreateParameter(command, "subjectValue", match.SubjectValue));

        var candidateRecordIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            candidateRecordIds.Add(reader.GetValue(0).ToString()!);
        }

        return candidateRecordIds;
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
