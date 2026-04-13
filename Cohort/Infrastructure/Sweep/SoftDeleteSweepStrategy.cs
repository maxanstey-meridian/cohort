using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Infrastructure.Sweep;

public sealed class SoftDeleteSweepStrategy(DbContext? db = null, IServiceProvider? services = null)
    : IRetentionSweepStrategy
{
    private static readonly MethodInfo ExecuteHandlerAwareSweepCoreMethod =
        typeof(SoftDeleteSweepStrategy).GetMethod(
            nameof(ExecuteHandlerAwareSweepCoreAsync),
            BindingFlags.Instance | BindingFlags.NonPublic
        )!;

    public Strategy HandlesStrategy => Strategy.SoftDelete;

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

        if (rule.Strategy != Strategy.SoftDelete)
        {
            throw new InvalidOperationException(
                $"SoftDeleteSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        var softDelete = entry.SoftDelete
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose soft-delete metadata for soft-delete previews."
            );

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
              AND target.{QuoteIdentifier(softDelete.IsDeletedColumn)} = FALSE
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
        CancellationToken ct,
        SweepMutationContext? execution = null
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

        var softDelete = entry.SoftDelete
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose soft-delete metadata for soft-delete sweeps."
            );

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);
        var candidateRecordIds = await SelectCandidateRecordIdsAsync(
            entry,
            entry.Tenant?.TenantColumn,
            softDelete,
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

        var handlers = RetentionHandlerSupport.ResolveHandlers(services, entry.EntityType);
        if (execution is not null && handlers.Count > 0)
        {
            return await ExecuteHandlerAwareSweepAsync(
                entry,
                rule,
                ctx,
                conn,
                transaction,
                candidateRecordIds,
                handlers,
                execution,
                softDelete,
                ct
            );
        }

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildCommandText(entry, entry.Tenant?.TenantColumn, softDelete, entry.RecordId.RecordIdColumn);
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        if (entry.Tenant is not null)
        {
            command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));
        }
        command.Parameters.Add(CreateParameter(command, "candidateIds", candidateRecordIds.ToArray()));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", ctx.Now));

        if (softDelete.DeletedAtColumn is not null)
        {
            command.Parameters.Add(
                CreateParameter(command, "deletedAt", CreateDeletedAtValue(entry, softDelete, ctx.Now))
            );
        }

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

    private Task<SweepExecutionResult> ExecuteHandlerAwareSweepAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        SweepMutationContext execution,
        SoftDeleteConvention softDelete,
        CancellationToken ct
    )
    {
        return (Task<SweepExecutionResult>)ExecuteHandlerAwareSweepCoreMethod
            .MakeGenericMethod(entry.EntityType)
            .Invoke(
                this,
                [
                    entry,
                    rule,
                    ctx,
                    conn,
                    transaction,
                    candidateRecordIds,
                    handlers,
                    execution,
                    softDelete,
                    ct,
                ]
            )!;
    }

    private async Task<SweepExecutionResult> ExecuteHandlerAwareSweepCoreAsync<TEntity>(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        SweepMutationContext execution,
        SoftDeleteConvention softDelete,
        CancellationToken ct
    )
        where TEntity : class
    {
        var runtimeDb = db
            ?? throw new InvalidOperationException(
                $"Handler-aware soft-delete for {entry.EntityType.FullName} requires a DbContext-backed strategy instance."
            );
        var rows = await LoadHandlerRowsAsync<TEntity>(
            runtimeDb,
            entry,
            softDelete,
            ctx.Tenant,
            ctx.Now,
            conn,
            candidateRecordIds,
            ct
        );
        var recordIdProperty =
            typeof(TEntity).GetProperty(entry.RecordId.RecordIdMember)
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} references missing record-id member '{entry.RecordId.RecordIdMember}'."
            );
        var affectedRecordIds = new List<string>();

        foreach (var row in rows)
        {
            var recordId = recordIdProperty.GetValue(row)?.ToString();
            if (string.IsNullOrWhiteSpace(recordId))
            {
                throw new InvalidOperationException(
                    $"Retention row for {entry.EntityType.FullName} produced an empty record id for member '{entry.RecordId.RecordIdMember}'."
                );
            }

            var beforeContext = new RetentionBeforeContext(
                execution.SweepId,
                entry.Category,
                rule.Strategy,
                ctx.Tenant.Id,
                execution.At
            );

            try
            {
                await RetentionHandlerSupport.InvokeOnBeforeAsync(handlers, row, beforeContext, ct);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch
            {
                continue;
            }

            if (
                !await SoftDeleteCapturedRowAsync(
                    entry,
                    softDelete,
                    ctx,
                    conn,
                    transaction,
                    recordId,
                    ct
                )
            )
            {
                continue;
            }

            await RetentionHandlerSupport.PersistCapturedRowAsync(
                conn,
                transaction,
                execution,
                entry,
                rule.Strategy,
                ctx.Tenant.Id,
                recordId,
                new Dictionary<string, object?>(beforeContext.Snapshot, StringComparer.Ordinal),
                handlers,
                ct
            );
            affectedRecordIds.Add(recordId);
        }

        return new SweepExecutionResult(
            affectedRecordIds,
            candidateRecordIds.Count - affectedRecordIds.Count,
            RowDetailsPersisted: true
        );
    }

    private static async Task<List<TEntity>> LoadHandlerRowsAsync<TEntity>(
        DbContext db,
        RetentionEntry entry,
        SoftDeleteConvention softDelete,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        IReadOnlyList<string> candidateRecordIds,
        CancellationToken ct
    )
        where TEntity : class
    {
        var tenantClause = entry.Tenant is not null
            ? $"AND target.{QuoteIdentifier(entry.Tenant.TenantColumn)} = @tenantId"
            : "";
        var sql =
            $"""
            SELECT *
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) = ANY(@candidateIds)
              {tenantClause}
              AND target.{QuoteIdentifier(softDelete.IsDeletedColumn)} = FALSE
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            ORDER BY CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text)
            """;
        var parameters = new List<object>
        {
            CreateProviderParameter(conn, "candidateIds", candidateRecordIds.ToArray()),
            CreateProviderParameter(conn, "holdTableName", entry.TableName),
            CreateProviderParameter(conn, "holdAsOf", now),
        };
        if (entry.Tenant is not null)
        {
            parameters.Add(CreateProviderParameter(conn, "tenantId", tenant.Id));
        }

        return await db.Set<TEntity>().FromSqlRaw(sql, parameters.ToArray()).AsNoTracking().ToListAsync(ct);
    }

    private static async Task<bool> SoftDeleteCapturedRowAsync(
        RetentionEntry entry,
        SoftDeleteConvention softDelete,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        string recordId,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildCapturedRowCommandText(
            entry,
            entry.Tenant?.TenantColumn,
            softDelete,
            entry.RecordId.RecordIdColumn
        );
        command.Parameters.Add(CreateParameter(command, "recordId", recordId));
        if (entry.Tenant is not null)
        {
            command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));
        }
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", ctx.Now));

        if (softDelete.DeletedAtColumn is not null)
        {
            command.Parameters.Add(
                CreateParameter(command, "deletedAt", CreateDeletedAtValue(entry, softDelete, ctx.Now))
            );
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct);
    }

    public async Task<int> PreviewEraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectMatch match,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(match);
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(conn);

        if (rule.Strategy != Strategy.SoftDelete)
        {
            throw new InvalidOperationException(
                $"SoftDeleteSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        var softDelete = entry.SoftDelete
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose soft-delete metadata for soft-delete erasure previews."
            );

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }

        var candidateRecordIds = await SelectPreviewErasureCandidateRecordIdsAsync(
            entry,
            entry.Tenant?.TenantColumn,
            softDelete,
            match,
            tenant,
            conn,
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
        command.CommandText =
            $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(match.SubjectColumn)} = @subjectValue
              {tenantClause}
              AND target.{QuoteIdentifier(softDelete.IsDeletedColumn)} = FALSE
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
        CancellationToken ct,
        SweepMutationContext? execution = null
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

        var softDelete = entry.SoftDelete
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose soft-delete metadata for soft-delete erasure."
            );

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }

        var candidateRecordIds = await SelectErasureCandidateRecordIdsAsync(
            entry,
            entry.Tenant?.TenantColumn,
            softDelete,
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

        var handlers = RetentionHandlerSupport.ResolveHandlers(services, entry.EntityType);
        if (execution is not null && handlers.Count > 0)
        {
            return await ExecuteHandlerAwareSweepAsync(
                entry,
                rule,
                new RetentionResolutionContext(entry.Category, tenant, now, []),
                conn,
                transaction,
                candidateRecordIds,
                handlers,
                execution,
                softDelete,
                ct
            );
        }

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildErasureCommandText(
            entry,
            entry.Tenant?.TenantColumn,
            softDelete,
            match,
            entry.RecordId.RecordIdColumn
        );
        if (entry.Tenant is not null)
        {
            command.Parameters.Add(CreateParameter(command, "tenantId", tenant.Id));
        }
        command.Parameters.Add(CreateParameter(command, "subjectValue", match.SubjectValue));
        command.Parameters.Add(CreateParameter(command, "candidateIds", candidateRecordIds.ToArray()));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", now));

        if (softDelete.DeletedAtColumn is not null)
        {
            command.Parameters.Add(
                CreateParameter(command, "deletedAt", CreateDeletedAtValue(entry, softDelete, now))
            );
        }

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
        string? tenantColumn,
        SoftDeleteConvention softDelete,
        string recordIdColumn
    )
    {
        var deletedAtAssignment = softDelete.DeletedAtColumn is null
            ? ""
            : $", {QuoteIdentifier(softDelete.DeletedAtColumn)} = @deletedAt";

        var tenantClause = tenantColumn is not null
            ? $"AND target.{QuoteIdentifier(tenantColumn)} = @tenantId"
            : "";

        return
            $"""
            UPDATE {QuoteIdentifier(entry.TableName)} AS target
            SET {QuoteIdentifier(softDelete.IsDeletedColumn)} = TRUE{deletedAtAssignment}
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {tenantClause}
              AND target.{QuoteIdentifier(softDelete.IsDeletedColumn)} = FALSE
              AND CAST(target.{QuoteIdentifier(recordIdColumn)} AS text) = ANY(@candidateIds)
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", recordIdColumn, tenantColumn)}
            RETURNING target.{QuoteIdentifier(recordIdColumn)}
            """;
    }

    private static string BuildErasureCommandText(
        RetentionEntry entry,
        string? tenantColumn,
        SoftDeleteConvention softDelete,
        ErasureSubjectMatch match,
        string recordIdColumn
    )
    {
        var deletedAtAssignment = softDelete.DeletedAtColumn is null
            ? ""
            : $", {QuoteIdentifier(softDelete.DeletedAtColumn)} = @deletedAt";

        var tenantClause = tenantColumn is not null
            ? $"AND target.{QuoteIdentifier(tenantColumn)} = @tenantId"
            : "";

        return
            $"""
            UPDATE {QuoteIdentifier(entry.TableName)} AS target
            SET {QuoteIdentifier(softDelete.IsDeletedColumn)} = TRUE{deletedAtAssignment}
            WHERE target.{QuoteIdentifier(match.SubjectColumn)} = @subjectValue
              {tenantClause}
              AND target.{QuoteIdentifier(softDelete.IsDeletedColumn)} = FALSE
              AND CAST(target.{QuoteIdentifier(recordIdColumn)} AS text) = ANY(@candidateIds)
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", recordIdColumn, tenantColumn)}
            RETURNING target.{QuoteIdentifier(recordIdColumn)}
            """;
    }

    private static string BuildCapturedRowCommandText(
        RetentionEntry entry,
        string? tenantColumn,
        SoftDeleteConvention softDelete,
        string recordIdColumn
    )
    {
        var deletedAtAssignment = softDelete.DeletedAtColumn is null
            ? ""
            : $", {QuoteIdentifier(softDelete.DeletedAtColumn)} = @deletedAt";

        var tenantClause = tenantColumn is not null
            ? $"AND target.{QuoteIdentifier(tenantColumn)} = @tenantId"
            : "";

        return
            $"""
            UPDATE {QuoteIdentifier(entry.TableName)} AS target
            SET {QuoteIdentifier(softDelete.IsDeletedColumn)} = TRUE{deletedAtAssignment}
            WHERE CAST(target.{QuoteIdentifier(recordIdColumn)} AS text) = @recordId
              {tenantClause}
              AND target.{QuoteIdentifier(softDelete.IsDeletedColumn)} = FALSE
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", recordIdColumn, tenantColumn)}
            RETURNING target.{QuoteIdentifier(recordIdColumn)}
            """;
    }

    private static async Task<IReadOnlyList<string>> SelectCandidateRecordIdsAsync(
        RetentionEntry entry,
        string? tenantColumn,
        SoftDeleteConvention softDelete,
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
              AND target.{QuoteIdentifier(softDelete.IsDeletedColumn)} = FALSE
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
        SoftDeleteConvention softDelete,
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
              AND target.{QuoteIdentifier(softDelete.IsDeletedColumn)} = FALSE
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

    private static async Task<IReadOnlyList<string>> SelectPreviewErasureCandidateRecordIdsAsync(
        RetentionEntry entry,
        string? tenantColumn,
        SoftDeleteConvention softDelete,
        ErasureSubjectMatch match,
        TenantContext erasureTenant,
        DbConnection conn,
        CancellationToken ct
    )
    {
        var tenantClause = tenantColumn is not null
            ? $"AND target.{QuoteIdentifier(tenantColumn)} = @tenantId"
            : "";

        await using var command = conn.CreateCommand();
        command.CommandText =
            $"""
            SELECT target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE target.{QuoteIdentifier(match.SubjectColumn)} = @subjectValue
              {tenantClause}
              AND target.{QuoteIdentifier(softDelete.IsDeletedColumn)} = FALSE
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

    private static DbParameter CreateProviderParameter(
        DbConnection conn,
        string name,
        object value
    )
    {
        using var command = conn.CreateCommand();
        return CreateParameter(command, name, value);
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }
}
