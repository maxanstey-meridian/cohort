using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Infrastructure.Sweep;

public sealed class PurgeSweepStrategy(DbContext? db = null, IServiceProvider? services = null)
    : IRetentionSweepStrategy
{
    private static readonly MethodInfo ExecuteHandlerAwareSweepCoreMethod = typeof(PurgeSweepStrategy)
        .GetMethod(
            nameof(ExecuteHandlerAwareSweepCoreAsync),
            BindingFlags.Instance | BindingFlags.NonPublic
        )!;

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
        CancellationToken ct,
        SweepMutationContext? execution = null
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
                ct
            );
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

    private Task<SweepExecutionResult> ExecuteHandlerAwareSweepAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        SweepMutationContext execution,
        CancellationToken ct
    )
    {
        return (Task<SweepExecutionResult>)ExecuteHandlerAwareSweepCoreMethod
            .MakeGenericMethod(entry.EntityType)
            .Invoke(
                this,
                [entry, rule, ctx, conn, transaction, candidateRecordIds, handlers, execution, ct]
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
        CancellationToken ct
    )
        where TEntity : class
    {
        var runtimeDb = db
            ?? throw new InvalidOperationException(
                $"Handler-aware purge for {entry.EntityType.FullName} requires a DbContext-backed strategy instance."
            );
        var rows = await LoadHandlerRowsAsync<TEntity>(
            runtimeDb,
            entry,
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
        var heldCount = candidateRecordIds.Count - rows.Count;

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

            if (!await DeleteCapturedRowAsync(entry, ctx, conn, transaction, recordId, ct))
            {
                heldCount++;
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
            heldCount,
            RowDetailsPersisted: true
        );
    }

    private static async Task<List<TEntity>> LoadHandlerRowsAsync<TEntity>(
        DbContext db,
        RetentionEntry entry,
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

    private static async Task<bool> DeleteCapturedRowAsync(
        RetentionEntry entry,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        string recordId,
        CancellationToken ct
    )
    {
        var tenantClause = entry.Tenant is not null
            ? $"AND target.{QuoteIdentifier(entry.Tenant.TenantColumn)} = @tenantId"
            : "";

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            DELETE FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) = @recordId
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            RETURNING target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)}
            """;
        command.Parameters.Add(CreateParameter(command, "recordId", recordId));
        if (entry.Tenant is not null)
        {
            command.Parameters.Add(CreateParameter(command, "tenantId", ctx.Tenant.Id));
        }
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", ctx.Now));

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

        var candidateRecordIds = await SelectPreviewErasureCandidateRecordIdsAsync(
            entry,
            entry.Tenant?.TenantColumn,
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
                ct
            );
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

    private static async Task<IReadOnlyList<string>> SelectPreviewErasureCandidateRecordIdsAsync(
        RetentionEntry entry,
        string? tenantColumn,
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
