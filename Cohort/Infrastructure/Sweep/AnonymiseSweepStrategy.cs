using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Cohort.Infrastructure.Sweep;

public sealed class AnonymiseSweepStrategy(
    DbContext db,
    IEnumerable<IAnonymiseValueFactory>? anonymiseValueFactories = null,
    IServiceProvider? services = null
) : IRetentionSweepStrategy
{
    private static readonly MethodInfo ExecuteHandlerAwareSweepCoreMethod =
        typeof(AnonymiseSweepStrategy).GetMethod(
            nameof(ExecuteHandlerAwareSweepCoreAsync),
            BindingFlags.Instance | BindingFlags.NonPublic
        )!;
    private readonly IReadOnlyDictionary<Type, IAnonymiseValueFactory> factories =
        (anonymiseValueFactories ?? Array.Empty<IAnonymiseValueFactory>())
        .GroupBy(factory => factory.GetType())
        .ToDictionary(group => group.Key, group => group.Last());
    private readonly DbContext modelDb = db ?? throw new ArgumentNullException(nameof(db));

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

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);
        return await PreviewMutationCountAsync(
            entry,
            rule,
            CreateCutoffFilter(entry.AnchorColumn, cutoff),
            ctx.Tenant,
            ctx.Now,
            conn,
            ct,
            "preview"
        );
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

        var cutoff = CutoffCalculator.Compute(ctx.Now, rule.Period, rule.LegalMin);
        return await ExecuteMutationAsync(
            entry,
            rule,
            ctx,
            CreateCutoffFilter(entry.AnchorColumn, cutoff),
            conn,
            transaction,
            execution,
            ct,
            "sweeps"
        );
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

        return await PreviewMutationCountAsync(
            entry,
            rule,
            CreateSubjectFilter(match.SubjectColumn, match.SubjectValue),
            tenant,
            now,
            conn,
            ct,
            "erasure previews"
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

        return await ExecuteMutationAsync(
            entry,
            rule,
            new RetentionResolutionContext(entry.Category, tenant, now, []),
            CreateSubjectFilter(match.SubjectColumn, match.SubjectValue),
            conn,
            transaction,
            execution,
            ct,
            "erasure"
        );
    }

    private async Task<int> PreviewMutationCountAsync(
        RetentionEntry entry,
        RetentionRule rule,
        SqlFilter filter,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        CancellationToken ct,
        string operation
    )
    {
        ValidateEntry(entry, rule, operation);
        await EnsureConnectionOpenAsync(conn, ct);

        await using var command = conn.CreateCommand();
        command.CommandText = BuildPreviewCountCommandText(
            entry,
            entry.Tenant?.TenantColumn,
            filter,
            entry.RecordId.RecordIdColumn
        );
        AddFilterParameters(command, filter);
        AddTenantParameter(command, entry.Tenant?.TenantColumn, tenant.Id);
        AddHoldParameters(command, entry.TableName, now);

        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private async Task<SweepExecutionResult> ExecuteMutationAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        SqlFilter filter,
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext? execution,
        CancellationToken ct,
        string operation
    )
    {
        ValidateEntry(entry, rule, operation);
        await EnsureConnectionOpenAsync(conn, ct);

        var candidateRecordIds = await SelectCandidateRecordIdsAsync(
            entry,
            entry.Tenant?.TenantColumn,
            ctx.Tenant.Id,
            conn,
            transaction,
            filter,
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

        return RequiresPerRowExecution(entry)
            ? await ExecutePerRowMutationAsync(
                entry,
                ctx.Tenant,
                ctx.Now,
                conn,
                transaction,
                candidateRecordIds,
                ct
            )
            : await ExecuteSetBasedMutationAsync(
                entry,
                ctx.Tenant,
                ctx.Now,
                conn,
                transaction,
                candidateRecordIds,
                filter,
                ct
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
        var rows = await LoadHandlerRowsAsync<TEntity>(
            modelDb,
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
        var staticAssignments = CreateStaticAssignments(entry, ctx.Tenant.Id, ctx.Now);
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

            var originalValues = CreateOriginalValuesFromEntity(entry, row);
            if (
                !await ExecuteCapturedRowUpdateAsync(
                    entry,
                    ctx.Tenant,
                    ctx.Now,
                    conn,
                    transaction,
                    recordId,
                    originalValues,
                    staticAssignments,
                    ct
                )
            )
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

    private async Task<SweepExecutionResult> ExecutePerRowMutationAsync(
        RetentionEntry entry,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        CancellationToken ct
    )
    {
        var updatableRows = await LoadUpdatableRowsAsync(
            entry,
            tenant,
            now,
            conn,
            transaction,
            candidateRecordIds,
            ct
        );
        if (updatableRows.Count == 0)
        {
            return new SweepExecutionResult([], candidateRecordIds.Count);
        }

        var affectedRecordIds = await ExecutePerRowUpdatesAsync(
            entry,
            tenant,
            now,
            conn,
            transaction,
            updatableRows,
            ct
        );
        return new SweepExecutionResult(
            affectedRecordIds,
            candidateRecordIds.Count - affectedRecordIds.Count
        );
    }

    private async Task<IReadOnlyList<string>> ExecutePerRowUpdatesAsync(
        RetentionEntry entry,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<AnonymiseRowSnapshot> rows,
        CancellationToken ct
    )
    {
        var staticAssignments = CreateStaticAssignments(entry, tenant.Id, now);
        var affectedRecordIds = new List<string>(rows.Count);

        foreach (var row in rows)
        {
            await using var command = conn.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = BuildPerRowCommandText(
                entry,
                entry.Tenant?.TenantColumn,
                entry.RecordId.RecordIdColumn
            );
            AddPerRowAssignmentParameters(command, entry, tenant, now, row.OriginalValues, staticAssignments);
            command.Parameters.Add(CreateParameter(command, "recordId", row.RecordId));
            AddTenantParameter(command, entry.Tenant?.TenantColumn, tenant.Id);
            AddHoldParameters(command, entry.TableName, now);

            await using var reader = await command.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                affectedRecordIds.Add(reader.GetValue(0).ToString()!);
            }
        }

        return affectedRecordIds;
    }

    private async Task<bool> ExecuteCapturedRowUpdateAsync(
        RetentionEntry entry,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        string recordId,
        IReadOnlyDictionary<string, object?> originalValues,
        IReadOnlyDictionary<string, object?> staticAssignments,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildPerRowCommandText(
            entry,
            entry.Tenant?.TenantColumn,
            entry.RecordId.RecordIdColumn
        );
        AddPerRowAssignmentParameters(command, entry, tenant, now, originalValues, staticAssignments);
        command.Parameters.Add(CreateParameter(command, "recordId", recordId));
        AddTenantParameter(command, entry.Tenant?.TenantColumn, tenant.Id);
        AddHoldParameters(command, entry.TableName, now);

        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct);
    }

    private async Task<IReadOnlyList<AnonymiseRowSnapshot>> LoadUpdatableRowsAsync(
        RetentionEntry entry,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        CancellationToken ct
    )
    {
        var originalValueFields = entry.AnonymiseFields
            .OfType<AnonymiseFactoryField>()
            .Where(field => ResolveFactory(field).RequiresOriginalValue)
            .ToArray();
        var tenantClause = entry.Tenant is not null
            ? $"AND target.{QuoteIdentifier(entry.Tenant.TenantColumn)} = @tenantId"
            : "";
        var selectedColumns = originalValueFields
            .Select(field => $"target.{QuoteIdentifier(field.ColumnName)}")
            .ToArray();

        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        var selectList = selectedColumns.Length == 0
            ? $"CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text)"
            : $"CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text), {string.Join(", ", selectedColumns)}";
        command.CommandText =
            $"""
            SELECT {selectList}
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text) = ANY(@candidateIds)
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", entry.RecordId.RecordIdColumn, entry.Tenant?.TenantColumn)}
            ORDER BY CAST(target.{QuoteIdentifier(entry.RecordId.RecordIdColumn)} AS text)
            """;
        command.Parameters.Add(CreateParameter(command, "candidateIds", candidateRecordIds.ToArray()));
        if (entry.Tenant is not null)
        {
            command.Parameters.Add(CreateParameter(command, "tenantId", tenant.Id));
        }
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", now));

        var rows = new List<AnonymiseRowSnapshot>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var originalValues = new Dictionary<string, object?>(StringComparer.Ordinal);
            for (var index = 0; index < originalValueFields.Length; index++)
            {
                var providerValue = reader.IsDBNull(index + 1) ? null : reader.GetValue(index + 1);
                originalValues[originalValueFields[index].MemberName] = ConvertOriginalValueFromProvider(
                    entry,
                    originalValueFields[index],
                    providerValue
                );
            }

            rows.Add(new AnonymiseRowSnapshot(reader.GetString(0), originalValues));
        }

        return rows;
    }

    private async Task<List<TEntity>> LoadHandlerRowsAsync<TEntity>(
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

    private IReadOnlyDictionary<string, object?> CreateOriginalValuesFromEntity<TEntity>(
        RetentionEntry entry,
        TEntity row
    )
        where TEntity : class
    {
        var originalValues = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (
            var field in entry.AnonymiseFields
                .OfType<AnonymiseFactoryField>()
                .Where(candidate => ResolveFactory(candidate).RequiresOriginalValue)
        )
        {
            var property =
                entry.EntityType.GetProperty(field.MemberName)
                ?? throw new InvalidOperationException(
                    $"Property '{field.MemberName}' on {entry.EntityType.FullName} is not mapped by the current EF model."
                );
            originalValues[field.MemberName] = property.GetValue(row);
        }

        return originalValues;
    }

    private Dictionary<string, object?> CreateStaticAssignments(
        RetentionEntry entry,
        Guid tenantId,
        DateTimeOffset now
    )
    {
        var values = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var field in entry.AnonymiseFields)
        {
            values[field.MemberName] = field switch
            {
                AnonymiseLiteralField literalField => CreateLiteralAssignmentValue(literalField),
                AnonymiseFactoryField factoryField when !ResolveFactory(factoryField).RequiresPerRowExecution
                    => ResolveFactory(factoryField)
                        .Create(
                            new AnonymiseValueContext(
                                entry.EntityType,
                                factoryField.MemberName,
                                null,
                                now,
                                tenantId
                            )
                        ),
                AnonymiseFactoryField => null,
                _ => throw new InvalidOperationException(
                    $"Anonymise field '{field.MemberName}' is not supported."
                ),
            };
        }

        return values;
    }

    private bool RequiresPerRowExecution(RetentionEntry entry)
    {
        return entry.AnonymiseFields
            .OfType<AnonymiseFactoryField>()
            .Any(field => ResolveFactory(field).RequiresPerRowExecution);
    }

    private IAnonymiseValueFactory ResolveFactory(AnonymiseFactoryField field)
    {
        if (!factories.TryGetValue(field.FactoryType, out var factory))
        {
            throw new InvalidOperationException(
                $"Anonymise field '{field.MemberName}' requires factory type {field.FactoryType.FullName}, but no matching {nameof(IAnonymiseValueFactory)} is registered."
            );
        }

        return factory;
    }

    private object? ConvertOriginalValueFromProvider(
        RetentionEntry entry,
        AnonymiseFactoryField field,
        object? providerValue
    )
    {
        if (providerValue is null)
        {
            return providerValue;
        }

        var property = ResolveEfProperty(entry, field.MemberName);
        var converter = property.GetTypeMapping().Converter;
        return converter?.ConvertFromProvider(providerValue) ?? providerValue;
    }

    private object? ConvertAssignmentValueToProvider(
        RetentionEntry entry,
        AnonymiseField field,
        object? value
    )
    {
        if (value is null or DBNull)
        {
            return value is DBNull ? null : value;
        }

        var property = ResolveEfProperty(entry, field.MemberName);
        var converter = property.GetTypeMapping().Converter;
        return converter?.ConvertToProvider(value) ?? value;
    }

    private IProperty ResolveEfProperty(RetentionEntry entry, string memberName)
    {
        var entityType =
            modelDb.Model.FindEntityType(entry.EntityType)
            ?? throw new InvalidOperationException(
                $"Entity {entry.EntityType.FullName} is not mapped by the current EF model."
            );
        return entityType.FindProperty(memberName)
            ?? throw new InvalidOperationException(
                $"Property '{memberName}' on {entry.EntityType.FullName} is not mapped by the current EF model."
            );
    }

    private async Task<List<string>> ReadAffectedRecordIdsAsync(
        DbCommand command,
        CancellationToken ct
    )
    {
        var affectedRecordIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            affectedRecordIds.Add(reader.GetValue(0).ToString()!);
        }

        return affectedRecordIds;
    }

    private async Task<SweepExecutionResult> ExecuteSetBasedMutationAsync(
        RetentionEntry entry,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        SqlFilter filter,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildSetBasedCommandText(
            entry,
            entry.Tenant?.TenantColumn,
            filter,
            entry.RecordId.RecordIdColumn
        );
        AddSetBasedAssignmentParameters(command, entry, tenant.Id, now);
        AddFilterParameters(command, filter);
        AddTenantParameter(command, entry.Tenant?.TenantColumn, tenant.Id);
        command.Parameters.Add(CreateParameter(command, "candidateIds", candidateRecordIds.ToArray()));
        AddHoldParameters(command, entry.TableName, now);

        var affectedRecordIds = await ReadAffectedRecordIdsAsync(command, ct);
        return new SweepExecutionResult(
            affectedRecordIds,
            candidateRecordIds.Count - affectedRecordIds.Count
        );
    }

    private void AddSetBasedAssignmentParameters(
        DbCommand command,
        RetentionEntry entry,
        Guid tenantId,
        DateTimeOffset now
    )
    {
        var staticAssignments = CreateStaticAssignments(entry, tenantId, now);

        for (var index = 0; index < entry.AnonymiseFields.Count; index++)
        {
            var field = entry.AnonymiseFields[index];
            command.Parameters.Add(
                CreateParameter(
                    command,
                    $"value{index}",
                    ConvertAssignmentValueToProvider(
                        entry,
                        field,
                        staticAssignments[field.MemberName]
                    )
                )
            );
        }
    }

    private void AddPerRowAssignmentParameters(
        DbCommand command,
        RetentionEntry entry,
        TenantContext tenant,
        DateTimeOffset now,
        IReadOnlyDictionary<string, object?> originalValues,
        IReadOnlyDictionary<string, object?> staticAssignments
    )
    {
        for (var index = 0; index < entry.AnonymiseFields.Count; index++)
        {
            var field = entry.AnonymiseFields[index];
            command.Parameters.Add(
                CreateParameter(
                    command,
                    $"value{index}",
                    ConvertAssignmentValueToProvider(
                        entry,
                        field,
                        ResolvePerRowAssignmentValue(
                            entry,
                            field,
                            tenant,
                            now,
                            originalValues,
                            staticAssignments
                        )
                    )
                )
            );
        }
    }

    private object? ResolvePerRowAssignmentValue(
        RetentionEntry entry,
        AnonymiseField field,
        TenantContext tenant,
        DateTimeOffset now,
        IReadOnlyDictionary<string, object?> originalValues,
        IReadOnlyDictionary<string, object?> staticAssignments
    )
    {
        return field switch
        {
            AnonymiseLiteralField literalField => CreateLiteralAssignmentValue(literalField),
            AnonymiseFactoryField factoryField when ResolveFactory(factoryField).RequiresPerRowExecution
                => ResolveFactory(factoryField)
                    .Create(
                        new AnonymiseValueContext(
                            entry.EntityType,
                            factoryField.MemberName,
                            originalValues.TryGetValue(factoryField.MemberName, out var originalValue)
                                ? originalValue
                                : null,
                            now,
                            tenant.Id
                        )
                    ),
            AnonymiseFactoryField factoryField => staticAssignments[factoryField.MemberName],
            _ => throw new InvalidOperationException(
                $"Anonymise field '{field.MemberName}' is not supported."
            ),
        };
    }

    private static string BuildPreviewCountCommandText(
        RetentionEntry entry,
        string? tenantColumn,
        SqlFilter filter,
        string recordIdColumn
    )
    {
        var tenantClause = tenantColumn is not null
            ? $"AND target.{QuoteIdentifier(tenantColumn)} = @tenantId"
            : "";

        return
            $"""
            SELECT COUNT(*)
            FROM {QuoteIdentifier(entry.TableName)} AS target
            WHERE {filter.PredicateSql}
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", recordIdColumn, tenantColumn)}
            """;
    }

    private static string BuildSetBasedCommandText(
        RetentionEntry entry,
        string? tenantColumn,
        SqlFilter filter,
        string recordIdColumn
    )
    {
        var assignments = entry.AnonymiseFields
            .Select((field, index) => $"{QuoteIdentifier(field.ColumnName)} = @value{index}");

        var tenantClause = tenantColumn is not null
            ? $"AND target.{QuoteIdentifier(tenantColumn)} = @tenantId"
            : "";

        return
            $"""
            UPDATE {QuoteIdentifier(entry.TableName)} AS target
            SET {string.Join(", ", assignments)}
            WHERE {filter.PredicateSql}
              {tenantClause}
              AND CAST(target.{QuoteIdentifier(recordIdColumn)} AS text) = ANY(@candidateIds)
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", recordIdColumn, tenantColumn)}
            RETURNING target.{QuoteIdentifier(recordIdColumn)}
            """;
    }

    private static string BuildPerRowCommandText(
        RetentionEntry entry,
        string? tenantColumn,
        string recordIdColumn
    )
    {
        var assignments = new List<string>(entry.AnonymiseFields.Count);

        for (var index = 0; index < entry.AnonymiseFields.Count; index++)
        {
            var field = entry.AnonymiseFields[index];
            assignments.Add($"{QuoteIdentifier(field.ColumnName)} = @value{index}");
        }

        var tenantClause = tenantColumn is not null
            ? $"AND target.{QuoteIdentifier(tenantColumn)} = @tenantId"
            : "";

        return
            $"""
            UPDATE {QuoteIdentifier(entry.TableName)} AS target
            SET {string.Join(", ", assignments)}
            WHERE CAST(target.{QuoteIdentifier(recordIdColumn)} AS text) = @recordId
              {tenantClause}
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", recordIdColumn, tenantColumn)}
            RETURNING target.{QuoteIdentifier(recordIdColumn)}
            """;
    }

    private static async Task<IReadOnlyList<string>> SelectCandidateRecordIdsAsync(
        RetentionEntry entry,
        string? tenantColumn,
        Guid tenantId,
        DbConnection conn,
        DbTransaction transaction,
        SqlFilter filter,
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
            WHERE {filter.PredicateSql}
              {tenantClause}
            FOR UPDATE
            """;
        AddFilterParameters(command, filter);
        AddTenantParameter(command, tenantColumn, tenantId);

        var candidateRecordIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            candidateRecordIds.Add(reader.GetValue(0).ToString()!);
        }

        return candidateRecordIds;
    }

    private static SqlFilter CreateCutoffFilter(string anchorColumn, DateTimeOffset cutoff)
    {
        return new SqlFilter(
            $"target.{QuoteIdentifier(anchorColumn)} < @cutoff",
            [new SqlFilterParameter("cutoff", cutoff)]
        );
    }

    private static SqlFilter CreateSubjectFilter(string subjectColumn, object subjectValue)
    {
        return new SqlFilter(
            $"target.{QuoteIdentifier(subjectColumn)} = @subjectValue",
            [new SqlFilterParameter("subjectValue", subjectValue)]
        );
    }

    private static void AddFilterParameters(DbCommand command, SqlFilter filter)
    {
        foreach (var parameter in filter.Parameters)
        {
            command.Parameters.Add(CreateParameter(command, parameter.Name, parameter.Value));
        }
    }

    private static void AddTenantParameter(DbCommand command, string? tenantColumn, Guid tenantId)
    {
        if (tenantColumn is not null)
        {
            command.Parameters.Add(CreateParameter(command, "tenantId", tenantId));
        }
    }

    private static void AddHoldParameters(
        DbCommand command,
        string tableName,
        DateTimeOffset now
    )
    {
        command.Parameters.Add(CreateParameter(command, "holdTableName", tableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", now));
    }

    private static object CreateLiteralAssignmentValue(AnonymiseLiteralField literalField)
    {
        return literalField.Method switch
        {
            AnonymiseMethod.Null => DBNull.Value,
            AnonymiseMethod.EmptyString => string.Empty,
            AnonymiseMethod.FixedLiteral => literalField.Literal
                ?? throw new InvalidOperationException(
                    $"Anonymise field '{literalField.MemberName}' requires a literal value."
                ),
            _ => throw new InvalidOperationException(
                $"Anonymise method '{literalField.Method}' is not supported."
            ),
        };
    }

    private static DbParameter CreateParameter(DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        return parameter;
    }

    private static DbParameter CreateProviderParameter(
        DbConnection conn,
        string name,
        object? value
    )
    {
        using var command = conn.CreateCommand();
        return CreateParameter(command, name, value);
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    private static void ValidateEntry(
        RetentionEntry entry,
        RetentionRule rule,
        string operation
    )
    {
        if (rule.Strategy != Strategy.Anonymise)
        {
            throw new InvalidOperationException(
                $"AnonymiseSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        if (entry.AnonymiseFields.Count == 0)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose anonymise metadata for anonymise {operation}."
            );
        }
    }

    private static async Task EnsureConnectionOpenAsync(DbConnection conn, CancellationToken ct)
    {
        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }
    }

    private sealed record AnonymiseRowSnapshot(
        string RecordId,
        IReadOnlyDictionary<string, object?> OriginalValues
    );

    private sealed record SqlFilter(string PredicateSql, IReadOnlyList<SqlFilterParameter> Parameters);

    private sealed record SqlFilterParameter(string Name, object? Value);
}
