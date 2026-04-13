using System.Data;
using System.Data.Common;
using System.Globalization;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Cohort.Infrastructure.Sweep;

public sealed class AnonymiseSweepStrategy(
    IEnumerable<IAnonymiseValueFactory>? anonymiseValueFactories = null,
    DbContext? db = null
) : IRetentionSweepStrategy
{
    private readonly IReadOnlyDictionary<Type, IAnonymiseValueFactory> factories =
        (anonymiseValueFactories ?? Array.Empty<IAnonymiseValueFactory>())
        .GroupBy(factory => factory.GetType())
        .ToDictionary(group => group.Key, group => group.Last());
    private readonly DbContext? modelDb = db;

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

        if (rule.Strategy != Strategy.Anonymise)
        {
            throw new InvalidOperationException(
                $"AnonymiseSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

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

        return RequiresPerRowExecution(entry)
            ? await ExecutePerRowSweepAsync(
                entry,
                ctx.Tenant,
                ctx.Now,
                conn,
                transaction,
                candidateRecordIds,
                ct
            )
            : await ExecuteSetBasedSweepAsync(
                entry,
                ctx.Tenant,
                ctx.Now,
                conn,
                transaction,
                candidateRecordIds,
                cutoff,
                ct
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

        if (rule.Strategy != Strategy.Anonymise)
        {
            throw new InvalidOperationException(
                $"AnonymiseSweepStrategy cannot execute {rule.Strategy} rules."
            );
        }

        if (entry.AnonymiseFields.Count == 0)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose anonymise metadata for anonymise erasure previews."
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

        return RequiresPerRowExecution(entry)
            ? await ExecutePerRowErasureAsync(
                entry,
                tenant,
                now,
                conn,
                transaction,
                candidateRecordIds,
                ct
            )
            : await ExecuteSetBasedErasureAsync(
                entry,
                match,
                tenant,
                now,
                conn,
                transaction,
                candidateRecordIds,
                ct
            );
    }

    private async Task<SweepExecutionResult> ExecuteSetBasedSweepAsync(
        RetentionEntry entry,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        DateTimeOffset cutoff,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildCommandText(
            entry,
            entry.Tenant?.TenantColumn,
            tenant.Id,
            now,
            command,
            entry.RecordId.RecordIdColumn
        );
        command.Parameters.Add(CreateParameter(command, "cutoff", cutoff));
        if (entry.Tenant is not null)
        {
            command.Parameters.Add(CreateParameter(command, "tenantId", tenant.Id));
        }
        command.Parameters.Add(CreateParameter(command, "candidateIds", candidateRecordIds.ToArray()));
        command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
        command.Parameters.Add(CreateParameter(command, "holdAsOf", now));

        var affectedRecordIds = await ReadAffectedRecordIdsAsync(command, ct);
        return new SweepExecutionResult(
            affectedRecordIds,
            candidateRecordIds.Count - affectedRecordIds.Count
        );
    }

    private async Task<SweepExecutionResult> ExecuteSetBasedErasureAsync(
        RetentionEntry entry,
        ErasureSubjectMatch match,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidateRecordIds,
        CancellationToken ct
    )
    {
        await using var command = conn.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildErasureCommandText(
            entry,
            entry.Tenant?.TenantColumn,
            tenant.Id,
            now,
            match,
            command,
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

        var affectedRecordIds = await ReadAffectedRecordIdsAsync(command, ct);
        return new SweepExecutionResult(
            affectedRecordIds,
            candidateRecordIds.Count - affectedRecordIds.Count
        );
    }

    private async Task<SweepExecutionResult> ExecutePerRowSweepAsync(
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

    private async Task<SweepExecutionResult> ExecutePerRowErasureAsync(
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

            for (var index = 0; index < entry.AnonymiseFields.Count; index++)
            {
                var field = entry.AnonymiseFields[index];
                var parameterName = $"value{index}";
                var value = field switch
                {
                    AnonymiseLiteralField literalField => CreateLiteralAssignmentValue(literalField),
                    AnonymiseFactoryField factoryField when ResolveFactory(factoryField).RequiresPerRowExecution
                        => ResolveFactory(factoryField)
                            .Create(
                                new AnonymiseValueContext(
                                    entry.EntityType,
                                    factoryField.MemberName,
                                    row.OriginalValues.TryGetValue(factoryField.MemberName, out var originalValue)
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
                command.Parameters.Add(CreateParameter(command, parameterName, value));
            }

            command.Parameters.Add(CreateParameter(command, "recordId", row.RecordId));
            if (entry.Tenant is not null)
            {
                command.Parameters.Add(CreateParameter(command, "tenantId", tenant.Id));
            }
            command.Parameters.Add(CreateParameter(command, "holdTableName", entry.TableName));
            command.Parameters.Add(CreateParameter(command, "holdAsOf", now));

            await using var reader = await command.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                affectedRecordIds.Add(reader.GetValue(0).ToString()!);
            }
        }

        return affectedRecordIds;
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
        if (providerValue is null || modelDb is null)
        {
            return providerValue;
        }

        var property = ResolveEfProperty(entry, field.MemberName);
        var converter = property.GetTypeMapping().Converter;
        return converter?.ConvertFromProvider(providerValue) ?? providerValue;
    }

    private IProperty ResolveEfProperty(RetentionEntry entry, string memberName)
    {
        var entityType =
            modelDb?.Model.FindEntityType(entry.EntityType)
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

    private string BuildCommandText(
        RetentionEntry entry,
        string? tenantColumn,
        Guid tenantId,
        DateTimeOffset now,
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
                CreateParameter(command, parameterName, CreateSetBasedAssignmentValue(entry, field, tenantId, now))
            );
        }

        var tenantClause = tenantColumn is not null
            ? $"AND target.{QuoteIdentifier(tenantColumn)} = @tenantId"
            : "";

        return
            $"""
            UPDATE {QuoteIdentifier(entry.TableName)} AS target
            SET {string.Join(", ", assignments)}
            WHERE target.{QuoteIdentifier(entry.AnchorColumn)} < @cutoff
              {tenantClause}
              AND CAST(target.{QuoteIdentifier(recordIdColumn)} AS text) = ANY(@candidateIds)
              AND {RetentionHoldSql.BuildActiveHoldExclusion("target", recordIdColumn, tenantColumn)}
            RETURNING target.{QuoteIdentifier(recordIdColumn)}
            """;
    }

    private string BuildErasureCommandText(
        RetentionEntry entry,
        string? tenantColumn,
        Guid tenantId,
        DateTimeOffset now,
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
                CreateParameter(command, parameterName, CreateSetBasedAssignmentValue(entry, field, tenantId, now))
            );
        }

        var tenantClause = tenantColumn is not null
            ? $"AND target.{QuoteIdentifier(tenantColumn)} = @tenantId"
            : "";

        return
            $"""
            UPDATE {QuoteIdentifier(entry.TableName)} AS target
            SET {string.Join(", ", assignments)}
            WHERE target.{QuoteIdentifier(match.SubjectColumn)} = @subjectValue
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

    private object? CreateSetBasedAssignmentValue(
        RetentionEntry entry,
        AnonymiseField field,
        Guid tenantId,
        DateTimeOffset now
    )
    {
        return field switch
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
            AnonymiseFactoryField factoryField => throw new InvalidOperationException(
                $"Anonymise field '{factoryField.MemberName}' requires per-row execution and cannot run on the set-based anonymise path."
            ),
            _ => throw new InvalidOperationException(
                $"Anonymise field '{field.MemberName}' is not supported."
            ),
        };
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

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    private sealed record AnonymiseRowSnapshot(
        string RecordId,
        IReadOnlyDictionary<string, object?> OriginalValues
    );
}
