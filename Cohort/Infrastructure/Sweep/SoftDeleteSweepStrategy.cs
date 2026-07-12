using System.Data.Common;
using Cohort.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure.Sweep;

internal sealed class SoftDeleteSweepStrategy : IRetentionSweepStrategy
{
    private readonly RelationalSweepStrategyCore core;

    public SoftDeleteSweepStrategy(
        [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext? db = null,
        IServiceProvider? services = null
    )
    {
        core = new RelationalSweepStrategyCore(
            Strategy.SoftDelete,
            nameof(SoftDeleteSweepStrategy),
            db,
            services,
            eligibilityClause: static entry =>
                $"AND target.{RelationalSweepStrategyCore.QuoteIdentifier(RequireSoftDelete(entry).IsDeletedColumn)} = FALSE",
            mutationHead: static entry =>
            {
                var softDelete = RequireSoftDelete(entry);
                var deletedAtAssignment = softDelete.DeletedAtColumn is null
                    ? ""
                    : $", {RelationalSweepStrategyCore.QuoteIdentifier(softDelete.DeletedAtColumn)} = @deletedAt";

                return $"""
                    UPDATE {PostgreSqlIdentifier.Format(entry.Table)} AS target
                    SET {RelationalSweepStrategyCore.QuoteIdentifier(
                        softDelete.IsDeletedColumn
                    )} = TRUE{deletedAtAssignment}
                    """;
            },
            addMutationParameters: static (command, entry, now) =>
            {
                var softDelete = RequireSoftDelete(entry);
                if (softDelete.DeletedAtColumn is not null)
                {
                    command.Parameters.Add(
                        RelationalSweepStrategyCore.CreateParameter(
                            command,
                            "deletedAt",
                            CreateDeletedAtValue(entry, softDelete, now)
                        )
                    );
                }
            }
        );
    }

    public Strategy HandlesStrategy => Strategy.SoftDelete;

    public Task<long> PreviewAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        CancellationToken ct
    )
    {
        return core.PreviewAsync(entry, rule, ctx, conn, ct);
    }

    public Task<long> CountHeldAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        CancellationToken ct
    )
    {
        return core.CountHeldAsync(entry, rule, ctx, conn, ct);
    }

    public Task<long> CountNullAnchorsAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        CancellationToken ct
    )
    {
        return core.CountNullAnchorsAsync(entry, rule, ctx, conn, ct);
    }

    public Task<SweepExecutionResult> SweepAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        DbTransaction transaction,
        CancellationToken ct,
        SweepMutationContext? execution = null
    )
    {
        return core.SweepAsync(entry, rule, ctx, conn, transaction, ct, execution);
    }

    public Task<long> PreviewEraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        CancellationToken ct
    )
    {
        return core.PreviewEraseAsync(entry, rule, predicate, tenant, now, conn, ct);
    }

    public Task<long> CountHeldForEraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        CancellationToken ct
    )
    {
        return core.CountHeldForEraseAsync(entry, rule, predicate, tenant, now, conn, ct);
    }

    public Task<long> CountNullAnchorsForEraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DbConnection conn,
        CancellationToken ct
    )
    {
        return core.CountNullAnchorsForEraseAsync(entry, rule, predicate, tenant, conn, ct);
    }

    public Task<SweepExecutionResult> EraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        DbTransaction transaction,
        CancellationToken ct,
        SweepMutationContext? execution = null
    )
    {
        return core.EraseAsync(
            entry,
            rule,
            predicate,
            tenant,
            now,
            conn,
            transaction,
            ct,
            execution
        );
    }

    private static SoftDeleteConvention RequireSoftDelete(RetentionEntry entry)
    {
        return entry.SoftDelete
            ?? throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} must expose soft-delete metadata for soft-delete operations."
            );
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

        var deletedAtProperty = ReflectionMemberResolver.FindPropertyByName(
            entry.EntityType,
            softDelete.DeletedAtMember
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
}
