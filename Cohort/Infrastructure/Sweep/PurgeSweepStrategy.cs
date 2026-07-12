using System.Data.Common;
using Cohort.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Infrastructure.Sweep;

internal sealed class PurgeSweepStrategy : IRetentionSweepStrategy
{
    private readonly RelationalSweepStrategyCore core;

    public PurgeSweepStrategy(
        [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext? db = null,
        IServiceProvider? services = null
    )
    {
        core = new RelationalSweepStrategyCore(
            Strategy.Purge,
            nameof(PurgeSweepStrategy),
            db,
            services,
            eligibilityClause: static _ => "",
            mutationHead: static entry =>
                $"DELETE FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target",
            addMutationParameters: static (_, _, _) => { }
        );
    }

    public Strategy HandlesStrategy => Strategy.Purge;

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
}
