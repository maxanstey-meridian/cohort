using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Infrastructure.Sweep;

public sealed class PurgeSweepStrategy : IRetentionSweepStrategy
{
    private readonly RelationalSweepStrategyCore core;

    public PurgeSweepStrategy(DbContext? db = null, IServiceProvider? services = null)
    {
        core = new RelationalSweepStrategyCore(
            Strategy.Purge,
            nameof(PurgeSweepStrategy),
            db,
            services,
            eligibilityClause: static _ => "",
            mutationHead: static entry =>
                $"DELETE FROM {RelationalSweepStrategyCore.QuoteIdentifier(entry.TableName)} AS target",
            addMutationParameters: static (_, _, _) => { }
        );
    }

    public Strategy HandlesStrategy => Strategy.Purge;

    public Task<int> PreviewAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        CancellationToken ct
    )
    {
        return core.PreviewAsync(entry, rule, ctx, conn, ct);
    }

    public Task<int> CountHeldAsync(
        RetentionEntry entry,
        RetentionRule rule,
        RetentionResolutionContext ctx,
        DbConnection conn,
        CancellationToken ct
    )
    {
        return core.CountHeldAsync(entry, rule, ctx, conn, ct);
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

    public Task<int> PreviewEraseAsync(
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

    public Task<int> CountHeldForEraseAsync(
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
        return core.EraseAsync(entry, rule, predicate, tenant, now, conn, transaction, ct, execution);
    }
}
