using System.Data;
using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Infrastructure.Sweep;

public sealed class AnonymiseSweepStrategy : IRetentionSweepStrategy
{
    private readonly AnonymiseAssignmentResolver assignmentResolver;
    private readonly AnonymiseHandlerAwareMutationExecutor handlerAwareMutationExecutor;
    private readonly AnonymiseMutationExecutor mutationExecutor;
    private readonly AnonymisePreviewExecutor previewExecutor;
    private readonly AnonymiseRowLoader rowLoader;
    private readonly IServiceProvider? services;

    public AnonymiseSweepStrategy(
        DbContext db,
        IEnumerable<IAnonymiseValueFactory>? anonymiseValueFactories = null,
        IServiceProvider? services = null
    )
    {
        ArgumentNullException.ThrowIfNull(db);

        assignmentResolver = new AnonymiseAssignmentResolver(db, anonymiseValueFactories);
        rowLoader = new AnonymiseRowLoader(db, assignmentResolver);
        mutationExecutor = new AnonymiseMutationExecutor(assignmentResolver, rowLoader);
        previewExecutor = new AnonymisePreviewExecutor();
        handlerAwareMutationExecutor = new AnonymiseHandlerAwareMutationExecutor(
            assignmentResolver,
            rowLoader,
            mutationExecutor
        );
        this.services = services;
    }

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
        ValidateEntry(entry, rule, "preview");
        await EnsureConnectionOpenAsync(conn, ct);

        return await previewExecutor.ExecuteAsync(
            entry,
            AnonymiseFilterBuilder.CreateCutoffFilter(entry.AnchorColumn, cutoff),
            ctx.Tenant,
            ctx.Now,
            conn,
            ct
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
            AnonymiseFilterBuilder.CreateCutoffFilter(entry.AnchorColumn, cutoff),
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
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DateTimeOffset now,
        DbConnection conn,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(conn);

        var cutoff = CutoffCalculator.Compute(now, rule.Period, rule.LegalMin);
        ValidateEntry(entry, rule, "erasure previews");
        await EnsureConnectionOpenAsync(conn, ct);

        return await previewExecutor.ExecuteAsync(
            entry,
            AnonymiseFilterBuilder.Combine(
                AnonymiseFilterBuilder.CreateSubjectFilter(predicate),
                AnonymiseFilterBuilder.CreateCutoffFilter(entry.AnchorColumn, cutoff)
            ),
            tenant,
            now,
            conn,
            ct
        );
    }

    public async Task<SweepExecutionResult> EraseAsync(
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
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(conn);
        ArgumentNullException.ThrowIfNull(transaction);

        var cutoff = CutoffCalculator.Compute(now, rule.Period, rule.LegalMin);
        return await ExecuteMutationAsync(
            entry,
            rule,
            new RetentionResolutionContext(entry.Category, tenant, now, []),
            AnonymiseFilterBuilder.Combine(
                AnonymiseFilterBuilder.CreateSubjectFilter(predicate),
                AnonymiseFilterBuilder.CreateCutoffFilter(entry.AnchorColumn, cutoff)
            ),
            conn,
            transaction,
            execution,
            ct,
            "erasure"
        );
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

        var candidateRecordIds = await rowLoader.SelectCandidateRecordIdsAsync(
            entry,
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
            return await handlerAwareMutationExecutor.ExecuteAsync(
                entry,
                rule,
                ctx,
                conn,
                transaction,
                candidateRecordIds,
                filter,
                handlers,
                execution,
                ct
            );
        }

        return assignmentResolver.RequiresPerRowExecution(entry)
            ? await mutationExecutor.ExecutePerRowMutationAsync(
                entry,
                ctx.Tenant,
                ctx.Now,
                conn,
                transaction,
                candidateRecordIds,
                filter,
                ct
            )
            : await mutationExecutor.ExecuteSetBasedMutationAsync(
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
}
