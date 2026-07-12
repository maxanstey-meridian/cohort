using System.Data;
using System.Data.Common;
using Cohort.Application;
using Cohort.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure.Sweep;

internal sealed class AnonymiseSweepStrategy : IRetentionSweepStrategy
{
    private readonly AnonymiseAssignmentResolver assignmentResolver;
    private readonly AnonymiseHandlerAwareMutationExecutor handlerAwareMutationExecutor;
    private readonly AnonymiseMutationExecutor mutationExecutor;
    private readonly AnonymisePreviewExecutor previewExecutor;
    private readonly AnonymiseRowLoader rowLoader;
    private readonly IServiceProvider? services;

    public AnonymiseSweepStrategy(
        [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
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
            mutationExecutor,
            services
                ?.GetService<ILoggerFactory>()
                ?.CreateLogger(typeof(RetentionHandlerSupport).FullName!)
        );
        this.services = services;
    }

    public Strategy HandlesStrategy => Strategy.Anonymise;

    public async Task<long> PreviewAsync(
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
            AnonymiseFilterBuilder.CreateCutoffFilter(entry, cutoff),
            ctx.Tenant,
            conn,
            ct
        );
    }

    public async Task<long> CountHeldAsync(
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
        ValidateEntry(entry, rule, "held counts");
        await EnsureConnectionOpenAsync(conn, ct);

        return await previewExecutor.ExecuteHeldCountAsync(
            entry,
            AnonymiseFilterBuilder.CreateCutoffFilter(entry, cutoff),
            ctx.Tenant,
            conn,
            ct
        );
    }

    public async Task<long> CountNullAnchorsAsync(
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

        ValidateEntry(entry, rule, "null-anchor counts");
        await EnsureConnectionOpenAsync(conn, ct);

        return await previewExecutor.ExecuteNullAnchorCountAsync(
            entry,
            AnonymiseFilterBuilder.CreateNullAnchorFilter(entry),
            ctx.Tenant,
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
            AnonymiseFilterBuilder.CreateCutoffFilter(entry, cutoff),
            conn,
            transaction,
            execution,
            ct,
            "sweeps",
            skipLocked: true
        );
    }

    public async Task<long> PreviewEraseAsync(
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

        var cutoff = CutoffCalculator.ComputeErasureCutoff(now, rule.LegalMin);
        ValidateEntry(entry, rule, "erasure previews");
        await EnsureConnectionOpenAsync(conn, ct);

        return await previewExecutor.ExecuteAsync(
            entry,
            AnonymiseFilterBuilder.CreateErasureFilter(entry, predicate, cutoff),
            tenant,
            conn,
            ct
        );
    }

    public async Task<long> CountHeldForEraseAsync(
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

        var cutoff = CutoffCalculator.ComputeErasureCutoff(now, rule.LegalMin);
        ValidateEntry(entry, rule, "erasure held counts");
        await EnsureConnectionOpenAsync(conn, ct);

        return await previewExecutor.ExecuteHeldCountAsync(
            entry,
            AnonymiseFilterBuilder.CreateErasureFilter(entry, predicate, cutoff),
            tenant,
            conn,
            ct
        );
    }

    public async Task<long> CountNullAnchorsForEraseAsync(
        RetentionEntry entry,
        RetentionRule rule,
        ErasureSubjectPredicate predicate,
        TenantContext tenant,
        DbConnection conn,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(rule);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(conn);

        ValidateEntry(entry, rule, "erasure null-anchor counts");
        await EnsureConnectionOpenAsync(conn, ct);

        return await previewExecutor.ExecuteNullAnchorCountAsync(
            entry,
            AnonymiseFilterBuilder.Combine(
                AnonymiseFilterBuilder.CreateSubjectFilter(predicate),
                AnonymiseFilterBuilder.CreateNullAnchorFilter(entry)
            ),
            tenant,
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

        var cutoff = CutoffCalculator.ComputeErasureCutoff(now, rule.LegalMin);
        return await ExecuteMutationAsync(
            entry,
            rule,
            new RetentionResolutionContext(entry.Category, tenant, now, []),
            AnonymiseFilterBuilder.CreateErasureFilter(entry, predicate, cutoff),
            conn,
            transaction,
            execution,
            ct,
            "erasure",
            skipLocked: false
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
        string operation,
        bool skipLocked
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
            execution,
            skipLocked,
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

    private static void ValidateEntry(RetentionEntry entry, RetentionRule rule, string operation)
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

        // The startup validator only enforces the marker for rules it can resolve at
        // boot (TryResolveAtStartup). A runtime-resolved Anonymise rule on an entity
        // without the marker would otherwise re-select the same rows every batch — the
        // mutation never shrinks the candidate filter, so the sweep loop never ends.
        if (entry.AnonymisedAt is null)
        {
            throw new InvalidOperationException(
                $"Retention entry for {entry.EntityType.FullName} resolves to the Anonymise strategy but has no AnonymisedAt marker (a nullable DateTimeOffset named AnonymisedAt by convention, or marked with [RetentionAnonymisedAt]). Without it anonymisation cannot tell scrubbed rows from pending ones and batched {operation} would never terminate."
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
