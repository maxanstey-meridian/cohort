using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Audit;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cohort.Infrastructure;

internal sealed class RetentionErasureService(
    [FromKeyedServices(CohortServiceKeys.DbContext)] DbContext db,
    RetentionRegistry registry,
    IRetentionRuleProvider ruleProvider,
    RetentionRuntimeReadinessValidator readinessValidator,
    RetentionValidationState validationState,
    EfRetentionAuditWriter auditWriter,
    RetentionAuditNotifier auditNotifier,
    SweepStrategy strategy,
    IEnumerable<IRetentionDependentsRegistration> dependentsRegistrations,
    IRetentionExecutionSettings options,
    CohortCommitInterceptor commitInterceptor,
    ILogger<RetentionErasureService> logger
)
{
    public async Task<ErasureResult> ExecuteAsync(
        RetentionErasureRequest request,
        CancellationToken ct = default
    )
    {
        var tenant = request.RunTenant;
        var entities = SweepEntityScopes.For(tenant);
        var scope = request.Scope;
        var callerCommit = request.Transaction == ErasureTransaction.Caller
            ? await ValidateCallerTransactionAsync(ct)
            : null;

        await readinessValidator.ValidateAsync(ct);
        // Refused before any run exists: a kind no retained entity declares would erase nothing.
        var validation = validationState.For(db.Model);
        ErasureSubjectValidation.Validate(
            validation,
            registry,
            scope.Kind,
            scope.Subject,
            entities,
            "the erasure"
        );
        var (now, startedAt) = await DatabaseClock.ResolveRunTimeAsync(db, request.At, scope.DryRun, ct);

        var run = new RetentionRun(
            db,
            auditWriter,
            auditNotifier,
            logger,
            new SweepEvent.Started(
                Guid.NewGuid(),
                startedAt,
                SweepTriggerKind.Erasure,
                scope.DryRun,
                tenant.Id,
                scope.Kind
            ),
            options.SweepBatchSize,
            callerCommit
        );
        // Built before the run exists: a request refused by policy leaves no attempted run.
        var plan = await BuildExecutionPlanAsync(tenant, entities, scope, now, run, ct);
        await run.ExecuteAsync(() => run.RunEntitiesAsync(plan, strategy, dependentsRegistrations, ct), ct);
        return run.CreateErasureResult(scope);
    }

    /// <summary>
    /// <see cref="ErasureTransaction.Caller"/> joins the transaction open on this scope's
    /// DbContext, begun through EF or a System.Transactions transaction the context is enlisted
    /// in. It must be open and READ COMMITTED (each statement's fresh snapshot is
    /// what lets the hold checks see a hold committed after the caller's transaction began),
    /// the context must hold no unsaved changes (they would commit unchecked inside the
    /// erasure's transaction), and for a transaction begun through EF Cohort's commit
    /// interceptor must be on the context, or observers would never hear of the run. A
    /// System.Transactions transaction reports its own completion, so it needs no interceptor.
    /// </summary>
    private async Task<CohortCommitInterceptor> ValidateCallerTransactionAsync(CancellationToken ct)
    {
        var transaction = ContextTransaction.Of(db.Database);
        if (transaction is ContextTransaction.None)
        {
            throw new InvalidOperationException(
                "ErasureTransaction.Caller needs an open transaction on the scoped DbContext."
            );
        }

        // EF's own condition: a retrying strategy refuses EF work in a transaction begun outside
        // it, unless the call is running inside the strategy. Checked first so the host gets this
        // message, not EF's from the first call that goes through the strategy (the run-time
        // read opens the connection through EF, and a step's work follows). Only strategies
        // derived from EF's ExecutionStrategy make that refusal, and only they set Current, so
        // one that implements IExecutionStrategy on its own is never refused here.
        if (
            db.Database.CreateExecutionStrategy() is ExecutionStrategy { RetriesOnFailure: true }
            && ExecutionStrategy.Current is null
        )
        {
            throw new InvalidOperationException(
                "ErasureTransaction.Caller on a DbContext with a retrying execution strategy (EnableRetryOnFailure) must run inside that strategy: wrap your transaction and this call in db.Database.CreateExecutionStrategy().ExecuteAsync(...), as EF requires of your own work in it."
            );
        }

        // Explicit, so a context with automatic change detection turned off is checked too.
        db.ChangeTracker.DetectChanges();
        if (db.ChangeTracker.HasChanges())
        {
            throw new InvalidOperationException(
                "ErasureTransaction.Caller needs a DbContext with no unsaved changes; save or discard them before erasing."
            );
        }

        if (transaction is ContextTransaction.Ef && !commitInterceptor.IsRegisteredOn(db))
        {
            throw new InvalidOperationException(
                $"ErasureTransaction.Caller needs Cohort's commit interceptor on {db.GetType().FullName}'s options. AddCohort<TContext>() adds it to contexts built from the container's DbContextOptions; resolve the DbContext from the same container instead of constructing its options yourself."
            );
        }

        // Opened through EF, so a System.Transactions transaction the context hasn't used yet is
        // joined (and its isolation read) rather than read from a closed connection.
        string? isolation;
        await db.Database.OpenConnectionAsync(ct);
        try
        {
            var statement = CohortStatements.TransactionIsolation();
            await using var command = statement.Parameters.CreateCommand(
                db.Database.GetDbConnection(),
                transaction.Commands,
                statement.Sql
            );
            isolation = (string?)await command.ExecuteScalarAsync(ct);
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }

        // PostgreSQL accepts READ UNCOMMITTED and runs it as READ COMMITTED.
        if (isolation is not ("read committed" or "read uncommitted"))
        {
            throw new InvalidOperationException(
                $"ErasureTransaction.Caller needs a READ COMMITTED transaction, but the caller's is {isolation?.ToUpperInvariant()}: under a transaction-wide snapshot the hold checks could miss a hold committed after the transaction began."
            );
        }

        return commitInterceptor;
    }

    private async Task<IReadOnlyList<SweepScope>> BuildExecutionPlanAsync(
        TenantContext tenant,
        SweepEntityScope entities,
        ErasureScope scope,
        DateTimeOffset now,
        RetentionRun run,
        CancellationToken ct
    )
    {
        var plan = new List<SweepScope>();
        var timeZone = options.TimeZone;
        foreach (var entry in registry.Scan().Values)
        {
            // A tenanted erasure never reaches tenantless rows (no tenant owns them), and a
            // tenantless one never reaches a tenant's.
            if (!entities.Includes(entry))
            {
                continue;
            }

            if (!entry.ErasureSubjects.TryGetValue(scope.Kind, out var subjectMetadata))
            {
                continue;
            }

            RetentionRule rule;
            try
            {
                rule = await RetentionRuleProviderResolution.ResolveAsync(
                    ruleProvider,
                    readinessValidator.ValidatedCapabilities,
                    new RetentionResolutionContext(entry.Category, tenant, now),
                    ct
                );
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                run.RecordEntityFailure(entry.EntityType, ex, "failed to prepare");
                continue;
            }

            if (rule.Strategy == Strategy.SoftDelete && !scope.AllowSoftDeleteAsErasure)
            {
                throw new InvalidOperationException(
                    $"Erasure for entity {entry.EntityType.FullName} (category '{entry.Category}') resolves to the SoftDelete strategy, which only sets the soft-delete flag and leaves personal data in place. If that genuinely satisfies the erasure request, opt in with new ErasureScope(kind, subject, allowSoftDeleteAsErasure: true)."
                );
            }

            try
            {
                plan.Add(
                    SweepScope.ForErasure(
                        entry,
                        rule,
                        subjectMetadata.CreatePredicate(scope.Subject),
                        tenant,
                        now,
                        timeZone
                    )
                );
            }
            catch (Exception ex)
            {
                run.RecordEntityFailure(entry.EntityType, ex, "failed to prepare");
            }
        }

        return plan;
    }
}
