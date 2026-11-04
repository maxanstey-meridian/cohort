using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Audit;
using Cohort.Infrastructure.Handlers;
using Cohort.Infrastructure.Holds;
using Cohort.Infrastructure.Sweep;

namespace Cohort.Infrastructure;

/// <summary>Which operation, and so which scope's role, sends a group of statements.</summary>
internal enum RetentionStatementGroupKind
{
    Sweep,
    Erasure,
    Preview,
    Holds,
    Deletion,
    Scrub,
    Dispatch,
    Worker,
    Pruning,
}

/// <summary>
/// What history pruning is configured to prune: settled runs, inactive holds, and whether it
/// reads the database clock to find the current occurrence.
/// </summary>
internal sealed record PruningScope(bool Runs, bool Holds, bool ReadsClock);

/// <summary>
/// The statements of one operation, checked together under the role that sends them. Only a
/// <see cref="RetentionStatementGroupKind.Pruning"/> group carries a <see cref="PruningScope"/>.
/// </summary>
internal sealed record RetentionStatementGroup
{
    public RetentionStatementGroup(
        RetentionStatementGroupKind kind,
        PruningScope? pruningScope = null
    )
    {
        if ((kind == RetentionStatementGroupKind.Pruning) != (pruningScope is not null))
        {
            throw new ArgumentException(
                "A pruning group carries its scope, and no other group does.",
                nameof(pruningScope)
            );
        }

        Kind = kind;
        PruningScope = pruningScope;
    }

    public RetentionStatementGroupKind Kind { get; }

    public PruningScope? PruningScope { get; }

    internal static RetentionStatementGroup Sweep { get; } = new(RetentionStatementGroupKind.Sweep);

    internal static RetentionStatementGroup Erasure { get; } =
        new(RetentionStatementGroupKind.Erasure);

    internal static RetentionStatementGroup Preview { get; } =
        new(RetentionStatementGroupKind.Preview);

    internal static RetentionStatementGroup Holds { get; } = new(RetentionStatementGroupKind.Holds);

    internal static RetentionStatementGroup Deletion { get; } =
        new(RetentionStatementGroupKind.Deletion);

    internal static RetentionStatementGroup Scrub { get; } = new(RetentionStatementGroupKind.Scrub);

    internal static RetentionStatementGroup Dispatch { get; } =
        new(RetentionStatementGroupKind.Dispatch);

    internal static RetentionStatementGroup Worker { get; } =
        new(RetentionStatementGroupKind.Worker);

    internal static RetentionStatementGroup Pruning(PruningScope scope) =>
        new(RetentionStatementGroupKind.Pruning, scope);
}

/// <summary>
/// The values readiness binds to the statements that name Cohort's own tables and the PostgreSQL
/// functions Cohort calls. Everything that is not data is a real, non-NULL value (an id, a
/// status, a time, a lock key, a one-element array): a strict function whose arguments were all
/// NULL would be folded away at plan time, and its <c>EXECUTE</c> privilege never checked, while
/// <c>EXPLAIN</c> never runs the statement. The data a run binds (a record id, a captured payload,
/// a hold's reason, an error text) is a <see cref="ReadinessTypedNull"/>, typed as the run's
/// value is. Readiness supplies the tenant and one retained entity's identity.
/// </summary>
internal sealed record CohortStatementValues(
    Guid TenantId,
    Type EntityType,
    Guid RetentionEntityId,
    string Category
)
{
    /// <summary>The run every statement of a group names.</summary>
    internal Guid SweepId { get; } = new("c0de0000-0000-0000-0000-000000000002");

    internal Guid HoldId { get; } = new("c0de0000-0000-0000-0000-000000000003");

    /// <summary>The handler a requeue is limited to.</summary>
    internal Guid HandlerId { get; } = new("c0de0000-0000-0000-0000-000000000004");

    /// <summary>The token a handler claim is made and owned under.</summary>
    internal Guid ClaimToken { get; } = new("c0de0000-0000-0000-0000-000000000005");

    /// <summary>The id of the row detail a handler status belongs to.</summary>
    internal long RowDetailId { get; } = 1;

    /// <summary>The id of the handler status a claim, settlement, or delivery lock names.</summary>
    internal long StatusId { get; } = 1;

    /// <summary>The time every timestamp and cutoff is bound to.</summary>
    internal DateTimeOffset Now { get; } = DateTimeOffset.UtcNow;

    /// <summary>The string a lock key is hashed from.</summary>
    internal string LockKey { get; } = "readiness";

    internal object? RecordId { get; } = new ReadinessTypedNull(typeof(string));

    internal object? CapturedPayload { get; } = new ReadinessTypedNull(typeof(string));

    internal object? Reason { get; } = new ReadinessTypedNull(typeof(string));

    internal object? Error { get; } = new ReadinessTypedNull(typeof(string));
}

/// <summary>
/// Every statement Cohort sends against its own five tables and the PostgreSQL functions it calls,
/// per port group: the same builders the run executes, over <see cref="CohortStatementValues"/>.
/// The host-table statements of a group are not here, except the worker's tenant-coverage scan,
/// which only the worker sends.
/// </summary>
internal static class CohortStatements
{
    /// <summary>A statement that names no entity and reads the given Cohort tables, if any.</summary>
    internal static SweepStatement Statement(
        string name,
        string sql,
        SqlParams parameters,
        params RelationalObjectName[] relations
    ) => new(name, null, sql, parameters, relations);

    /// <summary>
    /// The isolation level of the transaction a call joins, which a hold or an erasure in a
    /// caller's transaction checks before it writes.
    /// </summary>
    internal static SweepStatement TransactionIsolation() =>
        Statement(
            "transaction-isolation",
            "SELECT pg_catalog.current_setting('transaction_isolation')",
            new SqlParams()
        );

    /// <summary>
    /// What a sweep, a dry run, or a scheduled pass sends: run recording (R1, S1, S2 with R2, D1,
    /// R3, and, with <paramref name="handlerBookkeeping"/>, D2 and T1), H9 when the hold guard
    /// reads tenantless holds with a dependents step, the pass ledger read with a tenant (R6), the
    /// run lock, the pass lock, the entity lock, the held-locks check, and the clock.
    /// </summary>
    internal static IReadOnlyList<SweepStatement> ForSweep(
        CohortStoreTables tables,
        bool handlerBookkeeping,
        bool guardReadsTenantlessHolds,
        CohortStatementValues values
    )
    {
        // A scheduled pass holds its pass lock on the run's session, so its run commits only while
        // it holds both keys (RetentionRun.HeldKeys).
        var runKey = RetentionRunAdvisoryLock.KeyFor(values.SweepId);
        var passKey = RetentionRunAdvisoryLock.ScheduledPassKeyFor(
            tables.SweepRun,
            values.TenantId,
            dryRun: false
        );
        var statements = new List<SweepStatement>();
        AddRun(
            statements,
            tables,
            handlerBookkeeping,
            guardReadsTenantlessHolds,
            values,
            [runKey, passKey]
        );
        statements.Add(RetentionRunAdvisoryLock.TryLockStatement(passKey));
        statements.Add(
            RetentionRunLedgerSql.OccurrenceStatement(
                tables.SweepRun,
                values.Now,
                dryRun: false,
                values.TenantId
            )
        );
        return statements;
    }

    /// <summary>
    /// What an erasure sends: run recording, H9 when the hold guard reads tenantless holds with a
    /// dependents step, the run lock and its unlock, the entity lock, the held-locks check over
    /// the run's key alone (an erasure takes no pass lock and reads no ledger occurrence), the
    /// clock, the commit check of a run in the caller's transaction (R7), and the caller's
    /// isolation level.
    /// </summary>
    internal static IReadOnlyList<SweepStatement> ForErasure(
        CohortStoreTables tables,
        bool handlerBookkeeping,
        bool guardReadsTenantlessHolds,
        CohortStatementValues values
    )
    {
        var statements = new List<SweepStatement>();
        AddRun(
            statements,
            tables,
            handlerBookkeeping,
            guardReadsTenantlessHolds,
            values,
            [RetentionRunAdvisoryLock.KeyFor(values.SweepId)]
        );
        statements.Add(RetentionRun.CommittedStatement(tables, values.SweepId));
        statements.Add(TransactionIsolation());
        return statements;
    }

    /// <summary>
    /// What a preview sends that is not a host-table count: the clock. It names no Cohort table
    /// and binds no value, so it takes neither.
    /// </summary>
    internal static IReadOnlyList<SweepStatement> ForPreview() => [DatabaseClock.NowStatement()];

    /// <summary>
    /// What the holds repository sends: H1, H2, H3, the entity lock, and the caller's isolation
    /// level. It reads the clock inline, in H1 and H3.
    /// </summary>
    internal static IReadOnlyList<SweepStatement> ForHolds(
        CohortStoreTables tables,
        CohortStatementValues values
    )
    {
        return
        [
            RetentionHoldSql.BuildCreateHold(
                tables,
                values.HoldId,
                values.RetentionEntityId,
                values.RecordId,
                values.TenantId,
                values.Reason,
                values.Now,
                values.Now.AddDays(1)
            ),
            RetentionHoldSql.BuildRemoveHold(tables, values.HoldId, values.Now),
            RetentionHoldSql.BuildActiveHolds(tables),
            RetentionEntityLockSql.LockStatement([values.LockKey]),
            TransactionIsolation(),
        ];
    }

    /// <summary>
    /// What the deletion port sends that is not a host-table statement: H9 when a retained entity
    /// is tenantless, and the entity lock.
    /// </summary>
    internal static IReadOnlyList<SweepStatement> ForDeletion(
        CohortStoreTables tables,
        bool tenantlessRetained,
        CohortStatementValues values
    )
    {
        var statements = new List<SweepStatement>();
        AddTenantlessHolds(statements, tables, tenantlessRetained);
        statements.Add(RetentionEntityLockSql.LockStatement([values.LockKey]));
        return statements;
    }

    /// <summary>
    /// What readiness itself reads in its own scope: the stored entity identities (H10), and H9
    /// when a retained entity is tenantless.
    /// </summary>
    internal static IReadOnlyList<SweepStatement> ForReadiness(
        CohortStoreTables tables,
        bool tenantlessRetained,
        CohortStatementValues values
    )
    {
        var statements = new List<SweepStatement>
        {
            RetentionHoldSql.StoredEntityIdentities(tables, new[] { values.RetentionEntityId }),
        };
        AddTenantlessHolds(statements, tables, tenantlessRetained);
        return statements;
    }

    /// <summary>What the dispatcher's payload scrub sends: Q3.</summary>
    internal static IReadOnlyList<SweepStatement> ForScrub(
        CohortStoreTables tables,
        CohortStatementValues values
    ) => [RowHandlerQueueSql.ScrubExpiredPayloadsStatement(tables, values.Now)];

    /// <summary>
    /// What the row-handler dispatcher sends besides the scrub: the queue (Q1, Q2, Q4 to Q6, T2,
    /// the four forms of T4, T5, and T6), abandoned-run recovery (R4 and R5), and the delivery
    /// locks (the multi-key lock, the try-lock, the unlock, and the unlock of every lock).
    /// </summary>
    internal static IReadOnlyList<SweepStatement> ForDispatch(
        CohortStoreTables tables,
        CohortStatementValues values
    )
    {
        const int batchSize = 1;
        var deliveryKey = RetentionRunAdvisoryLock.RowHandlerKeyFor(
            tables.SweepRowHandlerStatus,
            values.StatusId
        );
        return
        [
            RowHandlerQueueSql.RemainingWorkStatement(tables),
            RowHandlerQueueSql.RequeueDeadLetteredStatement(
                tables,
                values.SweepId,
                values.HandlerId,
                values.Now
            ),
            RowHandlerQueueSql.ClearDeliveredPayloadStatement(tables, values.RowDetailId),
            RowHandlerQueueSql.ExpiredClaimsStatement(tables, values.Now, after: null, batchSize),
            RowHandlerQueueSql.ReclaimLockStatement(tables, values.StatusId, values.ClaimToken),
            RowHandlerQueueSql.ClaimStatement(tables, values.ClaimToken, batchSize, values.Now),
            .. Enum.GetValues<OwnedUpdate>()
                .Select(form =>
                    RowHandlerQueueSql.OwnedUpdateStatement(
                        tables,
                        form,
                        values.StatusId,
                        values.ClaimToken,
                        values.Now,
                        values.Error
                    )
                ),
            RowHandlerQueueSql.RequeueInterruptedStatement(tables, values.ClaimToken),
            RowHandlerQueueSql.DeadLetterDependentsStatement(
                tables,
                values.RowDetailId,
                values.StatusId,
                values.Now,
                values.Error
            ),
            RetentionRunLedgerSql.AbandonedRunsStatement(tables, values.Now),
            RetentionRunLedgerSql.AbandonedRunFailedStatement(
                tables,
                values.SweepId,
                values.Now,
                values.Error
            ),
            RetentionRunAdvisoryLock.LockAllStatement([deliveryKey]),
            RetentionRunAdvisoryLock.TryLockStatement(deliveryKey),
            RetentionRunAdvisoryLock.UnlockStatement(deliveryKey),
            RetentionRunAdvisoryLock.UnlockAllStatement(),
        ];
    }

    /// <summary>
    /// What the scheduled worker sends on its coordination connection: the clock, the worker's
    /// try-lock, the tenant-coverage scan of each tenanted entity (a host-table statement the
    /// worker alone sends), the ledger read without a tenant (R6), and the unlock of every lock.
    /// </summary>
    internal static IReadOnlyList<SweepStatement> ForWorker(
        CohortStoreTables tables,
        IEnumerable<RetentionEntry> entries,
        IReadOnlyDictionary<string, RetentionCategoryCapabilities> capabilities,
        CohortStatementValues values
    )
    {
        var statements = new List<SweepStatement>
        {
            DatabaseClock.NowStatement(),
            RetentionRunAdvisoryLock.TryLockStatement(
                RetentionRunAdvisoryLock.WorkerKeyFor(tables.SweepRun)
            ),
        };
        foreach (var entry in entries)
        {
            if (
                RetentionTenantCoverage.Statement(entry, capabilities, new[] { values.TenantId }) is
                { } scan
            )
            {
                statements.Add(scan);
            }
        }

        statements.Add(
            RetentionRunLedgerSql.OccurrenceStatement(
                tables.SweepRun,
                values.Now,
                dryRun: false,
                tenantId: null
            )
        );
        statements.Add(RetentionRunAdvisoryLock.UnlockAllStatement());
        return statements;
    }

    /// <summary>
    /// What history pruning sends: a batch of settled runs (R8, D8, S3, and R9) when it prunes
    /// runs, a batch of inactive holds (H11) when it prunes holds, and the clock when it reads it
    /// to find the schedule's current occurrence.
    /// </summary>
    internal static IReadOnlyList<SweepStatement> ForPruning(
        CohortStoreTables tables,
        PruningScope scope,
        CohortStatementValues values
    )
    {
        const int batchSize = 1;
        var statements = new List<SweepStatement>();
        if (scope.Runs)
        {
            Guid[] sweepIds = [values.SweepId];
            statements.Add(
                HistoryPruningSql.EligibleRunsStatement(
                    tables,
                    values.Now,
                    values.Now,
                    values.Now,
                    batchSize
                )
            );
            statements.Add(HistoryPruningSql.DeleteRowDetailsStatement(tables, sweepIds));
            statements.Add(HistoryPruningSql.DeleteEntitySummariesStatement(tables, sweepIds));
            statements.Add(HistoryPruningSql.DeleteRunsStatement(tables, sweepIds));
        }

        if (scope.Holds)
        {
            statements.Add(HistoryPruningSql.PruneHoldsStatement(tables, values.Now, batchSize));
        }

        if (scope.ReadsClock)
        {
            statements.Add(DatabaseClock.NowStatement());
        }

        return statements;
    }

    private static void AddRunRecording(
        List<SweepStatement> statements,
        CohortStoreTables tables,
        bool handlerBookkeeping,
        CohortStatementValues values
    )
    {
        var period = new RetentionPeriod(Years: 1);
        const Strategy strategy = Strategy.Purge;
        statements.Add(
            EfRetentionAuditWriter.StartedStatement(
                tables,
                new SweepEvent.Started(
                    values.SweepId,
                    values.Now,
                    SweepTriggerKind.Manual,
                    false,
                    values.TenantId
                )
            )
        );
        statements.Add(
            EfRetentionAuditWriter.EntitySummaryStatement(
                tables,
                new SweepEvent.EntitySummary(
                    values.SweepId,
                    values.Now,
                    values.EntityType,
                    values.RetentionEntityId,
                    values.Category,
                    values.TenantId,
                    strategy,
                    period,
                    Affected: 0,
                    HeldCount: 0,
                    SkippedCount: 0,
                    NullAnchorCount: 0,
                    RetainedCount: 0,
                    DeferredCount: 0,
                    BlockedCount: 0
                )
            )
        );
        statements.Add(
            EfRetentionAuditWriter.EntityProgressStatement(
                tables,
                new SweepEvent.EntityProgress(
                    values.SweepId,
                    values.Now,
                    values.EntityType,
                    values.RetentionEntityId,
                    values.Category,
                    values.TenantId,
                    strategy,
                    period,
                    Affected: 0,
                    SkippedCount: 0,
                    BlockedCount: 0
                )
            )
        );
        statements.Add(
            EfRetentionAuditWriter.RowDetailStatement(
                tables,
                values.SweepId,
                values.Now,
                values.EntityType,
                values.RetentionEntityId,
                values.RecordId,
                values.Category,
                strategy,
                values.TenantId
            )
        );
        statements.Add(
            EfRetentionAuditWriter.TerminalStatement(
                tables,
                values.SweepId,
                SweepRunStatus.Failed,
                values.Now,
                TimeSpan.Zero,
                0,
                values.Error
            )
        );
        if (!handlerBookkeeping)
        {
            return;
        }

        statements.Add(
            RetentionHandlerSupport.RowDetailWithPayloadStatement(
                tables,
                values.SweepId,
                values.Now,
                values.EntityType,
                values.RetentionEntityId,
                values.RecordId,
                values.Category,
                strategy,
                values.TenantId,
                values.CapturedPayload
            )
        );
        statements.Add(
            RetentionHandlerSupport.HandlerStatusStatement(
                tables,
                values.RowDetailId,
                values.LockKey,
                RowHandlerDispatchPhase.Immediate,
                SweepRowHandlerDispatchState.DeadLettered,
                attempt: 1,
                values.Now,
                values.Now,
                values.Error
            )
        );
    }

    private static void AddTenantlessHolds(
        List<SweepStatement> statements,
        CohortStoreTables tables,
        bool tenantlessRetained
    )
    {
        if (tenantlessRetained)
        {
            statements.Add(RetentionHoldSql.BuildActiveTenantlessHoldIds(tables.RetentionHolds));
        }
    }

    // The run lock and its unlock, the held-locks check over the given keys, and the entity lock,
    // each over a key from the run's own key functions.
    private static void AddRun(
        List<SweepStatement> statements,
        CohortStoreTables tables,
        bool handlerBookkeeping,
        bool tenantlessRetained,
        CohortStatementValues values,
        long[] heldKeys
    )
    {
        AddRunRecording(statements, tables, handlerBookkeeping, values);
        AddTenantlessHolds(statements, tables, tenantlessRetained);
        var runKey = RetentionRunAdvisoryLock.KeyFor(values.SweepId);
        statements.Add(RetentionRunAdvisoryLock.LockStatement(runKey));
        statements.Add(RetentionRunAdvisoryLock.UnlockStatement(runKey));
        statements.Add(RetentionRunAdvisoryLock.HoldsAllStatement(heldKeys));
        statements.Add(RetentionEntityLockSql.LockStatement([values.LockKey]));
        statements.Add(DatabaseClock.NowStatement());
    }
}
