using System.Collections.Concurrent;
using System.Data.Common;
using System.Reflection;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Holds;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Npgsql;

namespace Cohort.Infrastructure.Sweep;

/// <summary>
/// The one SQL pipeline behind every strategy. A <see cref="SweepScope"/> says which rows
/// are in play; <see cref="SweepEligibilitySql"/> says which of those the strategy has not
/// yet handled, and the strategy says how it changes one (<see cref="CreateMutation"/>).
/// Candidates are discovered, entity-locked and row-locked, then mutated in one statement,
/// or row by row when handlers or a dependents step run or the new values depend on each row.
/// A row an integrity constraint refuses is blocked rather than failing its batch: the bulk
/// statement is bisected under savepoints to find it, and on the row-by-row path each row's
/// change has its own savepoint, which its dependents step shares.
/// </summary>
internal abstract class SweepStrategy(
    DbContext db,
    IEnumerable<IRetentionHandlerRegistration> handlerRegistrations,
    IEnumerable<IRetentionDependentsRegistration> dependentsRegistrations,
    RetentionHoldGuard holdGuard,
    IServiceScopeFactory scopeFactory,
    ILogger logger
)
{
    private const string MutationSavepoint = "cohort_mutation";
    private const string OnBeforeSavepoint = "cohort_on_before";

    private static readonly MethodInfo RowByRowMethod = typeof(SweepStrategy).GetMethod(
        nameof(ExecuteRowByRowAsync),
        BindingFlags.Instance | BindingFlags.NonPublic
    )!;
    private static readonly ConcurrentDictionary<Type, MethodInfo> RowByRowMethods = new();

    public abstract Strategy HandlesStrategy { get; }

    protected abstract SweepMutation CreateMutation(SweepScope scope);

    /// <summary>Rejects entries this strategy cannot act on.</summary>
    protected virtual void Validate(RetentionEntry entry) { }

    public async Task<long> CountAsync(
        SweepScope scope,
        SweepCount count,
        DbConnection conn,
        CancellationToken ct
    )
    {
        EnsureHandles(scope);
        return await ExecuteCountAsync(SweepStatements.CountByAnchor(scope, HandlesStrategy, count), conn, ct);
    }

    /// <summary>
    /// What a dry run or a preview predicts, in one statement
    /// (<see cref="SweepStatements.CountPredicted"/>).
    /// </summary>
    public async Task<(long Eligible, long Held, long Blocked)> CountPredictedAsync(
        SweepScope scope,
        DbConnection conn,
        CancellationToken ct
    )
    {
        EnsureHandles(scope);

        var statement = SweepStatements.CountPredicted(scope, HandlesStrategy);
        await using var command = statement.Parameters.CreateCommand(conn, null, statement.Sql);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
    }

    /// <summary>
    /// What is left once run <paramref name="sweepId"/> has finished its batches for the
    /// entity, in one statement (<see cref="SweepStatements.CountLeft"/>), leaving out the rows
    /// the run <paramref name="refused"/>.
    /// </summary>
    public async Task<(long Deferred, long Held, long Referenced)> CountLeftAsync(
        SweepScope scope,
        Guid sweepId,
        IReadOnlyCollection<string> refused,
        DbConnection conn,
        CancellationToken ct
    )
    {
        EnsureHandles(scope);

        var hasRefused = SweepStatements.HasRefusedIds(refused);
        var statement = SweepStatements.CountLeft(
            scope,
            HandlesStrategy,
            sweepId,
            hasRefused,
            hasRefused ? refused.ToArray() : null
        );
        await using var command = statement.Parameters.CreateCommand(conn, null, statement.Sql);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
    }

    /// <summary>
    /// Every row of the scope's subject in its tenant, with no cutoff, eligibility or hold
    /// filter: what an Exempt rule keeps from an erasure. Static because Exempt has no
    /// strategy instance.
    /// </summary>
    internal static async Task<long> CountSubjectRowsAsync(
        SweepScope scope,
        DbConnection conn,
        CancellationToken ct
    ) => await ExecuteCountAsync(SweepStatements.CountSubjectRows(scope), conn, ct);

    private static async Task<long> ExecuteCountAsync(SweepStatement statement, DbConnection conn, CancellationToken ct)
    {
        await using var command = statement.Parameters.CreateCommand(conn, null, statement.Sql);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    public async Task<SweepExecutionResult> ExecuteAsync(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext execution,
        CancellationToken ct
    )
    {
        EnsureHandles(scope);

        var (candidates, reached) = await LockCandidatesAsync(scope, conn, transaction, execution, ct);
        var result = candidates.Count == 0
            ? new SweepExecutionResult([])
            : await MutateCandidatesAsync(scope, conn, transaction, candidates, execution, ct);
        return result with { Reached = reached };
    }

    private async Task<SweepExecutionResult> MutateCandidatesAsync(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidates,
        SweepMutationContext execution,
        CancellationToken ct
    )
    {
        var mutation = CreateMutation(scope);
        var hasHandlers = RetentionHandlerSupport.HasHandlers(handlerRegistrations, scope.Entry.EntityType);
        var hasStep = dependentsRegistrations.HasStep(scope.Entry.EntityType);
        var rowByRow = SweepStatements.TakesRowByRowPath(HandlesStrategy, hasHandlers, hasStep, mutation.RowByRow);
        if (!hasHandlers && !hasStep)
        {
            return await MutateAsync(scope, conn, transaction, candidates, mutation, rowByRow, handlerScope: null, execution, ct);
        }

        // Handlers and steps never see the run's own context, which in a caller-transaction
        // erasure is the caller's: they get a scope of their own per batch, whose context joins
        // the run's connection and transaction before any of them is constructed.
        return await scopeFactory.RunInScopeAsync(
            RetentionExecutionScopes.For(scope.Tenant),
            async handlerServices =>
            {
                var handlerDb = handlerServices.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext);
                // A hook may have opened the context to isolate it. The run's connection
                // already carries the same tenant's isolation, and an open connection can't
                // be swapped.
                await handlerDb.Database.CloseConnectionAsync();
                handlerDb.Database.SetDbConnection(conn, contextOwnsConnection: false);
                try
                {
                    await handlerDb.Database.UseTransactionAsync(transaction, ct);
                    var handlerScope = new RetentionHandlerScope(
                        handlerDb,
                        hasHandlers ? RetentionHandlerSupport.ResolveHandlers(handlerServices, scope.Entry.EntityType) : [],
                        RetentionHandlerSupport.ResolveDependents(handlerServices, scope.Entry.EntityType)
                    );
                    if (handlerScope.Dependents is { } step)
                    {
                        // The guard reads only the holds this scope can see.
                        await holdGuard.EnsureTenantlessHoldsVisibleAsync(
                            conn,
                            transaction,
                            $"Dependents step {step.StepType.FullName}",
                            ct
                        );
                    }

                    return await MutateAsync(scope, conn, transaction, candidates, mutation, rowByRow, handlerScope, execution, ct);
                }
                finally
                {
                    await handlerDb.Database.UseTransactionAsync(null, CancellationToken.None);
                    RestoreConfiguredConnection(handlerDb);
                }
            },
            ct
        );
    }

    /// <summary>
    /// Puts back the connection <paramref name="handlerDb"/>'s options configure. EF doesn't
    /// reset a connection the context doesn't own when a pooled context goes back to its
    /// pool, so without this the next lease would get the run's connection. Without a
    /// configured connection or connection string (a data source), the provider creates
    /// the next one itself.
    /// </summary>
    private static void RestoreConfiguredConnection(DbContext handlerDb)
    {
        var configured = RelationalOptionsExtension.Extract(handlerDb.GetService<IDbContextOptions>());
        if (configured.Connection is not null)
        {
            handlerDb.Database.SetDbConnection(configured.Connection, configured.IsConnectionOwned);
            return;
        }

        handlerDb.Database.SetDbConnection(null);
        if (configured.ConnectionString is not null)
        {
            handlerDb.Database.SetConnectionString(configured.ConnectionString);
        }
    }

    private async Task<SweepExecutionResult> MutateAsync(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidates,
        SweepMutation mutation,
        bool rowByRow,
        RetentionHandlerScope? handlerScope,
        SweepMutationContext execution,
        CancellationToken ct
    )
    {
        if (!rowByRow)
        {
            var affected = new List<string>();
            var blocked = new BlockedRows();
            await MutateBisectingAsync(scope, mutation, conn, transaction, candidates, affected, blocked, ct);
            LogBlocked(scope, execution, blocked);
            return new SweepExecutionResult(
                affected,
                CandidateCount: candidates.Count,
                BlockedRecordIds: blocked.RecordIds,
                BlockedFailures: blocked.Failures
            );
        }

        var rowByRowMethod = RowByRowMethods.GetOrAdd(
            scope.Entry.EntityType,
            static entityType => RowByRowMethod.MakeGenericMethod(entityType)
        );
        return await (Task<SweepExecutionResult>)
            rowByRowMethod.Invoke(
                this,
                [scope, conn, transaction, candidates, mutation, handlerScope, execution, ct]
            )!;
    }

    /// <summary>
    /// Changes <paramref name="recordIds"/> in one statement under a savepoint. When an
    /// integrity constraint refuses it, the savepoint is rolled back and each half is tried
    /// on its own, until the refused rows are found one by one: about 2k·log2(n/k)
    /// statements for k refused rows, where a savepoint per row would cost n.
    /// </summary>
    private async Task MutateBisectingAsync(
        SweepScope scope,
        SweepMutation mutation,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> recordIds,
        List<string> affected,
        BlockedRows blocked,
        CancellationToken ct
    )
    {
        var headParameters = new SqlParams();
        mutation.AddParameters(headParameters, null);
        var statement = SweepStatements.MutateBatch(scope, HandlesStrategy, headParameters, recordIds.ToArray());
        await using var command = statement.Parameters.CreateCommand(conn, transaction, statement.Sql);

        // Cohort's own savepoints ignore cancellation, like the entity savepoint: a cancelled
        // ROLLBACK TO would leave the batch aborted with no way back.
        await transaction.SaveAsync(MutationSavepoint, CancellationToken.None);
        DbException refusal;
        try
        {
            var changed = await ReadRecordIdsAsync(command, ct);
            await transaction.ReleaseAsync(MutationSavepoint, CancellationToken.None);
            affected.AddRange(changed);
            return;
        }
        catch (DbException ex) when (IsIntegrityViolation(ex))
        {
            await RollBackSavepointAsync(transaction, MutationSavepoint);
            refusal = ex;
        }

        if (recordIds.Count == 1)
        {
            blocked.Add(recordIds[0], refusal);
            return;
        }

        var half = recordIds.Count / 2;
        await MutateBisectingAsync(scope, mutation, conn, transaction, recordIds.Take(half).ToArray(), affected, blocked, ct);
        await MutateBisectingAsync(scope, mutation, conn, transaction, recordIds.Skip(half).ToArray(), affected, blocked, ct);
    }

    private async Task<SweepExecutionResult> ExecuteRowByRowAsync<TEntity>(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<string> candidates,
        SweepMutation mutation,
        RetentionHandlerScope? handlerScope,
        SweepMutationContext execution,
        CancellationToken ct
    )
        where TEntity : class
    {
        var entry = scope.Entry;
        var load = SweepStatements.LoadRows(scope, HandlesStrategy, candidates.ToArray());
        var rows = await db.Set<TEntity>()
            .FromSqlRaw(load.Sql, load.Parameters.ToDbParameters(conn))
            .IgnoreQueryFilters()
            .AsNoTracking()
            .ToListAsync(ct);

        var recordIdProperty = db.Model.FindEntityType(typeof(TEntity))!.FindProperty(entry.RecordId.RecordIdMember)!;
        var recordIdGetter = recordIdProperty.GetGetter();
        var recordIdConverter = recordIdProperty.GetTypeMapping().Converter;
        var onBefore = handlerScope is { Handlers.Count: > 0 } ? handlerScope : null;
        var step = handlerScope?.Dependents;

        var affected = new List<string>();
        var skipped = 0;
        var blocked = new BlockedRows();
        foreach (var row in rows)
        {
            var recordIdValue =
                recordIdGetter.GetClrValueUsingContainingEntity(row)
                ?? throw new InvalidOperationException(
                    $"Retention row for {entry.EntityType.FullName} produced an empty record id for member '{entry.RecordId.RecordIdMember}'."
                );
            var recordKey = recordIdConverter?.ConvertToProvider(recordIdValue) ?? recordIdValue;

            string? capturedPayload = null;
            if (onBefore is not null)
            {
                var before = new RetentionBeforeContext(
                    execution.SweepId,
                    entry.Category,
                    scope.Rule.Strategy,
                    scope.Tenant.Id,
                    execution.At
                );
                var result = await InvokeOnBeforeReadOnlyAsync(conn, transaction, onBefore, entry.EntityType, row, before, ct);
                if (!result.Succeeded)
                {
                    skipped++;
                    await PersistSkippedAsync(scope, conn, transaction, execution, recordKey, result.ToFailure(), ct);
                    continue;
                }

                capturedPayload = result.CapturedPayload;
            }

            var headParameters = new SqlParams();
            mutation.AddParameters(headParameters, row);
            var update = SweepStatements.MutateRow(scope, HandlesStrategy, headParameters, recordKey);
            await using var command = update.Parameters.CreateCommand(conn, transaction, update.Sql);

            // One savepoint for the step and the mutation: whatever undoes one undoes both, so
            // a row's dependents are never retired without it.
            await transaction.SaveAsync(MutationSavepoint, CancellationToken.None);
            if (step is not null)
            {
                var dependents = new RetentionDependentsContext(
                    execution.SweepId,
                    entry.Category,
                    scope.Rule.Strategy,
                    scope.Tenant.Id,
                    execution.At
                );
                var stepFailure = await RetireDependentsAsync(
                    conn,
                    transaction,
                    handlerScope!.Db,
                    step,
                    (TEntity)row,
                    dependents,
                    execution,
                    ct
                );
                if (stepFailure is not null)
                {
                    await RollBackSavepointAsync(transaction, MutationSavepoint);
                    skipped++;
                    await PersistSkippedAsync(scope, conn, transaction, execution, recordKey, stepFailure, ct);
                    continue;
                }
            }

            object? changed;
            try
            {
                changed = await command.ExecuteScalarAsync(ct);
            }
            catch (DbException ex) when (IsIntegrityViolation(ex))
            {
                // Refused by a constraint: nothing changed, the step is undone with it, and no
                // OnAfter work is queued.
                await RollBackSavepointAsync(transaction, MutationSavepoint);
                blocked.Add(await RecordIdSql.CanonicalizeAsync(conn, transaction, entry, recordKey, ct), ex);
                continue;
            }

            if (changed is not string recordId)
            {
                // No row back. The rollback undoes the step, so if the row matches the
                // statement's own WHERE again, the step had taken it out of scope; otherwise a
                // hold landed after the row was loaded, and the engine counts it as held.
                await RollBackSavepointAsync(transaction, MutationSavepoint);
                if (step is not null && await IsStillDueAsync(scope, conn, transaction, recordKey, ct))
                {
                    skipped++;
                    await PersistSkippedAsync(
                        scope,
                        conn,
                        transaction,
                        execution,
                        recordKey,
                        BeforeMutationFailure.FromReason(step, BeforeMutationFailure.DependentsChangedRowReason),
                        ct
                    );
                }

                continue;
            }

            await transaction.ReleaseAsync(MutationSavepoint, CancellationToken.None);
            if (capturedPayload is not null)
            {
                await RetentionHandlerSupport.PersistCapturedRowAsync(
                    conn,
                    transaction,
                    execution,
                    entry,
                    scope.Rule.Strategy,
                    scope.Tenant.Id,
                    recordId,
                    capturedPayload,
                    onBefore!.Handlers,
                    ct
                );
            }
            affected.Add(recordId);
        }

        LogBlocked(scope, execution, blocked);
        return new SweepExecutionResult(
            affected,
            // Only handlers write a row detail for each mutated row; a step alone leaves the
            // run's own row-detail audit to the engine.
            RowDetailsPersisted: onBefore is not null,
            SkippedCount: skipped,
            CandidateCount: candidates.Count,
            BlockedRecordIds: blocked.RecordIds,
            BlockedFailures: blocked.Failures
        );
    }

    private async Task PersistSkippedAsync(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext execution,
        object recordKey,
        BeforeMutationFailure failure,
        CancellationToken ct
    )
    {
        var entry = scope.Entry;
        var skippedId = await RecordIdSql.CanonicalizeAsync(conn, transaction, entry, recordKey, ct);
        await RetentionHandlerSupport.PersistBeforeFailureAsync(
            conn,
            transaction,
            execution,
            entry,
            scope.Rule.Strategy,
            scope.Tenant.Id,
            skippedId,
            failure,
            logger,
            ct
        );
    }

    /// <summary>
    /// Runs the dependents step read-write inside the open mutation savepoint, and returns
    /// why it failed, or null when the row may be mutated. Like OnBefore, only the run's own
    /// cancellation propagates; anything else the step throws, a database error included, is
    /// the step failing. It also fails for unsaved changes it leaves on its context and for
    /// any change to or removal of a row under an active hold, found by diffing every visible
    /// held row's physical version before and after it (<see cref="RetentionHoldGuard"/>).
    /// Whatever it leaves tracked is discarded after every row.
    /// </summary>
    private async Task<BeforeMutationFailure?> RetireDependentsAsync<TEntity>(
        DbConnection conn,
        DbTransaction transaction,
        DbContext stepDb,
        ResolvedRetentionDependents step,
        TEntity row,
        RetentionDependentsContext dependents,
        SweepMutationContext execution,
        CancellationToken ct
    )
    {
        var baseline = await holdGuard.ReadHeldRowVersionsAsync(conn, transaction, ct);
        try
        {
            try
            {
                await ((IRetentionDependents<TEntity>)step.Instance).RetireAsync(row, dependents, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                return BeforeMutationFailure.FromException(step, ex);
            }

            if (RetentionHandlerSupport.HasUnsavedChanges(stepDb))
            {
                return BeforeMutationFailure.FromReason(step, BeforeMutationFailure.DependentsUnsavedChangesReason);
            }

            IReadOnlyList<Guid> changedHolds;
            try
            {
                changedHolds = await holdGuard.ReadChangedHeldRowsAsync(baseline, conn, transaction, ct);
            }
            catch (DbException ex) when (!ct.IsCancellationRequested)
            {
                // A step that swallowed its own database error leaves the transaction aborted;
                // the rollback to the savepoint recovers it, and the row is the step's failure.
                return BeforeMutationFailure.FromException(step, ex);
            }

            if (changedHolds.Count > 0)
            {
                logger.LogWarning(
                    "Cohort run {SweepId}: dependents step {StepType} changed or removed rows under active hold(s) {HoldIds}, so its row is skipped and the step undone.",
                    execution.SweepId,
                    step.StepType.FullName,
                    string.Join(", ", changedHolds)
                );
                return BeforeMutationFailure.FromReason(step, BeforeMutationFailure.DependentsChangedHeldRowReason);
            }

            return null;
        }
        finally
        {
            stepDb.ChangeTracker.Clear();
        }
    }

    /// <summary>
    /// Whether the row still matches the mutation statement's own WHERE: in scope, eligible
    /// and not held.
    /// </summary>
    private async Task<bool> IsStillDueAsync(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        object recordKey,
        CancellationToken ct
    )
    {
        var statement = SweepStatements.IsStillDue(scope, HandlesStrategy, recordKey);
        await using var command = statement.Parameters.CreateCommand(conn, transaction, statement.Sql);
        return await command.ExecuteScalarAsync(ct) is true;
    }

    /// <summary>
    /// Runs the OnBefore handlers inside a read-only savepoint that is always rolled back.
    /// The handlers' context shares the sweep's connection and transaction: a database error
    /// in one leaves the transaction usable for the rest of the batch, and a write fails
    /// (SQLSTATE 25006) instead of committing with it. Rolling back also restores read-write
    /// mode and releases anything the handler locked. Read-only can't reach the change
    /// tracker, so whatever the handlers left tracked on their context is discarded after
    /// every row.
    /// </summary>
    private static async Task<OnBeforeInvocationResult> InvokeOnBeforeReadOnlyAsync(
        DbConnection conn,
        DbTransaction transaction,
        RetentionHandlerScope onBefore,
        Type entityType,
        object row,
        RetentionBeforeContext before,
        CancellationToken ct
    )
    {
        await transaction.SaveAsync(OnBeforeSavepoint, CancellationToken.None);
        try
        {
            OnBeforeInvocationResult result;
            try
            {
                await using (var readOnly = new SqlParams().CreateCommand(conn, transaction, "SET LOCAL transaction_read_only = on"))
                {
                    await readOnly.ExecuteNonQueryAsync(ct);
                }

                result = await RetentionHandlerSupport.InvokeOnBeforeAsync(onBefore.Db, onBefore.Handlers, entityType, row, before, ct);
            }
            catch
            {
                // Cancelled, or Cohort's own statement failed: the batch is abandoned with the
                // original exception, so a failed rollback may not replace it.
                try
                {
                    await transaction.RollbackAsync(OnBeforeSavepoint, CancellationToken.None);
                }
                catch (Exception)
                {
                    // The batch transaction is discarded either way.
                }

                throw;
            }

            await RollBackSavepointAsync(transaction, OnBeforeSavepoint);
            return result;
        }
        finally
        {
            onBefore.Db.ChangeTracker.Clear();
        }
    }

    /// <summary>
    /// ROLLBACK TO keeps the savepoint open, so release it too: otherwise each later row's
    /// savepoint nests one level deeper for the rest of the batch. Ignores cancellation, like
    /// every Cohort savepoint: a cancelled rollback would leave the batch aborted.
    /// </summary>
    private static async Task RollBackSavepointAsync(DbTransaction transaction, string savepoint)
    {
        await transaction.RollbackAsync(savepoint, CancellationToken.None);
        await transaction.ReleaseAsync(savepoint, CancellationToken.None);
    }

    private static bool IsIntegrityViolation(DbException exception) =>
        exception.SqlState is { Length: 5 } sqlState && sqlState.StartsWith("23", StringComparison.Ordinal);

    /// <summary>
    /// One warning per refusing constraint, naming it and its SQLSTATE but no row: the
    /// constraint is schema metadata, the record ids are not logged.
    /// </summary>
    private void LogBlocked(SweepScope scope, SweepMutationContext execution, BlockedRows blocked)
    {
        foreach (var group in blocked.Failures.GroupBy(BlockedRows.RefusalKey))
        {
            var first = group.First();
            var diagnostic = RetentionFailureDiagnostic.Create(first);
            logger.LogWarning(
                first,
                "Cohort run {SweepId} left {Count} row(s) of entity {EntityType} unchanged: constraint {ConstraintName} refused them (sqlstate {SqlState}). They count as blocked and are retried by the next run. Diagnostic {DiagnosticId}.",
                execution.SweepId,
                group.Count(),
                scope.Entry.EntityType.FullName,
                group.Key.ConstraintName ?? "(unnamed)",
                group.Key.SqlState,
                diagnostic.DiagnosticIdText
            );
        }
    }

    /// <summary>
    /// Selects up to one batch of candidates after the row the run's previous batch reached,
    /// and locks them: the entity lock serialises against creating a hold on a candidate, the
    /// row lock against other runs and against creating a hold on a related row, which locks the
    /// rows it will cover <c>FOR SHARE</c> before it commits (<c>EfRetentionHoldsRepository</c>).
    /// Held rows are excluded up front so they are neither locked nor reselected.
    /// A discovered row that fails the lock re-check (locked by another run, or no longer
    /// due) is passed over, and discovery continues after it until the batch is full or no
    /// row is left: a sweep leaves a locked row for its next run, which counts it as deferred.
    /// Returns the locked rows and where the next batch continues.
    /// </summary>
    private async Task<(IReadOnlyList<string> Locked, SweepCursor? Reached)> LockCandidatesAsync(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext execution,
        CancellationToken ct
    )
    {
        var entry = scope.Entry;
        var locked = new List<string>();
        var reached = execution.After;
        while (locked.Count < execution.BatchSize)
        {
            var limit = execution.BatchSize - locked.Count;
            var (discovered, last) = await DiscoverAsync(scope, conn, transaction, execution.SweepId, reached, limit, ct);
            if (discovered.Count == 0)
            {
                break;
            }

            reached = last;
            await RetentionEntityLockSql.AcquireAsync(
                conn,
                transaction,
                entry.RetentionEntityId,
                entry.Tenant is not null ? scope.Tenant.Id : null,
                discovered,
                ct
            );
            var statement = SweepStatements.LockCandidates(scope, HandlesStrategy, discovered.ToArray());
            await using var command = statement.Parameters.CreateCommand(conn, transaction, statement.Sql);
            locked.AddRange(await ReadRecordIdsAsync(command, ct));
            if (discovered.Count < limit)
            {
                break;
            }
        }

        return (locked, reached);
    }

    /// <summary>
    /// Up to <paramref name="limit"/> due rows after <paramref name="after"/> in discovery's
    /// order, and the last of them. A keyset rather than an exclusion list, after the
    /// dispatcher's expired-claim recovery on (<c>ClaimedAt</c>, <c>Id</c>): each statement
    /// costs one batch however many rows the run has already passed over.
    /// </summary>
    private async Task<(List<string> RecordIds, SweepCursor? Last)> DiscoverAsync(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        Guid sweepId,
        SweepCursor? after,
        int limit,
        CancellationToken ct
    )
    {
        var statement = SweepStatements.Discover(
            scope,
            HandlesStrategy,
            sweepId,
            limit,
            SweepStatements.CursorKindOf(after),
            after?.Anchor,
            after?.RecordId
        );
        await using var command = statement.Parameters.CreateCommand(conn, transaction, statement.Sql);
        var recordIds = new List<string>();
        SweepCursor? last = null;
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var recordId = reader.GetString(0);
            recordIds.Add(recordId);
            last = new SweepCursor(reader.IsDBNull(1) ? null : reader.GetFieldValue<DateTimeOffset>(1), recordId);
        }

        return (recordIds, last);
    }

    private void EnsureHandles(SweepScope scope)
    {
        if (scope.Rule.Strategy != HandlesStrategy)
        {
            throw new InvalidOperationException(
                $"{GetType().Name} cannot execute {scope.Rule.Strategy} rules."
            );
        }

        Validate(scope.Entry);
    }

    private static async Task<List<string>> ReadRecordIdsAsync(DbCommand command, CancellationToken ct)
    {
        var recordIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            recordIds.Add(reader.GetString(0));
        }

        return recordIds;
    }
}

/// <summary>Rows one batch found blocked, with the refusals that blocked them.</summary>
internal sealed class BlockedRows
{
    private readonly List<string> recordIds = [];
    private readonly List<DbException> failures = [];

    public IReadOnlyList<string> RecordIds => recordIds;

    public IReadOnlyList<DbException> Failures => failures;

    /// <summary>
    /// What refused a row: its SQLSTATE and the constraint named, if any. The warning groups
    /// by it, and the engine's circuit breaker compares it across a batch.
    /// </summary>
    public static (string? SqlState, string? ConstraintName) RefusalKey(DbException failure) =>
        (failure.SqlState, (failure as PostgresException)?.ConstraintName);

    public void Add(string recordId, DbException failure)
    {
        recordIds.Add(recordId);
        failures.Add(failure);
    }
}

/// <summary>
/// How a strategy changes rows: the values its statement head binds (<see cref="SweepStatements.HeadSql"/>
/// holds the text), for one loaded row or, when <c>row</c> is null, for every row, and whether the
/// values depend on the row so rows must be loaded and changed one at a time.
/// </summary>
internal sealed record SweepMutation(Action<SqlParams, object?> AddParameters, bool RowByRow = false);
