using System.Collections;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Reflection;

using Cohort.Domain;
using Cohort.Infrastructure.Holds;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.Extensions.Logging;

using Npgsql;

namespace Cohort.Infrastructure.Sweep;

// Raw SQL here interpolates only identifiers quoted from the EF model and validated store
// types; every value is a named parameter.
#pragma warning disable EF1002

/// <summary>
/// The one SQL pipeline behind every strategy. A <see cref="SweepScope"/> says which rows
/// are in play; the strategy says which of those it has not yet handled
/// (<see cref="EligibilitySql"/>) and how it changes one (<see cref="CreateMutation"/>).
/// Candidates are discovered, entity-locked and row-locked, then mutated in one statement,
/// or row by row when handlers run or the new values depend on each row. A row an
/// integrity constraint refuses is blocked rather than failing its batch: the bulk statement
/// is bisected under savepoints to find it, and on the row-by-row path each row's change
/// has its own savepoint.
/// </summary>
internal abstract class SweepStrategy(DbContext db, IServiceProvider services, ILogger logger)
{
    private const string MutationSavepoint = "cohort_mutation";
    private const string OnBeforeSavepoint = "cohort_on_before";

    private static readonly MethodInfo RowByRowMethod = typeof(SweepStrategy).GetMethod(
        nameof(ExecuteRowByRowAsync),
        BindingFlags.Instance | BindingFlags.NonPublic
    )!;
    private static readonly ConcurrentDictionary<Type, MethodInfo> RowByRowMethods = new();

    public abstract Strategy HandlesStrategy { get; }

    /// <summary>
    /// Predicate over <c>target</c> for rows this strategy has not already handled, or ""
    /// when every row in scope qualifies.
    /// </summary>
    protected abstract string EligibilitySql(RetentionEntry entry);

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
        if (count == SweepCount.Blocked)
        {
            return await CountBlockedAsync(scope, [], conn, ct);
        }

        var parameters = new SqlParams();
        var where = count switch
        {
            SweepCount.Eligible => DueSql(scope, parameters),
            SweepCount.Held => $"{ScopeSql(scope, parameters)} AND NOT {HoldExclusion(scope, parameters)}",
            // No hold exclusion: a held NULL-anchor row is just as invisible to retention.
            SweepCount.NullAnchor => ScopeSql(scope, parameters, AnchorFilter.Null),
            // No hold exclusion: the rule keeps the row whether or not it is also held.
            SweepCount.RetainedByRule => ScopeSql(scope, parameters, AnchorFilter.OnOrAfterCutoff),
            _ => throw new ArgumentOutOfRangeException(nameof(count), count, null),
        };
        return await CountWhereAsync(scope.Entry, where, parameters, conn, ct);
    }

    /// <summary>
    /// Rows that would be due but for a blocking reference, leaving out
    /// <paramref name="alreadyBlocked"/>: rows the run already counted as blocked when a
    /// constraint refused them.
    /// </summary>
    public async Task<long> CountBlockedAsync(
        SweepScope scope,
        IReadOnlyCollection<string> alreadyBlocked,
        DbConnection conn,
        CancellationToken ct
    )
    {
        EnsureHandles(scope);
        if (scope.BlockingReferences.Count == 0)
        {
            return 0;
        }

        var parameters = new SqlParams();
        return await CountWhereAsync(
            scope.Entry,
            $"{ScopeSql(scope, parameters)} AND {HoldExclusion(scope, parameters)} AND NOT ({ReferencingForeignKeySql.NotReferencedSql(scope)}){NotAnyRecordIdSql(scope.Entry, "alreadyBlockedIds", alreadyBlocked, parameters)}",
            parameters,
            conn,
            ct
        );
    }

    /// <summary>
    /// Rows still due once run <paramref name="sweepId"/> has finished its batches for the
    /// entity that the run did not record (as skipped by a failing OnBefore) or find
    /// <paramref name="blocked"/>: locked by another transaction, not reached before the loop
    /// stopped, or come into scope mid-run. Measured once after the loop rather than per
    /// batch, because a row that stays locked is rediscovered by every batch. Referenced
    /// rows aren't due, so blocked rows are never counted twice.
    /// </summary>
    public async Task<long> CountDeferredAsync(
        SweepScope scope,
        Guid sweepId,
        IReadOnlyCollection<string> blocked,
        DbConnection conn,
        CancellationToken ct
    )
    {
        EnsureHandles(scope);

        var parameters = new SqlParams();
        return await CountWhereAsync(
            scope.Entry,
            $"{DueSql(scope, parameters)}{NotRecordedByRunSql(scope, sweepId, parameters)}{NotAnyRecordIdSql(scope.Entry, "blockedRecordIds", blocked, parameters)}",
            parameters,
            conn,
            ct
        );
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
    )
    {
        if (scope.Subject is null)
        {
            throw new ArgumentException("Only an erasure scope has subject rows.", nameof(scope));
        }

        var parameters = new SqlParams();
        var clauses = SubjectAndTenantSql(scope, parameters);
        return await CountWhereAsync(scope.Entry, string.Join(" AND ", clauses), parameters, conn, ct);
    }

    private static async Task<long> CountWhereAsync(
        RetentionEntry entry,
        string where,
        SqlParams parameters,
        DbConnection conn,
        CancellationToken ct
    )
    {
        await using var command = parameters.CreateCommand(
            conn,
            null,
            $"SELECT pg_catalog.count(*) FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target WHERE {where}"
        );
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    public async Task<SweepExecutionResult> ExecuteAsync(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext? execution,
        CancellationToken ct
    )
    {
        EnsureHandles(scope);

        var candidates = await LockCandidatesAsync(scope, conn, transaction, execution, ct);
        if (candidates.Count == 0)
        {
            return new SweepExecutionResult([], 0);
        }

        var entry = scope.Entry;
        var mutation = CreateMutation(scope);
        var handlers = execution is null
            ? []
            : RetentionHandlerSupport.ResolveHandlers(services, entry.EntityType);
        if (handlers.Count == 0 && !mutation.RowByRow)
        {
            var affected = new List<string>();
            var blocked = new BlockedRows();
            await MutateBisectingAsync(scope, mutation, conn, transaction, candidates, affected, blocked, ct);
            LogBlocked(scope, execution, blocked);
            return new SweepExecutionResult(
                affected,
                candidates.Count - affected.Count - blocked.RecordIds.Count,
                CandidateCount: candidates.Count,
                BlockedRecordIds: blocked.RecordIds,
                BlockFailure: blocked.FirstFailure
            );
        }

        var rowByRow = RowByRowMethods.GetOrAdd(
            entry.EntityType,
            static entityType => RowByRowMethod.MakeGenericMethod(entityType)
        );
        return await (Task<SweepExecutionResult>)
            rowByRow.Invoke(
                this,
                [scope, conn, transaction, candidates, mutation, handlers, execution, ct]
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
        var entry = scope.Entry;
        var parameters = new SqlParams { ["candidateIds"] = recordIds.ToArray() };
        mutation.AddParameters(parameters, null);
        await using var command = parameters.CreateCommand(
            conn,
            transaction,
            $"""
            {mutation.HeadSql}
            WHERE {ScopeSql(scope, parameters)}
              AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
              AND {HoldExclusion(scope, parameters)}
            RETURNING {RecordIdSql.TextExpression("target", entry.RecordId)}
            """
        );

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
            await transaction.RollbackAsync(MutationSavepoint, CancellationToken.None);
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
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        SweepMutationContext? execution,
        CancellationToken ct
    )
        where TEntity : class
    {
        var entry = scope.Entry;
        var load = new SqlParams { ["candidateIds"] = candidates.ToArray() };
        var rows = await db.Set<TEntity>()
            .FromSqlRaw(
                $"""
                SELECT {string.Join(", ", entry.MaterializationColumns.Select(column => $"target.{PostgreSqlIdentifier.Quote(column)}"))}
                FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
                WHERE {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
                  AND {ScopeSql(scope, load)}
                  AND {HoldExclusion(scope, load)}
                ORDER BY {OrderBySql(entry)}
                """,
                load.ToDbParameters(conn)
            )
            .IgnoreQueryFilters()
            .AsNoTracking()
            .ToListAsync(ct);

        var recordIdProperty = db.Model.FindEntityType(typeof(TEntity))!.FindProperty(entry.RecordId.RecordIdMember)!;
        var recordIdGetter = recordIdProperty.GetGetter();
        var recordIdConverter = recordIdProperty.GetTypeMapping().Converter;

        var affected = new List<string>();
        var skipped = new List<string>();
        var blocked = new BlockedRows();
        var heldCount = candidates.Count - rows.Count;
        foreach (var row in rows)
        {
            var recordIdValue =
                recordIdGetter.GetClrValueUsingContainingEntity(row)
                ?? throw new InvalidOperationException(
                    $"Retention row for {entry.EntityType.FullName} produced an empty record id for member '{entry.RecordId.RecordIdMember}'."
                );
            var recordKey = recordIdConverter?.ConvertToProvider(recordIdValue) ?? recordIdValue;

            string? capturedPayload = null;
            if (handlers.Count > 0)
            {
                var before = new RetentionBeforeContext(
                    execution!.SweepId,
                    entry.Category,
                    scope.Rule.Strategy,
                    scope.Tenant.Id,
                    execution.At
                );
                var result = await InvokeOnBeforeReadOnlyAsync(conn, transaction, handlers, entry.EntityType, row, before, execution.OnCallersContext, ct);
                if (!result.Succeeded)
                {
                    var skippedId = await RecordIdSql.CanonicalizeAsync(
                        conn,
                        transaction,
                        entry.RecordId,
                        recordKey,
                        ct
                    );
                    skipped.Add(skippedId);
                    await RetentionHandlerSupport.PersistBeforeFailureAsync(
                        conn,
                        transaction,
                        execution,
                        entry,
                        scope.Rule.Strategy,
                        scope.Tenant.Id,
                        skippedId,
                        result,
                        logger,
                        ct
                    );
                    continue;
                }

                capturedPayload = result.CapturedPayload;
            }

            var update = new SqlParams { ["recordKey"] = recordKey };
            mutation.AddParameters(update, row);
            await using var command = update.CreateCommand(
                conn,
                transaction,
                $"""
                {mutation.HeadSql}
                WHERE {RecordIdSql.EqualsParameter("target", entry.RecordId, "recordKey")}
                  AND {ScopeSql(scope, update)}
                  AND {HoldExclusion(scope, update)}
                RETURNING {RecordIdSql.TextExpression("target", entry.RecordId)}
                """
            );
            await transaction.SaveAsync(MutationSavepoint, CancellationToken.None);
            object? changed;
            try
            {
                changed = await command.ExecuteScalarAsync(ct);
                await transaction.ReleaseAsync(MutationSavepoint, CancellationToken.None);
            }
            catch (DbException ex) when (IsIntegrityViolation(ex))
            {
                // Refused by a constraint: nothing changed, so no OnAfter work is queued.
                await transaction.RollbackAsync(MutationSavepoint, CancellationToken.None);
                blocked.Add(await RecordIdSql.CanonicalizeAsync(conn, transaction, entry.RecordId, recordKey, ct), ex);
                continue;
            }

            // No row back: a hold landed after the row was loaded.
            if (changed is not string recordId)
            {
                heldCount++;
                continue;
            }

            if (capturedPayload is not null)
            {
                await RetentionHandlerSupport.PersistCapturedRowAsync(
                    conn,
                    transaction,
                    execution!,
                    entry,
                    scope.Rule.Strategy,
                    scope.Tenant.Id,
                    recordId,
                    capturedPayload,
                    handlers,
                    ct
                );
            }
            affected.Add(recordId);
        }

        LogBlocked(scope, execution, blocked);
        return new SweepExecutionResult(
            affected,
            heldCount,
            RowDetailsPersisted: handlers.Count > 0,
            SkippedCount: skipped.Count,
            CandidateCount: candidates.Count,
            SkippedRecordIds: skipped,
            BlockedRecordIds: blocked.RecordIds,
            BlockFailure: blocked.FirstFailure
        );
    }

    /// <summary>
    /// Runs the OnBefore handlers inside a read-only savepoint that is always rolled back.
    /// Handlers that inject the host's DbContext share the sweep's connection and
    /// transaction: a database error in one leaves the transaction usable for the rest of
    /// the batch, and a write fails (SQLSTATE 25006) instead of committing with it. Rolling
    /// back also restores read-write mode and releases anything the handler locked.
    /// Read-only can't reach the change tracker, so whatever a handler left tracked there
    /// (an unsaved Add, the entries of a save that failed, or entities it loaded) is
    /// discarded afterwards: see <see cref="DiscardTrackedChanges"/>.
    /// </summary>
    private async Task<OnBeforeInvocationResult> InvokeOnBeforeReadOnlyAsync(
        DbConnection conn,
        DbTransaction transaction,
        IReadOnlyList<ResolvedRetentionHandler> handlers,
        Type entityType,
        object row,
        RetentionBeforeContext before,
        bool onCallersContext,
        CancellationToken ct
    )
    {
        // The caller's context had no unsaved changes on entry, so what it tracks now is what
        // the discard restores; anything tracked after the handlers is theirs.
        var trackedBefore = onCallersContext
            ? db.ChangeTracker.Entries().Select(entry => entry.Entity).ToHashSet(ReferenceEqualityComparer.Instance)
            : null;
        await transaction.SaveAsync(OnBeforeSavepoint, CancellationToken.None);
        OnBeforeInvocationResult result;
        try
        {
            await using (var readOnly = new SqlParams().CreateCommand(conn, transaction, "SET LOCAL transaction_read_only = on"))
            {
                await readOnly.ExecuteNonQueryAsync(ct);
            }

            result = await RetentionHandlerSupport.InvokeOnBeforeAsync(db, handlers, entityType, row, before, ct);
        }
        catch
        {
            // Cancelled, or Cohort's own statement failed: the batch is abandoned with the
            // original exception, so neither a failed rollback nor a failed discard may
            // replace it.
            try
            {
                await transaction.RollbackAsync(OnBeforeSavepoint, CancellationToken.None);
            }
            catch (Exception)
            {
                // The batch transaction is discarded either way.
            }

            try
            {
                DiscardTrackedChanges(trackedBefore);
            }
            catch (Exception)
            {
                // The tracker was cleared if it couldn't be restored.
            }

            throw;
        }

        await transaction.RollbackAsync(OnBeforeSavepoint, CancellationToken.None);
        DiscardTrackedChanges(trackedBefore);
        return result;
    }

    /// <summary>
    /// Leaves the change tracker as OnBefore found it. Cohort loads rows untracked, so on
    /// its own context (<paramref name="trackedBefore"/> null) there is nothing to keep and
    /// the tracker is cleared. On the caller's context, entities the handlers began
    /// tracking (added or loaded) are detached, and those the caller tracked get their
    /// original values back and stay tracked. When that can't be proven complete (changes
    /// are still detected, an entity the caller didn't track is tracked again, a navigation
    /// still holds an untracked entity, or restoring throws), the tracker is cleared and
    /// the erasure fails with <see cref="OnBeforeChangesNotUndoneException"/>.
    /// </summary>
    private void DiscardTrackedChanges(IReadOnlySet<object>? trackedBefore)
    {
        if (trackedBefore is null)
        {
            db.ChangeTracker.Clear();
            return;
        }

        bool restored;
        try
        {
            restored = TryRestoreTrackedChanges(trackedBefore);
        }
        catch (Exception)
        {
            restored = false;
        }

        if (!restored)
        {
            db.ChangeTracker.Clear();
            throw new OnBeforeChangesNotUndoneException();
        }
    }

    private bool TryRestoreTrackedChanges(IReadOnlySet<object> trackedBefore)
    {
        var tracker = db.ChangeTracker;
        var autoDetectChanges = tracker.AutoDetectChangesEnabled;
        // Detection would throw on the very change being undone, such as a modified key.
        tracker.AutoDetectChangesEnabled = false;
        try
        {
            foreach (var entry in tracker.Entries().ToArray())
            {
                if (!trackedBefore.Contains(entry.Entity))
                {
                    entry.State = EntityState.Detached;
                    continue;
                }

                // Unchanged too: a change detection never got to mark is still on the entity.
                RestoreKeys(entry);
                entry.CurrentValues.SetValues(entry.OriginalValues);
                entry.State = EntityState.Unchanged;
            }
        }
        finally
        {
            tracker.AutoDetectChangesEnabled = autoDetectChanges;
        }

        return !RetentionHandlerSupport.HasUnsavedChanges(db)
            && tracker.Entries().All(entry => trackedBefore.Contains(entry.Entity))
            && !ReachesUntrackedEntity();
    }

    /// <summary>
    /// EF refuses to set a tracked entity's key, even back to its original value, but the
    /// entity's own member can be. A key with no writable member throws, which clears the
    /// tracker and fails the erasure.
    /// </summary>
    private static void RestoreKeys(EntityEntry entry)
    {
        foreach (var key in entry.Metadata.GetProperties().Where(property => property.IsKey() && !property.IsShadowProperty()))
        {
            var original = entry.OriginalValues[key];
            if (key.GetKeyValueComparer().Equals(entry.CurrentValues[key], original))
            {
                continue;
            }

            if (key.PropertyInfo is { CanWrite: true } member)
            {
                member.SetValue(entry.Entity, original);
            }
            else
            {
                (key.FieldInfo ?? throw new InvalidOperationException($"Key {key.Name} has no writable member.")).SetValue(entry.Entity, original);
            }
        }
    }

    /// <summary>
    /// Whether a tracked entity's navigation still holds an entity the tracker doesn't
    /// track, which the caller's next attach of the graph would pick up.
    /// </summary>
    private bool ReachesUntrackedEntity()
    {
        var entries = db.ChangeTracker.Entries().ToArray();
        var tracked = entries.Select(entry => entry.Entity).ToHashSet(ReferenceEqualityComparer.Instance);
        foreach (var navigation in entries.SelectMany(entry => entry.Navigations))
        {
            var targets = navigation.CurrentValue switch
            {
                null => [],
                IEnumerable collection when navigation.Metadata.IsCollection => collection.Cast<object>(),
                var single => [single],
            };
            if (targets.Any(target => !tracked.Contains(target)))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsIntegrityViolation(DbException exception) =>
        exception.SqlState is { Length: 5 } sqlState && sqlState.StartsWith("23", StringComparison.Ordinal);

    /// <summary>
    /// One warning per refusing constraint, naming it and its SQLSTATE but no row: the
    /// constraint is schema metadata, the record ids are not logged.
    /// </summary>
    private void LogBlocked(SweepScope scope, SweepMutationContext? execution, BlockedRows blocked)
    {
        foreach (var group in blocked.Failures.GroupBy(failure => (failure.SqlState, Constraint: (failure as PostgresException)?.ConstraintName)))
        {
            var first = group.First();
            var diagnostic = RetentionFailureDiagnostic.Create(first);
            logger.LogWarning(
                first,
                "Cohort run {SweepId} left {Count} row(s) of entity {EntityType} unchanged: constraint {ConstraintName} refused them (sqlstate {SqlState}). They count as blocked and are retried by the next run. Diagnostic {DiagnosticId}.",
                execution?.SweepId,
                group.Count(),
                scope.Entry.EntityType.FullName,
                group.Key.Constraint ?? "(unnamed)",
                group.Key.SqlState,
                diagnostic.DiagnosticIdText
            );
        }
    }

    /// <summary>
    /// Selects up to one batch of candidates and locks them: the entity lock serialises
    /// against hold creation, the row lock against other runs. Held rows are excluded up
    /// front so they are neither locked nor reselected by every batch; rows an earlier
    /// batch of this run already recorded (e.g. skipped by a failing OnBefore) are excluded
    /// too, since they stay eligible and would otherwise be re-failed forever.
    /// </summary>
    private async Task<IReadOnlyList<string>> LockCandidatesAsync(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext? execution,
        CancellationToken ct
    )
    {
        var entry = scope.Entry;
        var locked = new List<string>();
        var attempted = new List<string>();
        var target = execution?.BatchSize ?? int.MaxValue;
        while (locked.Count < target)
        {
            int? limit = execution?.BatchSize is null ? null : target - locked.Count;
            var discovered = await DiscoverAsync(scope, conn, transaction, execution, attempted, limit, ct);
            if (discovered.Count == 0)
            {
                break;
            }

            await RetentionEntityLockSql.AcquireAsync(
                conn,
                transaction,
                entry.RetentionEntityId,
                entry.Tenant is not null ? scope.Tenant.Id : null,
                discovered,
                ct
            );
            var parameters = new SqlParams { ["candidateIds"] = discovered.ToArray() };
            await using var command = parameters.CreateCommand(
                conn,
                transaction,
                $"""
                SELECT {RecordIdSql.TextExpression("target", entry.RecordId)}
                FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
                WHERE {DueSql(scope, parameters)}
                  AND {RecordIdSql.EqualsAnyParameter("target", entry.RecordId, "candidateIds")}
                ORDER BY {OrderBySql(entry)}
                FOR UPDATE{(scope.SkipLocked ? " SKIP LOCKED" : "")}
                """
            );
            locked.AddRange(await ReadRecordIdsAsync(command, ct));
            attempted.AddRange(discovered);
            if (limit is null || discovered.Count < limit)
            {
                break;
            }
        }

        return locked;
    }

    private async Task<List<string>> DiscoverAsync(
        SweepScope scope,
        DbConnection conn,
        DbTransaction transaction,
        SweepMutationContext? execution,
        IReadOnlyList<string> attempted,
        int? limit,
        CancellationToken ct
    )
    {
        var entry = scope.Entry;
        var parameters = new SqlParams();
        var exclusions = NotAnyRecordIdSql(
            entry,
            "attemptedRecordIds",
            [.. attempted, .. execution?.ExcludedRecordIds ?? []],
            parameters
        );
        if (execution is not null)
        {
            exclusions += NotRecordedByRunSql(scope, execution.SweepId, parameters);
        }
        if (limit is not null)
        {
            parameters["batchSize"] = limit.Value;
        }

        await using var command = parameters.CreateCommand(
            conn,
            transaction,
            $"""
            SELECT {RecordIdSql.TextExpression("target", entry.RecordId)}
            FROM {PostgreSqlIdentifier.Format(entry.Table)} AS target
            WHERE {DueSql(scope, parameters)}{exclusions}
            ORDER BY {OrderBySql(entry)}
            {(limit is null ? "" : "LIMIT @batchSize")}
            """
        );
        return await ReadRecordIdsAsync(command, ct);
    }

    /// <summary>
    /// Rows the strategy may act on now: in scope, not actively held, and not referenced
    /// through a blocking foreign key. Discovery, the row lock and the eligible and deferred
    /// counts share it, so a row excluded from one is excluded from all.
    /// </summary>
    private string DueSql(SweepScope scope, SqlParams parameters) =>
        ReferencingForeignKeySql.NotReferencedSql(scope) is { Length: > 0 } notReferenced
            ? $"{ScopeSql(scope, parameters)} AND {HoldExclusion(scope, parameters)} AND {notReferenced}"
            : $"{ScopeSql(scope, parameters)} AND {HoldExclusion(scope, parameters)}";

    /// <summary>Excludes <paramref name="recordIds"/>, or "" when there are none.</summary>
    private static string NotAnyRecordIdSql(
        RetentionEntry entry,
        string parameterName,
        IReadOnlyCollection<string> recordIds,
        SqlParams parameters
    )
    {
        if (recordIds.Count == 0)
        {
            return "";
        }

        parameters[parameterName] = recordIds.ToArray();
        return $"\n  AND NOT ({RecordIdSql.EqualsAnyParameter("target", entry.RecordId, parameterName)})";
    }

    /// <summary>
    /// Excludes rows run <paramref name="sweepId"/> already recorded for this entity, such as
    /// rows a failing OnBefore skipped: they stay eligible, so reselecting them would re-fail
    /// them forever, and counting them as deferred would count them twice.
    /// </summary>
    private string NotRecordedByRunSql(SweepScope scope, Guid sweepId, SqlParams parameters)
    {
        var entry = scope.Entry;
        parameters["excludedSweepId"] = sweepId;
        parameters["excludedRetentionEntityId"] = entry.RetentionEntityId;
        parameters["excludedCategory"] = entry.Category;
        parameters["excludedStrategy"] = (int)HandlesStrategy;
        parameters["excludedTenantId"] = scope.Tenant.Id;
        return $"""

              AND NOT EXISTS (
                  SELECT 1
                  FROM {PostgreSqlIdentifier.Format(entry.CohortTables.SweepRunRowDetail)} AS prior_detail
                  WHERE prior_detail."SweepId" = @excludedSweepId
                    AND prior_detail."RetentionEntityId" = @excludedRetentionEntityId
                    AND prior_detail."RecordId" = {RecordIdSql.TextExpression("target", entry.RecordId)}
                    AND prior_detail."Category" = @excludedCategory
                    AND prior_detail."Strategy" = @excludedStrategy
                    AND prior_detail."TenantId" = @excludedTenantId
              )
            """;
    }

    /// <summary>
    /// The scope's rows, restricted to the tenant and to rows this strategy has not handled.
    /// <paramref name="anchor"/> picks which side of the cutoff the anchor must fall:
    /// before it (the rows retention acts on), NULL (rows no cutoff ever matches), or on or
    /// after it (rows the rule keeps; none without a cutoff).
    /// </summary>
    private string ScopeSql(SweepScope scope, SqlParams parameters, AnchorFilter anchor = AnchorFilter.BeforeCutoff)
    {
        var entry = scope.Entry;
        var anchorColumn = $"target.{PostgreSqlIdentifier.Quote(entry.AnchorColumn)}";
        var clauses = SubjectAndTenantSql(scope, parameters);
        switch (anchor)
        {
            case AnchorFilter.Null:
                clauses.Add($"{anchorColumn} IS NULL");
                break;
            case AnchorFilter.BeforeCutoff when scope.Cutoff is { } cutoff:
                parameters["cutoff"] = cutoff;
                clauses.Add($"{anchorColumn} < @cutoff");
                break;
            case AnchorFilter.OnOrAfterCutoff when scope.Cutoff is { } cutoff:
                parameters["cutoff"] = cutoff;
                clauses.Add($"{anchorColumn} >= @cutoff");
                break;
            case AnchorFilter.OnOrAfterCutoff:
                clauses.Add("FALSE");
                break;
        }
        if (EligibilitySql(entry) is { Length: > 0 } eligibility)
        {
            clauses.Add(eligibility);
        }

        return clauses.Count == 0 ? "TRUE" : string.Join(" AND ", clauses);
    }

    /// <summary>The scope's subject (for an erasure) and tenant clauses.</summary>
    private static List<string> SubjectAndTenantSql(SweepScope scope, SqlParams parameters)
    {
        var entry = scope.Entry;
        var clauses = new List<string>();
        if (scope.Subject is { } subject)
        {
            var matches = new List<string>();
            for (var index = 0; index < subject.Matches.Count; index++)
            {
                parameters[$"subjectValue{index}"] = subject.Matches[index].SubjectValue;
                matches.Add(subject.Matches[index].EqualsParameterSql("target", $"subjectValue{index}"));
            }
            clauses.Add($"({string.Join(" OR ", matches)})");
        }
        if (entry.Tenant is { } tenant)
        {
            parameters["tenantId"] = scope.Tenant.Id;
            clauses.Add($"target.{PostgreSqlIdentifier.Quote(tenant.TenantColumn)} = @tenantId");
        }

        return clauses;
    }

    private static string HoldExclusion(SweepScope scope, SqlParams parameters)
    {
        var entry = scope.Entry;
        parameters["retentionEntityId"] = entry.RetentionEntityId;
        if (entry.Tenant is not null)
        {
            parameters["tenantId"] = scope.Tenant.Id;
        }

        return RetentionHoldSql.BuildActiveHoldExclusion(
            entry.CohortTables.RetentionHolds,
            "target",
            entry.RecordId.RecordIdColumn,
            entry.Tenant?.TenantColumn
        );
    }

    private static string OrderBySql(RetentionEntry entry) =>
        $"target.{PostgreSqlIdentifier.Quote(entry.AnchorColumn)} ASC, {RecordIdSql.TextExpression("target", entry.RecordId)} ASC";

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

    public DbException? FirstFailure => failures.Count == 0 ? null : failures[0];

    public void Add(string recordId, DbException failure)
    {
        recordIds.Add(recordId);
        failures.Add(failure);
    }
}

/// <summary>Which side of the cutoff a scope's anchor must fall; see <c>ScopeSql</c>.</summary>
internal enum AnchorFilter
{
    BeforeCutoff,
    Null,
    OnOrAfterCutoff,
}

/// <summary>
/// How a strategy changes rows: a statement head aliased as <c>target</c>, the values it
/// binds (for one loaded row, or for every row when <c>row</c> is null), and whether the
/// values depend on the row so rows must be loaded and changed one at a time.
/// </summary>
internal sealed record SweepMutation(
    string HeadSql,
    Action<SqlParams, object?> AddParameters,
    bool RowByRow = false
);
