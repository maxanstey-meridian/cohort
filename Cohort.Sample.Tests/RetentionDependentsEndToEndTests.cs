using System.Collections.Concurrent;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Npgsql;

namespace Cohort.Sample.Tests;

// ─── End-to-end test (copied from RestrictiveForeignKeySweepEndToEndTests) ───
//
// A dependents step retires what depends on a row in the row's own transaction, before Cohort
// mutates it, under the mutation's savepoint and the hold guard. Most tests build a host on
// their own database with a test-local model: a Purge parent referenced through RESTRICT keys by
// an unretained and a retained child, and an Anonymise parent whose retained audit events
// copy its values, as Casebridge's do. The tracked-graph tests use a model of their own, whose
// step leaves changes on its context; the sample-entity tests run on the shared fixture.
// A failing step's row is recorded once, in the row detail's Failure.
// ────────────────────────────────────────────────────────────────────────────

public sealed class RetentionDependentsEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    private static readonly DateTimeOffset AsOf = new(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task A_Purge_Whose_Step_Removes_Its_Dependents_Deletes_The_Parent()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var script = new StepScript { Parent = DeletePlainChildrenAsync };
        await using var services = BuildServiceProvider(database.ConnectionString, script);
        var tenantId = Guid.NewGuid();
        var referencedId = Guid.NewGuid();
        var plainId = Guid.NewGuid();

        await SeedAsync(services, db =>
        {
            db.Parents.AddRange(Parent(referencedId, tenantId, AsOf.AddDays(-200)), Parent(plainId, tenantId, AsOf.AddDays(-100)));
            db.PlainChildren.AddRange(PlainChild(referencedId), PlainChild(referencedId));
        });

        var result = await SweepAsync(services, tenantId);

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ADependentsParent))
            .Which.Should().Match<EntitySweepCount>(count =>
                count.Affected == 2 && count.BlockedCount == 0 && count.SkippedCount == 0 && count.DeferredCount == 0);
        (await LoadRunStatusAsync(database.ConnectionString, result.SweepId)).Should().Be(SweepRunStatus.Succeeded);
        (await CountAsync(database.ConnectionString, "dependents_parents")).Should().Be(0);
        (await CountAsync(database.ConnectionString, "dependents_plain_children")).Should().Be(0);
        script.Calls.Should().Be(2);
        // Per-row audit: the engine writes one detail per mutated row, and none records a failure.
        (await LoadRowDetailsAsync(database.ConnectionString, result.SweepId)).Should().BeEquivalentTo([
            (referencedId.ToString(), (string?)null),
            (plainId.ToString(), (string?)null),
        ]);
    }

    // What a step can put in an outbox message: the run, the row's category and strategy, the
    // tenant, and the time the batch stamps the row's own detail with.
    [Fact]
    public async Task A_Step_Is_Told_The_Run_It_Retires_Its_Row_For()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var script = new StepScript();
        await using var services = BuildServiceProvider(database.ConnectionString, script);
        var tenantId = Guid.NewGuid();
        var parentId = Guid.NewGuid();
        await SeedAsync(services, db => db.Parents.Add(Parent(parentId, tenantId, AsOf.AddDays(-100))));

        var result = await SweepAsync(services, tenantId);

        result.EntityFailures.Should().BeEmpty();
        var detailAt = (await QueryAsync(
            database.ConnectionString,
            "SELECT \"At\" FROM \"sweep_run_row_detail\" WHERE \"SweepId\" = @sweepId AND \"RecordId\" = @recordId",
            reader => reader.GetFieldValue<DateTimeOffset>(0),
            ("sweepId", result.SweepId),
            ("recordId", parentId.ToString())
        )).Should().ContainSingle().Subject;
        var ctx = script.RunContexts.Should().ContainSingle().Subject;
        ctx.SweepId.Should().Be(result.SweepId);
        ctx.Category.Should().Be("dependents-purge");
        ctx.Strategy.Should().Be(Strategy.Purge);
        ctx.TenantId.Should().Be(tenantId);
        ctx.At.Should().Be(detailAt);
    }

    [Fact]
    public async Task A_Parent_Still_Referenced_After_Its_Step_Is_Blocked_And_Its_Step_Undone()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var script = new StepScript { Parent = DeletePlainChildrenAsync };
        await using var services = BuildServiceProvider(database.ConnectionString, script);
        var tenantId = Guid.NewGuid();
        var parentId = Guid.NewGuid();

        await SeedAsync(services, db =>
        {
            db.Parents.Add(Parent(parentId, tenantId, AsOf.AddDays(-200)));
            db.PlainChildren.Add(PlainChild(parentId));
            // Not due for years: the step leaves it, and it keeps the parent.
            db.RetainedChildren.Add(RetainedChild(parentId, tenantId));
        });

        var result = await SweepAsync(services, tenantId);

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ADependentsParent))
            .Which.Should().Match<EntitySweepCount>(count =>
                count.Affected == 0 && count.BlockedCount == 1 && count.SkippedCount == 0);
        (await LoadRunStatusAsync(database.ConnectionString, result.SweepId)).Should().Be(SweepRunStatus.Succeeded);
        (await CountAsync(database.ConnectionString, "dependents_parents")).Should().Be(1);
        // The block rolled the step back with it: dependents never go without their row.
        (await CountAsync(database.ConnectionString, "dependents_plain_children")).Should().Be(1);
        (await LoadStepFailuresAsync(database.ConnectionString, result.SweepId)).Should().BeEmpty();
        script.Calls.Should().Be(1);
    }

    [Fact]
    public async Task A_Step_That_Does_Not_Retire_Referencing_Rows_Never_Runs_For_A_Still_Referenced_Row()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        // The step would clear the reference if it ran: only the pre-filter can block the row.
        var script = new StepScript { Parent = DeletePlainChildrenAsync };
        await using var services = BuildServiceProvider(database.ConnectionString, script, parentRetiresReferencingRows: false);
        var tenantId = Guid.NewGuid();
        var referencedId = Guid.NewGuid();
        var plainId = Guid.NewGuid();

        await SeedAsync(services, db =>
        {
            db.Parents.AddRange(Parent(referencedId, tenantId, AsOf.AddDays(-200)), Parent(plainId, tenantId, AsOf.AddDays(-100)));
            db.PlainChildren.Add(PlainChild(referencedId));
        });

        var result = await SweepAsync(services, tenantId);

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ADependentsParent))
            .Which.Should().Match<EntitySweepCount>(count =>
                count.Affected == 1 && count.BlockedCount == 1 && count.SkippedCount == 0);
        script.Calls.Should().Be(1);
        (await LoadIdsAsync(database.ConnectionString, "dependents_parents")).Should().Equal(referencedId);
        (await LoadParentIdsOfPlainChildrenAsync(database.ConnectionString)).Should().Equal(referencedId);
        (await LoadStepFailuresAsync(database.ConnectionString, result.SweepId)).Should().BeEmpty();
    }

    public enum StepFailure
    {
        Throws,
        DatabaseError,
        SwallowedDatabaseError,
        UnsavedChanges,
    }

    [Theory]
    [InlineData(StepFailure.Throws, "^type=System\\.InvalidOperationException;code=hresult:0x[0-9A-F]{8};diagnosticId=[0-9a-f]{32}$")]
    [InlineData(StepFailure.DatabaseError, "^type=Npgsql\\.PostgresException;code=sqlstate:22012;diagnosticId=[0-9a-f]{32}$")]
    // Caught and returned from: the transaction is left aborted, so the guard's own read fails.
    [InlineData(StepFailure.SwallowedDatabaseError, "^type=Npgsql\\.PostgresException;code=sqlstate:25P02;diagnosticId=[0-9a-f]{32}$")]
    [InlineData(StepFailure.UnsavedChanges, "^reason=dependents-unsaved-changes$")]
    public async Task A_Step_That_Throws_Skips_Only_Its_Row(StepFailure failure, string expectedError)
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var failingId = Guid.NewGuid();
        var script = new StepScript
        {
            Parent = async (row, db, ct) =>
            {
                // A partial write first: the failure must undo it.
                await DeletePlainChildrenAsync(row, db, ct);
                if (row.Id != failingId)
                {
                    return;
                }

                switch (failure)
                {
                    case StepFailure.Throws:
                        throw new InvalidOperationException("the step failed");
                    case StepFailure.DatabaseError:
                        await db.Database.ExecuteSqlRawAsync("SELECT 1/0", ct);
                        break;
                    case StepFailure.SwallowedDatabaseError:
                        try
                        {
                            await db.Database.ExecuteSqlRawAsync("SELECT 1/0", ct);
                        }
                        catch (PostgresException)
                        {
                        }

                        break;
                    case StepFailure.UnsavedChanges:
                        db.PlainChildren.Add(PlainChild(row.Id));
                        break;
                }
            },
        };
        await using var services = BuildServiceProvider(database.ConnectionString, script);
        var tenantId = Guid.NewGuid();
        var ids = new[] { Guid.NewGuid(), failingId, Guid.NewGuid() };

        await SeedAsync(services, db =>
        {
            for (var index = 0; index < ids.Length; index++)
            {
                db.Parents.Add(Parent(ids[index], tenantId, AsOf.AddDays(-300 + index)));
                db.PlainChildren.Add(PlainChild(ids[index]));
            }
        });

        // All three share one batch: the failing row must not abort it.
        var result = await SweepAsync(services, tenantId);

        result.EntityFailures.Should().ContainSingle()
            .Which.Should().Be($"reason=rows-skipped;entityType={typeof(ADependentsParent).FullName};count=1");
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ADependentsParent))
            .Which.Should().Match<EntitySweepCount>(count =>
                count.Affected == 2 && count.SkippedCount == 1 && count.BlockedCount == 0 && count.DeferredCount == 0);
        (await LoadRunStatusAsync(database.ConnectionString, result.SweepId)).Should().Be(SweepRunStatus.PartiallyFailed);
        var skipped = (await LoadStepFailuresAsync(database.ConnectionString, result.SweepId)).Should().ContainSingle().Subject;
        skipped.RecordId.Should().Be(failingId.ToString());
        skipped.Failure.Should().MatchRegex(expectedError);
        (await LoadIdsAsync(database.ConnectionString, "dependents_parents")).Should().Equal(failingId);
        (await LoadParentIdsOfPlainChildrenAsync(database.ConnectionString)).Should().Equal(failingId);
        // One detail per row: the skip's own for the failing row, the engine's for the others.
        (await LoadRowDetailsAsync(database.ConnectionString, result.SweepId)).Select(detail => detail.RecordId)
            .Should().BeEquivalentTo(ids.Select(id => id.ToString()));
    }

    [Fact]
    public async Task A_Step_That_Blanks_A_Held_Retained_Row_Skips_Its_Parent()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var logs = new RecordingLogProvider();
        var script = new StepScript { Anonymised = BlankAuditCopiesAsync };
        await using var services = BuildServiceProvider(
            database.ConnectionString,
            script,
            configure: services => services.AddSingleton<ILoggerProvider>(logs)
        );
        var tenantId = Guid.NewGuid();
        var heldParentId = Guid.NewGuid();
        var parentId = Guid.NewGuid();
        var heldEventId = Guid.NewGuid();

        await SeedAsync(services, db =>
        {
            db.AnonymisedParents.AddRange(
                AnonymisedParent(heldParentId, tenantId, AsOf.AddDays(-200)),
                AnonymisedParent(parentId, tenantId, AsOf.AddDays(-100))
            );
            db.AuditEvents.AddRange(
                AuditEvent(heldEventId, heldParentId, tenantId, "held-copy"),
                AuditEvent(Guid.NewGuid(), parentId, tenantId, "unheld-copy")
            );
        });
        var holdId = await CreateHoldAsync(services, AuditEventEntityId, heldEventId, tenantId);

        var result = await SweepAsync(services, tenantId);

        AssertSkippedWithReason(result, typeof(AAnonymisedParent), affected: 1);
        var skipped = (await LoadStepFailuresAsync(database.ConnectionString, result.SweepId)).Should().ContainSingle().Subject;
        skipped.RecordId.Should().Be(heldParentId.ToString());
        skipped.Failure.Should().Be("reason=dependents-changed-held-row");
        (await LoadAuditCopiesAsync(database.ConnectionString)).Should().BeEquivalentTo([
            (heldParentId, (string?)"held-copy"),
            (parentId, (string?)null),
        ]);
        (await LoadAnonymisedParentIdsAsync(database.ConnectionString)).Should().Equal(parentId);
        // The hold's id is Cohort's own, so the warning names it; the row is never named.
        logs.Entries.Should().Contain(entry =>
            entry.Level == LogLevel.Warning && entry.Message.Contains(holdId.ToString(), StringComparison.Ordinal));
        logs.Entries.Should().NotContain(entry =>
            entry.Message.Contains(heldParentId.ToString(), StringComparison.Ordinal)
            || entry.Message.Contains(heldEventId.ToString(), StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_Step_That_Deletes_A_Held_Retained_Row_Skips_Its_Parent()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var script = new StepScript
        {
            Parent = async (row, db, ct) =>
            {
                await DeletePlainChildrenAsync(row, db, ct);
                await db.RetainedChildren.Where(child => child.ParentId == row.Id).ExecuteDeleteAsync(ct);
            },
        };
        await using var services = BuildServiceProvider(database.ConnectionString, script);
        var tenantId = Guid.NewGuid();
        var heldParentId = Guid.NewGuid();
        var parentId = Guid.NewGuid();
        var heldChild = RetainedChild(heldParentId, tenantId);

        await SeedAsync(services, db =>
        {
            db.Parents.AddRange(Parent(heldParentId, tenantId, AsOf.AddDays(-200)), Parent(parentId, tenantId, AsOf.AddDays(-100)));
            db.PlainChildren.Add(PlainChild(heldParentId));
            db.RetainedChildren.AddRange(heldChild, RetainedChild(parentId, tenantId));
        });
        await CreateHoldAsync(services, RetainedChildEntityId, heldChild.Id, tenantId);

        var result = await SweepAsync(services, tenantId);

        AssertSkippedWithReason(result, typeof(ADependentsParent), affected: 1);
        (await LoadStepFailuresAsync(database.ConnectionString, result.SweepId)).Should().ContainSingle()
            .Which.Failure.Should().Be("reason=dependents-changed-held-row");
        (await LoadIdsAsync(database.ConnectionString, "dependents_parents")).Should().Equal(heldParentId);
        (await LoadIdsAsync(database.ConnectionString, "dependents_retained_children")).Should().Equal(heldChild.Id);
        // Rolled back with the rest of the step.
        (await LoadParentIdsOfPlainChildrenAsync(database.ConnectionString)).Should().Equal(heldParentId);
    }

    [Fact]
    public async Task A_Hold_Created_While_A_Step_Waits_On_Its_Row_Fails_The_Step()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new StepScript
        {
            Anonymised = async (row, db, ct) =>
            {
                entered.TrySetResult();
                await gate.Task.WaitAsync(ct);
                await BlankAuditCopiesAsync(row, db, ct);
            },
        };
        await using var services = BuildServiceProvider(database.ConnectionString, script);
        var tenantId = Guid.NewGuid();
        var parentId = Guid.NewGuid();
        var eventId = Guid.NewGuid();

        await SeedAsync(services, db =>
        {
            db.AnonymisedParents.Add(AnonymisedParent(parentId, tenantId, AsOf.AddDays(-200)));
            db.AuditEvents.Add(AuditEvent(eventId, parentId, tenantId, "copy"));
        });

        var sweep = Task.Run(() => SweepAsync(services, tenantId));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));

        // The step has taken its guard baseline. A hold is created on the audit row it is about
        // to blank, and holds the row FOR SHARE until it commits: the step's UPDATE waits on it.
        await using (var holdScope = services.CreateAsyncScope())
        {
            var holdDb = holdScope.ServiceProvider.GetRequiredService<DependentsDbContext>();
            await using var holdTransaction = await holdDb.Database.BeginTransactionAsync();
            var holdBackendId = ((NpgsqlConnection)holdDb.Database.GetDbConnection()).ProcessID;
            await holdScope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                new RetentionHoldRequest(Guid.NewGuid(), AuditEventEntityId, eventId.ToString(), tenantId, "litigation", DateTimeOffset.UtcNow.AddMinutes(-1)),
                CancellationToken.None
            );
            gate.SetResult();
            await WaitForBlockedStepAsync(database.ConnectionString, holdBackendId);
            await holdTransaction.CommitAsync();
        }

        var result = await sweep.WaitAsync(TimeSpan.FromSeconds(30));

        AssertSkippedWithReason(result, typeof(AAnonymisedParent), affected: 0);
        (await LoadStepFailuresAsync(database.ConnectionString, result.SweepId)).Should().ContainSingle()
            .Which.Failure.Should().Be("reason=dependents-changed-held-row");
        (await LoadAuditCopiesAsync(database.ConnectionString)).Should().Equal((parentId, (string?)"copy"));
        (await LoadAnonymisedParentIdsAsync(database.ConnectionString)).Should().BeEmpty();
    }

    [Fact]
    public async Task A_Step_That_Retires_Its_Own_Row_Is_Skipped_Not_Held()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var script = new StepScript
        {
            // A counter-cache trigger that marks the row scrubbed would do the same.
            Anonymised = (row, db, ct) => db.AnonymisedParents
                .Where(parent => parent.Id == row.Id)
                .ExecuteUpdateAsync(set => set.SetProperty(parent => parent.AnonymisedAt, DateTimeOffset.UtcNow), ct),
        };
        await using var services = BuildServiceProvider(database.ConnectionString, script);
        var tenantId = Guid.NewGuid();
        var parentId = Guid.NewGuid();

        await SeedAsync(services, db => db.AnonymisedParents.Add(AnonymisedParent(parentId, tenantId, AsOf.AddDays(-200))));

        var result = await SweepAsync(services, tenantId);

        AssertSkippedWithReason(result, typeof(AAnonymisedParent), affected: 0);
        (await LoadStepFailuresAsync(database.ConnectionString, result.SweepId)).Should().ContainSingle()
            .Which.Failure.Should().Be("reason=dependents-changed-row");
        // The step's own change was rolled back with it.
        (await LoadAnonymisedParentIdsAsync(database.ConnectionString)).Should().BeEmpty();
        (await ScalarAsync<string>(database.ConnectionString, "SELECT \"Name\" FROM dependents_anonymised_parents")).Should().Be("name");
    }

    [Theory]
    // The prediction: the step clears the references, so nothing is blocked up front.
    [InlineData(true, 2, 0)]
    // The step leaves the references, so the referenced row is blocked up front.
    [InlineData(false, 1, 1)]
    public async Task Preview_And_Dry_Run_Count_Referenced_Rows_Of_A_Step_Entity_As_Its_Registration_Says_And_Never_Run_It(
        bool retiresReferencingRows,
        int affected,
        int blocked
    )
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var script = new StepScript { Parent = DeletePlainChildrenAsync };
        await using var services = BuildServiceProvider(
            database.ConnectionString,
            script,
            parentRetiresReferencingRows: retiresReferencingRows
        );
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var subjectId = Guid.NewGuid();
        var referencedId = Guid.NewGuid();

        await SeedAsync(services, db =>
        {
            db.Parents.AddRange(
                Parent(referencedId, tenant.Id, AsOf.AddDays(-200), subjectId),
                Parent(Guid.NewGuid(), tenant.Id, AsOf.AddDays(-100), subjectId)
            );
            db.PlainChildren.Add(PlainChild(referencedId));
        });

        RetentionSweepResult preview;
        RetentionSweepResult dryRun;
        ErasureResult dryRunErasure;
        await using (var scope = services.CreateAsyncScope())
        {
            preview = await scope.ServiceProvider.GetRequiredService<IRetentionPreview>().PreviewAsync(tenant, AsOf);
            dryRun = await scope.ServiceProvider.GetRequiredService<IRetentionSweep>()
                .ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, AsOf, dryRun: true));
            dryRunErasure = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                RetentionErasureRequest.Tenanted(tenant, new ErasureScope("person", subjectId, dryRun: true), AsOf)
            );
        }

        foreach (var counts in new[] { preview.Counts, dryRun.Counts, dryRunErasure.Counts })
        {
            counts.Should().ContainSingle(count => count.EntityType == typeof(ADependentsParent))
                .Which.Should().Match<EntitySweepCount>(count => count.Affected == affected && count.BlockedCount == blocked);
        }
        dryRunErasure.Complete.Should().Be(retiresReferencingRows);
        script.Calls.Should().Be(0);
        (await CountAsync(database.ConnectionString, "dependents_parents")).Should().Be(2);
        (await CountAsync(database.ConnectionString, "dependents_plain_children")).Should().Be(1);
    }

    [Fact]
    public async Task A_Caller_Transaction_Erasure_Runs_The_Step_In_The_Callers_Transaction()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var script = new StepScript { Parent = DeletePlainChildrenAsync };
        await using var services = BuildServiceProvider(database.ConnectionString, script);
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var subjectId = Guid.NewGuid();
        var parentId = Guid.NewGuid();

        await SeedAsync(services, db =>
        {
            db.Parents.Add(Parent(parentId, tenant.Id, AsOf.AddDays(-1), subjectId));
            db.PlainChildren.Add(PlainChild(parentId));
        });

        foreach (var commit in new[] { false, true })
        {
            await using var scope = services.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<DependentsDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();

            var result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                RetentionErasureRequest.Tenanted(tenant, new ErasureScope("person", subjectId), AsOf, ErasureTransaction.Caller)
            );

            result.EntityFailures.Should().BeEmpty();
            result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ADependentsParent))
                .Which.Affected.Should().Be(1);
            // The step's own context, inside the caller's transaction: the caller's is never handed over.
            script.Contexts.Should().NotBeEmpty().And.NotContain(context => ReferenceEquals(context, db));
            (await db.Database.SqlQueryRaw<long>("SELECT count(*) AS \"Value\" FROM dependents_plain_children").SingleAsync())
                .Should().Be(0);
            if (commit)
            {
                await transaction.CommitAsync();
            }
            else
            {
                await transaction.RollbackAsync();
                (await CountAsync(database.ConnectionString, "dependents_parents")).Should().Be(1);
                (await CountAsync(database.ConnectionString, "dependents_plain_children")).Should().Be(1);
            }
        }

        (await CountAsync(database.ConnectionString, "dependents_parents")).Should().Be(0);
        (await CountAsync(database.ConnectionString, "dependents_plain_children")).Should().Be(0);
    }

    [Theory]
    [InlineData(TrackedGraphStep.StagingBody, UnsavedChanges, false)]
    [InlineData(TrackedGraphStep.StagingBody, UnsavedChanges, true)]
    // A save the database refuses leaves its entry tracked and fails only its row: later rows
    // of the batch still run.
    [InlineData(TrackedGraphStep.FailedSaveBody, ForeignKeyRefusal, false)]
    [InlineData(TrackedGraphStep.FailedSaveBody, ForeignKeyRefusal, true)]
    // Change detection itself throws on a changed key: still the step's unsaved change.
    [InlineData(TrackedGraphStep.KeyChangingBody, UnsavedChanges, false)]
    [InlineData(TrackedGraphStep.KeyChangingBody, UnsavedChanges, true)]
    [InlineData(TrackedGraphStep.EditingBody, UnsavedChanges, false)]
    [InlineData(TrackedGraphStep.EditingBody, UnsavedChanges, true)]
    // With automatic detection off, only an explicit DetectChanges sees the step's edit.
    [InlineData(TrackedGraphStep.UndetectedEditBody, UnsavedChanges, false)]
    [InlineData(TrackedGraphStep.UndetectedEditBody, UnsavedChanges, true)]
    [InlineData(TrackedGraphStep.AddingBody, UnsavedChanges, false)]
    [InlineData(TrackedGraphStep.AddingBody, UnsavedChanges, true)]
    [InlineData(TrackedGraphStep.ManyToManyBody, UnsavedChanges, false)]
    [InlineData(TrackedGraphStep.ManyToManyBody, UnsavedChanges, true)]
    // Nothing unsaved: the row is erased.
    [InlineData(TrackedGraphStep.TrackingReadBody, null, false)]
    [InlineData(TrackedGraphStep.TrackingReadBody, null, true)]
    [InlineData(TrackedGraphStep.ClearingBody, null, false)]
    [InlineData(TrackedGraphStep.ClearingBody, null, true)]
    public async Task A_Caller_Transaction_Erasure_Leaves_The_Callers_Context_As_It_Was_Whatever_A_Step_Does(
        string body,
        string? expectedError,
        bool callerTracks
    )
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildTrackedGraphServices(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        await SeedTrackedGraphAsync(
            services,
            tenantId,
            subjectId,
            (body, AsOf.AddDays(-2)),
            // Later in the batch than the step's row: it still runs.
            ("healthy", AsOf.AddDays(-1))
        );
        var bystanderId = await SeedTrackedGraphBystandersAsync(services, tenantId);

        ErasureResult result;
        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TrackedGraphDbContext>();
            TrackedGraphRecord? bystander = null;
            TrackedGraphFolder? folder = null;
            if (callerTracks)
            {
                bystander = await db.Records.SingleAsync(record => record.Id == bystanderId);
                // Its comments not loaded, so a step's tracking read of one would fix up into it.
                folder = await db.Folders.Include(item => item.Tags).SingleAsync();
            }

            var trackedBefore = db.ChangeTracker.Entries().Select(entry => entry.Entity).ToArray();
            await using var transaction = await db.Database.BeginTransactionAsync();

            result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                RetentionErasureRequest.Tenanted(
                    new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                    new ErasureScope("person", subjectId),
                    AsOf,
                    transaction: ErasureTransaction.Caller
                )
            );

            // The caller's context is exactly as it was: the same entities, unchanged.
            db.ChangeTracker.DetectChanges();
            db.ChangeTracker.HasChanges().Should().BeFalse();
            db.ChangeTracker.Entries().Select(entry => entry.Entity).ToHashSet(ReferenceEqualityComparer.Instance)
                .SetEquals(trackedBefore).Should().BeTrue();
            db.ChangeTracker.Entries().Where(entry => entry.State != EntityState.Unchanged).Should().BeEmpty();
            if (callerTracks)
            {
                bystander!.Id.Should().Be(bystanderId);
                bystander.Body.Should().Be(TrackedGraphStep.BystanderBody);
                folder!.Comments.Should().BeEmpty();
                folder.Tags.Should().BeEmpty();
            }

            // The caller's own save and commit carry nothing of the step's.
            await db.SaveChangesAsync();
            await transaction.CommitAsync();
        }

        var skipped = expectedError is null ? 0 : 1;
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(TrackedGraphRecord))
            .Which.Should().Match<EntitySweepCount>(count => count.Affected == 2 - skipped && count.SkippedCount == skipped);
        if (expectedError is null)
        {
            result.EntityFailures.Should().BeEmpty();
            (await LoadStepFailuresAsync(database.ConnectionString, result.SweepId)).Should().BeEmpty();
        }
        else
        {
            result.EntityFailures.Should().ContainSingle()
                .Which.Should().Be($"reason=rows-skipped;entityType={typeof(TrackedGraphRecord).FullName};count=1");
            (await LoadStepFailuresAsync(database.ConnectionString, result.SweepId)).Should().ContainSingle()
                .Which.Failure.Should().MatchRegex(expectedError);
        }

        await using var verifyScope = services.CreateAsyncScope();
        var verify = verifyScope.ServiceProvider.GetRequiredService<TrackedGraphDbContext>();
        (await verify.Records.Select(record => record.Body).ToListAsync()).Should().BeEquivalentTo(
            expectedError is null ? [TrackedGraphStep.BystanderBody] : [body, TrackedGraphStep.BystanderBody]
        );
        (await verify.Records.AnyAsync(record => record.Id == bystanderId)).Should().BeTrue();
        (await verify.Comments.CountAsync()).Should().Be(1);
        (await verify.Folders.SelectMany(item => item.Tags).CountAsync()).Should().Be(0);
    }

    [Fact]
    public async Task A_Caller_Transaction_Erasures_Step_Reads_Through_Its_Own_Context_Inside_The_Callers_Transaction()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildTrackedGraphServices(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        await SeedTrackedGraphAsync(services, tenantId, subjectId);
        var probe = services.GetRequiredService<TrackedGraphProbe>();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TrackedGraphDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            // Saved in the caller's transaction only: a step outside it couldn't see it.
            db.Records.Add(new TrackedGraphRecord
            {
                Id = Guid.NewGuid(),
                TenantId = tenantId,
                SubjectId = subjectId,
                CreatedAt = AsOf.AddDays(-1),
                Body = TrackedGraphStep.ReadingOwnRowBody,
            });
            await db.SaveChangesAsync();

            var result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                RetentionErasureRequest.Tenanted(
                    new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                    new ErasureScope("person", subjectId),
                    AsOf,
                    transaction: ErasureTransaction.Caller
                )
            );

            result.EntityFailures.Should().BeEmpty();
            probe.Contexts.Should().ContainSingle().Which.Should().NotBeSameAs(db);
            await transaction.CommitAsync();
        }

        probe.SawOwnRow.Should().Equal(true);
        await using var verifyScope = services.CreateAsyncScope();
        (await verifyScope.ServiceProvider.GetRequiredService<TrackedGraphDbContext>().Records.CountAsync()).Should().Be(0);
    }

    [Fact]
    public async Task An_Owned_Sweep_Fails_Only_The_Row_Whose_Step_Left_A_Navigation_Change()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildTrackedGraphServices(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        await SeedTrackedGraphAsync(
            services,
            tenantId,
            subjectId: null,
            (TrackedGraphStep.AddingBody, AsOf.AddDays(-300)),
            ("healthy-1", AsOf.AddDays(-200)),
            ("healthy-2", AsOf.AddDays(-100))
        );

        var result = await services.GetRequiredService<IRetentionSweep>().ExecuteAsync(
            RetentionSweepRequest.Tenanted(new TenantContext(tenantId, "uk", new Dictionary<string, string>()), AsOf)
        );

        result.EntityFailures.Should().ContainSingle()
            .Which.Should().Be($"reason=rows-skipped;entityType={typeof(TrackedGraphRecord).FullName};count=1");
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(TrackedGraphRecord))
            .Which.Should().Match<EntitySweepCount>(count => count.Affected == 2 && count.SkippedCount == 1);
        (await LoadStepFailuresAsync(database.ConnectionString, result.SweepId)).Should().ContainSingle()
            .Which.Failure.Should().Be("reason=dependents-unsaved-changes");

        await using var verifyScope = services.CreateAsyncScope();
        var verify = verifyScope.ServiceProvider.GetRequiredService<TrackedGraphDbContext>();
        (await verify.Comments.CountAsync()).Should().Be(0);
        (await verify.Records.Select(record => record.Body).ToListAsync())
            .Should().BeEquivalentTo([TrackedGraphStep.AddingBody]);
    }

    [Fact]
    public async Task A_Live_Erasure_Whose_Step_Skips_A_Row_Is_Partially_Failed_And_Not_Complete()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var failingNoteId = Guid.NewGuid();
        var erasedNoteId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = failingNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = AsOf.AddDays(-2),
                    Body = SelectivelyFailingNoteStep.FailingBody,
                },
                new Note
                {
                    Id = erasedNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = AsOf.AddDays(-1),
                    Body = "erase-me",
                }
            );
            await db.SaveChangesAsync();
        }

        using var stepHost = new CohortTestHost(
            ConnectionString,
            configureServices: services =>
                services.AddRetentionDependents<Note, SelectivelyFailingNoteStep>(retiresReferencingRows: false)
        );

        var result = await stepHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true),
            AsOf
        );

        const string reason = "reason=rows-skipped;entityType=Cohort.Sample.Entities.Note;count=1";
        result.EntityFailures.Should().ContainSingle().Which.Should().Be(reason);
        result.Complete.Should().BeFalse();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
            .Which.Should().Match<EntitySweepCount>(count => count.Affected == 1 && count.SkippedCount == 1 && count.DeferredCount == 0);
        (await LoadRunStatusAndErrorAsync(ConnectionString, result.SweepId))
            .Should()
            .Be((SweepRunStatus.PartiallyFailed, reason));
        var skipped = (await LoadStepFailuresAsync(ConnectionString, result.SweepId)).Should().ContainSingle().Subject;
        skipped.RecordId.Should().Be(failingNoteId.ToString());
        skipped.Failure.Should().MatchRegex("^type=System\\.InvalidOperationException;code=hresult:0x[0-9A-F]{8};diagnosticId=[0-9a-f]{32}$");

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.Where(note => note.SubjectId == subjectId).Select(note => note.Id).ToListAsync())
            .Should()
            .Equal(failingNoteId);
    }

    [Fact]
    public async Task Sweep_Continues_Past_A_Batch_Of_Only_Failing_Step_Rows()
    {
        // Regression: with batch size 1, the oldest row's step failing produced a
        // zero-affected batch, which used to stop the entity's loop: every younger row
        // was starved on every sweep, forever.
        var tenantId = Guid.NewGuid();
        var failingNoteId = Guid.NewGuid();
        var healthyNoteId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = failingNoteId,
                    TenantId = tenantId,
                    CreatedAt = AsOf.AddDays(-300), // oldest: fills the whole first batch
                    Body = SelectivelyFailingNoteStep.FailingBody,
                },
                new Note
                {
                    Id = healthyNoteId,
                    TenantId = tenantId,
                    CreatedAt = AsOf.AddDays(-100),
                    Body = "behind-the-failure",
                }
            );
            await db.SaveChangesAsync();
        }

        using var stepHost = new CohortTestHost(
            ConnectionString,
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:SweepBatchSize"] = "1",
            },
            configureServices: services =>
                services.AddRetentionDependents<Note, SelectivelyFailingNoteStep>(retiresReferencingRows: false)
        );

        var result = await stepHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            AsOf
        );

        // The skipped row makes the run PartiallyFailed, with a plain reason naming the entity.
        result.EntityFailures.Should().ContainSingle().Which.Should().Be("reason=rows-skipped;entityType=Cohort.Sample.Entities.Note;count=1");
        (await LoadRunStatusAndErrorAsync(ConnectionString, result.SweepId))
            .Should()
            .Be((SweepRunStatus.PartiallyFailed, "reason=rows-skipped;entityType=Cohort.Sample.Entities.Note;count=1"));
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(Note),
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    1,
                    HeldCount: 0,
                    SkippedCount: 1,
                    NullAnchorCount: 0,
                    RetainedCount: 0,
                    DeferredCount: 0,
                    BlockedCount: 0
                )
            );

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == healthyNoteId)).Should().BeFalse();
        (await verify.Notes.AnyAsync(note => note.Id == failingNoteId)).Should().BeTrue();
    }

    [Fact]
    public async Task A_Steps_Own_Cancellation_Skips_Its_Row_Instead_Of_Failing_The_Run()
    {
        var tenantId = Guid.NewGuid();
        var synchronousId = Guid.NewGuid();
        var asynchronousId = Guid.NewGuid();
        var healthyId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = synchronousId, TenantId = tenantId, CreatedAt = AsOf.AddDays(-300), Body = TimingOutNoteStep.SynchronousBody },
                new Note { Id = asynchronousId, TenantId = tenantId, CreatedAt = AsOf.AddDays(-200), Body = TimingOutNoteStep.AsynchronousBody },
                new Note { Id = healthyId, TenantId = tenantId, CreatedAt = AsOf.AddDays(-100), Body = "healthy" }
            );
            await db.SaveChangesAsync();
        }

        using var stepHost = new CohortTestHost(
            ConnectionString,
            configureServices: services =>
                services.AddRetentionDependents<Note, TimingOutNoteStep>(retiresReferencingRows: false)
        );

        // An HttpClient timeout is a TaskCanceledException, but the sweep was not cancelled.
        var result = await stepHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            AsOf
        );

        result.EntityFailures.Should().ContainSingle().Which.Should().Be("reason=rows-skipped;entityType=Cohort.Sample.Entities.Note;count=2");
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
            .Which.Should().Match<EntitySweepCount>(count => count.Affected == 1 && count.SkippedCount == 2);
        (await LoadStepFailuresAsync(ConnectionString, result.SweepId))
            .Should().HaveCount(2)
            .And.OnlyContain(skipped => skipped.Failure.StartsWith("type=System.Threading.Tasks.TaskCanceledException;", StringComparison.Ordinal));

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.Where(note => note.TenantId == tenantId).Select(note => note.Id).ToListAsync())
            .Should().BeEquivalentTo([synchronousId, asynchronousId]);
    }

    [Fact]
    public async Task A_Live_Erasure_Runs_No_Step_For_Held_Or_Exempt_Rows()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var heldNoteId = Guid.NewGuid();
        var softDeleteId = Guid.NewGuid();
        var heldSoftDeleteId = Guid.NewGuid();
        var contactId = Guid.NewGuid();
        var heldContactId = Guid.NewGuid();
        var exemptTombstoneId = Guid.NewGuid();
        var recorder = new StepRecorder();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = noteId, TenantId = tenantId, SubjectId = subjectId, CreatedAt = AsOf.AddDays(-1), Body = "erase-note" },
                new Note { Id = heldNoteId, TenantId = tenantId, SubjectId = subjectId, CreatedAt = AsOf.AddDays(-1), Body = "held-note" }
            );
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord
                {
                    Id = softDeleteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = AsOf.AddDays(-1),
                    Body = "erase-soft-delete",
                },
                new SoftDeleteRecord
                {
                    Id = heldSoftDeleteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = AsOf.AddDays(-1),
                    Body = "held-soft-delete",
                }
            );
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = contactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = AsOf.AddDays(-1),
                    EmailAddress = "subject@example.com",
                    GivenName = "Target",
                    Surname = "Contact",
                    Notes = "keep-notes",
                },
                new AnonymisedContact
                {
                    Id = heldContactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = AsOf.AddDays(-1),
                    EmailAddress = "held@example.com",
                    GivenName = "Held",
                    Surname = "Contact",
                    Notes = "held-notes",
                }
            );
            db.TombstoneRecords.Add(
                new TombstoneRecord
                {
                    Id = exemptTombstoneId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = AsOf.AddDays(-1),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "exempt-tombstone",
                    ContactEmail = "tombstone@example.com",
                }
            );
            await db.SaveChangesAsync();
        }

        await CreateSampleHoldAsync("notes", heldNoteId, tenantId);
        await CreateSampleHoldAsync("soft_delete_records", heldSoftDeleteId, tenantId);
        await CreateSampleHoldAsync("anonymised_contacts", heldContactId, tenantId);

        using var stepHost = new CohortTestHost(
            ConnectionString,
            new ExemptTombstoneRuleProvider(),
            configureServices: services =>
            {
                services.AddSingleton(recorder);
                services.AddRetentionDependents<Note, RecordingErasureStep>(retiresReferencingRows: false);
                services.AddRetentionDependents<SoftDeleteRecord, RecordingErasureStep>(retiresReferencingRows: false);
                services.AddRetentionDependents<AnonymisedContact, RecordingErasureStep>(retiresReferencingRows: false);
                // A retained entity with a step, whose rule resolves Exempt.
                services.AddRetentionDependents<TombstoneRecord, RecordingErasureStep>(retiresReferencingRows: false);
            }
        );

        var result = await stepHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true),
            AsOf
        );

        // First, so a step run for a held or exempt row is what fails.
        recorder.Ran.Should().BeEquivalentTo([
            "note:erase-note",
            "soft:erase-soft-delete",
            "contact:subject@example.com",
        ]);
        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().Contain(new EntitySweepCount(
            typeof(Note), "short-lived", tenantId, Strategy.Purge, 1,
            HeldCount: 1, SkippedCount: 0, NullAnchorCount: 0, RetainedCount: 0, DeferredCount: 0, BlockedCount: 0
        ));
        result.Counts.Should().Contain(new EntitySweepCount(
            typeof(SoftDeleteRecord), "soft-delete", tenantId, Strategy.SoftDelete, 1,
            HeldCount: 1, SkippedCount: 0, NullAnchorCount: 0, RetainedCount: 0, DeferredCount: 0, BlockedCount: 0
        ));
        // Held counts are measured directly (subject-matching, past cutoff, actively held), so
        // the anonymise erase path reports the held contact too.
        result.Counts.Should().Contain(new EntitySweepCount(
            typeof(AnonymisedContact), "anonymise", tenantId, Strategy.Anonymise, 1,
            HeldCount: 1, SkippedCount: 0, NullAnchorCount: 0, RetainedCount: 0, DeferredCount: 0, BlockedCount: 0
        ));

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeFalse();
        (await verify.Notes.AnyAsync(note => note.Id == heldNoteId)).Should().BeTrue();
        var softDelete = await verify.SoftDeleteRecords.SingleAsync(record => record.Id == softDeleteId);
        softDelete.IsDeleted.Should().BeTrue();
        softDelete.DeletedAt.Should().Be(AsOf);
        (await verify.SoftDeleteRecords.SingleAsync(record => record.Id == heldSoftDeleteId)).IsDeleted.Should().BeFalse();
        var contact = await verify.AnonymisedContacts.SingleAsync(candidate => candidate.Id == contactId);
        contact.EmailAddress.Should().BeNull();
        contact.GivenName.Should().BeEmpty();
        contact.Surname.Should().Be("[redacted]");
        (await verify.AnonymisedContacts.SingleAsync(candidate => candidate.Id == heldContactId))
            .EmailAddress.Should().Be("held@example.com");
        var tombstone = await verify.TombstoneRecords.SingleAsync(record => record.Id == exemptTombstoneId);
        tombstone.DisplayName.Should().Be("exempt-tombstone");
        tombstone.ContactEmail.Should().Be("tombstone@example.com");
        tombstone.AnonymisedAt.Should().BeNull();
    }

    private const string UnsavedChanges = "^reason=dependents-unsaved-changes$";
    private const string ForeignKeyRefusal =
        "^type=Npgsql\\.PostgresException;code=sqlstate:23503;diagnosticId=[0-9a-f]{32}$";

    private static readonly Guid AuditEventEntityId = Guid.Parse("00000000-0000-0000-0003-000000000005");
    private static readonly Guid RetainedChildEntityId = Guid.Parse("00000000-0000-0000-0003-000000000003");

    private static void AssertSkippedWithReason(RetentionSweepResult result, Type entityType, int affected)
    {
        result.EntityFailures.Should().ContainSingle()
            .Which.Should().Be($"reason=rows-skipped;entityType={entityType.FullName};count=1");
        result.Counts.Should().ContainSingle(count => count.EntityType == entityType)
            .Which.Should().Match<EntitySweepCount>(count =>
                count.Affected == affected && count.SkippedCount == 1 && count.BlockedCount == 0 && count.DeferredCount == 0);
    }

    private static Task DeletePlainChildrenAsync(ADependentsParent row, DependentsDbContext db, CancellationToken ct) =>
        db.PlainChildren.Where(child => child.ParentId == row.Id).ExecuteDeleteAsync(ct);

    private static Task BlankAuditCopiesAsync(AAnonymisedParent row, DependentsDbContext db, CancellationToken ct) =>
        db.AuditEvents
            .Where(audit => audit.ParentId == row.Id)
            .ExecuteUpdateAsync(set => set.SetProperty(audit => audit.Copied, (string?)null), ct);

    private static ADependentsParent Parent(Guid id, Guid tenantId, DateTimeOffset createdAt, Guid? subjectId = null) =>
        new() { Id = id, TenantId = tenantId, CreatedAt = createdAt, SubjectId = subjectId };

    private static ZPlainChild PlainChild(Guid parentId) => new() { Id = Guid.NewGuid(), ParentId = parentId };

    private static ZRetainedChild RetainedChild(Guid parentId, Guid tenantId) =>
        new() { Id = Guid.NewGuid(), ParentId = parentId, TenantId = tenantId, CreatedAt = AsOf.AddDays(-1) };

    private static AAnonymisedParent AnonymisedParent(Guid id, Guid tenantId, DateTimeOffset createdAt) =>
        new() { Id = id, TenantId = tenantId, CreatedAt = createdAt, Name = "name" };

    private static ZParentAuditEvent AuditEvent(Guid id, Guid parentId, Guid tenantId, string copied) =>
        new() { Id = id, ParentId = parentId, TenantId = tenantId, CreatedAt = AsOf.AddDays(-1), Copied = copied };

    private static async Task SeedAsync(ServiceProvider services, Action<DependentsDbContext> seed)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DependentsDbContext>();
        await db.Database.EnsureCreatedAsync();
        seed(db);
        await db.SaveChangesAsync();
    }

    private static async Task<Guid> CreateHoldAsync(ServiceProvider services, Guid entityId, Guid recordId, Guid tenantId)
    {
        var holdId = Guid.NewGuid();
        await using var scope = services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
            new RetentionHoldRequest(holdId, entityId, recordId.ToString(), tenantId, "litigation", DateTimeOffset.UtcNow.AddMinutes(-1)),
            CancellationToken.None
        );
        return holdId;
    }

    private Task CreateSampleHoldAsync(string tableName, Guid recordId, Guid tenantId) =>
        Host.RunWithServicesAsync(services =>
            services.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    RetentionEntityIdentity.ForTable(tableName),
                    recordId.ToString(),
                    tenantId,
                    "step-erasure-hold",
                    AsOf.AddDays(-1)
                ),
                CancellationToken.None
            )
        );

    private static async Task<RetentionSweepResult> SweepAsync(ServiceProvider services, Guid tenantId)
    {
        await using var scope = services.CreateAsyncScope();
        return await scope.ServiceProvider
            .GetRequiredService<IRetentionSweep>()
            .ExecuteAsync(RetentionSweepRequest.Tenanted(new TenantContext(tenantId, "uk", new Dictionary<string, string>()), AsOf));
    }

    /// <summary>
    /// Waits until the step's UPDATE of the audit events is blocked by the hold's transaction
    /// (<paramref name="blockerBackendId"/>): a condition, polled, rather than a timed sleep.
    /// </summary>
    private static async Task WaitForBlockedStepAsync(string connectionString, int blockerBackendId)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (true)
        {
            var blocked = await ScalarAsync<bool>(
                connectionString,
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity waiter
                    WHERE @blockerBackendId = ANY(pg_blocking_pids(waiter.pid))
                      AND waiter.query LIKE '%dependents_audit_events%'
                      AND waiter.query LIKE '%UPDATE%'
                )
                """,
                ("blockerBackendId", blockerBackendId)
            );
            if (blocked)
            {
                return;
            }

            await Task.Delay(20, timeout.Token);
        }
    }

    private static async Task<long> CountAsync(string connectionString, string table) =>
        await ScalarAsync<long>(connectionString, $"SELECT count(*) FROM {table}");

    private static async Task<List<Guid>> LoadIdsAsync(string connectionString, string table) =>
        await QueryAsync(connectionString, $"SELECT \"Id\" FROM {table} ORDER BY \"Id\"", reader => reader.GetGuid(0));

    private static async Task<List<Guid>> LoadParentIdsOfPlainChildrenAsync(string connectionString) =>
        await QueryAsync(connectionString, "SELECT \"ParentId\" FROM dependents_plain_children ORDER BY \"ParentId\"", reader => reader.GetGuid(0));

    private static async Task<List<Guid>> LoadAnonymisedParentIdsAsync(string connectionString) =>
        await QueryAsync(
            connectionString,
            "SELECT \"Id\" FROM dependents_anonymised_parents WHERE \"AnonymisedAt\" IS NOT NULL ORDER BY \"Id\"",
            reader => reader.GetGuid(0)
        );

    private static async Task<List<(Guid ParentId, string? Copied)>> LoadAuditCopiesAsync(string connectionString) =>
        await QueryAsync(
            connectionString,
            "SELECT \"ParentId\", \"Copied\" FROM dependents_audit_events ORDER BY \"ParentId\"",
            reader => (reader.GetGuid(0), reader.IsDBNull(1) ? null : reader.GetString(1))
        );

    /// <summary>The rows a failing step skipped in the run: each one's detail, with why.</summary>
    private static async Task<List<(string RecordId, string Failure)>> LoadStepFailuresAsync(string connectionString, Guid sweepId) =>
        await QueryAsync(
            connectionString,
            """
            SELECT "RecordId", "Failure"
            FROM "sweep_run_row_detail"
            WHERE "SweepId" = @sweepId AND "Failure" IS NOT NULL
            """,
            reader => (reader.GetString(0), reader.GetString(1)),
            ("sweepId", sweepId)
        );

    private static async Task<List<(string RecordId, string? Failure)>> LoadRowDetailsAsync(
        string connectionString,
        Guid sweepId
    ) =>
        await QueryAsync(
            connectionString,
            "SELECT \"RecordId\", \"Failure\" FROM \"sweep_run_row_detail\" WHERE \"SweepId\" = @sweepId",
            reader => (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)),
            ("sweepId", sweepId)
        );

    private static async Task<SweepRunStatus> LoadRunStatusAsync(string connectionString, Guid sweepId) =>
        (SweepRunStatus)await ScalarAsync<int>(
            connectionString,
            "SELECT \"Status\" FROM \"sweep_run\" WHERE \"SweepId\" = @sweepId",
            ("sweepId", sweepId)
        );

    private static async Task<(SweepRunStatus Status, string? Error)> LoadRunStatusAndErrorAsync(string connectionString, Guid sweepId) =>
        (await QueryAsync(
            connectionString,
            "SELECT \"Status\", \"Error\" FROM \"sweep_run\" WHERE \"SweepId\" = @sweepId",
            reader => ((SweepRunStatus)reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetString(1)),
            ("sweepId", sweepId)
        )).Single();

    private static async Task<T> ScalarAsync<T>(string connectionString, string sql, params (string Name, object Value)[] parameters)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }
        return (T)(await command.ExecuteScalarAsync())!;
    }

    private static async Task<List<T>> QueryAsync<T>(
        string connectionString,
        string sql,
        Func<NpgsqlDataReader, T> read,
        params (string Name, object Value)[] parameters
    )
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }
        var rows = new List<T>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(read(reader));
        }
        return rows;
    }

    private static ServiceProvider BuildServiceProvider(
        string connectionString,
        StepScript script,
        bool parentRetiresReferencingRows = true,
        Action<IServiceCollection>? configure = null
    )
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddLogging();
        services.AddDbContext<DependentsDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(new DependentsRuleProvider());
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddSingleton(script);
        services.AddCohort<DependentsDbContext>();
        services.AddRetentionDependents<ADependentsParent, ParentStep>(parentRetiresReferencingRows);
        services.AddRetentionDependents<AAnonymisedParent, AnonymisedParentStep>();

        configure?.Invoke(services);
        return services.BuildServiceProvider(validateScopes: true);
    }

    private static ServiceProvider BuildTrackedGraphServices(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddLogging();
        services.AddDbContext<TrackedGraphDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(new DependentsRuleProvider());
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddSingleton<TrackedGraphProbe>();
        services.AddCohort<TrackedGraphDbContext>();
        services.AddRetentionDependents<TrackedGraphRecord, TrackedGraphStep>(retiresReferencingRows: false);
        return services.BuildServiceProvider(validateScopes: true);
    }

    private static async Task SeedTrackedGraphAsync(
        ServiceProvider services,
        Guid tenantId,
        Guid? subjectId,
        params (string Body, DateTimeOffset CreatedAt)[] records
    )
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TrackedGraphDbContext>();
        await db.Database.EnsureCreatedAsync();
        db.Folders.Add(new TrackedGraphFolder { Id = Guid.NewGuid() });
        db.Records.AddRange(
            records.Select(record => new TrackedGraphRecord
            {
                Id = Guid.NewGuid(),
                TenantId = tenantId,
                SubjectId = subjectId,
                CreatedAt = record.CreatedAt,
                Body = record.Body,
            })
        );
        await db.SaveChangesAsync();
    }

    /// <summary>
    /// What the caller's context might track that a step could change: a record that isn't
    /// the subject's, a comment in the folder and a tag not yet on it.
    /// </summary>
    private static async Task<Guid> SeedTrackedGraphBystandersAsync(ServiceProvider services, Guid tenantId)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TrackedGraphDbContext>();
        var bystander = new TrackedGraphRecord
        {
            Id = Guid.NewGuid(),
            TenantId = tenantId,
            CreatedAt = DateTimeOffset.UtcNow,
            Body = TrackedGraphStep.BystanderBody,
        };
        db.Records.Add(bystander);
        db.Comments.Add(new TrackedGraphComment { Id = Guid.NewGuid(), FolderId = (await db.Folders.SingleAsync()).Id });
        db.Tags.Add(new TrackedGraphTag { Id = Guid.NewGuid() });
        await db.SaveChangesAsync();
        return bystander.Id;
    }

    /// <summary>What each test's steps do, and what they saw.</summary>
    private sealed class StepScript
    {
        private int calls;

        public Func<ADependentsParent, DependentsDbContext, CancellationToken, Task> Parent { get; init; } =
            (_, _, _) => Task.CompletedTask;

        public Func<AAnonymisedParent, DependentsDbContext, CancellationToken, Task> Anonymised { get; init; } =
            (_, _, _) => Task.CompletedTask;

        public int Calls => Volatile.Read(ref calls);

        public ConcurrentQueue<DbContext> Contexts { get; } = new();

        public ConcurrentQueue<RetentionDependentsContext> RunContexts { get; } = new();

        public void Called(DbContext db, RetentionDependentsContext ctx)
        {
            Interlocked.Increment(ref calls);
            Contexts.Enqueue(db);
            RunContexts.Enqueue(ctx);
        }
    }

    private sealed class ParentStep(DependentsDbContext db, StepScript script) : IRetentionDependents<ADependentsParent>
    {
        public Task RetireAsync(ADependentsParent row, RetentionDependentsContext ctx, CancellationToken ct)
        {
            script.Called(db, ctx);
            return script.Parent(row, db, ct);
        }
    }

    private sealed class AnonymisedParentStep(DependentsDbContext db, StepScript script) : IRetentionDependents<AAnonymisedParent>
    {
        public Task RetireAsync(AAnonymisedParent row, RetentionDependentsContext ctx, CancellationToken ct)
        {
            script.Called(db, ctx);
            return script.Anonymised(row, db, ct);
        }
    }

    private sealed class SelectivelyFailingNoteStep : IRetentionDependents<Note>
    {
        public const string FailingBody = "fail-before-delete";

        public Task RetireAsync(Note row, RetentionDependentsContext ctx, CancellationToken ct) =>
            string.Equals(row.Body, FailingBody, StringComparison.Ordinal)
                ? throw new InvalidOperationException("Simulated step failure.")
                : Task.CompletedTask;
    }

    /// <summary>Throws its own cancellation, as an HttpClient timeout does, chosen by the row's body.</summary>
    private sealed class TimingOutNoteStep : IRetentionDependents<Note>
    {
        public const string SynchronousBody = "time-out-synchronously";
        public const string AsynchronousBody = "time-out-asynchronously";

        public Task RetireAsync(Note row, RetentionDependentsContext ctx, CancellationToken ct)
        {
            if (string.Equals(row.Body, SynchronousBody, StringComparison.Ordinal))
            {
                throw new TaskCanceledException("Simulated HttpClient timeout.");
            }

            return string.Equals(row.Body, AsynchronousBody, StringComparison.Ordinal)
                ? TimeOutAsync()
                : Task.CompletedTask;
        }

        private static async Task TimeOutAsync()
        {
            await Task.Yield();
            throw new TaskCanceledException("Simulated HttpClient timeout.");
        }
    }

    /// <summary>Every row a <see cref="RecordingErasureStep"/> ran for.</summary>
    private sealed class StepRecorder
    {
        private readonly ConcurrentQueue<string> ran = new();

        public IReadOnlyList<string> Ran => ran.ToArray();

        public void Record(string row) => ran.Enqueue(row);
    }

    /// <summary>The step of several sample entities, recording each row it runs for.</summary>
    private sealed class RecordingErasureStep(StepRecorder recorder)
        : IRetentionDependents<Note>,
            IRetentionDependents<SoftDeleteRecord>,
            IRetentionDependents<AnonymisedContact>,
            IRetentionDependents<TombstoneRecord>
    {
        public Task RetireAsync(Note row, RetentionDependentsContext ctx, CancellationToken ct) =>
            Record($"note:{row.Body}");

        public Task RetireAsync(SoftDeleteRecord row, RetentionDependentsContext ctx, CancellationToken ct) =>
            Record($"soft:{row.Body}");

        public Task RetireAsync(AnonymisedContact row, RetentionDependentsContext ctx, CancellationToken ct) =>
            Record($"contact:{row.EmailAddress}");

        public Task RetireAsync(TombstoneRecord row, RetentionDependentsContext ctx, CancellationToken ct) =>
            Record($"tombstone:{row.DisplayName}");

        private Task Record(string row)
        {
            recorder.Record(row);
            return Task.CompletedTask;
        }
    }

    /// <summary>The sample's rules, except that tombstone records are exempt.</summary>
    private sealed class ExemptTombstoneRuleProvider : IRetentionRuleProvider
    {
        private const string TombstoneCategory = "tombstone-anonymise";
        private readonly SampleRetentionRuleProvider sample = new();

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == TombstoneCategory ? new([Strategy.Exempt]) : sample.GetCapabilities(category);

        public Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct) =>
            context.Category == TombstoneCategory
                ? Task.FromResult<RetentionRule?>(new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Exempt))
                : sample.ResolveAsync(context, ct);
    }

    private sealed class DependentsRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) => category switch
        {
            "dependents-purge" or "dependents-kept" or "tracked-graph" => new([Strategy.Purge]),
            "dependents-anonymise" or "dependents-audit" => new([Strategy.Anonymise]),
            _ => null,
        };

        public Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct) =>
            Task.FromResult<RetentionRule?>(context.Category switch
            {
                // Per-row audit, so the tests pin who writes each mutated row's detail.
                "dependents-purge" => new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge, AuditRowDetail: AuditRowDetail.PerRow),
                "dependents-kept" => new RetentionRule(new RetentionPeriod(Days: 3650), Strategy.Purge),
                "dependents-anonymise" => new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Anonymise),
                "dependents-audit" => new RetentionRule(new RetentionPeriod(Days: 3650), Strategy.Anonymise),
                "tracked-graph" => new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge),
                _ => null,
            });
    }

    private sealed class DependentsDbContext(DbContextOptions<DependentsDbContext> options) : DbContext(options)
    {
        public DbSet<ADependentsParent> Parents => Set<ADependentsParent>();
        public DbSet<ZPlainChild> PlainChildren => Set<ZPlainChild>();
        public DbSet<ZRetainedChild> RetainedChildren => Set<ZRetainedChild>();
        public DbSet<AAnonymisedParent> AnonymisedParents => Set<AAnonymisedParent>();
        public DbSet<ZParentAuditEvent> AuditEvents => Set<ZParentAuditEvent>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ADependentsParent>(entity =>
            {
                entity.ToTable("dependents_parents");
                entity.HasKey(parent => parent.Id);
            });
            // A host table Cohort doesn't retain, still holding its parent.
            modelBuilder.Entity<ZPlainChild>(entity =>
            {
                entity.ToTable("dependents_plain_children");
                entity.HasKey(child => child.Id);
                entity.HasOne<ADependentsParent>().WithMany().HasForeignKey(child => child.ParentId).OnDelete(DeleteBehavior.Restrict);
            });
            modelBuilder.Entity<ZRetainedChild>(entity =>
            {
                entity.ToTable("dependents_retained_children");
                entity.HasKey(child => child.Id);
                entity.HasOne<ADependentsParent>().WithMany().HasForeignKey(child => child.ParentId).OnDelete(DeleteBehavior.Restrict);
            });
            modelBuilder.Entity<AAnonymisedParent>(entity =>
            {
                entity.ToTable("dependents_anonymised_parents");
                entity.HasKey(parent => parent.Id);
            });
            modelBuilder.Entity<ZParentAuditEvent>(entity =>
            {
                entity.ToTable("dependents_audit_events");
                entity.HasKey(audit => audit.Id);
                entity.HasOne<AAnonymisedParent>().WithMany().HasForeignKey(audit => audit.ParentId).OnDelete(DeleteBehavior.Restrict);
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("dependents-purge", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0003-000000000001")]
    private sealed class ADependentsParent
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [ErasureSubject("person")]
        public Guid? SubjectId { get; init; }
    }

    private sealed class ZPlainChild
    {
        public Guid Id { get; init; }
        public Guid ParentId { get; init; }
    }

    [Retain("dependents-kept", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0003-000000000003")]
    private sealed class ZRetainedChild
    {
        public Guid Id { get; init; }
        public Guid ParentId { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("dependents-anonymise", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0003-000000000004")]
    private sealed class AAnonymisedParent
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.Null)]
        public string? Name { get; init; }

        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    /// <summary>Copies its parent's values, as Casebridge's case audit events do.</summary>
    [Retain("dependents-audit", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0003-000000000005")]
    private sealed class ZParentAuditEvent
    {
        public Guid Id { get; init; }
        public Guid ParentId { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.Null)]
        public string? Copied { get; init; }

        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    private sealed class TrackedGraphDbContext(DbContextOptions<TrackedGraphDbContext> options)
        : DbContext(options)
    {
        public DbSet<TrackedGraphRecord> Records => Set<TrackedGraphRecord>();
        public DbSet<TrackedGraphFolder> Folders => Set<TrackedGraphFolder>();
        public DbSet<TrackedGraphComment> Comments => Set<TrackedGraphComment>();
        public DbSet<TrackedGraphTag> Tags => Set<TrackedGraphTag>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TrackedGraphRecord>(entity =>
            {
                entity.ToTable("tracked_graph_records");
                entity.HasKey(record => record.Id);
            });
            modelBuilder.Entity<TrackedGraphFolder>(entity =>
            {
                entity.ToTable("tracked_graph_folders");
                entity.HasKey(folder => folder.Id);
                entity.HasMany(folder => folder.Comments).WithOne().HasForeignKey(comment => comment.FolderId);
                entity.HasMany(folder => folder.Tags).WithMany().UsingEntity(join => join.ToTable("tracked_graph_folder_tags"));
            });
            modelBuilder.Entity<TrackedGraphTag>(entity =>
            {
                entity.ToTable("tracked_graph_tags");
                entity.HasKey(tag => tag.Id);
            });
            modelBuilder.Entity<TrackedGraphComment>(entity =>
            {
                entity.ToTable("tracked_graph_comments");
                entity.HasKey(comment => comment.Id);
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("tracked-graph", nameof(CreatedAt))]
    [RetentionEntityId("6d0f3a52-9a8e-4d1b-8f3c-2b7e5a1c9d40")]
    private sealed class TrackedGraphRecord
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; init; }

        [ErasureSubject("person")]
        public Guid? SubjectId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public string Body { get; set; } = "";
    }

    /// <summary>Unretained, so only a step's change can touch it.</summary>
    private sealed class TrackedGraphFolder
    {
        public Guid Id { get; init; }
        public List<TrackedGraphComment> Comments { get; init; } = [];
        public List<TrackedGraphTag> Tags { get; init; } = [];
    }

    private sealed class TrackedGraphTag
    {
        public Guid Id { get; init; }
    }

    private sealed class TrackedGraphComment
    {
        public Guid Id { get; init; }
        public Guid FolderId { get; init; }
    }

    private sealed class TrackedGraphProbe
    {
        public List<bool> SawOwnRow { get; } = [];

        public List<DbContext> Contexts { get; } = [];
    }

    /// <summary>
    /// Changes its own context in one way, chosen by the row's body, and leaves the change
    /// unsaved or saves it where the database refuses it: the kinds of change that once leaked
    /// into the caller's context.
    /// </summary>
    private sealed class TrackedGraphStep(TrackedGraphDbContext db, TrackedGraphProbe probe)
        : IRetentionDependents<TrackedGraphRecord>
    {
        public const string BystanderBody = "bystander";
        public const string StagingBody = "stage-before-delete";
        public const string FailedSaveBody = "save-before-delete";
        public const string KeyChangingBody = "change-key-before-delete";
        public const string EditingBody = "edit-before-delete";
        public const string UndetectedEditBody = "edit-undetected-before-delete";
        public const string AddingBody = "add-comment-before-delete";
        public const string ManyToManyBody = "tag-folder-before-delete";
        public const string TrackingReadBody = "read-tracking-before-delete";
        public const string ClearingBody = "clear-before-delete";
        public const string ReadingOwnRowBody = "read-own-row-before-delete";

        public async Task RetireAsync(TrackedGraphRecord row, RetentionDependentsContext ctx, CancellationToken ct)
        {
            switch (row.Body)
            {
                case StagingBody:
                    db.Records.Add(new TrackedGraphRecord { Id = Guid.NewGuid(), TenantId = row.TenantId, Body = "staged-by-step" });
                    break;
                case FailedSaveBody:
                    // No such folder: the save is refused (23503) and leaves its entry Added.
                    db.Comments.Add(new TrackedGraphComment { Id = Guid.NewGuid(), FolderId = Guid.NewGuid() });
                    await db.SaveChangesAsync(ct);
                    break;
                case KeyChangingBody:
                    (await BystanderAsync(ct)).Id = Guid.NewGuid();
                    break;
                case EditingBody:
                    (await BystanderAsync(ct)).Body = "overwritten-by-step";
                    break;
                case UndetectedEditBody:
                    db.ChangeTracker.AutoDetectChangesEnabled = false;
                    (await BystanderAsync(ct)).Body = "overwritten-by-step";
                    break;
                case AddingBody:
                    (await db.Folders.SingleAsync(ct)).Comments.Add(new TrackedGraphComment { Id = Guid.NewGuid() });
                    break;
                case ManyToManyBody:
                    var folder = await db.Folders.Include(item => item.Tags).SingleAsync(ct);
                    folder.Tags.Add(await db.Tags.SingleAsync(ct));
                    break;
                case TrackingReadBody:
                    await db.Comments.SingleAsync(ct);
                    break;
                case ClearingBody:
                    await BystanderAsync(ct);
                    db.ChangeTracker.Clear();
                    break;
                case ReadingOwnRowBody:
                    probe.SawOwnRow.Add(await db.Records.AsNoTracking().AnyAsync(record => record.Id == row.Id, ct));
                    probe.Contexts.Add(db);
                    break;
            }
        }

        private Task<TrackedGraphRecord> BystanderAsync(CancellationToken ct) =>
            db.Records.SingleAsync(record => record.Body == BystanderBody, ct);
    }

    private sealed record LogEntry(LogLevel Level, string Message);

    private sealed class RecordingLogProvider : ILoggerProvider
    {
        private readonly ConcurrentQueue<LogEntry> entries = new();

        public IReadOnlyList<LogEntry> Entries => entries.ToArray();

        public ILogger CreateLogger(string categoryName) => new RecordingLogger(entries);

        public void Dispose() { }

        private sealed class RecordingLogger(ConcurrentQueue<LogEntry> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter
            ) => entries.Enqueue(new LogEntry(logLevel, formatter(state, exception)));
        }
    }
}
