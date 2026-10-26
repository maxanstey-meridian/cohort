using System.Collections.Concurrent;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Handlers;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Npgsql;

namespace Cohort.Sample.Tests;

// ─── End-to-end test (copied from RestrictiveForeignKeySweepEndToEndTests) ───
//
// A dependents step retires what depends on a row in the row's own transaction, before Cohort
// mutates it, under the mutation's savepoint and the hold guard. Each test builds a host on
// its own database with a test-local model: a Purge parent referenced through RESTRICT keys by
// an unretained and a retained child, and an Anonymise parent whose retained audit events
// copy its values, as Casebridge's do.
// ────────────────────────────────────────────────────────────────────────────

public sealed class RetentionDependentsEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    private const string StepId = "4a454132-b95e-4435-8616-5f52789c5420";
    private const string AnonymisedStepId = "3ee2fa4e-4d4f-4e6b-bd27-f6aefdb8e40f";
    private const string SharedId = "7c0d6b2e-5f8a-4e1d-9b3c-2a6f4e8d1c05";
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
        // Per-row audit: the engine writes one detail per mutated row when no handler does.
        (await LoadRowDetailsAsync(database.ConnectionString, result.SweepId)).Select(detail => detail.RecordId)
            .Should().BeEquivalentTo([referencedId.ToString(), plainId.ToString()]);
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
        (await LoadStepStatusesAsync(database.ConnectionString, result.SweepId)).Should().BeEmpty();
        script.Calls.Should().Be(1);
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
        var status = (await LoadStepStatusesAsync(database.ConnectionString, result.SweepId)).Should().ContainSingle().Subject;
        status.RecordId.Should().Be(failingId.ToString());
        status.HandlerType.Should().Be(StepId);
        status.State.Should().Be(SweepRowHandlerDispatchState.DeadLettered);
        status.LastError.Should().MatchRegex(expectedError);
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
        var status = (await LoadStepStatusesAsync(database.ConnectionString, result.SweepId)).Should().ContainSingle().Subject;
        status.RecordId.Should().Be(heldParentId.ToString());
        status.HandlerType.Should().Be(AnonymisedStepId);
        status.LastError.Should().Be("reason=dependents-changed-held-row");
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
        (await LoadStepStatusesAsync(database.ConnectionString, result.SweepId)).Should().ContainSingle()
            .Which.LastError.Should().Be("reason=dependents-changed-held-row");
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
        (await LoadStepStatusesAsync(database.ConnectionString, result.SweepId)).Should().ContainSingle()
            .Which.LastError.Should().Be("reason=dependents-changed-held-row");
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
        (await LoadStepStatusesAsync(database.ConnectionString, result.SweepId)).Should().ContainSingle()
            .Which.LastError.Should().Be("reason=dependents-changed-row");
        // The step's own change was rolled back with it.
        (await LoadAnonymisedParentIdsAsync(database.ConnectionString)).Should().BeEmpty();
        (await ScalarAsync<string>(database.ConnectionString, "SELECT \"Name\" FROM dependents_anonymised_parents")).Should().Be("name");
    }

    [Fact]
    public async Task Preview_And_Dry_Run_Count_Referenced_Rows_Of_A_Step_Entity_As_Eligible_And_Never_Run_It()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var script = new StepScript { Parent = DeletePlainChildrenAsync };
        await using var services = BuildServiceProvider(database.ConnectionString, script);
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

        // The prediction: the step clears the references, so nothing is blocked up front.
        foreach (var counts in new[] { preview.Counts, dryRun.Counts, dryRunErasure.Counts })
        {
            counts.Should().ContainSingle(count => count.EntityType == typeof(ADependentsParent))
                .Which.Should().Match<EntitySweepCount>(count => count.Affected == 2 && count.BlockedCount == 0);
        }
        dryRunErasure.Complete.Should().BeTrue();
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

    [Fact]
    public async Task An_Entity_With_Handlers_And_A_Step_Captures_Before_The_Step_Retires()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var script = new StepScript { Parent = DeletePlainChildrenAsync };
        await using var services = BuildServiceProvider(
            database.ConnectionString,
            script,
            configure: services => services.AddRowHandler<ADependentsParent, CapturingParentHandler>()
        );
        var tenantId = Guid.NewGuid();
        var parentId = Guid.NewGuid();
        var child = PlainChild(parentId);

        await SeedAsync(services, db =>
        {
            db.Parents.Add(Parent(parentId, tenantId, AsOf.AddDays(-200)));
            db.PlainChildren.Add(child);
        });

        var result = await SweepAsync(services, tenantId);
        // The handlers' detail is the row's only one, and carries their payload until delivered.
        var detail = (await LoadRowDetailsAsync(database.ConnectionString, result.SweepId)).Should().ContainSingle().Subject;
        detail.RecordId.Should().Be(parentId.ToString());
        detail.CapturedPayload.Should().NotBeNull();
        var flush = await services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync();

        result.EntityFailures.Should().BeEmpty();
        flush.Settled.Should().BeTrue();
        // OnBefore saw the dependent the step then retired, and OnAfter got what it captured.
        script.Delivered.Should().Equal(child.Id.ToString());
        (await CountAsync(database.ConnectionString, "dependents_parents")).Should().Be(0);
        (await CountAsync(database.ConnectionString, "dependents_plain_children")).Should().Be(0);
    }

    [Fact]
    public async Task Requeuing_A_Shared_Id_Revives_Its_Handler_Work_And_Leaves_Its_Step_Failure_Dead_Lettered()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var script = new StepScript { Anonymised = (_, _, _) => throw new InvalidOperationException("the step failed") };
        await using var services = BuildServiceProvider(
            database.ConnectionString,
            script,
            anonymisedStep: false,
            configure: services =>
            {
                services.AddRowHandler<ADependentsParent, SharedIdWork>();
                services.AddRetentionDependents<AAnonymisedParent, SharedIdWork>();
            }
        );
        var tenantId = Guid.NewGuid();
        var parentId = Guid.NewGuid();
        var anonymisedId = Guid.NewGuid();

        await SeedAsync(services, db =>
        {
            db.Parents.Add(Parent(parentId, tenantId, AsOf.AddDays(-200)));
            db.AnonymisedParents.Add(AnonymisedParent(anonymisedId, tenantId, AsOf.AddDays(-200)));
        });

        var result = await SweepAsync(services, tenantId);
        var dispatcher = services.GetRequiredService<IRetentionRowDispatcher>();
        await dispatcher.FlushAsync();
        (await LoadStepStatusesAsync(database.ConnectionString, result.SweepId))
            .Should().OnlyContain(status => status.HandlerType == SharedId && status.State == SweepRowHandlerDispatchState.DeadLettered)
            .And.HaveCount(2);

        // A step failure has nothing to deliver, so only the handler's work comes back.
        (await dispatcher.RequeueDeadLetteredAsync(handlerId: Guid.Parse(SharedId))).Should().Be(1);

        var statuses = await LoadStepStatusesAsync(database.ConnectionString, result.SweepId);
        statuses.Should().ContainSingle(status => status.RecordId == parentId.ToString())
            .Which.State.Should().Be(SweepRowHandlerDispatchState.Pending);
        statuses.Should().ContainSingle(status => status.RecordId == anonymisedId.ToString())
            .Which.State.Should().Be(SweepRowHandlerDispatchState.DeadLettered);
    }

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

    private static async Task<List<StepStatus>> LoadStepStatusesAsync(string connectionString, Guid sweepId) =>
        await QueryAsync(
            connectionString,
            """
            SELECT detail."RecordId", status."HandlerType", status."State", status."LastError"
            FROM "sweep_row_handler_status" AS status
            INNER JOIN "sweep_run_row_detail" AS detail ON detail."Id" = status."SweepRunRowDetailId"
            WHERE detail."SweepId" = @sweepId
            """,
            reader => new StepStatus(
                reader.GetString(0),
                reader.GetString(1),
                (SweepRowHandlerDispatchState)reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetString(3)
            ),
            ("sweepId", sweepId)
        );

    private static async Task<List<(string RecordId, string? CapturedPayload)>> LoadRowDetailsAsync(
        string connectionString,
        Guid sweepId
    ) =>
        await QueryAsync(
            connectionString,
            "SELECT \"RecordId\", \"CapturedPayload\" FROM \"sweep_run_row_detail\" WHERE \"SweepId\" = @sweepId",
            reader => (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)),
            ("sweepId", sweepId)
        );

    private static async Task<SweepRunStatus> LoadRunStatusAsync(string connectionString, Guid sweepId) =>
        (SweepRunStatus)await ScalarAsync<int>(
            connectionString,
            "SELECT \"Status\" FROM \"sweep_run\" WHERE \"SweepId\" = @sweepId",
            ("sweepId", sweepId)
        );

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

    private sealed record StepStatus(string RecordId, string HandlerType, SweepRowHandlerDispatchState State, string? LastError);

    private static ServiceProvider BuildServiceProvider(
        string connectionString,
        StepScript script,
        bool anonymisedStep = true,
        Action<IServiceCollection>? configure = null
    )
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(
            new ConfigurationBuilder()
                .AddInMemoryCollection(
                    new Dictionary<string, string?> { [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "1" }
                )
                .Build()
        );
        services.AddLogging();
        services.AddDbContext<DependentsDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(new DependentsRuleProvider());
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddSingleton(script);
        services.AddCohort<DependentsDbContext>();
        services.AddRetentionDependents<ADependentsParent, ParentStep>();
        if (anonymisedStep)
        {
            services.AddRetentionDependents<AAnonymisedParent, AnonymisedParentStep>();
        }

        configure?.Invoke(services);
        return services.BuildServiceProvider(validateScopes: true);
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

        public ConcurrentQueue<string> Delivered { get; } = new();

        public void Called(DbContext db)
        {
            Interlocked.Increment(ref calls);
            Contexts.Enqueue(db);
        }
    }

    [RowHandlerId(StepId)]
    private sealed class ParentStep(DependentsDbContext db, StepScript script) : IRetentionDependents<ADependentsParent>
    {
        public Task RetireAsync(ADependentsParent row, RetentionDependentsContext ctx, CancellationToken ct)
        {
            script.Called(db);
            return script.Parent(row, db, ct);
        }
    }

    [RowHandlerId(AnonymisedStepId)]
    private sealed class AnonymisedParentStep(DependentsDbContext db, StepScript script) : IRetentionDependents<AAnonymisedParent>
    {
        public Task RetireAsync(AAnonymisedParent row, RetentionDependentsContext ctx, CancellationToken ct)
        {
            script.Called(db);
            return script.Anonymised(row, db, ct);
        }
    }

    [RowHandlerId("059a3eba-c75c-4e68-aa54-d26725fafabc")]
    private sealed class CapturingParentHandler(DependentsDbContext db, StepScript script) : IRetentionHandler<ADependentsParent>
    {
        public async Task OnBeforeAsync(ADependentsParent row, RetentionBeforeContext ctx, CancellationToken ct)
        {
            ctx.Snapshot["children"] = await db.PlainChildren
                .Where(child => child.ParentId == row.Id)
                .Select(child => child.Id.ToString())
                .ToArrayAsync(ct);
        }

        public Task OnAfterAsync(RetentionAfterContext<ADependentsParent> ctx, CancellationToken ct)
        {
            foreach (var child in (string[])ctx.Snapshot["children"]!)
            {
                script.Delivered.Enqueue(child);
            }

            return Task.CompletedTask;
        }
    }

    /// <summary>One class, one id: the handler of one entity and the step of another.</summary>
    [RowHandlerId(SharedId)]
    private sealed class SharedIdWork(DependentsDbContext db, StepScript script)
        : IRetentionHandler<ADependentsParent>, IRetentionDependents<AAnonymisedParent>
    {
        public Task OnBeforeAsync(ADependentsParent row, RetentionBeforeContext ctx, CancellationToken ct) => Task.CompletedTask;

        public Task OnAfterAsync(RetentionAfterContext<ADependentsParent> ctx, CancellationToken ct) =>
            throw new InvalidOperationException("the handler failed");

        public Task RetireAsync(AAnonymisedParent row, RetentionDependentsContext ctx, CancellationToken ct)
        {
            script.Called(db);
            return script.Anonymised(row, db, ct);
        }
    }

    private sealed class DependentsRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) => category switch
        {
            "dependents-purge" or "dependents-kept" => new([Strategy.Purge]),
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
