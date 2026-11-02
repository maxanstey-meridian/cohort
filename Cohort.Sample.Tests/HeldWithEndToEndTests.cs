using System.Data;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Holds;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using Npgsql;

namespace Cohort.Sample.Tests;

// ─── End-to-end test (copied from PurgeSweepStrategyTests) ──────────────────
//
// A hold on a row also holds the rows related to it. Each test builds a host on its own
// database with a test-local model: a Case, the Submission a Case came from (shared by two
// Cases, as in Casebridge), the Case's links, and the Submission's uploads. The entities have no
// database foreign keys, because a relation doesn't need one (Cohort joins on the link column and
// the tenant itself) and this keeps foreign-key blocking and plan order out of the hold
// assertions. Every row is due: all are in one Purge category, 30 days, anchored on CreatedAt.
//
// The races pause hold creation at its INSERT, after it has row-locked every row it will cover,
// then run the work that races it (following RaceHoldAgainstMutationAsync in
// RetentionHoldsEndToEndTests, with a waiter for row locks instead of advisory ones).
// ────────────────────────────────────────────────────────────────────────────

[Collection("Integration")]
public sealed class HeldWithEndToEndTests(PostgresFixture fixture)
{
    private static readonly DateTimeOffset AsOf = new(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

    private static readonly Guid TenantOne = Guid.Parse("10000000-0000-0000-0000-000000000001");
    private static readonly Guid TenantTwo = Guid.Parse("20000000-0000-0000-0000-000000000001");
    private static readonly Guid Person = Guid.Parse("30000000-0000-0000-0000-000000000001");

    // Tenant one: two Cases that share one Submission, which has one upload. One link per case,
    // a link that points at nothing, and a link that points at the other tenant's Case.
    private static readonly Guid CaseOne = Guid.Parse("11000000-0000-0000-0000-000000000001");
    private static readonly Guid CaseTwo = Guid.Parse("11000000-0000-0000-0000-000000000002");
    private static readonly Guid Submission = Guid.Parse("12000000-0000-0000-0000-000000000001");
    private static readonly Guid Upload = Guid.Parse("13000000-0000-0000-0000-000000000001");
    // Rows the dependents-step theory adds: one the step inserts, one it re-points.
    private static readonly Guid InsertedUpload = Guid.Parse("13000000-0000-0000-0000-000000000003");
    private static readonly Guid UncoveredUpload = Guid.Parse("13000000-0000-0000-0000-000000000002");
    private static readonly Guid LinkOne = Guid.Parse("14000000-0000-0000-0000-000000000001");
    private static readonly Guid LinkTwo = Guid.Parse("14000000-0000-0000-0000-000000000002");
    private static readonly Guid LinkNull = Guid.Parse("14000000-0000-0000-0000-000000000003");
    private static readonly Guid LinkCross = Guid.Parse("14000000-0000-0000-0000-000000000004");

    // Tenant two has its own chain; record ids are unique per table, so its ids differ.
    private static readonly Guid OtherCase = Guid.Parse("21000000-0000-0000-0000-000000000001");
    private static readonly Guid OtherSubmission = Guid.Parse("22000000-0000-0000-0000-000000000001");
    private static readonly Guid OtherUpload = Guid.Parse("23000000-0000-0000-0000-000000000001");

    // The other tenant's upload that points at this tenant's Submission: a relation joins rows of
    // one tenant, so the hold on this tenant's Case doesn't cover it.
    private static readonly Guid CrossTenantUpload = Guid.Parse("23000000-0000-0000-0000-000000000002");
    private static readonly Guid OtherLink = Guid.Parse("24000000-0000-0000-0000-000000000001");

    // The uploads the diamond tests add under tenant one: both reached by a hold on CaseOne, one
    // by the Case directly and through the shared Submission (BothUpload), one by the Case only
    // (DirectUpload). Created so the sweep order is Upload, BothUpload, DirectUpload.
    private static readonly Guid BothUpload = Guid.Parse("13000000-0000-0000-0000-000000000004");
    private static readonly Guid DirectUpload = Guid.Parse("13000000-0000-0000-0000-000000000005");

    // Tenantless pair: a holder, and a child linked to it, one linked to the other holder, and one
    // linked to nothing.
    private static readonly Guid HolderOne = Guid.Parse("41000000-0000-0000-0000-000000000001");
    private static readonly Guid HolderTwo = Guid.Parse("41000000-0000-0000-0000-000000000002");
    private static readonly Guid ChildOne = Guid.Parse("42000000-0000-0000-0000-000000000001");
    private static readonly Guid ChildTwo = Guid.Parse("42000000-0000-0000-0000-000000000002");
    private static readonly Guid ChildNull = Guid.Parse("42000000-0000-0000-0000-000000000003");

    private static readonly TenantContext TenantOneContext = new(TenantOne, "uk", new Dictionary<string, string>());

    [Fact]
    public async Task A_Hold_On_One_Of_Two_Cases_Sharing_A_Submission_Holds_Its_Links_The_Submission_And_Its_Uploads()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        await HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne);
        await HoldAsync(services, EntityId<ACase>(), OtherCase, TenantTwo);

        // Preview and the sweep judge coverage the same way.
        var preview = await PreviewAsync(services);
        var result = await SweepAsync(services);

        foreach (var counts in new[] { preview, result })
        {
            CountOf<ACase>(counts).HeldCount.Should().Be(1);
            CountOf<ASubmission>(counts).HeldCount.Should().Be(1);
            CountOf<ZCaseLink>(counts).HeldCount.Should().Be(1);
            CountOf<ZUpload>(counts).HeldCount.Should().Be(1);
        }

        // The other Case, and every link that isn't on the held one, is swept: a NULL link, a link
        // to the second Case, and a link to the other tenant's held Case.
        CountOf<ACase>(result).Affected.Should().Be(1);
        CountOf<ZCaseLink>(result).Affected.Should().Be(3);
        CountOf<ASubmission>(result).Affected.Should().Be(0);
        CountOf<ZUpload>(result).Affected.Should().Be(0);
        (await IdsAsync(database, "held_with_cases", TenantOne)).Should().Equal(CaseOne);
        (await IdsAsync(database, "held_with_links", TenantOne)).Should().Equal(LinkOne);
        (await IdsAsync(database, "held_with_submissions", TenantOne)).Should().Equal(Submission);
        (await IdsAsync(database, "held_with_uploads", TenantOne)).Should().Equal(Upload);
        // The other tenant wasn't swept at all.
        (await IdsAsync(database, "held_with_cases", TenantTwo)).Should().Equal(OtherCase);
    }

    [Fact]
    public async Task Erasure_Of_A_Person_Whose_Link_Is_Covered_Counts_It_As_Held_And_Is_Not_Complete()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        await HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne);

        var preview = await PreviewAsync(services);
        var dryRun = await EraseAsync(services, dryRun: true);
        var real = await EraseAsync(services, dryRun: false);

        foreach (var result in new[] { dryRun, real })
        {
            CountOf<ZCaseLink>(result).HeldCount.Should().Be(1);
            CountOf<ZCaseLink>(result).Affected.Should().Be(3);
            result.Complete.Should().BeFalse();
        }

        CountOf<ZCaseLink>(preview).HeldCount.Should().Be(1);
        (await IdsAsync(database, "held_with_links", TenantOne)).Should().Equal(LinkOne);
        // The same person's link in the other tenant is no part of this tenant's erasure.
        (await IdsAsync(database, "held_with_links", TenantTwo)).Should().Equal(OtherLink);
    }

    [Fact]
    public async Task Deletion_Of_A_Target_Covered_Through_A_Case_Is_Protected_And_A_Callback_Deleting_An_Unlisted_Covered_Upload_Rolls_Back()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        await HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne);

        var callbackRan = false;
        var protectedOutcome = await DeleteAsync(
            services,
            [new RetentionTarget(EntityId<ZUpload>(), Upload.ToString(), TenantOne)],
            (_, _) =>
            {
                callbackRan = true;
                return Task.CompletedTask;
            }
        );

        protectedOutcome.Should().Be(RetentionDeletionOutcome.Protected);
        callbackRan.Should().BeFalse();

        var unlistedCovered = () => DeleteAsync(
            services,
            [new RetentionTarget(EntityId<ZCaseLink>(), LinkNull.ToString(), TenantOne)],
            async (db, ct) =>
            {
                await db.Links.Where(link => link.Id == LinkNull).ExecuteDeleteAsync(ct);
                await db.Uploads.Where(upload => upload.Id == Upload).ExecuteDeleteAsync(ct);
            }
        );
        (await unlistedCovered.Should().ThrowAsync<InvalidOperationException>())
            .Which.Message.Should().Contain("deleted rows protected by active holds");
        (await IdsAsync(database, "held_with_uploads", TenantOne)).Should().Equal(Upload);
        (await IdsAsync(database, "held_with_links", TenantOne)).Should().Contain(LinkNull);

        // An unheld row, and an unlisted one no hold covers, go as before.
        var control = await DeleteAsync(
            services,
            [new RetentionTarget(EntityId<ZCaseLink>(), LinkNull.ToString(), TenantOne)],
            async (db, ct) =>
            {
                await db.Links.Where(link => link.Id == LinkNull).ExecuteDeleteAsync(ct);
                await db.Links.Where(link => link.Id == LinkTwo).ExecuteDeleteAsync(ct);
            }
        );
        control.Should().Be(RetentionDeletionOutcome.Executed);
        (await IdsAsync(database, "held_with_links", TenantOne)).Should().NotContain(new[] { LinkNull, LinkTwo });
    }

    public enum StepWork
    {
        UpdateCoveredUpload,
        CutCoveredLinkThenChangeIt,
        UpdateOtherTenantsUploadLinkedToCoveredSubmission,
        InsertUploadUnderCoveredSubmission,
        RepointUncoveredUploadToCoveredSubmission,
    }

    [Theory]
    [InlineData(StepWork.UpdateCoveredUpload, true)]
    [InlineData(StepWork.CutCoveredLinkThenChangeIt, true)]
    [InlineData(StepWork.UpdateOtherTenantsUploadLinkedToCoveredSubmission, false)]
    [InlineData(StepWork.InsertUploadUnderCoveredSubmission, true)]
    [InlineData(StepWork.RepointUncoveredUploadToCoveredSubmission, true)]
    public async Task A_Dependents_Step_That_Touches_A_Covered_Row_Is_Skipped(StepWork work, bool skipped)
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var script = new StepScript
        {
            Work = (row, db, ct) => row.Id != LinkTwo
                ? Task.CompletedTask
                : work switch
                {
                    StepWork.UpdateCoveredUpload => db.Uploads
                        .Where(upload => upload.Id == Upload)
                        .ExecuteUpdateAsync(set => set.SetProperty(upload => upload.Body, "changed"), ct),
                    StepWork.CutCoveredLinkThenChangeIt => CutLinkThenChangeAsync(db, ct),
                    // A new row linked under the held Submission: a new (hold, row) key appears.
                    StepWork.InsertUploadUnderCoveredSubmission => InsertUploadAsync(db, ct),
                    // A row no hold covers is linked to the held Submission, so it becomes covered.
                    StepWork.RepointUncoveredUploadToCoveredSubmission => db.Uploads
                        .Where(upload => upload.Id == UncoveredUpload)
                        .ExecuteUpdateAsync(set => set.SetProperty(upload => upload.SubmissionId, (Guid?)Submission), ct),
                    _ => db.Uploads
                        .Where(upload => upload.Id == CrossTenantUpload)
                        .ExecuteUpdateAsync(set => set.SetProperty(upload => upload.Body, "changed"), ct),
                },
        };
        await using var services = await BuildSeededServicesAsync(database.ConnectionString, script);
        if (work == StepWork.RepointUncoveredUploadToCoveredSubmission)
        {
            // Same tenant, linked to nothing, and not due, so the sweep leaves it alone.
            await using var seed = services.CreateAsyncScope();
            var seedDb = seed.ServiceProvider.GetRequiredService<HeldWithDbContext>();
            seedDb.Uploads.Add(new ZUpload { Id = UncoveredUpload, TenantId = TenantOne, CreatedAt = AsOf, SubmissionId = null, Body = "original" });
            await seedDb.SaveChangesAsync();
        }

        await HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne);

        var result = await SweepAsync(services);

        var links = CountOf<ZCaseLink>(result);
        links.HeldCount.Should().Be(1);
        links.SkippedCount.Should().Be(skipped ? 1 : 0);
        links.Affected.Should().Be(skipped ? 2 : 3);
        (await IdsAsync(database, "held_with_links", TenantOne))
            .Should().BeEquivalentTo(skipped ? new[] { LinkOne, LinkTwo } : new[] { LinkOne });
        // Rolled back with the step: the covered rows are as they were.
        (await ScalarAsync<string?>(database, "SELECT \"Body\" FROM held_with_uploads WHERE \"Id\" = @id", ("id", Upload)))
            .Should().Be("original");
        (await ScalarAsync<Guid?>(database, "SELECT \"CaseId\" FROM held_with_links WHERE \"Id\" = @id", ("id", LinkOne)))
            .Should().Be(CaseOne);
        if (skipped)
        {
            (await LoadStepFailuresAsync(database, result.SweepId)).Should().Equal("reason=dependents-changed-held-row");
            // No inserted row survives, and nothing was linked to the held Submission.
            (await IdsAsync(database, "held_with_uploads", TenantOne)).Should().Equal(
                work == StepWork.RepointUncoveredUploadToCoveredSubmission ? new[] { Upload, UncoveredUpload } : new[] { Upload }
            );
            (await ScalarAsync<long>(database, "SELECT count(*) FROM held_with_uploads WHERE \"SubmissionId\" = @id", ("id", Submission)))
                .Should().Be(2, "the covered upload and the other tenant's, as seeded");
        }
        else
        {
            (await LoadStepFailuresAsync(database, result.SweepId)).Should().BeEmpty();
            (await ScalarAsync<string?>(database, "SELECT \"Body\" FROM held_with_uploads WHERE \"Id\" = @id", ("id", CrossTenantUpload)))
                .Should().Be("changed");
        }
    }

    [Fact]
    public async Task Reads_Return_The_Cases_Hold_For_A_Covered_Upload_And_For_The_Person_Without_Running_Anything()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        var holdId = await HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne);
        await using var scope = services.CreateAsyncScope();
        var holds = scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>();

        (await holds.HasActiveHoldAsync(EntityId<ZUpload>(), Upload.ToString(), TenantOne, CancellationToken.None)).Should().BeTrue();
        var covering = await holds.ListCoveringAsync(EntityId<ZUpload>(), Upload.ToString(), TenantOne, CancellationToken.None);
        covering.Should().ContainSingle().Which.Should().Match<RetentionHold>(hold =>
            hold.HoldId == holdId && hold.RetentionEntityId == EntityId<ACase>() && hold.RecordId == CaseOne.ToString());

        // Another tenant's id sees nothing of this tenant's row.
        (await holds.HasActiveHoldAsync(EntityId<ZUpload>(), Upload.ToString(), TenantTwo, CancellationToken.None)).Should().BeFalse();
        (await holds.ListCoveringAsync(EntityId<ZUpload>(), Upload.ToString(), TenantTwo, CancellationToken.None)).Should().BeEmpty();
        // A row nothing covers.
        (await holds.HasActiveHoldAsync(EntityId<ZCaseLink>(), LinkTwo.ToString(), TenantOne, CancellationToken.None)).Should().BeFalse();

        var runsBefore = await ScalarAsync<long>(database, "SELECT count(*) FROM sweep_run");
        var forPerson = await holds.ListCoveringSubjectAsync("person", Person, TenantOne, CancellationToken.None);
        forPerson.Should().ContainSingle().Which.HoldId.Should().Be(holdId);
        (await ScalarAsync<long>(database, "SELECT count(*) FROM sweep_run")).Should().Be(runsBefore);
        // Nobody of this kind in the other tenant holds anything.
        (await holds.ListCoveringSubjectAsync("person", Person, TenantTwo, CancellationToken.None)).Should().BeEmpty();

        var undeclared = () => holds.ListCoveringSubjectAsync("nobody", Person, TenantOne, CancellationToken.None);
        (await undeclared.Should().ThrowAsync<InvalidOperationException>()).Which.Message.Should().Be(
            "Erasure subject kind 'nobody' isn't declared by any tenanted retained entity's [ErasureSubject], so the hold lookup would match nothing. Declared kinds: 'person'."
        );
        var wrongType = () => holds.ListCoveringSubjectAsync("person", "not-a-guid", TenantOne, CancellationToken.None);
        (await wrongType.Should().ThrowAsync<InvalidOperationException>()).Which.Message.Should().Be(
            "Erasure subject kind 'person' identifies subjects by Guid, but the subject is a String."
        );
    }

    [Fact]
    public async Task Creating_A_Hold_On_A_Case_Needs_Read_Committed_And_On_An_Upload_Does_Not()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        var caseHold = Guid.NewGuid();
        var uploadHold = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<HeldWithDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync(IsolationLevel.RepeatableRead);
            var create = () => scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                Request(caseHold, EntityId<ACase>(), CaseOne, TenantOne),
                CancellationToken.None
            );

            (await create.Should().ThrowAsync<InvalidOperationException>()).Which.Message
                .Should().Contain("needs a READ COMMITTED transaction, but the caller's is REPEATABLE READ");
        }

        // An upload covers nothing, so nothing needs locking and any isolation is fine.
        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<HeldWithDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync(IsolationLevel.RepeatableRead);
            await scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                Request(uploadHold, EntityId<ZUpload>(), Upload, TenantOne),
                CancellationToken.None
            );
            await transaction.CommitAsync();
        }

        await using var verify = services.CreateAsyncScope();
        (await verify.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>().ListActiveAsync(CancellationToken.None))
            .Select(hold => hold.HoldId).Should().Equal(uploadHold);
    }

    // PostgreSQL accepts READ UNCOMMITTED and runs it as READ COMMITTED, so the walk reads current
    // rows and the caller's isolation is no reason to refuse.
    [Fact]
    public async Task Creating_A_Hold_On_A_Case_In_A_Read_Uncommitted_Transaction_Succeeds()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        var holdId = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<HeldWithDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync(IsolationLevel.ReadUncommitted);
            await scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                Request(holdId, EntityId<ACase>(), CaseOne, TenantOne),
                CancellationToken.None
            );
            await transaction.CommitAsync();
        }

        (await ActiveHoldIdsAsync(services)).Should().Equal(holdId);
    }

    [Fact]
    public async Task Hold_Creation_Paused_After_Its_Walk_Makes_An_Erasure_Wait_Then_Report_The_Link_Held()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        var holdId = Guid.NewGuid();
        await using var paused = await PausedHold.StartAsync(
            database.ConnectionString,
            holdId,
            () => HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne, holdId)
        );

        var erasure = Task.Run(() => CallerErasureAsync(services));
        await WaitForRowLockWaiterAsync(database.ConnectionString);
        erasure.IsCompleted.Should().BeFalse();
        await paused.ReleaseAsync();
        var result = await erasure.WaitAsync(TimeSpan.FromSeconds(15));

        CountOf<ZCaseLink>(result).HeldCount.Should().Be(1);
        CountOf<ZCaseLink>(result).Affected.Should().Be(3);
        result.Complete.Should().BeFalse();
        (await IdsAsync(database, "held_with_links", TenantOne)).Should().Equal(LinkOne);
        (await ActiveHoldIdsAsync(services)).Should().Equal(holdId);
    }

    [Fact]
    public async Task An_Erasure_That_Has_Mutated_The_Link_Makes_Hold_Creation_Wait_And_The_Hold_Is_Created_After_The_Link_Is_Gone()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        var holdId = Guid.NewGuid();
        var mutated = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var commit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var erasure = Task.Run(() => CallerErasureAsync(services, afterErasing: async () =>
        {
            mutated.SetResult();
            await commit.Task;
        }));
        await mutated.Task.WaitAsync(TimeSpan.FromSeconds(15));

        var createHold = Task.Run(() => HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne, holdId));
        await WaitForRowLockWaiterAsync(database.ConnectionString);
        createHold.IsCompleted.Should().BeFalse();
        commit.SetResult();
        await Task.WhenAll(erasure, createHold).WaitAsync(TimeSpan.FromSeconds(15));

        (await ActiveHoldIdsAsync(services)).Should().Equal(holdId);
        (await IdsAsync(database, "held_with_links", TenantOne)).Should().BeEmpty();
    }

    [Fact]
    public async Task A_Sweep_Skips_The_Link_While_Hold_Creation_Is_Paused_And_Reports_It_Deferred()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        // The held Case isn't due: a sweep takes the advisory key of every Case it finds due, and
        // that key is the one hold creation holds, so the sweep would wait for the hold instead of
        // reaching the rows the hold locked.
        await using var services = await BuildSeededServicesAsync(database.ConnectionString, caseOneDue: false);
        var holdId = Guid.NewGuid();
        await using var paused = await PausedHold.StartAsync(
            database.ConnectionString,
            holdId,
            () => HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne, holdId)
        );

        var result = await SweepAsync(services);

        var links = CountOf<ZCaseLink>(result);
        links.DeferredCount.Should().Be(1);
        links.HeldCount.Should().Be(0);
        links.Affected.Should().Be(3);
        (await IdsAsync(database, "held_with_links", TenantOne)).Should().Equal(LinkOne);
        await paused.ReleaseAsync();
    }

    // A hold that commits while the after-loop count of the links runs, after the count's snapshot
    // was taken, must not move the link between counts: the Deferred, Held, and Blocked counts are
    // read in one statement, so the link is in exactly the one its snapshot says. The count is
    // held up by a gate (a row-level security policy on the links table that waits on an advisory
    // lock for any statement that counts), so the test can commit the hold while it waits.
    // Splitting the count into more than one statement puts the link in two counts (Deferred and
    // Held) or none (a referenced link, which is not Held in the first statement and, with the
    // hold now committed, not Blocked in the second).
    [Theory]
    [InlineData(false, 1L, 0L, 0L)]
    [InlineData(true, 0L, 0L, 1L)]
    public async Task A_Hold_Committing_While_The_After_Loop_Count_Runs_Leaves_The_Link_In_Exactly_One_Count(
        bool linkIsReferenced,
        long deferred,
        long held,
        long blocked
    )
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        // The held Case isn't due, as in the sweep test above. The sweep's batches skip the link
        // hold creation has locked and pass the gate; the after-loop count waits at the gate with
        // its snapshot taken, so the hold commits behind it.
        var result = await RunWhileHoldCommitsBehindCountGateAsync(database, linkIsReferenced, SweepAsync);

        var links = CountOf<ZCaseLink>(result);
        (links.DeferredCount, links.HeldCount, links.BlockedCount).Should().Be((deferred, held, blocked));
        links.Affected.Should().Be(3);
        (await IdsAsync(database, "held_with_links", TenantOne)).Should().Equal(LinkOne);
    }

    // A dry run (an erasure's) and a preview take no row locks, so a hold can commit between their
    // counts. They read Eligible, Held, and Blocked in one statement, so each link is in exactly
    // the one its snapshot says. Split into separate statements, the link whose hold commits
    // between them is counted twice (Eligible and Held) or moves from Blocked to Held.
    [Theory]
    [InlineData(false, false, 4L, 0L, 0L)]
    [InlineData(false, true, 3L, 0L, 1L)]
    [InlineData(true, false, 4L, 0L, 0L)]
    [InlineData(true, true, 3L, 0L, 1L)]
    public async Task A_Hold_Committing_While_A_Dry_Run_Or_Preview_Counts_Leaves_The_Link_In_Exactly_One_Count(
        bool preview,
        bool linkIsReferenced,
        long eligible,
        long held,
        long blocked
    )
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);

        var links = preview
            ? CountOf<ZCaseLink>(await RunWhileHoldCommitsBehindCountGateAsync(database, linkIsReferenced, PreviewAsync))
            : CountOf<ZCaseLink>(
                await RunWhileHoldCommitsBehindCountGateAsync(
                    database,
                    linkIsReferenced,
                    services => EraseAsync(services, dryRun: true)
                )
            );

        (links.Affected, links.HeldCount, links.BlockedCount).Should().Be((eligible, held, blocked));
        // Neither mutates anything.
        (await IdsAsync(database, "held_with_links", TenantOne)).Should().BeEquivalentTo([LinkOne, LinkTwo, LinkNull, LinkCross]);
    }

    /// <summary>
    /// Seeds the database (the held Case isn't due), gates every statement that counts the
    /// links, and starts <paramref name="run"/> while a hold on the Case waits to commit. Once
    /// the run waits at the gate, with its counting statement's snapshot taken, the hold commits
    /// and the gate opens.
    /// </summary>
    private static async Task<T> RunWhileHoldCommitsBehindCountGateAsync<T>(
        TemporaryDatabase database,
        bool linkIsReferenced,
        Func<ServiceProvider, Task<T>> run
    )
    {
        await (await BuildSeededServicesAsync(database.ConnectionString, caseOneDue: false)).DisposeAsync();
        await using var gate = await CountGate.CreateAsync(database, linkIsReferenced);
        await using var services = BuildServices(gate.ConnectionString);
        var holdId = Guid.NewGuid();
        await gate.CloseAsync();
        await using var paused = await PausedHold.StartAsync(
            database.ConnectionString,
            holdId,
            () => HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne, holdId)
        );

        var running = Task.Run(() => run(services));
        try
        {
            await WaitForAdvisoryLockWaiterAsync(database.ConnectionString, CountGate.Key, running);
            running.IsCompleted.Should().BeFalse();
            await paused.ReleaseAsync();
        }
        finally
        {
            await gate.OpenAsync();
        }

        var result = await running.WaitAsync(TimeSpan.FromSeconds(15));
        (await ActiveHoldIdsAsync(services)).Should().Equal(holdId);
        return result;
    }

    [Fact]
    public async Task A_Deletion_Whose_Callback_Deletes_A_Covered_Upload_Fails_When_The_Hold_Commits_Meanwhile()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        var holdId = Guid.NewGuid();
        await using var paused = await PausedHold.StartAsync(
            database.ConnectionString,
            holdId,
            () => HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne, holdId)
        );

        // Nothing is held yet, so the up-front check passes; the callback's DELETE of the upload
        // then waits for the hold creation's lock on it.
        var deletion = Task.Run(() => DeleteAsync(
            services,
            [new RetentionTarget(EntityId<ZCaseLink>(), LinkNull.ToString(), TenantOne)],
            (db, ct) => db.Uploads.Where(upload => upload.Id == Upload).ExecuteDeleteAsync(ct)
        ));
        await WaitForRowLockWaiterAsync(database.ConnectionString);
        await paused.ReleaseAsync();

        var failed = () => deletion.WaitAsync(TimeSpan.FromSeconds(15));
        (await failed.Should().ThrowAsync<InvalidOperationException>()).Which.Message
            .Should().Contain("deleted rows protected by active holds");
        (await IdsAsync(database, "held_with_uploads", TenantOne)).Should().Equal(Upload);
        (await IdsAsync(database, "held_with_links", TenantOne)).Should().Contain(LinkNull);
        (await ActiveHoldIdsAsync(services)).Should().Equal(holdId);
    }

    // The second lock of the cycle is taken by the test, through RetentionEntityLockSql, after the
    // erasure has returned: this fixture's plan order can't make the erasure take it itself. So
    // this proves the hold side of the deadlock (40P01 from either side, and no hold over a removed
    // row), and doesn't exercise the erasure's own handling of a failure of that entity.
    [Fact]
    public async Task Erasure_And_Hold_Creation_Locking_In_Opposite_Orders_Deadlock_And_Exactly_One_Side_Fails_With_40P01()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        var holdId = Guid.NewGuid();
        var erased = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var proceed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // The caller-transaction erasure purges the person's links and keeps its row locks until
        // it commits. An erasure then takes the next entity's advisory keys; here the next
        // entity's key is the Case's, taken the way an erasure of that entity takes it. In this
        // model the Case sorts before its links, so the plan order can't produce the cycle.
        var erasure = Task.Run(() => CallerErasureAsync(
            services,
            afterErasing: async () =>
            {
                erased.SetResult();
                await proceed.Task;
            },
            thenLock: (connection, transaction) => RetentionEntityLockSql.AcquireAsync(
                connection,
                transaction,
                EntityId<ACase>(),
                TenantOne,
                CaseOne.ToString(),
                CancellationToken.None
            )
        ));
        await erased.Task.WaitAsync(TimeSpan.FromSeconds(15));

        // Hold creation takes the Case's advisory key, then waits for the link the erasure holds.
        var createHold = Task.Run(() => HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne, holdId));
        await WaitForRowLockWaiterAsync(database.ConnectionString);
        proceed.SetResult();

        var erasureError = await CaptureAsync(erasure);
        var holdError = await CaptureAsync(createHold);

        var failures = new[] { erasureError, holdError }.OfType<PostgresException>().ToArray();
        failures.Should().ContainSingle().Which.SqlState.Should().Be("40P01");
        new[] { erasureError, holdError }.Count(error => error is not null).Should().Be(1);
        var holdExists = (await ActiveHoldIdsAsync(services)).Contains(holdId);
        var linkExists = (await IdsAsync(database, "held_with_links", TenantOne)).Contains(LinkOne);
        holdExists.Should().Be(holdError is null);
        // A hold over a removed row is the one outcome that must never happen.
        (holdExists && !linkExists).Should().BeFalse();
        linkExists.Should().Be(erasureError is not null);
    }

    // ─── Rows covered twice: a diamond of relations ──────────────────────────

    // BothUpload is under the shared Submission and carries CaseOne, so a hold on CaseOne reaches it
    // by two paths. Everything that reads or judges coverage must still see it once: the guard's
    // read is DISTINCT (a duplicate key makes HeldRowVersions throw out of the guard), and the
    // record read is a de-duplicating UNION.
    [Fact]
    public async Task A_Dependents_Step_That_Touches_Nothing_Passes_For_Its_Rows_When_A_Held_Row_Is_Covered_By_Two_Paths()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString, new StepScript());
        await AddDiamondUploadsAsync(services);
        await HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne);

        var result = await SweepAsync(services);

        var links = CountOf<ZCaseLink>(result);
        links.HeldCount.Should().Be(1);
        links.SkippedCount.Should().Be(0);
        links.Affected.Should().Be(3);
        CountOf<ZUpload>(result).HeldCount.Should().Be(3);
        (await LoadStepFailuresAsync(database, result.SweepId)).Should().BeEmpty();
        (await IdsAsync(database, "held_with_links", TenantOne)).Should().Equal(LinkOne);
    }

    [Fact]
    public async Task A_Deletion_Of_An_Unrelated_Row_Executes_When_A_Held_Row_Is_Covered_By_Two_Paths()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        await AddDiamondUploadsAsync(services);
        await HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne);

        var outcome = await DeleteAsync(
            services,
            [new RetentionTarget(EntityId<ZCaseLink>(), LinkNull.ToString(), TenantOne)],
            (db, ct) => db.Links.Where(link => link.Id == LinkNull).ExecuteDeleteAsync(ct)
        );

        outcome.Should().Be(RetentionDeletionOutcome.Executed);
        (await IdsAsync(database, "held_with_links", TenantOne)).Should().NotContain(LinkNull);
        (await IdsAsync(database, "held_with_uploads", TenantOne)).Should().Contain([Upload, BothUpload, DirectUpload]);
    }

    [Fact]
    public async Task ListCovering_Returns_The_Cases_Hold_Once_For_An_Upload_Covered_By_Two_Paths()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        await AddDiamondUploadsAsync(services);
        var holdId = await HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne);
        await using var scope = services.CreateAsyncScope();
        var holds = scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>();

        (await holds.HasActiveHoldAsync(EntityId<ZUpload>(), BothUpload.ToString(), TenantOne, CancellationToken.None)).Should().BeTrue();
        (await holds.ListCoveringAsync(EntityId<ZUpload>(), BothUpload.ToString(), TenantOne, CancellationToken.None))
            .Select(hold => hold.HoldId).Should().Equal(holdId);
    }

    // ─── The creation walk: one sorted statement per entity, every path, no other tenant ─────

    [Fact]
    public async Task Hold_Creation_Locks_An_Entitys_Covered_Rows_In_One_Sorted_Statement_Across_Its_Paths()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        await AddDiamondUploadsAsync(services);
        var holdId = Guid.NewGuid();

        // DirectUpload sorts last of the three covered uploads and is reached by the direct path
        // only. The walk blocks on it, and must by then have locked Upload, which only the path
        // through the Submission reaches: both are one statement, sorted.
        await using var blocker = await RowLocker.StartAsync(database.ConnectionString, "held_with_uploads", DirectUpload);
        var creation = Task.Run(() => HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne, holdId));
        await WaitForRowLockWaiterAsync(database.ConnectionString);

        creation.IsCompleted.Should().BeFalse();
        (await IsRowLockedAsync(database.ConnectionString, "held_with_uploads", Upload)).Should().BeTrue();
        (await IsRowLockedAsync(database.ConnectionString, "held_with_uploads", BothUpload)).Should().BeTrue();

        await blocker.ReleaseAsync();
        await creation.WaitAsync(TimeSpan.FromSeconds(15));
        (await ActiveHoldIdsAsync(services)).Should().Equal(holdId);
    }

    [Fact]
    public async Task Hold_Creation_Locks_The_Rows_Every_Path_Reaches_And_None_Of_Another_Tenant()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        await AddDiamondUploadsAsync(services);
        var holdId = Guid.NewGuid();
        await using var paused = await PausedHold.StartAsync(
            database.ConnectionString,
            holdId,
            () => HoldAsync(services, EntityId<ACase>(), CaseOne, TenantOne, holdId)
        );

        // Through the Submission only, through both paths, and through the Case only.
        (await IsRowLockedAsync(database.ConnectionString, "held_with_uploads", Upload)).Should().BeTrue();
        (await IsRowLockedAsync(database.ConnectionString, "held_with_uploads", BothUpload)).Should().BeTrue();
        (await IsRowLockedAsync(database.ConnectionString, "held_with_uploads", DirectUpload)).Should().BeTrue();
        // Another tenant's upload linked to this tenant's Submission isn't covered.
        (await IsRowLockedAsync(database.ConnectionString, "held_with_uploads", CrossTenantUpload)).Should().BeFalse();
        await paused.ReleaseAsync();
    }

    // ─── Tenantless entities ─────────────────────────────────────────────────

    [Fact]
    public async Task A_Tenantless_Sweep_Leaves_The_Child_Of_A_Held_Holder_Unchanged_And_Counts_It_Held()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        await HoldAsync(services, EntityId<TenantlessHolder>(), HolderOne, null);

        var result = await TenantlessSweepAsync(services);

        CountOf<TenantlessHolder>(result).HeldCount.Should().Be(1);
        CountOf<TenantlessHolder>(result).Affected.Should().Be(1);
        CountOf<TenantlessChild>(result).HeldCount.Should().Be(1);
        CountOf<TenantlessChild>(result).Affected.Should().Be(2);
        (await IdsAsync(database, "held_with_holders")).Should().Equal(HolderOne);
        (await IdsAsync(database, "held_with_children")).Should().Equal(ChildOne);
    }

    [Fact]
    public async Task Reads_Without_A_Tenant_Return_The_Tenantless_Holders_Hold()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        var holdId = await HoldAsync(services, EntityId<TenantlessHolder>(), HolderOne, null);
        await using var scope = services.CreateAsyncScope();
        var holds = scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>();

        (await holds.HasActiveHoldAsync(EntityId<TenantlessChild>(), ChildOne.ToString(), null, CancellationToken.None)).Should().BeTrue();
        (await holds.ListCoveringAsync(EntityId<TenantlessChild>(), ChildOne.ToString(), null, CancellationToken.None))
            .Select(hold => hold.HoldId).Should().Equal(holdId);
        (await holds.ListCoveringAsync(EntityId<TenantlessChild>(), ChildTwo.ToString(), null, CancellationToken.None)).Should().BeEmpty();
        (await holds.ListCoveringAsync(EntityId<TenantlessChild>(), ChildNull.ToString(), null, CancellationToken.None)).Should().BeEmpty();

        (await holds.ListCoveringSubjectAsync("person", Person, null, CancellationToken.None))
            .Select(hold => hold.HoldId).Should().Equal(holdId);
    }

    [Fact]
    public async Task Hold_Creation_On_A_Tenantless_Holder_Locks_Its_Children_And_Not_Another_Holders()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = await BuildSeededServicesAsync(database.ConnectionString);
        var holdId = Guid.NewGuid();
        await using var paused = await PausedHold.StartAsync(
            database.ConnectionString,
            holdId,
            () => HoldAsync(services, EntityId<TenantlessHolder>(), HolderOne, null, holdId)
        );

        (await IsRowLockedAsync(database.ConnectionString, "held_with_children", ChildOne)).Should().BeTrue();
        (await IsRowLockedAsync(database.ConnectionString, "held_with_children", ChildTwo)).Should().BeFalse();
        (await IsRowLockedAsync(database.ConnectionString, "held_with_children", ChildNull)).Should().BeFalse();
        await paused.ReleaseAsync();
        (await ActiveHoldIdsAsync(services)).Should().Equal(holdId);
    }

    private static async Task AddDiamondUploadsAsync(ServiceProvider services)
    {
        var created = AsOf.AddDays(-45);
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<HeldWithDbContext>();
        db.Uploads.AddRange(
            new ZUpload { Id = BothUpload, TenantId = TenantOne, CreatedAt = created.AddMinutes(1), SubmissionId = Submission, CaseId = CaseOne, Body = "original" },
            new ZUpload { Id = DirectUpload, TenantId = TenantOne, CreatedAt = created.AddMinutes(2), SubmissionId = null, CaseId = CaseOne, Body = "original" }
        );
        await db.SaveChangesAsync();
    }

    private static async Task<Exception?> CaptureAsync(Task task)
    {
        try
        {
            await task.WaitAsync(TimeSpan.FromSeconds(30));
            return null;
        }
        catch (Exception exception)
        {
            return exception;
        }
    }

    private static async Task InsertUploadAsync(HeldWithDbContext db, CancellationToken ct)
    {
        db.Uploads.Add(new ZUpload { Id = InsertedUpload, TenantId = TenantOne, CreatedAt = AsOf, SubmissionId = Submission, Body = "inserted" });
        await db.SaveChangesAsync(ct);
    }

    private static async Task CutLinkThenChangeAsync(HeldWithDbContext db, CancellationToken ct)
    {
        await db.Links
            .Where(link => link.Id == LinkOne)
            .ExecuteUpdateAsync(set => set.SetProperty(link => link.CaseId, (Guid?)null), ct);
        await db.Links
            .Where(link => link.Id == LinkOne)
            .ExecuteUpdateAsync(set => set.SetProperty(link => link.Body, "changed"), ct);
    }

    // ─── Helpers ────────────────────────────────────────────────────────────

    private static Guid EntityId<TEntity>() => RetentionEntityIdentity.For<TEntity>();

    private static EntitySweepCount CountOf<TEntity>(RetentionSweepResult result) =>
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(TEntity)).Subject;

    private static EntitySweepCount CountOf<TEntity>(ErasureResult result) =>
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(TEntity)).Subject;

    private static RetentionHoldRequest Request(Guid holdId, Guid entityId, Guid recordId, Guid? tenantId) =>
        new(holdId, entityId, recordId.ToString(), tenantId, "litigation", DateTimeOffset.UtcNow.AddMinutes(-1));

    private static async Task<Guid> HoldAsync(
        ServiceProvider services,
        Guid entityId,
        Guid recordId,
        Guid? tenantId,
        Guid? holdId = null
    )
    {
        var id = holdId ?? Guid.NewGuid();
        await using var scope = services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
            Request(id, entityId, recordId, tenantId),
            CancellationToken.None
        );
        return id;
    }

    private static async Task<List<Guid>> ActiveHoldIdsAsync(ServiceProvider services)
    {
        await using var scope = services.CreateAsyncScope();
        return (await scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>().ListActiveAsync(CancellationToken.None))
            .Select(hold => hold.HoldId)
            .ToList();
    }

    private static async Task<RetentionSweepResult> SweepAsync(ServiceProvider services)
    {
        await using var scope = services.CreateAsyncScope();
        return await scope.ServiceProvider
            .GetRequiredService<IRetentionSweep>()
            .ExecuteAsync(RetentionSweepRequest.Tenanted(TenantOneContext, AsOf));
    }

    private static async Task<RetentionSweepResult> TenantlessSweepAsync(ServiceProvider services)
    {
        await using var scope = services.CreateAsyncScope();
        return await scope.ServiceProvider
            .GetRequiredService<IRetentionSweep>()
            .ExecuteAsync(RetentionSweepRequest.Tenantless(AsOf));
    }

    private static async Task<RetentionSweepResult> PreviewAsync(ServiceProvider services)
    {
        await using var scope = services.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<IRetentionPreview>().PreviewAsync(TenantOneContext, AsOf);
    }

    private static async Task<ErasureResult> EraseAsync(ServiceProvider services, bool dryRun)
    {
        await using var scope = services.CreateAsyncScope();
        return await scope.ServiceProvider
            .GetRequiredService<IRetentionErasureService>()
            .ExecuteAsync(RetentionErasureRequest.Tenanted(TenantOneContext, new ErasureScope("person", Person, dryRun: dryRun)));
    }

    /// <summary>
    /// Erases the person in the caller's own transaction. <paramref name="afterErasing"/> runs
    /// once the erasure has mutated rows, still uncommitted; <paramref name="thenLock"/> runs
    /// after it, in the same transaction.
    /// </summary>
    private static async Task<ErasureResult> CallerErasureAsync(
        ServiceProvider services,
        Func<Task>? afterErasing = null,
        Func<System.Data.Common.DbConnection, System.Data.Common.DbTransaction, Task>? thenLock = null
    )
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<HeldWithDbContext>();
        await using var transaction = await db.Database.BeginTransactionAsync();
        var result = await scope.ServiceProvider
            .GetRequiredService<IRetentionErasureService>()
            .ExecuteAsync(
                RetentionErasureRequest.Tenanted(
                    TenantOneContext,
                    new ErasureScope("person", Person),
                    transaction: ErasureTransaction.Caller
                )
            );
        if (afterErasing is not null)
        {
            await afterErasing();
        }

        if (thenLock is not null)
        {
            await thenLock(db.Database.GetDbConnection(), transaction.GetDbTransaction());
        }

        await transaction.CommitAsync();
        return result;
    }

    private static async Task<RetentionDeletionOutcome> DeleteAsync(
        ServiceProvider services,
        RetentionTarget[] targets,
        Func<HeldWithDbContext, CancellationToken, Task> callback
    )
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<HeldWithDbContext>();
        return await scope.ServiceProvider
            .GetRequiredService<IRetentionDeletion>()
            .ExecuteAsync(targets, ct => callback(db, ct));
    }

    private static async Task<List<Guid>> IdsAsync(TemporaryDatabase database, string table, Guid tenantId)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT \"Id\" FROM {table} WHERE \"TenantId\" = @tenantId ORDER BY \"Id\"", connection);
        command.Parameters.AddWithValue("tenantId", tenantId);
        var ids = new List<Guid>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            ids.Add(reader.GetGuid(0));
        }

        return ids;
    }

    /// <summary>The ids of a tenantless table, ordered.</summary>
    private static async Task<List<Guid>> IdsAsync(TemporaryDatabase database, string table)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT \"Id\" FROM {table} ORDER BY \"Id\"", connection);
        var ids = new List<Guid>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            ids.Add(reader.GetGuid(0));
        }

        return ids;
    }

    private static async Task<T> ScalarAsync<T>(
        TemporaryDatabase database,
        string sql,
        params (string Name, object Value)[] parameters
    )
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        var scalar = await command.ExecuteScalarAsync();
        return scalar is null or DBNull ? default! : (T)scalar;
    }

    private static async Task<List<string>> LoadStepFailuresAsync(TemporaryDatabase database, Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT status."LastError"
            FROM "sweep_row_handler_status" AS status
            INNER JOIN "sweep_run_row_detail" AS detail ON detail."Id" = status."SweepRunRowDetailId"
            WHERE detail."SweepId" = @sweepId
              AND status."LastError" IS NOT NULL
            """,
            connection
        );
        command.Parameters.AddWithValue("sweepId", sweepId);
        var errors = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            errors.Add(reader.GetString(0));
        }

        return errors;
    }

    private static async Task<ServiceProvider> BuildSeededServicesAsync(
        string connectionString,
        StepScript? script = null,
        bool caseOneDue = true
    )
    {
        var provider = BuildServices(connectionString, script);
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<HeldWithDbContext>();
        await db.Database.EnsureCreatedAsync();
        var created = AsOf.AddDays(-45);
        db.Cases.AddRange(
            new ACase { Id = CaseOne, TenantId = TenantOne, CreatedAt = caseOneDue ? created : AsOf, SourceSubmissionId = Submission },
            new ACase { Id = CaseTwo, TenantId = TenantOne, CreatedAt = created.AddMinutes(1), SourceSubmissionId = Submission },
            new ACase { Id = OtherCase, TenantId = TenantTwo, CreatedAt = created, SourceSubmissionId = OtherSubmission }
        );
        db.Submissions.AddRange(
            new ASubmission { Id = Submission, TenantId = TenantOne, CreatedAt = created },
            new ASubmission { Id = OtherSubmission, TenantId = TenantTwo, CreatedAt = created }
        );
        db.Links.AddRange(
            new ZCaseLink { Id = LinkOne, TenantId = TenantOne, CreatedAt = created, CaseId = CaseOne, PersonId = Person },
            new ZCaseLink { Id = LinkTwo, TenantId = TenantOne, CreatedAt = created.AddMinutes(1), CaseId = CaseTwo, PersonId = Person },
            new ZCaseLink { Id = LinkNull, TenantId = TenantOne, CreatedAt = created.AddMinutes(2), CaseId = null, PersonId = Person },
            new ZCaseLink { Id = LinkCross, TenantId = TenantOne, CreatedAt = created.AddMinutes(3), CaseId = OtherCase, PersonId = Person },
            new ZCaseLink { Id = OtherLink, TenantId = TenantTwo, CreatedAt = created, CaseId = OtherCase, PersonId = Person }
        );
        db.Uploads.AddRange(
            new ZUpload { Id = Upload, TenantId = TenantOne, CreatedAt = created, SubmissionId = Submission, Body = "original" },
            new ZUpload { Id = OtherUpload, TenantId = TenantTwo, CreatedAt = created, SubmissionId = OtherSubmission, Body = "original" },
            new ZUpload { Id = CrossTenantUpload, TenantId = TenantTwo, CreatedAt = created, SubmissionId = Submission, Body = "original" }
        );
        db.Holders.AddRange(
            new TenantlessHolder { Id = HolderOne, CreatedAt = created },
            new TenantlessHolder { Id = HolderTwo, CreatedAt = created.AddMinutes(1) }
        );
        db.Children.AddRange(
            new TenantlessChild { Id = ChildOne, CreatedAt = created, HolderId = HolderOne, PersonId = Person },
            new TenantlessChild { Id = ChildTwo, CreatedAt = created.AddMinutes(1), HolderId = HolderTwo, PersonId = Person },
            new TenantlessChild { Id = ChildNull, CreatedAt = created.AddMinutes(2), HolderId = null, PersonId = Person }
        );
        await db.SaveChangesAsync();
        return provider;
    }

    private static ServiceProvider BuildServices(string connectionString, StepScript? script = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().AddInMemoryCollection().Build());
        services.AddLogging();
        services.AddDbContext<HeldWithDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(new PurgeCategoryProvider());
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<HeldWithDbContext>();
        if (script is not null)
        {
            services.AddSingleton(script);
            services.AddRetentionDependents<ZCaseLink, LinkStep>();
        }

        return services.BuildServiceProvider(validateScopes: true);
    }

    // ─── Pausing hold creation ──────────────────────────────────────────────

    /// <summary>
    /// Starts creating a hold and pauses it at its INSERT, which is after it has row-locked every
    /// row it will cover: a trigger on the holds table waits on an advisory lock this class holds.
    /// <see cref="ReleaseAsync"/> lets it commit.
    /// </summary>
    private sealed class PausedHold : IAsyncDisposable
    {
        private const long BarrierKey = 7_310_042_119;

        private readonly string connectionString;
        private NpgsqlConnection? blockerConnection;
        private NpgsqlTransaction? blockerTransaction;
        private Task creation = Task.CompletedTask;
        private bool released;

        private PausedHold(string connectionString) => this.connectionString = connectionString;

        public static async Task<PausedHold> StartAsync(string connectionString, Guid holdId, Func<Task> createHold)
        {
            var paused = new PausedHold(connectionString);
            try
            {
                await paused.StartCoreAsync(holdId, createHold);
                return paused;
            }
            catch
            {
                await paused.DisposeAsync();
                throw;
            }
        }

        private async Task StartCoreAsync(Guid holdId, Func<Task> createHold)
        {
            await using (var setup = new NpgsqlConnection(connectionString))
            {
                await setup.OpenAsync();
                await ExecuteAsync(
                    setup,
                    null,
                    $"""
                    CREATE OR REPLACE FUNCTION cohort_test_pause_hold_insert() RETURNS trigger AS $$
                    BEGIN
                        IF NEW."HoldId" = '{holdId}'::uuid THEN
                            PERFORM pg_advisory_xact_lock({BarrierKey});
                            NEW."CreatedAt" := clock_timestamp();
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                    DROP TRIGGER IF EXISTS cohort_test_pause_hold_insert ON "retention_holds";
                    CREATE TRIGGER cohort_test_pause_hold_insert
                    BEFORE INSERT ON "retention_holds"
                    FOR EACH ROW EXECUTE FUNCTION cohort_test_pause_hold_insert();
                    """
                );
            }

            blockerConnection = new NpgsqlConnection(connectionString);
            await blockerConnection.OpenAsync();
            blockerTransaction = await blockerConnection.BeginTransactionAsync();
            await ExecuteAsync(blockerConnection, blockerTransaction, $"SELECT pg_advisory_xact_lock({BarrierKey})");

            creation = Task.Run(createHold);
            await WaitForBarrierWaiterAsync(blockerConnection.ProcessID);
        }

        public async Task ReleaseAsync()
        {
            await blockerTransaction!.CommitAsync();
            released = true;
            await creation.WaitAsync(TimeSpan.FromSeconds(15));
        }

        public async ValueTask DisposeAsync()
        {
            if (!released && blockerTransaction is not null)
            {
                await blockerTransaction.RollbackAsync();
            }

            if (blockerTransaction is not null)
            {
                await blockerTransaction.DisposeAsync();
            }

            if (blockerConnection is not null)
            {
                await blockerConnection.DisposeAsync();
            }

            try
            {
                await creation.WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (Exception)
            {
                // A paused creation that was abandoned by a failing test.
            }

            await using var cleanup = new NpgsqlConnection(connectionString);
            await cleanup.OpenAsync();
            await ExecuteAsync(
                cleanup,
                null,
                """
                DROP TRIGGER IF EXISTS cohort_test_pause_hold_insert ON "retention_holds";
                DROP FUNCTION IF EXISTS cohort_test_pause_hold_insert();
                """
            );
        }

        /// <summary>Waits, polling a condition rather than sleeping, until the INSERT's trigger waits on the barrier.</summary>
        private async Task WaitForBarrierWaiterAsync(int blockerBackendId)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            while (true)
            {
                await using var command = connection.CreateCommand();
                command.CommandText = """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_locks waiter
                        WHERE waiter.locktype = 'advisory'
                          AND waiter.classid = ((@key >> 32) & 4294967295)::oid
                          AND waiter.objid = (@key & 4294967295)::oid
                          AND waiter.objsubid = 1
                          AND NOT waiter.granted
                          AND @blockerBackendId = ANY(pg_blocking_pids(waiter.pid))
                    )
                    """;
                command.Parameters.AddWithValue("key", BarrierKey);
                command.Parameters.AddWithValue("blockerBackendId", blockerBackendId);
                if ((bool)(await command.ExecuteScalarAsync(timeout.Token))!)
                {
                    return;
                }

                if (creation.IsFaulted)
                {
                    await creation;
                }

                await Task.Delay(20, timeout.Token);
            }
        }
    }

    /// <summary>
    /// Holds up every statement that counts the links, once it has read its snapshot. The
    /// connecting user of a test container is a superuser, whom row-level security never
    /// binds, so this makes a login role without superuser for the Cohort services. A policy
    /// on the links table, forced on the table's owner too, calls a function that takes and
    /// releases a shared advisory lock when the running statement contains <c>count(</c>, so
    /// while <see cref="CloseAsync"/> holds the lock exclusively such a statement waits at its
    /// first link row, after its snapshot. The sweep's batches contain no <c>count(</c>.
    /// </summary>
    private sealed class CountGate : IAsyncDisposable
    {
        public const long Key = 7_310_042_120;
        private const string Password = "cohort-gate";

        private readonly TemporaryDatabase database;
        private readonly string role;
        private NpgsqlConnection? lockConnection;

        private CountGate(TemporaryDatabase database, string role)
        {
            this.database = database;
            this.role = role;
        }

        public string ConnectionString => new NpgsqlConnectionStringBuilder(database.ConnectionString)
        {
            Username = role,
            Password = Password,
        }.ConnectionString;

        /// <summary>
        /// Sets up the gate on the seeded database. With <paramref name="linkIsReferenced"/>, a
        /// table whose foreign key references the held Case's link is added, which blocks the
        /// link's deletion.
        /// </summary>
        public static async Task<CountGate> CreateAsync(TemporaryDatabase database, bool linkIsReferenced)
        {
            var gate = new CountGate(database, $"cohort_gate_{Guid.NewGuid():N}");
            await using var connection = new NpgsqlConnection(database.ConnectionString);
            await connection.OpenAsync();
            if (linkIsReferenced)
            {
                await ExecuteAsync(
                    connection,
                    null,
                    $"""
                    CREATE TABLE held_with_link_refs (
                        "Id" uuid PRIMARY KEY,
                        "LinkId" uuid NOT NULL REFERENCES held_with_links ("Id")
                    );
                    INSERT INTO held_with_link_refs VALUES ('{Guid.NewGuid()}', '{LinkOne}');
                    """
                );
            }

            await ExecuteAsync(
                connection,
                null,
                $$"""
                CREATE FUNCTION cohort_test_count_gate() RETURNS boolean AS $gate$
                BEGIN
                    IF pg_catalog.strpos(pg_catalog.current_query(), 'count(') > 0 THEN
                        PERFORM pg_catalog.pg_advisory_lock_shared({{Key}});
                        PERFORM pg_catalog.pg_advisory_unlock_shared({{Key}});
                    END IF;
                    RETURN true;
                END;
                $gate$ LANGUAGE plpgsql;
                ALTER TABLE held_with_links ENABLE ROW LEVEL SECURITY;
                ALTER TABLE held_with_links FORCE ROW LEVEL SECURITY;
                CREATE POLICY cohort_test_count_gate ON held_with_links USING (cohort_test_count_gate());
                CREATE ROLE "{{gate.role}}" LOGIN PASSWORD '{{Password}}' NOSUPERUSER;
                GRANT ALL ON ALL TABLES IN SCHEMA public TO "{{gate.role}}";
                GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO "{{gate.role}}";
                """
            );
            return gate;
        }

        /// <summary>Takes the gate's lock exclusively: statements that count the links now wait.</summary>
        public async Task CloseAsync()
        {
            lockConnection = new NpgsqlConnection(database.ConnectionString);
            await lockConnection.OpenAsync();
            await ExecuteAsync(lockConnection, null, $"SELECT pg_advisory_lock({Key})");
        }

        public async Task OpenAsync()
        {
            if (lockConnection is not null)
            {
                await ExecuteAsync(lockConnection, null, $"SELECT pg_advisory_unlock({Key})");
                await lockConnection.DisposeAsync();
                lockConnection = null;
            }
        }

        public async ValueTask DisposeAsync()
        {
            await OpenAsync();
            await using var connection = new NpgsqlConnection(database.ConnectionString);
            await connection.OpenAsync();
            await ExecuteAsync(connection, null, $"""DROP OWNED BY "{role}"; DROP ROLE "{role}";""");
        }
    }

    /// <summary>
    /// Waits until a statement of the database is blocked on a row lock (it waits for the locking
    /// transaction to end). A paused hold creation waits on an advisory lock, so it is no row-lock
    /// waiter. The statement text isn't matched: <c>pg_stat_activity</c> cuts it short.
    /// </summary>
    private static async Task WaitForRowLockWaiterAsync(string connectionString)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (true)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND pid <> pg_backend_pid()
                      AND wait_event_type = 'Lock'
                      AND wait_event IN ('transactionid', 'tuple')
                )
                """;
            if ((bool)(await command.ExecuteScalarAsync(timeout.Token))!)
            {
                return;
            }

            await Task.Delay(20, timeout.Token);
        }
    }

    /// <summary>
    /// Whether another transaction holds a lock on the row that conflicts with a row update. The
    /// probe is a <c>FOR UPDATE NOWAIT</c> in autocommit on its own connection, so a probe that
    /// gets the row releases it at once.
    /// </summary>
    private static async Task<bool> IsRowLockedAsync(string connectionString, string table, Guid id)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT 1 FROM {table} WHERE \"Id\" = @id FOR UPDATE NOWAIT", connection);
        command.Parameters.AddWithValue("id", id);
        try
        {
            await command.ExecuteScalarAsync();
            return false;
        }
        catch (PostgresException exception) when (exception.SqlState == "55P03")
        {
            return true;
        }
    }

    /// <summary>Holds a <c>FOR UPDATE</c> lock on one row, in a transaction of its own, until released.</summary>
    private sealed class RowLocker : IAsyncDisposable
    {
        private NpgsqlConnection? connection;
        private NpgsqlTransaction? transaction;

        public static async Task<RowLocker> StartAsync(string connectionString, string table, Guid id)
        {
            var locker = new RowLocker { connection = new NpgsqlConnection(connectionString) };
            await locker.connection.OpenAsync();
            locker.transaction = await locker.connection.BeginTransactionAsync();
            await ExecuteAsync(locker.connection, locker.transaction, $"SELECT 1 FROM {table} WHERE \"Id\" = '{id}' FOR UPDATE");
            return locker;
        }

        public async Task ReleaseAsync()
        {
            if (transaction is not null)
            {
                await transaction.RollbackAsync();
                await transaction.DisposeAsync();
                transaction = null;
            }
        }

        public async ValueTask DisposeAsync()
        {
            await ReleaseAsync();
            if (connection is not null)
            {
                await connection.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// Waits until a statement waits for the advisory lock <paramref name="key"/>, or fails with
    /// <paramref name="running"/> if it ends first.
    /// </summary>
    private static async Task WaitForAdvisoryLockWaiterAsync(string connectionString, long key, Task running)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (true)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks waiter
                    WHERE waiter.locktype = 'advisory'
                      AND waiter.classid = ((@key >> 32) & 4294967295)::oid
                      AND waiter.objid = (@key & 4294967295)::oid
                      AND waiter.objsubid = 1
                      AND NOT waiter.granted
                )
                """;
            command.Parameters.AddWithValue("key", key);
            if ((bool)(await command.ExecuteScalarAsync(timeout.Token))!)
            {
                return;
            }

            if (running.IsCompleted)
            {
                await running;
                throw new InvalidOperationException("The statement ended without waiting for the advisory lock.");
            }

            await Task.Delay(20, timeout.Token);
        }
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, string sql)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    // ─── The model ──────────────────────────────────────────────────────────

    private sealed class PurgeCategoryProvider : ITestRetentionRuleProvider
    {
        private static readonly ITestRetentionRule Resolver = new StaticTestRetentionRule(
            new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge)
        );

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct) =>
            Task.FromResult<ITestRetentionRule?>(category == "held-with" ? Resolver : null);
    }

    /// <summary>What the dependents step of a link does, per row.</summary>
    private sealed class StepScript
    {
        public Func<ZCaseLink, HeldWithDbContext, CancellationToken, Task> Work { get; init; } =
            (_, _, _) => Task.CompletedTask;
    }

    [RowHandlerId("6c1f0a52-2d4b-4f3e-9a67-1b8e5d0c7a41")]
    private sealed class LinkStep(HeldWithDbContext db, StepScript script) : IRetentionDependents<ZCaseLink>
    {
        public Task RetireAsync(ZCaseLink row, RetentionDependentsContext ctx, CancellationToken ct) =>
            script.Work(row, db, ct);
    }

    private sealed class HeldWithDbContext(DbContextOptions<HeldWithDbContext> options) : DbContext(options)
    {
        public DbSet<ACase> Cases => Set<ACase>();
        public DbSet<ASubmission> Submissions => Set<ASubmission>();
        public DbSet<ZCaseLink> Links => Set<ZCaseLink>();
        public DbSet<ZUpload> Uploads => Set<ZUpload>();
        public DbSet<TenantlessHolder> Holders => Set<TenantlessHolder>();
        public DbSet<TenantlessChild> Children => Set<TenantlessChild>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ACase>(entity => entity.ToTable("held_with_cases"));
            modelBuilder.Entity<ASubmission>(entity => entity.ToTable("held_with_submissions"));
            modelBuilder.Entity<ZCaseLink>(entity => entity.ToTable("held_with_links"));
            modelBuilder.Entity<ZUpload>(entity => entity.ToTable("held_with_uploads"));
            modelBuilder.Entity<TenantlessHolder>(entity => entity.ToTable("held_with_holders"));
            modelBuilder.Entity<TenantlessChild>(entity => entity.ToTable("held_with_children"));
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0006-000000000001")]
    private sealed class ACase
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid? SourceSubmissionId { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0006-000000000002")]
    [HeldWithReferencing(typeof(ACase), nameof(ACase.SourceSubmissionId))]
    private sealed class ASubmission
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0006-000000000003")]
    [HeldWith(typeof(ACase), nameof(CaseId))]
    private sealed class ZCaseLink
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid? CaseId { get; init; }

        [ErasureSubject("person")]
        public Guid? PersonId { get; init; }

        public string? Body { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0006-000000000004")]
    [HeldWith(typeof(ASubmission), nameof(SubmissionId))]
    [HeldWith(typeof(ACase), nameof(CaseId))]
    private sealed class ZUpload
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid? SubmissionId { get; init; }

        // A second path from a Case to an upload: the seeded uploads leave it NULL, and the
        // diamond tests add uploads that carry it.
        public Guid? CaseId { get; init; }
        public string? Body { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0006-000000000005")]
    [RetentionTenantless]
    private sealed class TenantlessHolder
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0006-000000000006")]
    [RetentionTenantless]
    [HeldWith(typeof(TenantlessHolder), nameof(HolderId))]
    private sealed class TenantlessChild
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid? HolderId { get; init; }

        [ErasureSubject("person")]
        public Guid? PersonId { get; init; }
    }
}
