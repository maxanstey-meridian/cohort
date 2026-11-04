using System.Reflection;
using System.Text.Json;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Handlers;
using Cohort.Infrastructure.Migrations;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace Cohort.Sample.Tests;

// ─── End-to-end test (copied from PurgeSweepStrategyTests) ──────────────────
//
// Readiness asks PostgreSQL to EXPLAIN every statement Cohort sends, against the host's tables
// and its own, under the role of the scope that sends them. That is only worth anything if the
// statements readiness builds are the statements a run sends, so this proves it from the
// server's side: with pg_stat_statements on, a run's statement and readiness's EXPLAIN of the
// same text both appear in the view (the EXPLAIN as "EXPLAIN <the same text>"), each under the
// role that ran it, and every statement a real run sent must have its EXPLAIN under that same
// role. When a statement is missing from readiness's set, the failure names its role and text.
//
// Every scope kind runs as a role of its own (ScopeRoles: the tenant, the tenantless pass, the
// readiness scope, the dispatcher, the worker, the pruner, and the caller's own scope, which has
// no execution context). The test's own SQL (seeding, reads) goes through an unintercepted
// context or a raw connection, so it runs as the connecting superuser and is never judged; a
// statement it must send on a connection Cohort's scopes share starts "/* host */". Host SQL a
// step runs is marked the same way. Catalog reads start "/* cohort catalog */" and transaction
// control (BEGIN, COMMIT, SAVEPOINT, and the rest) cannot be explained: both are skipped.
//
// Each model's groups run in one test, each in its own fresh database, seeded first, then
// pg_stat_statements is reset (the whole view, with its eviction count noted), and the operations
// run: batch size 1 with several due rows (the cursor statements), dry runs and live sweeps,
// previews, erasures (tenant, tenantless, and inside a caller's transaction), holds, a deletion
// check, the worker's scheduled iteration, pruning, and hosts with a row handler (the
// dispatcher, with rows settled through every form) and with a dependents step.
// ────────────────────────────────────────────────────────────────────────────

[Collection("Integration")]
public sealed class ReadinessStatementCoverageTests(PostgresFixture fixture)
{
    private const string HostComment = "/* host */";
    private const string CatalogComment = "/* cohort catalog */";
    private const string Explain = "EXPLAIN ";
    private const string CanonicalText = "SELECT CAST(";
    private const string Schedule = "0 0 3 * * *";
    private static readonly DateTimeOffset AsOf = DateTimeOffset.UtcNow;

    // Statements EXPLAIN cannot plan and that need no privilege.
    private static readonly string[] TransactionControl =
    [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
        "SET ",
        "RESET",
        "DISCARD",
        "START TRANSACTION",
    ];

    // Each model's scenario proves, for its own recording, that every statement a scope's role sent
    // was explained under that role. Together they prove that every builder of every group was
    // executed by some model: the sample has no hold relations, so a statement that rereads the
    // rows a cut link left behind is sent only by the hold relation model, and only the sample has
    // a per-row Anonymise.
    [Fact]
    public async Task Every_statement_every_scope_sends_is_explained_by_readiness_under_its_own_role()
    {
        var ledger = new ReachLedger();

        await PlainRunsAsync(ledger);
        await RowHandlerHostAsync(ledger);
        await DependentsStepHostAsync(ledger);
        await HoldRelationModelAsync(ledger);
        await HoldRelationModelAroundDependentsStepAsync(ledger);
        await ErasureModelAsync(ledger);

        // Joined, as the other claims are: a collection assertion names only its first item.
        string.Join("\n", ledger.Unreached())
            .Should()
            .BeEmpty(
                "every builder in every group's enumeration must be executed under the role of a scope that ran the group's operation, in at least one of the models"
            );
    }

    private async Task PlainRunsAsync(ReachLedger ledger)
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedSampleDatabaseAsync();
        var tenants = new CoverageTenants();
        await SeedAsync(database, tenants);
        await SeedPrunableHistoryAsync(database);
        await roles.GrantAsync(database);
        // A role that only previews: no other operation explains the preview's counts under it.
        var previewRole = await roles.AddTenantRoleAsync(database, tenants.Previewed);
        using var host = new CohortTestHost(
            database.ConnectionString,
            new CoverageRuleProvider(tenants),
            WithPruning(BatchSizeOne),
            services =>
            {
                roles.Install<SampleDbContext>(services);
                services.AddScoped<IRetentionTenantSource>(_ => new FixedTenantSource(tenants.All));
            }
        );

        var recording = await StartRecordingAsync(database);
        await host.RunWithServicesAsync(services =>
            services
                .GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None)
        );

        // Sweeps and previews: S1 to S7, with the cursor on the second batch of each entity.
        (await SweepAsync(host, tenants.Swept, dryRun: true))
            .EntityFailures.Should()
            .BeEmpty();
        await host.RunPreviewAsync(Tenant(tenants.Swept), AsOf);
        await host.RunPreviewAsync(Tenant(tenants.Previewed), AsOf);
        (await SweepAsync(host, tenants.Swept, dryRun: false)).EntityFailures.Should().BeEmpty();
        // The tenant whose rule for the soft-delete-or-anonymise category resolves to
        // Anonymise: with the others, every strategy a category declares has been resolved.
        (await SweepAsync(host, tenants.Anonymising, dryRun: false)).EntityFailures.Should().BeEmpty();
        (
            await host.RunWithServicesAsync(services =>
                services
                    .GetRequiredService<IRetentionSweep>()
                    .ExecuteAsync(RetentionSweepRequest.Tenantless(AsOf), CancellationToken.None)
            )
        )
            .EntityFailures.Should()
            .BeEmpty();

        // Erasure: each kind, with a legal minimum (a cutoff) and without (none), the tenantless
        // entities, and an Exempt rule, which only counts the subject's rows. A dry run first.
        (
            await host.RunErasureAsync(
                Tenant(tenants.WithLegalMinimum),
                new ErasureScope("user", tenants.Subject, allowSoftDeleteAsErasure: true, dryRun: true),
                AsOf
            )
        )
            .EntityFailures.Should()
            .BeEmpty();
        foreach (var kind in new[] { "user", "person" })
        {
            await EraseAsync(host, tenants.WithLegalMinimum, kind, tenants.Subject);
            await EraseAsync(host, tenants.WithoutLegalMinimum, kind, tenants.Subject);
        }

        await EraseAsync(host, tenants.Exempted, "user", tenants.Subject);
        (
            await host.RunWithServicesAsync(services =>
                services
                    .GetRequiredService<IRetentionErasureService>()
                    .ExecuteAsync(
                        RetentionErasureRequest.Tenantless(
                            new ErasureScope("user", tenants.Subject),
                            AsOf
                        ),
                        CancellationToken.None
                    )
            )
        )
            .EntityFailures.Should()
            .BeEmpty();

        // An erasure in the caller's own transaction: the run's statements go through the
        // caller's scope and role, and its commit is confirmed afterwards (R7).
        await host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            var erased = await services
                .GetRequiredService<IRetentionErasureService>()
                .ExecuteAsync(
                    RetentionErasureRequest.Tenanted(
                        Tenant(tenants.CallerErased),
                        new ErasureScope("user", tenants.Subject, allowSoftDeleteAsErasure: true),
                        AsOf,
                        transaction: ErasureTransaction.Caller
                    ),
                    CancellationToken.None
                );
            erased.EntityFailures.Should().BeEmpty();
            await transaction.CommitAsync();
        });

        // Holds, and the deletion check.
        await host.RunWithServicesAsync(async services =>
        {
            var holds = services.GetRequiredService<IRetentionHoldsRepository>();
            var held = await FirstNoteIdAsync(database, tenants.Held);
            var free = await FirstNoteIdAsync(database, tenants.Swept);
            var holdId = Guid.NewGuid();
            await holds.CreateAsync(
                new RetentionHoldRequest(
                    holdId,
                    Note.RetentionIdentity,
                    held.ToString(),
                    tenants.Held,
                    "coverage",
                    AsOf
                ),
                CancellationToken.None
            );
            await holds.ListActiveAsync(CancellationToken.None);
            (
                await holds.HasActiveHoldAsync(
                    Note.RetentionIdentity,
                    held.ToString(),
                    tenants.Held,
                    CancellationToken.None
                )
            )
                .Should()
                .BeTrue();
            (
                await holds.ListCoveringAsync(
                    Note.RetentionIdentity,
                    held.ToString(),
                    tenants.Held,
                    CancellationToken.None
                )
            )
                .Should()
                .ContainSingle();
            (
                await holds.ListCoveringSubjectAsync(
                    "user",
                    tenants.Subject,
                    tenants.Held,
                    CancellationToken.None
                )
            )
                .Should()
                .ContainSingle();
            await holds.ListCoveringSubjectAsync(
                "user",
                tenants.Subject,
                null,
                CancellationToken.None
            );

            var deletion = services.GetRequiredService<IRetentionDeletion>();
            (
                await deletion.ExecuteAsync(
                    [new RetentionTarget(Note.RetentionIdentity, held.ToString(), tenants.Held)],
                    _ => Task.CompletedTask
                )
            )
                .Should()
                .Be(RetentionDeletionOutcome.Protected);
            (
                await deletion.ExecuteAsync(
                    [new RetentionTarget(Note.RetentionIdentity, free.ToString(), tenants.Swept)],
                    _ => Task.CompletedTask
                )
            )
                .Should()
                .Be(RetentionDeletionOutcome.Executed);
            await holds.RemoveAsync(holdId, AsOf, CancellationToken.None);

            // A tenantless entity's row with an active tenantless hold on another of its rows:
            // the guard reads the tenantless holds in the readiness scope (H9), finds one, and
            // reads them again in the caller's scope.
            var logIdentity = RetentionEntityIdentity.For<TenantlessLog>();
            var (heldLog, freeLog) = await TwoTenantlessLogIdsAsync(database);
            var logHold = Guid.NewGuid();
            await holds.CreateAsync(
                new RetentionHoldRequest(
                    logHold,
                    logIdentity,
                    heldLog.ToString(),
                    null,
                    "coverage",
                    AsOf
                ),
                CancellationToken.None
            );
            (
                await deletion.ExecuteAsync(
                    [new RetentionTarget(logIdentity, freeLog.ToString(), null)],
                    _ => Task.CompletedTask
                )
            )
                .Should()
                .Be(RetentionDeletionOutcome.Executed);
            await holds.RemoveAsync(logHold, AsOf, CancellationToken.None);
        });

        // The worker's iteration: the clock, the tenant-coverage scan, the ledger, and a
        // scheduled pass for every tenant and the tenantless pass.
        await host.RunWithServicesAsync(services =>
            services
                .GetServices<IHostedService>()
                .OfType<RetentionWorker>()
                .Single()
                .RunScheduledIterationAsync(Schedule, dryRun: false, settled: null, CancellationToken.None)
        );

        // Pruning both retentions under a schedule: settled runs (R8, D8, S3, R9), inactive holds
        // (H11), and the clock for the schedule's current occurrence.
        var pruned = await host.RunWithServicesAsync(services =>
            services.GetRequiredService<RetentionHistoryPruner>().PruneAsync()
        );
        pruned.Runs.Should().BeGreaterThan(0);
        pruned.Holds.Should().BeGreaterThan(0);

        // The tenantless sweep and erasure reach the tenantless tables; the second batch of each
        // entity reaches S5's cursor, the Exempt erasure S4, and the worker's scan S12.
        await AssertEveryStatementIsExplainedAsync<SampleDbContext>(
            database,
            roles,
            recording,
            ledger,
            action => host.RunWithServicesAsync(action),
            runsReachTenantless: true,
            [
                new Reach(RetentionStatementGroup.Sweep, roles.Tenant, roles.TenantlessPass),
                new Reach(RetentionStatementGroup.Erasure, roles.Tenant, roles.TenantlessPass, roles.Caller),
                new Reach(RetentionStatementGroup.Preview, roles.Tenant, previewRole),
                new Reach(RetentionStatementGroup.Holds, roles.Caller),
                new Reach(RetentionStatementGroup.Deletion, roles.Caller),
                new Reach(RetentionStatementGroup.Worker, roles.WorkerCoordination),
                new Reach(
                    RetentionStatementGroup.Pruning(new PruningScope(true, true, true)),
                    roles.HistoryPruning
                ),
                Reach.ForReadiness(roles.Readiness),
            ],
            AnchoredCursor,
            SubjectCount,
            TenantCoverage
        );
    }

    private async Task RowHandlerHostAsync(ReachLedger ledger)
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedSampleDatabaseAsync();
        var tenants = new CoverageTenants();
        var sink = new CoverageHandlerSink();
        await SeedAsync(database, tenants);
        await roles.GrantAsync(database);
        using var host = new CohortTestHost(
            database.ConnectionString,
            new CoverageRuleProvider(tenants),
            new Dictionary<string, string?>(BatchSizeOne)
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "3",
            },
            services =>
            {
                roles.Install<SampleDbContext>(services);
                services.AddSingleton(sink);
                services.AddRowHandler<Note, CoverageNoteHandler>();
            }
        );

        var recording = await StartRecordingAsync(database);
        await host.RunWithServicesAsync(services =>
            services
                .GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None)
        );

        // A handler takes a Purge entity down the row-by-row path: three rows queue handler work.
        (await SweepAsync(host, tenants.Swept, dryRun: true)).EntityFailures.Should().BeEmpty();
        (await SweepAsync(host, tenants.Swept, dryRun: false))
            .EntityFailures.Should()
            .BeEmpty();
        // The erasure's first row fails its handler's OnBefore: the row is skipped, and its
        // record id is canonicalised for the audit row.
        sink.FailNextBefore = true;
        (
            await host.RunErasureAsync(
                Tenant(tenants.WithoutLegalMinimum),
                new ErasureScope("user", tenants.Subject, allowSoftDeleteAsErasure: true),
                AsOf
            )
        )
            .EntityFailures.Should()
            .ContainSingle(failure =>
                failure.Contains("reason=rows-skipped", StringComparison.Ordinal)
                && failure.Contains(typeof(Note).FullName!, StringComparison.Ordinal)
            );

        // Work left behind: a dispatcher died holding one claim with attempts to spare (the
        // reclaim requeues it) and one that used them all (the reclaim dead-letters it); a run
        // was left Started past the settle timeout (recovery fails it).
        await AbandonClaimAsync(database, skip: 0, attempt: 1, DateTimeOffset.UtcNow.AddMinutes(-10));
        await AbandonClaimAsync(database, skip: 1, attempt: 3, DateTimeOffset.UtcNow.AddMinutes(-10));
        await SeedStaleStartedRunAsync(database);
        // A captured payload past its retention, for the scrub.
        await ExecuteAsync(
            database,
            """
            UPDATE public.sweep_run_row_detail SET "CapturedPayload" = '{}', "At" = now() - INTERVAL '60 days'
            WHERE "Id" = (SELECT min("Id") FROM public.sweep_run_row_detail)
            """
        );

        // The flush recovers the run, reclaims the claims, scrubs, then delivers what is left:
        // the first delivery fails and retries (the sink makes it so), the rest succeed.
        await host.RunWithServicesAsync(services =>
            services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync()
        );
        sink.Failures.Should().Be(1, "one delivery was made to fail, which settles for retry");
        (await HandlerStatesAsync(database))
            .Should()
            .Contain([SweepRowHandlerDispatchState.Succeeded, SweepRowHandlerDispatchState.DeadLettered, SweepRowHandlerDispatchState.Pending]);

        // The operator requeues the dead-lettered work; a background poll then delivers it.
        await host.RunWithServicesAsync(services =>
            services.GetRequiredService<IRetentionRowDispatcher>().RequeueDeadLetteredAsync()
        );
        await host.RunWithServicesAsync(services =>
            services.GetRequiredService<RetentionRowDispatcher>().PollAsync(CancellationToken.None)
        );

        // A flush cancelled while a handler runs requeues the delivery it interrupted (T5).
        sink.BlockDeliveries = true;
        (await SweepAsync(host, tenants.WithLegalMinimum, dryRun: false)).EntityFailures.Should().BeEmpty();
        using var cancellation = new CancellationTokenSource();
        var cancelled = host.RunWithServicesAsync(services =>
            services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync(cancellation.Token)
        );
        await sink.Started.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await cancellation.CancelAsync();
        await cancelled.Awaiting(task => task).Should().ThrowAsync<OperationCanceledException>();

        // Only tenanted runs: the tenantless tables are not reached.
        await AssertEveryStatementIsExplainedAsync<SampleDbContext>(
            database,
            roles,
            recording,
            ledger,
            action => host.RunWithServicesAsync(action),
            runsReachTenantless: false,
            [
                new Reach(RetentionStatementGroup.Sweep, roles.Tenant),
                new Reach(RetentionStatementGroup.Erasure, roles.Tenant),
                new Reach(RetentionStatementGroup.Holds, roles.Caller),
                new Reach(RetentionStatementGroup.Scrub, roles.RowDispatch),
                new Reach(RetentionStatementGroup.Dispatch, roles.RowDispatch),
                Reach.ForReadiness(roles.Readiness),
            ],
            AnchoredCursor
        );
    }

    private async Task DependentsStepHostAsync(ReachLedger ledger)
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await MigratedSampleDatabaseAsync();
        var tenants = new CoverageTenants();
        await SeedAsync(database, tenants);
        // A child of the second note the sweep reaches: the foreign key refuses its delete, which
        // a step's entity does not exclude beforehand (the step may retire the child).
        await ExecuteAsync(
            database,
            """
            CREATE TABLE public.note_children ("NoteId" uuid NOT NULL REFERENCES public.notes ("Id"));
            """
        );
        await ExecuteAsync(
            database,
            $"""
            INSERT INTO public.note_children ("NoteId")
            SELECT "Id" FROM public.notes WHERE "TenantId" = '{tenants.Swept}'
            ORDER BY "CreatedAt" OFFSET 1 LIMIT 1
            """
        );
        await roles.GrantAsync(database);
        using var host = new CohortTestHost(
            database.ConnectionString,
            new CoverageRuleProvider(tenants),
            BatchSizeOne,
            services =>
            {
                roles.Install<SampleDbContext>(services);
                services.AddRetentionDependents<Note, NoteStep>();
            }
        );

        var recording = await StartRecordingAsync(database);
        var (heldLog, _) = await TwoTenantlessLogIdsAsync(database);
        await host.RunWithServicesAsync(async services =>
        {
            var holds = services.GetRequiredService<IRetentionHoldsRepository>();
            await holds.ListActiveAsync(CancellationToken.None);
            // An active tenantless hold, so the guard around the step reads the tenantless holds in
            // the readiness scope, finds one, and reads them again in the run's own scope.
            await holds.CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    RetentionEntityIdentity.For<TenantlessLog>(),
                    heldLog.ToString(),
                    null,
                    "coverage",
                    AsOf
                ),
                CancellationToken.None
            );
        });

        (await SweepAsync(host, tenants.Swept, dryRun: true)).EntityFailures.Should().BeEmpty();
        // The step moved the first note out of scope and the key refused the second's delete.
        (await SweepAsync(host, tenants.Swept, dryRun: false))
            .EntityFailures.Should()
            .ContainSingle(failure =>
                failure.Contains("reason=rows-skipped", StringComparison.Ordinal)
                && failure.Contains(typeof(Note).FullName!, StringComparison.Ordinal)
            );
        // The erasure meets the same key: it leaves the note with the child unchanged, blocked.
        (
            await host.RunErasureAsync(
                Tenant(tenants.Swept),
                new ErasureScope("user", tenants.Subject, allowSoftDeleteAsErasure: true),
                AsOf
            )
        )
            .EntityFailures.Should()
            .ContainSingle(failure =>
                failure.Contains("reason=rows-blocked", StringComparison.Ordinal)
                && failure.Contains(typeof(Note).FullName!, StringComparison.Ordinal)
            );

        // Only tenanted runs, but the held-row guard's statements span the whole model, so the
        // tenantless tables appear too.
        await AssertEveryStatementIsExplainedAsync<SampleDbContext>(
            database,
            roles,
            recording,
            ledger,
            action => host.RunWithServicesAsync(action),
            runsReachTenantless: true,
            [
                new Reach(RetentionStatementGroup.Sweep, roles.Tenant),
                new Reach(RetentionStatementGroup.Erasure, roles.Tenant),
                new Reach(RetentionStatementGroup.Holds, roles.Caller),
                Reach.ForReadiness(roles.Readiness),
            ],
            AnchoredCursor,
            StillDue
        );
    }

    private async Task HoldRelationModelAsync(ReachLedger ledger)
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenant = Guid.NewGuid();
        await using var services = HeldWithReadinessEndToEndTests.BuildServices(
            database.ConnectionString,
            configure: collection =>
            {
                roles.Install<HeldWithReadinessEndToEndTests.HeldWithReadinessDbContext>(collection);
                collection.AddScoped<IRetentionTenantSource>(_ => new FixedTenantSource([tenant]));
                collection.RemoveAll<IRetentionRuleProvider>();
                collection.AddSingleton<IRetentionRuleProvider>(new PerRowAuditedHeldWithRuleProvider());
            },
            configuration: BatchSizeOne
        );
        await EnsureCreatedAsync(database);
        await SeedHeldWithAsync(database, tenant);
        await roles.GrantAsync(database);

        var recording = await StartRecordingAsync(database);
        await using (var scope = services.CreateAsyncScope())
        {
            var provider = scope.ServiceProvider;
            var holds = provider.GetRequiredService<IRetentionHoldsRepository>();
            await holds.ListActiveAsync(CancellationToken.None);

            // A hold on a case covers its links and the submission it references. Created in the
            // caller's own transaction, so the isolation the hold is taken at is checked too.
            var heldWithDb =
                provider.GetRequiredService<HeldWithReadinessEndToEndTests.HeldWithReadinessDbContext>();
            await using (var transaction = await heldWithDb.Database.BeginTransactionAsync())
            {
                await holds.CreateAsync(
                    new RetentionHoldRequest(
                        Guid.NewGuid(),
                        CaseIdentity,
                        HeldWithIds.Case.ToString(),
                        tenant,
                        "coverage",
                        AsOf
                    ),
                    CancellationToken.None
                );
                await transaction.CommitAsync();
            }

            (
                await holds.HasActiveHoldAsync(
                    LinkIdentity,
                    HeldWithIds.Link.ToString(),
                    tenant,
                    CancellationToken.None
                )
            )
                .Should()
                .BeTrue();
            (
                await holds.ListCoveringAsync(
                    SubmissionIdentity,
                    HeldWithIds.Submission.ToString(),
                    tenant,
                    CancellationToken.None
                )
            )
                .Should()
                .ContainSingle();
            (
                await provider
                    .GetRequiredService<IRetentionDeletion>()
                    .ExecuteAsync(
                        [new RetentionTarget(LinkIdentity, HeldWithIds.Link.ToString(), tenant)],
                        _ => Task.CompletedTask
                    )
            )
                .Should()
                .Be(RetentionDeletionOutcome.Protected);

            // An unheld note whose callback cuts the link the case's hold covers: the guard reads
            // the held rows after the callback, rereads the link by record id to see why it is
            // gone, and refuses the deletion.
            var cutLink = () =>
                provider
                    .GetRequiredService<IRetentionDeletion>()
                    .ExecuteAsync(
                        [new RetentionTarget(NoteIdentity, "note-0", tenant)],
                        ct =>
                            heldWithDb.Database.ExecuteSqlAsync(
                                $"/* host */ DELETE FROM public.held_with_links WHERE \"Id\" = {HeldWithIds.Link}",
                                ct
                            )
                    );
            (await cutLink.Should().ThrowAsync<InvalidOperationException>())
                .Which.Message.Should()
                .Contain("deleted rows protected by active holds");

            foreach (var dryRun in new[] { true, false })
            {
                (
                    await provider
                        .GetRequiredService<IRetentionSweep>()
                        .ExecuteAsync(
                            RetentionSweepRequest.Tenanted(Tenant(tenant), AsOf, dryRun),
                            CancellationToken.None
                        )
                )
                    .EntityFailures.Should()
                    .BeEmpty();
            }

            // The worker's iteration: the tenant-coverage scan over the tenanted entities.
            await provider
                .GetServices<IHostedService>()
                .OfType<RetentionWorker>()
                .Single()
                .RunScheduledIterationAsync(Schedule, dryRun: false, settled: null, CancellationToken.None);
        }

        await AssertEveryStatementIsExplainedAsync<HeldWithReadinessEndToEndTests.HeldWithReadinessDbContext>(
            database,
            roles,
            recording,
            ledger,
            action => WithServicesAsync(services, action),
            runsReachTenantless: true,
            [
                new Reach(RetentionStatementGroup.Sweep, roles.Tenant),
                new Reach(RetentionStatementGroup.Holds, roles.Caller),
                new Reach(RetentionStatementGroup.Deletion, roles.Caller),
                new Reach(RetentionStatementGroup.Worker, roles.WorkerCoordination),
                Reach.ForReadiness(roles.Readiness),
            ],
            AnchoredCursor,
            TenantCoverage,
            CoveredRowLocks
        );
    }

    private async Task HoldRelationModelAroundDependentsStepAsync(ReachLedger ledger)
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = HeldWithReadinessEndToEndTests.BuildServices(
            database.ConnectionString,
            configure: collection =>
            {
                roles.Install<HeldWithReadinessEndToEndTests.HeldWithReadinessDbContext>(collection);
                collection.AddRetentionDependents<
                    HeldWithReadinessEndToEndTests.ReadinessNote,
                    LinkDeletingNoteStep
                >();
                collection.RemoveAll<IRetentionRuleProvider>();
                collection.AddSingleton<IRetentionRuleProvider>(new PerRowAuditedHeldWithRuleProvider());
            },
            configuration: BatchSizeOne
        );
        var tenant = Guid.NewGuid();
        await EnsureCreatedAsync(database);
        await SeedHeldWithAsync(database, tenant);
        await roles.GrantAsync(database);

        var recording = await StartRecordingAsync(database);
        await using (var scope = services.CreateAsyncScope())
        {
            var provider = scope.ServiceProvider;
            var holds = provider.GetRequiredService<IRetentionHoldsRepository>();
            await holds.CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    CaseIdentity,
                    HeldWithIds.Case.ToString(),
                    tenant,
                    "coverage",
                    AsOf
                ),
                CancellationToken.None
            );

            (
                await provider
                    .GetRequiredService<IRetentionSweep>()
                    .ExecuteAsync(
                        RetentionSweepRequest.Tenanted(Tenant(tenant), AsOf, dryRun: true),
                        CancellationToken.None
                    )
            )
                .EntityFailures.Should()
                .BeEmpty();

            // The step deletes a link the case's hold covers. The guard that reads the held rows
            // before and after it finds the link gone, rereads to see why, and refuses the
            // step's work.
            var result = await provider
                .GetRequiredService<IRetentionSweep>()
                .ExecuteAsync(
                    RetentionSweepRequest.Tenanted(Tenant(tenant), AsOf),
                    CancellationToken.None
                );

            result
                .EntityFailures.Should()
                .ContainSingle(failure =>
                    failure.Contains("reason=rows-skipped", StringComparison.Ordinal)
                    && failure.Contains(
                        typeof(HeldWithReadinessEndToEndTests.ReadinessNote).FullName!,
                        StringComparison.Ordinal
                    )
                );
        }

        await AssertEveryStatementIsExplainedAsync<HeldWithReadinessEndToEndTests.HeldWithReadinessDbContext>(
            database,
            roles,
            recording,
            ledger,
            action => WithServicesAsync(services, action),
            runsReachTenantless: true,
            [
                new Reach(RetentionStatementGroup.Sweep, roles.Tenant),
                new Reach(RetentionStatementGroup.Holds, roles.Caller),
                Reach.ForReadiness(roles.Readiness),
            ],
            AnchoredCursor,
            CoveredRowLocks
        );
    }

    // The erasure statements the sample cannot send. An erasure with no cutoff reaches the rows
    // whose anchor is NULL, and a second batch after them continues from the NULL anchor, but none
    // of the sample's entities with an erasure subject has a nullable anchor. And the guard
    // rereads the rows a cut link left behind only for hold relations, which the sample has none
    // of. This model has both: rows with a NULL anchor and a subject, and a hold on a case whose
    // link a note's step deletes while the note is erased.
    private async Task ErasureModelAsync(ReachLedger ledger)
    {
        await using var roles = await ScopeRoles.CreateAsync(fixture.ConnectionString);
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenant = Guid.NewGuid();
        var subject = Guid.NewGuid();
        var heldCase = Guid.NewGuid();
        var collection = new ServiceCollection();
        collection.AddLogging();
        collection.AddSingleton<IConfiguration>(
            new ConfigurationBuilder().AddInMemoryCollection(BatchSizeOne).Build()
        );
        collection.AddDbContext<ErasureModelDbContext>(options => options.UseNpgsql(database.ConnectionString));
        collection.AddSingleton<IRetentionRuleProvider>(new SampleRetentionRuleProvider());
        collection.AddSingleton(RetentionExecutionScope.Unisolated);
        collection.AddCohort<ErasureModelDbContext>();
        collection.AddRetentionDependents<ErasedNote, LinkDeletingErasedNoteStep>();
        roles.Install<ErasureModelDbContext>(collection);
        await using var services = collection.BuildServiceProvider(validateScopes: true);
        await using (var db = new ErasureModelDbContext(
            new DbContextOptionsBuilder<ErasureModelDbContext>().UseNpgsql(database.ConnectionString).Options
        ))
        {
            await db.Database.EnsureCreatedAsync();
            for (var index = 0; index < 3; index++)
            {
                db.Rows.Add(new NullAnchorRow { Id = Guid.NewGuid(), TenantId = tenant, SubjectId = subject });
                db.Notes.Add(
                    new ErasedNote { Id = Guid.NewGuid(), TenantId = tenant, SubjectId = subject, CreatedAt = AsOf.AddDays(-400) }
                );
            }

            db.Cases.Add(new ErasedCase { Id = heldCase, TenantId = tenant, CreatedAt = AsOf });
            db.Links.Add(new ErasedLink { Id = Guid.NewGuid(), TenantId = tenant, CreatedAt = AsOf, CaseId = heldCase });
            await db.SaveChangesAsync();
        }

        await roles.GrantAsync(database);
        var recording = await StartRecordingAsync(database);

        await using (var scope = services.CreateAsyncScope())
        {
            await scope
                .ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                .CreateAsync(
                    new RetentionHoldRequest(
                        Guid.NewGuid(),
                        RetentionEntityIdentity.For<ErasedCase>(),
                        heldCase.ToString(),
                        tenant,
                        "coverage",
                        AsOf
                    ),
                    CancellationToken.None
                );

            // The notes' steps delete the held case's link: the guard rereads it by record id to
            // see why it is gone, and refuses the notes' step. The rows with no anchor erase.
            (
                await scope
                    .ServiceProvider.GetRequiredService<IRetentionErasureService>()
                    .ExecuteAsync(
                        RetentionErasureRequest.Tenanted(Tenant(tenant), new ErasureScope("user", subject), AsOf),
                        CancellationToken.None
                    )
            )
                .EntityFailures.Should()
                .ContainSingle(failure =>
                    failure.Contains("reason=rows-skipped", StringComparison.Ordinal)
                    && failure.Contains(typeof(ErasedNote).FullName!, StringComparison.Ordinal)
                );
        }

        await AssertEveryStatementIsExplainedAsync<ErasureModelDbContext>(
            database,
            roles,
            recording,
            ledger,
            action => WithServicesAsync(services, action),
            runsReachTenantless: false,
            [
                new Reach(RetentionStatementGroup.Erasure, roles.Tenant),
                new Reach(RetentionStatementGroup.Holds, roles.Caller),
                Reach.ForReadiness(roles.Readiness),
            ]
        );
    }

    [Retain("nullable-anchor-purge", nameof(OccurredAt))]
    [RetentionEntityId("7c4e9a1d-2b86-4f03-a5d7-1e8b3c6f9042")]
    private sealed class NullAnchorRow
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }

        [ErasureSubject("user")]
        public Guid? SubjectId { get; set; }

        public DateTimeOffset? OccurredAt { get; set; }
    }

    [Retain("short-lived", nameof(CreatedAt))]
    [RetentionEntityId("a9d2c7e4-5f18-4b63-8e0a-3d6b1f4c7285")]
    private sealed class ErasedCase
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }

    [Retain("short-lived", nameof(CreatedAt))]
    [RetentionEntityId("d4b8f1a6-9c37-4e25-b0d3-6a2e8c5f1974")]
    [HeldWith(typeof(ErasedCase), nameof(CaseId))]
    private sealed class ErasedLink
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public Guid? CaseId { get; set; }
    }

    [Retain("short-lived", nameof(CreatedAt))]
    [RetentionEntityId("e1c5a9d3-7b42-4f86-a8e0-2c9d6b3f5147")]
    private sealed class ErasedNote
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }

        [ErasureSubject("user")]
        public Guid? SubjectId { get; set; }

        public DateTimeOffset CreatedAt { get; set; }
    }

    // On the first note it retires, takes the note out of the erasure's scope (the mutation then
    // changes nothing, and the run asks whether the note is still due); on the others, deletes
    // every link. Its SQL starts with the host comment.
    [RowHandlerId("2f7a4c91-6d38-4e05-b1a9-8c3e5d0f7a62")]
    private sealed class LinkDeletingErasedNoteStep(ErasureModelDbContext db) : IRetentionDependents<ErasedNote>
    {
        private static int calls;

        public async Task RetireAsync(ErasedNote row, RetentionDependentsContext ctx, CancellationToken ct)
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                await db.Database.ExecuteSqlAsync(
                    $"/* host */ UPDATE public.erased_notes SET \"SubjectId\" = NULL WHERE \"Id\" = {row.Id}",
                    ct
                );
                return;
            }

            await db.Database.ExecuteSqlRawAsync("/* host */ DELETE FROM public.erased_links", ct);
        }
    }

    private sealed class ErasureModelDbContext(DbContextOptions<ErasureModelDbContext> options) : DbContext(options)
    {
        public DbSet<NullAnchorRow> Rows => Set<NullAnchorRow>();

        public DbSet<ErasedCase> Cases => Set<ErasedCase>();

        public DbSet<ErasedLink> Links => Set<ErasedLink>();

        public DbSet<ErasedNote> Notes => Set<ErasedNote>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<NullAnchorRow>(entity =>
            {
                entity.ToTable("null_anchor_rows");
                entity.HasKey(row => row.Id);
            });
            modelBuilder.Entity<ErasedCase>(entity => entity.ToTable("erased_cases"));
            modelBuilder.Entity<ErasedLink>(entity => entity.ToTable("erased_links"));
            modelBuilder.Entity<ErasedNote>(entity => entity.ToTable("erased_notes"));
            modelBuilder.ConfigureCohortTables();
        }
    }

    // ── the proof ───────────────────────────────────────────────────────────

    // Six claims, each its own assertion so none can stand in for another:
    //   1. pg_stat_statements evicted nothing while the runs went (it caps its entries at
    //      pg_stat_statements.max, shared by every database, and an evicted statement would drop
    //      out of the comparison below without a word);
    //   2. every retained host table the runs reach shows up in at least one executed statement,
    //      so a table filter that stopped matching can't leave "sent" holding only
    //      canonical-text statements;
    //   3. the record key's canonical-text statements (S11, S18) were executed, as their own
    //      clause;
    //   4. each statement family the group was built to reach is still executed. The family is
    //      read from the statement text the server recorded (a marker the family's builder always
    //      writes), not from an outcome: an outcome like "several batches ran" can hold while the
    //      statement it should prove goes unsent;
    //   5. every statement a scope's role sent (other than transaction control, catalog reads, and
    //      host SQL) was explained under that same role;
    //   6. every builder in each group's enumeration was executed under the role of a scope that
    //      ran the group's operation, counted by builder name and matched by PostgreSQL's query
    //      identifier (the builder's, from EXPLAIN (VERBOSE, FORMAT JSON), against the one
    //      pg_stat_statements recorded for an execution): its per-strategy and per-entity
    //      variants are covered by claim 5, which every sent text must meet. Only the guard's
    //      attribution forms are exempt: they are explained so that a refusal names its entity,
    //      and no operation sends them.
    private async Task AssertEveryStatementIsExplainedAsync<TContext>(
        TemporaryDatabase database,
        ScopeRoles roles,
        Recording recording,
        ReachLedger ledger,
        Func<Func<IServiceProvider, Task>, Task> withServices,
        bool runsReachTenantless,
        IReadOnlyList<Reach> reach,
        params Family[] reached
    )
        where TContext : DbContext
    {
        var hostTables = ModelTables<TContext>(database, _ => true);
        // The retained tables this group's runs reach: a tenantless entity is reached only by a
        // tenantless run or a statement that spans the model.
        var retainedTables = ModelTables<TContext>(
            database,
            type =>
                type.GetCustomAttribute<RetainAttribute>() is not null
                && (
                    runsReachTenantless
                    || type.GetCustomAttribute<RetentionTenantlessAttribute>() is null
                )
        );
        var recorded = await RecordedStatementsAsync(database, roles);
        (await DeallocationsAsync(database))
            .Should()
            .Be(
                recording.Deallocations,
                "pg_stat_statements evicted entries during the runs, so a sent statement or its EXPLAIN may be missing from the view; raise pg_stat_statements.max for the test server"
            );

        var explained = recorded
            .Where(statement => statement.Query.StartsWith(Explain, StringComparison.Ordinal))
            .Select(statement => (statement.Role, Query: statement.Query[Explain.Length..]))
            .ToHashSet();
        // The statements the roles sent, one entry per role and text. pg_stat_statements also
        // lists a statement PostgreSQL only analysed, with no call: EXPLAIN's inner statement, or
        // one that raised an error. Those count as sent for the comparison with readiness's set (a
        // run statement that errors is still one readiness must have explained), but not for the
        // claims that a run reached something: only a statement a run executed (Calls above 0)
        // does, or readiness's own EXPLAIN of a table or family would satisfy them.
        var runStatements = recorded
            .Where(statement => !IsExcluded(statement.Query))
            .GroupBy(statement => (statement.Role, statement.Query, statement.QueryId))
            .Select(group =>
                new Recorded(group.Key.Role, group.Key.Query, group.Sum(statement => statement.Calls), group.Key.QueryId)
            )
            .ToList();
        bool IsOnHostTable(Recorded statement) =>
            hostTables.Any(table => statement.Query.Contains(table, StringComparison.Ordinal));
        // A record key's canonical text (S11, S18) reads no table, but it casts to the model's
        // store type, which the database may not know.
        bool IsCanonicalText(Recorded statement) =>
            statement.Query.StartsWith(CanonicalText, StringComparison.Ordinal);
        var executed = runStatements.Where(statement => statement.Calls > 0).ToList();
        var executedOnHostTables = executed.Where(s => IsOnHostTable(s) || IsCanonicalText(s)).ToList();

        retainedTables
            .Where(table =>
                !executedOnHostTables.Any(statement =>
                    IsOnHostTable(statement)
                    && statement.Query.Contains(table, StringComparison.Ordinal)
                )
            )
            .Should()
            .BeEmpty(
                "every retained host table the runs reach must appear in a statement a run executed"
            );
        executedOnHostTables
            .Where(IsCanonicalText)
            .Should()
            .NotBeEmpty("the runs must have executed the record key's canonical-text statements");
        string.Join(
                "; ",
                reached
                    .Where(family => !executed.Any(statement => family.IsSent(statement.Query)))
                    .Select(family => family.Name)
            )
            .Should()
            .BeEmpty("the group was built to reach these statement families, and no run executed them");
        runStatements
            .Where(statement => !explained.Contains((statement.Role, statement.Query)))
            .Select(statement => $"{roles.Describe(statement.Role)}: {statement.Query}")
            .Should()
            .BeEmpty(
                "readiness must EXPLAIN every statement a scope sends under that scope's role; these were sent and never explained"
            );

        foreach (var item in reach)
        {
            IReadOnlyList<ReadinessStatement> enumerated = [];
            await withServices(async services => enumerated = await EnumerateAsync(services, item));
            var executedHere = executed
                .Where(statement => item.Roles.Contains(statement.Role))
                .Select(statement => statement.QueryId)
                .ToHashSet();
            // The guard's attribution forms are explained so that a refusal names its entity, and
            // no operation sends them.
            foreach (var builder in enumerated
                .Where(statement => statement.Name != "held-row-versions-of-entity")
                .GroupBy(statement => statement.Name))
            {
                // A builder counts as reached when the server recorded an execution with the query
                // id PostgreSQL gives its statement: the id tells a constant from a parameter, so
                // two forms of one builder (`= NULL` and `= @lastError`) are told apart.
                var executedByBuilder = false;
                foreach (var statement in builder)
                {
                    if (executedHere.Contains(await QueryIdAsync(database, statement)))
                    {
                        executedByBuilder = true;
                        break;
                    }
                }

                ledger.Record(item.Label, builder.Key, ReadableSql(builder.First().Sql), executedByBuilder);
            }
        }
    }

    // The query identifier PostgreSQL computes for a statement, from EXPLAIN (VERBOSE, FORMAT JSON)
    // on the superuser connection: the id pg_stat_statements keys the run's execution of the same
    // text and parameter types by. Parameters are bound as readiness binds them. The superuser's
    // statements are not among those the proof judges.
    private static async Task<long> QueryIdAsync(TemporaryDatabase database, ReadinessStatement statement)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"EXPLAIN (VERBOSE, FORMAT JSON) {statement.Sql}",
            connection
        );
        command.Parameters.AddRange(ReadinessParameters.From(statement.Parameters));
        var plan = (string)(await command.ExecuteScalarAsync())!;
        using var document = JsonDocument.Parse(plan);
        return document.RootElement[0].TryGetProperty("Query Identifier", out var id)
            ? id.GetInt64()
            : throw new InvalidOperationException(
                $"PostgreSQL gave no Query Identifier for {statement.Name}; compute_query_id must be on: {statement.Sql}"
            );
    }

    private static string ReadableSql(string sql) =>
        string.Join(' ', sql.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

    // What each model's groups enumerated and what its runs executed, united by group and builder
    // name: a builder counts as reached when some model executed one of its statements under the
    // role of a scope that ran the group's operation.
    private sealed class ReachLedger
    {
        private readonly Dictionary<(string Group, string Builder), (string Text, bool Reached)> builders = [];

        public void Record(string group, string builder, string text, bool reached)
        {
            builders[(group, builder)] = builders.TryGetValue((group, builder), out var seen)
                ? (seen.Text, seen.Reached || reached)
                : (text, reached);
        }

        public IEnumerable<string> Unreached() =>
            builders
                .Where(pair => !pair.Value.Reached)
                .OrderBy(pair => pair.Key.Group, StringComparer.Ordinal)
                .ThenBy(pair => pair.Key.Builder, StringComparer.Ordinal)
                .Select(pair => $"{pair.Key.Group}: {pair.Key.Builder}: {pair.Value.Text}");
    }

    // The readiness statements of one group, as a scope of the host would enumerate them.
    private static async Task<IReadOnlyList<ReadinessStatement>> EnumerateAsync(
        IServiceProvider services,
        Reach item
    )
    {
        var db = services.GetRequiredKeyedService<DbContext>(CohortServiceKeys.DbContext);
        await services.GetRequiredService<RetentionStartupValidator>().ValidateAsync(CancellationToken.None);
        var readiness = new RetentionStatementReadiness(
            db,
            services.GetRequiredService<RetentionRegistry>(),
            services.GetRequiredService<RetentionValidationState>(),
            services.GetRequiredService<Cohort.Infrastructure.Sweep.AnonymiseAssignmentResolver>(),
            services.GetServices<IRetentionHandlerRegistration>(),
            services.GetServices<IRetentionDependentsRegistration>(),
            services.GetRequiredService<IRetentionExecutionSettings>()
        );
        if (item.Group is null)
        {
            return readiness.EnumerateReadiness();
        }

        await db.Database.OpenConnectionAsync();
        try
        {
            return await readiness.EnumerateAsync(
                item.Group,
                services.GetRequiredService<RetentionRuntimeReadinessValidator>().ValidatedCapabilities,
                CancellationToken.None
            );
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }
    }

    private static async Task WithServicesAsync(ServiceProvider services, Func<IServiceProvider, Task> action)
    {
        await using var scope = services.CreateAsyncScope();
        await action(scope.ServiceProvider);
    }

    // Statements the proof doesn't judge: readiness's own EXPLAINs, host SQL, catalog reads, and
    // transaction control.
    private static bool IsExcluded(string query) =>
        query.StartsWith(Explain, StringComparison.Ordinal)
        || query.StartsWith(HostComment, StringComparison.Ordinal)
        || query.StartsWith(CatalogComment, StringComparison.Ordinal)
        || TransactionControl.Any(prefix => query.StartsWith(prefix, StringComparison.OrdinalIgnoreCase));

    // What the server's statements carry for each family, with Npgsql's $n for every parameter.
    private sealed record Family(string Name, Func<string, bool> IsSent);

    private sealed record Recording(long Deallocations);

    // One group's statements, and the roles of the scopes that ran its operation. Readiness is
    // the group of the stored-identities scope, which belongs to no caller.
    private sealed record Reach(RetentionStatementGroup? Group, params string[] Roles)
    {
        public static Reach ForReadiness(params string[] roles) => new(null, roles);

        public string Label => Group is null ? "Readiness" : Group.Kind.ToString();
    }

    // S5: the keyset after the first batch, `(anchor, record id) > ($n, $n)`.
    private static readonly Family AnchoredCursor = new(
        "S5, discovery's anchored cursor (a second batch)",
        query => query.Contains(") > ($", StringComparison.Ordinal)
    );

    // S4: an Exempt erasure only counts the subject's rows, so the count has the subject and the
    // tenant and no anchor or eligibility test (the other counts all mention the anchor).
    private static readonly Family SubjectCount = new(
        "S4, an Exempt erasure's count of the subject's rows",
        query =>
            query.StartsWith("SELECT pg_catalog.count(*) FROM ", StringComparison.Ordinal)
            && query.Contains("\"SubjectId\" = CAST(", StringComparison.Ordinal)
            && !query.Contains("\"CreatedAt\"", StringComparison.Ordinal)
    );

    // S10: a dependents step's still-due check, the only statement that is a bare SELECT EXISTS.
    private static readonly Family StillDue = new(
        "S10, the dependents step's still-due check",
        query => query.StartsWith("SELECT EXISTS (", StringComparison.Ordinal)
    );

    // S12: the worker's tenant-coverage scan, the only SELECT DISTINCT of a tenant column.
    private static readonly Family TenantCoverage = new(
        "S12, the tenant-coverage scan",
        query => query.StartsWith("SELECT DISTINCT target.", StringComparison.Ordinal)
    );

    // S14: the covered-row locks of a hold on a row with hold paths. S13's lock of the held row is a
    // plain FOR SHARE.
    private static readonly Family CoveredRowLocks = new(
        "S14, the covered-row locks",
        query => query.Contains("FOR SHARE OF target", StringComparison.Ordinal)
    );

    // The quoted, schema-qualified names of the model's own tables (not Cohort's), as Cohort's
    // statements spell them, for the entity types that pass include.
    private static List<string> ModelTables<TContext>(
        TemporaryDatabase database,
        Func<Type, bool> include
    )
        where TContext : DbContext
    {
        using var db = (TContext)
            Activator.CreateInstance(
                typeof(TContext),
                new DbContextOptionsBuilder<TContext>().UseNpgsql(database.ConnectionString).Options
            )!;
        var cohort = CohortStoreTables.FromModel(db.Model);
        var cohortTables = new[]
        {
            cohort.RetentionHolds,
            cohort.SweepRun,
            cohort.SweepRunEntitySummary,
            cohort.SweepRunRowDetail,
            cohort.SweepRowHandlerStatus,
        };
        return
        [
            .. db
                .Model.GetEntityTypes()
                .Where(entityType => entityType.GetTableName() is not null)
                .Where(entityType => include(entityType.ClrType))
                .Select(entityType => new RelationalObjectName(
                    entityType.GetSchema() ?? "public",
                    entityType.GetTableName()!
                ))
                .Where(table => !cohortTables.Contains(table))
                .Select(PostgreSqlIdentifier.Format),
        ];
    }

    // Resets the whole view, not only this database's entries: pg_stat_statements.max (5000 by
    // default) is shared by every temporary database in the suite, and the Integration collection
    // runs one test at a time, so nothing else is writing to it. Then notes how many times the
    // view has evicted entries, so the proof can fail loudly if it evicts during the runs.
    private static async Task<Recording> StartRecordingAsync(TemporaryDatabase database)
    {
        await ExecuteAsync(database, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        await ExecuteAsync(database, "SELECT pg_stat_statements_reset()");
        return new Recording(await DeallocationsAsync(database));
    }

    private static async Task<long> DeallocationsAsync(TemporaryDatabase database)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT dealloc FROM pg_stat_statements_info",
            connection
        );
        return (long)(await command.ExecuteScalarAsync())!;
    }

    // What the scope roles ran in this database: the connecting superuser's own statements
    // (type loading, setup, the test's SQL) are not in it.
    private static async Task<List<Recorded>> RecordedStatementsAsync(
        TemporaryDatabase database,
        ScopeRoles roles
    )
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT role.rolname, statement.query, statement.calls, statement.queryid
            FROM pg_stat_statements AS statement
            JOIN pg_roles AS role ON role.oid = statement.userid
            WHERE statement.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
              AND role.rolname = ANY(@roles)
            """,
            connection
        );
        command.Parameters.AddWithValue("roles", roles.Names.ToArray());
        var statements = new List<Recorded>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            statements.Add(
                new Recorded(reader.GetString(0), reader.GetString(1), reader.GetInt64(2), reader.GetInt64(3))
            );
        }

        return statements;
    }

    private sealed record Recorded(string Role, string Query, long Calls, long QueryId);

    // ── the sample model ────────────────────────────────────────────────────

    private static readonly Dictionary<string, string?> BatchSizeOne = new()
    {
        [$"{CohortOptions.SectionName}:SweepBatchSize"] = "1",
    };

    // Both retentions and a schedule, so pruning reads the clock for the current occurrence.
    private static Dictionary<string, string?> WithPruning(Dictionary<string, string?> settings) =>
        new(settings)
        {
            [$"{CohortOptions.SectionName}:Schedule"] = Schedule,
            [$"{CohortOptions.SectionName}:HistoryPruning:SucceededRunRetention"] = "1.00:00:00",
            [$"{CohortOptions.SectionName}:HistoryPruning:FailedRunRetention"] = "1.00:00:00",
            [$"{CohortOptions.SectionName}:HistoryPruning:InactiveHoldRetention"] = "1.00:00:00",
        };

    private async Task<TemporaryDatabase> MigratedSampleDatabaseAsync()
    {
        var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var db = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>()
                .UseNpgsql(database.ConnectionString)
                .Options
        );
        await db.Database.MigrateAsync();
        return database;
    }

    // Three due rows of each entity that carries an erasure subject, per tenant, so a batch size
    // of 1 reaches every cursor, and one tenantless set.
    private static async Task SeedAsync(TemporaryDatabase database, CoverageTenants tenants)
    {
        await using var db = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>()
                .UseNpgsql(database.ConnectionString)
                .Options
        );
        foreach (var tenantId in tenants.All)
        {
            // A recent note no run reaches: the hold and the deletion check use it.
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = tenants.Subject,
                    CreatedAt = AsOf,
                    Body = "recent",
                }
            );
            for (var index = 0; index < 3; index++)
            {
                var created = AsOf.AddDays(-400 - index);
                db.Notes.Add(
                    new Note
                    {
                        Id = Guid.NewGuid(),
                        TenantId = tenantId,
                        SubjectId = tenants.Subject,
                        PersonId = tenants.Subject,
                        CreatedAt = created,
                        Body = "note",
                    }
                );
                db.SoftDeleteRecords.Add(
                    new SoftDeleteRecord
                    {
                        Id = Guid.NewGuid(),
                        TenantId = tenantId,
                        SubjectId = tenants.Subject,
                        CreatedAt = created,
                        Body = "record",
                    }
                );
                db.AnonymisedContacts.Add(
                    new AnonymisedContact
                    {
                        Id = Guid.NewGuid(),
                        TenantId = tenantId,
                        SubjectId = tenants.Subject,
                        CreatedAt = created,
                        EmailAddress = "a@example.com",
                        GivenName = "Given",
                        Surname = "Surname",
                        Notes = "notes",
                    }
                );
                db.TombstoneRecords.Add(
                    new TombstoneRecord
                    {
                        Id = Guid.NewGuid(),
                        TenantId = tenantId,
                        SubjectId = tenants.Subject,
                        CreatedAt = created,
                        ExternalId = Guid.NewGuid(),
                        DisplayName = "Display",
                        ContactEmail = "c@example.com",
                        Notes = "notes",
                    }
                );
                db.PerRowAuditedLogs.Add(
                    new PerRowAuditedLog
                    {
                        Id = Guid.NewGuid(),
                        TenantId = tenantId,
                        CreatedAt = created,
                        Payload = "audited",
                    }
                );
                db.SoftDeletedContacts.Add(
                    new SoftDeletedContact
                    {
                        Id = Guid.NewGuid(),
                        TenantId = tenantId,
                        CreatedAt = created,
                        EmailAddress = "s@example.com",
                    }
                );
            }
        }

        for (var index = 0; index < 3; index++)
        {
            db.TenantlessLogs.Add(
                new TenantlessLog
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = AsOf.AddDays(-400 - index),
                    Payload = "log",
                    SubjectId = tenants.Subject,
                }
            );
        }

        // Two recent logs no run reaches, for the hold and the deletion over a tenantless entity.
        for (var index = 0; index < 2; index++)
        {
            db.TenantlessLogs.Add(
                new TenantlessLog { Id = Guid.NewGuid(), CreatedAt = AsOf, Payload = "recent" }
            );
        }

        await db.SaveChangesAsync();
    }

    // A settled run past the pruning retention, with its summary and row detail, and an inactive
    // hold past it too.
    private static async Task SeedPrunableHistoryAsync(TemporaryDatabase database)
    {
        var sweepId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var at = DateTimeOffset.UtcNow.AddDays(-100);
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO "sweep_run"
                ("SweepId", "StartedAt", "Status", "SettledAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected")
            VALUES (@sweepId, @at, @succeeded, @at, INTERVAL '1 minute', @manual, FALSE, @tenantId, 1);
            INSERT INTO "sweep_run_entity_summary"
                ("SweepId", "At", "EntityType", "RetentionEntityId", "Category", "TenantId", "Strategy",
                 "ResolvedMonths", "ResolvedDays", "Affected", "HeldCount", "SkippedCount", "NullAnchorCount",
                 "RetainedCount", "DeferredCount", "BlockedCount")
            VALUES (@sweepId, @at, 'Cohort.Sample.Entities.Note', @entityId, 'short-lived', @tenantId, 0,
                    0, 30, 1, 0, 0, 0, 0, 0, 0);
            INSERT INTO "sweep_run_row_detail"
                ("SweepId", "At", "EntityType", "RetentionEntityId", "RecordId", "Category", "Strategy", "TenantId")
            VALUES (@sweepId, @at, 'Cohort.Sample.Entities.Note', @entityId, @recordId, 'short-lived', 0, @tenantId);
            INSERT INTO "retention_holds"
                ("HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt")
            VALUES (gen_random_uuid(), @entityId, @recordId, @tenantId, 'pruning', @at - INTERVAL '1 day', NULL, @at);
            """,
            connection
        );
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("at", at);
        command.Parameters.AddWithValue("succeeded", (int)SweepRunStatus.Succeeded);
        command.Parameters.AddWithValue("manual", (int)SweepTriggerKind.Manual);
        command.Parameters.AddWithValue("tenantId", tenantId);
        command.Parameters.AddWithValue("entityId", Note.RetentionIdentity);
        command.Parameters.AddWithValue("recordId", Guid.NewGuid().ToString());
        await command.ExecuteNonQueryAsync();
    }

    // A dispatcher that died: a run left Started well past SweepSettleTimeout, with no session
    // holding its lock.
    private static Task SeedStaleStartedRunAsync(TemporaryDatabase database) =>
        ExecuteAsync(
            database,
            $"""
            INSERT INTO public.sweep_run
                ("SweepId", "StartedAt", "Status", "SettledAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected")
            VALUES (gen_random_uuid(), now() - INTERVAL '3 hours', {(int)SweepRunStatus.Started}, NULL, NULL,
                    {(int)SweepTriggerKind.Manual}, FALSE, gen_random_uuid(), 0)
            """
        );

    // Marks the pending handler status `skip` places in from the oldest as claimed by a
    // dispatcher that died, `attempt` deliveries spent.
    private static Task AbandonClaimAsync(
        TemporaryDatabase database,
        int skip,
        int attempt,
        DateTimeOffset claimedAt
    ) =>
        ExecuteAsync(
            database,
            $"""
            UPDATE public.sweep_row_handler_status
            SET "State" = {(int)SweepRowHandlerDispatchState.InFlight},
                "ClaimedAt" = '{claimedAt:O}', "ClaimToken" = gen_random_uuid(), "Attempt" = {attempt}
            WHERE "Id" = (
                SELECT "Id" FROM public.sweep_row_handler_status
                WHERE "State" = {(int)SweepRowHandlerDispatchState.Pending}
                ORDER BY "Id" OFFSET {skip} LIMIT 1
            )
            """
        );

    private static async Task<List<SweepRowHandlerDispatchState>> HandlerStatesAsync(TemporaryDatabase database)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """SELECT "State" FROM public.sweep_row_handler_status""",
            connection
        );
        var states = new List<SweepRowHandlerDispatchState>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            states.Add((SweepRowHandlerDispatchState)reader.GetInt32(0));
        }

        return states;
    }

    private static async Task<Guid> FirstNoteIdAsync(TemporaryDatabase database, Guid tenantId)
    {
        await using var db = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>()
                .UseNpgsql(database.ConnectionString)
                .Options
        );
        return await db
            .Notes.Where(note => note.TenantId == tenantId)
            .OrderByDescending(note => note.CreatedAt)
            .Select(note => note.Id)
            .FirstAsync();
    }

    private static async Task<(Guid Held, Guid Free)> TwoTenantlessLogIdsAsync(TemporaryDatabase database)
    {
        await using var db = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>()
                .UseNpgsql(database.ConnectionString)
                .Options
        );
        var ids = await db.TenantlessLogs.Where(log => log.Payload == "recent").Select(log => log.Id).Take(2).ToListAsync();
        return (ids[0], ids[1]);
    }

    private static TenantContext Tenant(Guid id) => new(id, "uk", new Dictionary<string, string>());

    private static Task<RetentionSweepResult> SweepAsync(
        CohortTestHost host,
        Guid tenantId,
        bool dryRun
    ) =>
        host.RunWithServicesAsync(services =>
            services
                .GetRequiredService<IRetentionSweep>()
                .ExecuteAsync(
                    RetentionSweepRequest.Tenanted(Tenant(tenantId), AsOf, dryRun),
                    CancellationToken.None
                )
        );

    private static async Task EraseAsync(
        CohortTestHost host,
        Guid tenantId,
        string kind,
        Guid subject
    )
    {
        var result = await host.RunErasureAsync(
            Tenant(tenantId),
            new ErasureScope(kind, subject, allowSoftDeleteAsErasure: true),
            AsOf
        );
        result.EntityFailures.Should().BeEmpty();
    }

    // Every tenant the plain runs use. Three kinds of rule differ by tenant: the one with a legal
    // minimum (an erasure has a cutoff), the one without (it has none), and the one whose
    // tombstone category resolves to Exempt (an erasure only counts the subject's rows). One more
    // resolves the soft-delete-or-anonymise category to Anonymise, which the sample resolves to
    // soft delete for the rest.
    private sealed class CoverageTenants
    {
        public Guid Subject { get; } = Guid.NewGuid();
        public Guid Swept { get; } = Guid.NewGuid();
        public Guid WithLegalMinimum { get; } = Guid.NewGuid();
        public Guid WithoutLegalMinimum { get; } = Guid.NewGuid();
        public Guid Exempted { get; } = Guid.NewGuid();
        public Guid Held { get; } = Guid.NewGuid();
        public Guid Anonymising { get; } = Guid.NewGuid();
        public Guid CallerErased { get; } = Guid.NewGuid();
        public Guid Previewed { get; } = Guid.NewGuid();

        public IReadOnlyList<Guid> All =>
            [Swept, WithLegalMinimum, WithoutLegalMinimum, Exempted, Held, Anonymising, CallerErased];
    }

    private sealed class FixedTenantSource(IReadOnlyList<Guid> tenantIds) : IRetentionTenantSource
    {
        public Task<IReadOnlyList<TenantContext>> GetTenantsAsync(CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<TenantContext>>([.. tenantIds.Select(Tenant)]);
    }

    private sealed class CoverageRuleProvider(CoverageTenants tenants) : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == "tombstone-anonymise"
                ? new RetentionCategoryCapabilities([Strategy.Anonymise, Strategy.Exempt])
                : inner.GetCapabilities(category);

        public async Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            var rule = await inner.ResolveAsync(context, ct);
            if (rule is null)
            {
                return null;
            }

            if (context.Category == "tombstone-anonymise" && context.Tenant.Id == tenants.Exempted)
            {
                return new RetentionRule(rule.Period, Strategy.Exempt);
            }

            if (context.Category == "soft-delete-or-anonymise" && context.Tenant.Id == tenants.Anonymising)
            {
                return new RetentionRule(rule.Period, Strategy.Anonymise);
            }

            return context.Tenant.Id == tenants.WithLegalMinimum
                ? new RetentionRule(rule.Period, rule.Strategy, new RetentionPeriod(Days: 1))
                : rule;
        }
    }

    // Counts deliveries so that exactly the first one fails, and can make every delivery wait
    // until its flush is cancelled.
    private sealed class CoverageHandlerSink
    {
        private int deliveries;

        public int Failures { get; private set; }

        public bool BlockDeliveries { get; set; }

        public bool FailNextBefore { get; set; }

        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool FailThisDelivery()
        {
            if (Interlocked.Increment(ref deliveries) != 1)
            {
                return false;
            }

            Failures++;
            return true;
        }
    }

    [RowHandlerId("b8e1c4a7-3d26-4f59-9a0e-7c5d2e8f1b34")]
    private sealed class CoverageNoteHandler(CoverageHandlerSink sink) : IRetentionHandler<Note>
    {
        public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
        {
            if (sink.FailNextBefore)
            {
                sink.FailNextBefore = false;
                throw new InvalidOperationException("OnBefore fails");
            }

            ctx.Snapshot["body"] = row.Body;
            return Task.CompletedTask;
        }

        public async Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
        {
            if (sink.BlockDeliveries)
            {
                sink.Started.TrySetResult();
                await Task.Delay(Timeout.Infinite, ct);
            }

            if (sink.FailThisDelivery())
            {
                throw new InvalidOperationException("the first delivery fails");
            }
        }
    }

    // On the first note it reaches, moves the note out of scope (it is no longer due); the
    // others it leaves alone. Its SQL starts with the host comment.
    [RowHandlerId("5d9a2e6c-1b73-4a84-8c0f-3e7b9d1a4f26")]
    private sealed class NoteStep(SampleDbContext db) : IRetentionDependents<Note>
    {
        private static int calls;

        public async Task RetireAsync(
            Note row,
            RetentionDependentsContext ctx,
            CancellationToken ct
        )
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                await db.Database.ExecuteSqlAsync(
                    $"/* host */ UPDATE public.notes SET \"CreatedAt\" = now() WHERE \"Id\" = {row.Id}",
                    ct
                );
            }
        }
    }

    // ── the hold relation model ─────────────────────────────────────────────

    private static readonly Guid CaseIdentity = new("00000000-0000-0000-0005-000000000001");
    private static readonly Guid LinkIdentity = new("00000000-0000-0000-0005-000000000002");
    private static readonly Guid SubmissionIdentity = new("00000000-0000-0000-0005-000000000003");
    private static readonly Guid NoteIdentity = new("00000000-0000-0000-0005-000000000004");

    private static class HeldWithIds
    {
        public static readonly Guid Case = Guid.NewGuid();
        public static readonly Guid Link = Guid.NewGuid();
        public static readonly Guid Submission = Guid.NewGuid();
    }

    private static async Task EnsureCreatedAsync(TemporaryDatabase database)
    {
        await using var db = new HeldWithReadinessEndToEndTests.HeldWithReadinessDbContext(
            new DbContextOptionsBuilder<HeldWithReadinessEndToEndTests.HeldWithReadinessDbContext>()
                .UseNpgsql(database.ConnectionString)
                .Options
        );
        await db.Database.EnsureCreatedAsync();
    }

    // The case under the hold, its link and the submission it references, none due; and three due
    // rows of every entity beside them, so the sweep reaches every cursor and the step has a
    // note to run on.
    private static async Task SeedHeldWithAsync(TemporaryDatabase database, Guid tenant)
    {
        var recent = AsOf.AddDays(-1);
        await ExecuteAsync(
            database,
            $"""
            INSERT INTO public.held_with_submissions ("Id", "TenantId", "CreatedAt")
            VALUES ('{HeldWithIds.Submission}', '{tenant}', '{recent:O}');
            INSERT INTO public.held_with_cases ("Id", "TenantId", "CreatedAt", "SourceSubmissionId")
            VALUES ('{HeldWithIds.Case}', '{tenant}', '{recent:O}', '{HeldWithIds.Submission}');
            INSERT INTO public.held_with_links ("Id", "TenantId", "CreatedAt", "CaseId")
            VALUES ('{HeldWithIds.Link}', '{tenant}', '{recent:O}', '{HeldWithIds.Case}');
            """
        );
        for (var index = 0; index < 3; index++)
        {
            var due = AsOf.AddDays(-400 - index);
            var note = $"note-{index}";
            await ExecuteAsync(
                database,
                $"""
                INSERT INTO public.held_with_submissions ("Id", "TenantId", "CreatedAt")
                VALUES (gen_random_uuid(), '{tenant}', '{due:O}');
                INSERT INTO public.held_with_cases ("Id", "TenantId", "CreatedAt", "SourceSubmissionId")
                VALUES (gen_random_uuid(), '{tenant}', '{due:O}', NULL);
                INSERT INTO public.held_with_links ("Id", "TenantId", "CreatedAt", "CaseId")
                VALUES (gen_random_uuid(), '{tenant}', '{due:O}', NULL);
                INSERT INTO public.held_with_notes ("Id", "TenantId", "CreatedAt")
                VALUES ('{note}', '{tenant}', '{due:O}');
                INSERT INTO public.held_with_note_links ("Id", "TenantId", "CreatedAt", "NoteId")
                VALUES (gen_random_uuid(), '{tenant}', '{due:O}', '{note}');
                """
            );
        }
    }

    // The hold relation model's category, with a per-row audit, so every run writes a row detail
    // (D1, which only the rule's AuditRowDetail asks for).
    private sealed class PerRowAuditedHeldWithRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == "held-with-readiness" ? new RetentionCategoryCapabilities([Strategy.Purge]) : null;

        public Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct) =>
            Task.FromResult<RetentionRule?>(
                context.Category == "held-with-readiness"
                    ? new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge, AuditRowDetail: AuditRowDetail.PerRow)
                    : null
            );
    }

    // Deletes every link when it retires a note: a host's step reaching a row a covered hold path
    // protects. Its SQL starts with the host comment.
    [RowHandlerId("e3a7c1d9-5f48-4b02-a6d1-8c4e0b2f7a95")]
    private sealed class LinkDeletingNoteStep(
        HeldWithReadinessEndToEndTests.HeldWithReadinessDbContext db
    ) : IRetentionDependents<HeldWithReadinessEndToEndTests.ReadinessNote>
    {
        public Task RetireAsync(
            HeldWithReadinessEndToEndTests.ReadinessNote row,
            RetentionDependentsContext ctx,
            CancellationToken ct
        ) => db.Database.ExecuteSqlRawAsync("/* host */ DELETE FROM public.held_with_links", ct);
    }

    private static async Task ExecuteAsync(TemporaryDatabase database, string sql)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
}
