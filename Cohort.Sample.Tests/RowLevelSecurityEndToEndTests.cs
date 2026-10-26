using System.Collections.Concurrent;
using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Handlers;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Npgsql;

namespace Cohort.Sample.Tests;

// ─── End-to-end test (copied from PurgeSweepStrategyTests) ──────────────────
//
// Real PostgreSQL row-level security over a host table, a tenantless table and Cohort's five
// tables, applied the way a host would: an EF connection interceptor reads whose work is
// being done and sets the role and tenant when the connection opens. The host's
// IRetentionExecutionScope sets it in an AsyncLocal around the work (a caller's own scope sets
// it on the DbContext). The connecting user is a superuser, so the System context (RESET ROLE)
// is the only way past the policies; a connection opened outside any run work gets the tenant
// role with no tenant and sees nothing.
// ────────────────────────────────────────────────────────────────────────────

[Collection("Integration")]
public sealed class RowLevelSecurityEndToEndTests(PostgresFixture fixture)
{
    private const string TenantRole = "cohort_rls_tenant";
    private static readonly Guid LogEntityId = Guid.Parse("6d1c2a9b-58e4-4f0a-b1d7-3c9e8f2a4b60");
    private static readonly Guid NoteEntityId = Guid.Parse("0b6f3c8e-2f43-4d55-9a57-6a3d0f1c7e21");
    private static readonly DateTimeOffset AsOf = new(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

    private static readonly RetentionExecutionContext WorkerCoordination =
        new RetentionExecutionContext.System(RetentionSystemWork.WorkerCoordination);
    private static readonly RetentionExecutionContext TenantlessPass =
        new RetentionExecutionContext.System(RetentionSystemWork.TenantlessPass);
    private static readonly RetentionExecutionContext RowDispatch =
        new RetentionExecutionContext.System(RetentionSystemWork.RowDispatch);
    private static readonly RetentionExecutionContext HistoryPruning =
        new RetentionExecutionContext.System(RetentionSystemWork.HistoryPruning);
    private static readonly RetentionExecutionContext Readiness =
        new RetentionExecutionContext.System(RetentionSystemWork.Readiness);

    [Fact]
    public async Task Worker_Sweeps_Each_Tenant_As_That_Tenant()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        await CreateSchemaWithPoliciesAsync(database.ConnectionString);
        await SeedAsync(
            database.ConnectionString,
            [Note(tenantA, "a-expired"), Note(tenantB, "b-expired")],
            [Log("log-expired")]
        );
        var entries = new EntryLog();
        using var host = BuildHost(
            database.ConnectionString,
            services =>
            {
                services.AddSingleton(entries);
                services.AddScoped<IRetentionExecutionScope, RecordingExecutionScope>();
                services.AddScoped<IRetentionTenantSource>(_ => new FixedTenantSource([tenantA, tenantB]));
            }
        );

        await GetWorker(host).RunIterationAsync((await DatabaseNowAsync(database.ConnectionString)).AddSeconds(-1), dryRun: false, CancellationToken.None);

        entries.Ran.Should().Equal(
            WorkerCoordination,
            Readiness,
            new RetentionExecutionContext.Tenant(tenantA),
            new RetentionExecutionContext.Tenant(tenantB),
            TenantlessPass
        );
        (await LoadNoteBodiesAsync(database.ConnectionString)).Should().BeEmpty();
        (await LoadLogPayloadsAsync(database.ConnectionString)).Should().BeEmpty();
        (await LoadRunsAsync(database.ConnectionString)).Should().BeEquivalentTo([
            new RunRow(tenantA, SweepRunStatus.Succeeded),
            new RunRow(tenantB, SweepRunStatus.Succeeded),
            new RunRow(Guid.Empty, SweepRunStatus.Succeeded),
        ]);
    }

    [Fact]
    public async Task Worker_Finds_A_Tenant_Missing_From_The_Source_Across_Tenants_And_Records_It_As_That_Tenant()
    {
        // The coverage check runs as System(WorkerCoordination): as a tenant it would see only
        // that tenant's rows and find nothing missing.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        await CreateSchemaWithPoliciesAsync(database.ConnectionString);
        await SeedAsync(database.ConnectionString, [Note(tenantA, "a-expired"), Note(tenantB, "b-expired")], []);
        var entries = new EntryLog();
        using var host = BuildHost(
            database.ConnectionString,
            services =>
            {
                services.AddSingleton(entries);
                services.AddScoped<IRetentionExecutionScope, RecordingExecutionScope>();
                services.AddScoped<IRetentionTenantSource>(_ => new FixedTenantSource([tenantA]));
            }
        );

        await GetWorker(host).RunIterationAsync((await DatabaseNowAsync(database.ConnectionString)).AddSeconds(-1), dryRun: false, CancellationToken.None);

        entries.Ran.Should().Contain(new RetentionExecutionContext.Tenant(tenantB));
        (await LoadNoteBodiesAsync(database.ConnectionString)).Should().Equal("b-expired");
        (await LoadRunsAsync(database.ConnectionString)).Should().BeEquivalentTo([
            new RunRow(tenantA, SweepRunStatus.Succeeded),
            new RunRow(tenantB, SweepRunStatus.Failed),
            new RunRow(Guid.Empty, SweepRunStatus.Succeeded),
        ]);
    }

    [Fact]
    public async Task Policies_Bite_Outside_Any_Scopes_Work()
    {
        // Guard for every test here: an unisolated host under these policies runs every scope
        // as the tenant role with no tenant, which sees and changes nothing.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenantA = Guid.NewGuid();
        await CreateSchemaWithPoliciesAsync(database.ConnectionString);
        await SeedAsync(database.ConnectionString, [Note(tenantA, "a-expired")], [Log("log-expired")]);
        using var host = BuildHost(
            database.ConnectionString,
            services =>
            {
                services.AddSingleton(RetentionExecutionScope.Unisolated);
                services.AddScoped<IRetentionTenantSource>(_ => new FixedTenantSource([tenantA]));
            }
        );

        await GetWorker(host).RunIterationAsync((await DatabaseNowAsync(database.ConnectionString)).AddSeconds(-1), dryRun: false, CancellationToken.None);

        (await LoadNoteBodiesAsync(database.ConnectionString)).Should().Equal("a-expired");
        (await LoadLogPayloadsAsync(database.ConnectionString)).Should().Equal("log-expired");
        (await LoadRunsAsync(database.ConnectionString)).Should().BeEmpty();
    }

    [Fact]
    public async Task Direct_Ports_Run_As_The_Requested_Tenant_Or_The_Tenantless_Pass()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var subject = Guid.NewGuid();
        await CreateSchemaWithPoliciesAsync(database.ConnectionString);
        await SeedAsync(
            database.ConnectionString,
            [
                Note(tenantA, "a-expired"),
                Note(tenantA, "a-subject", AsOf.AddDays(-1), subject),
                Note(tenantB, "b-expired"),
                Note(tenantB, "b-subject", AsOf.AddDays(-1), subject),
            ],
            [Log("log-expired")]
        );
        var entries = new EntryLog();
        using var host = BuildHost(
            database.ConnectionString,
            services =>
            {
                services.AddSingleton(entries);
                services.AddScoped<IRetentionExecutionScope, RecordingExecutionScope>();
            }
        );
        var services = host.Services;
        var tenant = Tenant(tenantA);

        var tenantPreview = await services.GetRequiredService<IRetentionPreview>().PreviewAsync(tenant, AsOf);
        var tenantlessPreview = await services
            .GetRequiredService<IRetentionPreview>()
            .ExecuteAsync(RetentionPreviewRequest.Tenantless(AsOf));
        await services.GetRequiredService<IRetentionSweep>().ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, AsOf));
        await services.GetRequiredService<IRetentionSweep>().ExecuteAsync(RetentionSweepRequest.Tenantless(AsOf));
        await using (var callerScope = services.CreateAsyncScope())
        {
            await callerScope.ServiceProvider
                .GetRequiredService<IRetentionErasureService>()
                .ExecuteAsync(RetentionErasureRequest.Tenanted(tenant, new ErasureScope("person", subject), AsOf));
        }

        // The first port's readiness checks holds across tenants in its own System scope, once.
        entries.Ran.Should().Equal(
            new RetentionExecutionContext.Tenant(tenantA),
            Readiness,
            TenantlessPass,
            new RetentionExecutionContext.Tenant(tenantA),
            TenantlessPass,
            new RetentionExecutionContext.Tenant(tenantA)
        );
        tenantPreview.Counts.Should().ContainSingle().Which.Affected.Should().Be(1);
        tenantlessPreview.Counts.Should().ContainSingle().Which.Affected.Should().Be(1);
        (await LoadNoteBodiesAsync(database.ConnectionString)).Should().Equal("b-expired", "b-subject");
        (await LoadLogPayloadsAsync(database.ConnectionString)).Should().BeEmpty();
    }

    [Fact]
    public async Task Direct_Sweep_Run_As_Another_Tenant_Cannot_Touch_The_Requested_Tenants_Rows()
    {
        // The hook, not Cohort's own tenant filter, is what scopes the work: running it as the
        // wrong tenant leaves the requested tenant's rows out of reach.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        await CreateSchemaWithPoliciesAsync(database.ConnectionString);
        await SeedAsync(database.ConnectionString, [Note(tenantA, "a-expired")], []);
        var entries = new EntryLog();
        using var host = BuildHost(
            database.ConnectionString,
            services => services.AddScoped<IRetentionExecutionScope>(sp =>
                new WrongTenantExecutionScope(entries, tenantB)
            )
        );

        var sweep = () => host.Services.GetRequiredService<IRetentionSweep>().ExecuteAsync(RetentionSweepRequest.Tenanted(Tenant(tenantA), AsOf));

        (await sweep.Should().ThrowAsync<PostgresException>())
            .Which.SqlState.Should().Be(PostgresErrorCodes.InsufficientPrivilege);
        entries.Ran.Should().Equal(new RetentionExecutionContext.Tenant(tenantA), Readiness);
        (await LoadNoteBodiesAsync(database.ConnectionString)).Should().Equal("a-expired");
        (await LoadRunsAsync(database.ConnectionString)).Should().BeEmpty();
    }

    [Fact]
    public async Task Caller_Transaction_Erasure_Runs_As_The_Callers_Tenant_And_Cannot_Erase_Another_Tenants_Rows()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var subject = Guid.NewGuid();
        await CreateSchemaWithPoliciesAsync(database.ConnectionString);
        await SeedAsync(
            database.ConnectionString,
            [Note(tenantA, "a-subject", AsOf.AddDays(-1), subject), Note(tenantB, "b-subject", AsOf.AddDays(-1), subject)],
            []
        );
        var entries = new EntryLog();
        using var host = BuildHost(
            database.ConnectionString,
            services =>
            {
                services.AddSingleton(entries);
                services.AddScoped<IRetentionExecutionScope, RecordingExecutionScope>();
            }
        );

        // The host's own request scope, already working as tenant A; Cohort joins it.
        async Task<ErasureResult> EraseInCallerScopeAsync(Guid requestedTenant)
        {
            await using var scope = host.Services.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<RlsDbContext>();
            db.Access = new RetentionExecutionContext.Tenant(tenantA);
            await using var transaction = await db.Database.BeginTransactionAsync();
            var result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                RetentionErasureRequest.Tenanted(
                    Tenant(requestedTenant),
                    new ErasureScope("person", subject),
                    AsOf,
                    ErasureTransaction.Caller
                )
            );
            await transaction.CommitAsync();
            return result;
        }

        var own = await EraseInCallerScopeAsync(tenantA);
        var other = () => EraseInCallerScopeAsync(tenantB);

        own.Counts.Should().ContainSingle(count => count.EntityType == typeof(RlsNote)).Which.Affected.Should().Be(1);
        (await other.Should().ThrowAsync<PostgresException>())
            .Which.SqlState.Should().Be(PostgresErrorCodes.InsufficientPrivilege);
        (await LoadNoteBodiesAsync(database.ConnectionString)).Should().Equal("b-subject");
        (await LoadRunsAsync(database.ConnectionString)).Should().BeEquivalentTo([
            new RunRow(tenantA, SweepRunStatus.Succeeded),
        ]);
        // The caller's scope carries its own tenant: without handlers, Cohort never ran its work
        // through the hook.
        entries.Ran.Should().NotContain(context => context is RetentionExecutionContext.Tenant);
    }

    [Fact]
    public async Task Caller_Transaction_Erasure_Runs_Each_Handled_Batch_Through_The_Hook_On_The_Callers_Connection()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenantA = Guid.NewGuid();
        var subject = Guid.NewGuid();
        await CreateSchemaWithPoliciesAsync(database.ConnectionString);
        await SeedAsync(
            database.ConnectionString,
            [Note(tenantA, "a-subject", AsOf.AddDays(-1), subject), Note(tenantA, "a-other", AsOf.AddDays(-1))],
            []
        );
        var entries = new EntryLog();
        var observations = new HandlerObservations();
        using var host = BuildHost(
            database.ConnectionString,
            services =>
            {
                services.AddSingleton(entries);
                services.AddSingleton(observations);
                services.AddScoped<IRetentionExecutionScope, RecordingExecutionScope>();
                services.AddRowHandler<RlsNote, SessionObservingNoteHandler>();
            }
        );

        await using (var scope = host.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<RlsDbContext>();
            db.Access = new RetentionExecutionContext.Tenant(tenantA);
            await using var transaction = await db.Database.BeginTransactionAsync();
            // Only the caller's connection, inside its transaction, carries this.
            await db.Database.ExecuteSqlRawAsync("SELECT set_config('app.request', 'caller-request', true)");
            var result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().ExecuteAsync(
                RetentionErasureRequest.Tenanted(
                    Tenant(tenantA),
                    new ErasureScope("person", subject),
                    AsOf,
                    ErasureTransaction.Caller
                )
            );
            await transaction.CommitAsync();
            result.Counts.Should().ContainSingle(count => count.EntityType == typeof(RlsNote)).Which.Affected.Should().Be(1);
        }

        // One handled batch, run through the hook as the request's tenant, and the handler's
        // own context read inside the caller's transaction, under its role and tenant.
        entries.Ran.Where(context => context is RetentionExecutionContext.Tenant)
            .Should()
            .Equal(new RetentionExecutionContext.Tenant(tenantA));
        observations.Seen.Should().Equal(
            new SessionObservation("before:a-subject", tenantA.ToString(), TenantRole, "caller-request")
        );
        (await LoadNoteBodiesAsync(database.ConnectionString)).Should().Equal("a-other");
    }

    [Fact]
    public async Task Row_Handler_Delivery_Runs_As_The_Rows_Tenant()
    {
        // Also settles the grants: the tenant role has table privileges only, with no grant on
        // the identity sequences behind sweep_run_row_detail.Id and sweep_row_handler_status.Id,
        // and its passes still write both.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        await CreateSchemaWithPoliciesAsync(database.ConnectionString);
        await SeedAsync(
            database.ConnectionString,
            [Note(tenantA, "a-expired"), Note(tenantB, "b-expired")],
            [Log("log-expired")]
        );
        var entries = new EntryLog();
        var observations = new HandlerObservations();
        using var host = BuildHost(
            database.ConnectionString,
            services =>
            {
                services.AddSingleton(entries);
                services.AddSingleton(observations);
                services.AddScoped<IRetentionExecutionScope, RecordingExecutionScope>();
                services.AddRowHandler<RlsNote, SessionObservingNoteHandler>();
                services.AddRowHandler<RlsLog, SessionObservingLogHandler>();
            }
        );
        var sweep = host.Services.GetRequiredService<IRetentionSweep>();
        await sweep.ExecuteAsync(RetentionSweepRequest.Tenanted(Tenant(tenantA), AsOf));
        await sweep.ExecuteAsync(RetentionSweepRequest.Tenanted(Tenant(tenantB), AsOf));
        await sweep.ExecuteAsync(RetentionSweepRequest.Tenantless(AsOf));
        entries.Clear();

        var flush = await host.Services.GetRequiredService<IRetentionRowDispatcher>().FlushAsync();

        flush.Settled.Should().BeTrue();
        (await LoadHandlerStatesAsync(database.ConnectionString)).Should().BeEquivalentTo([
            (tenantA, SweepRowHandlerDispatchState.Succeeded),
            (tenantB, SweepRowHandlerDispatchState.Succeeded),
            (Guid.Empty, SweepRowHandlerDispatchState.Succeeded),
        ]);
        var owner = await LoadSuperuserNameAsync(database.ConnectionString);
        observations.Seen.Should().BeEquivalentTo([
            new SessionObservation("before:a-expired", tenantA.ToString(), TenantRole),
            new SessionObservation("before:b-expired", tenantB.ToString(), TenantRole),
            new SessionObservation("before:log-expired", "", owner),
            new SessionObservation("after:a-expired", tenantA.ToString(), TenantRole),
            new SessionObservation("after:b-expired", tenantB.ToString(), TenantRole),
            new SessionObservation("after:log-expired", "", owner),
        ]);
        entries.Ran.Where(context => context is RetentionExecutionContext.Tenant || context == TenantlessPass)
            .Should()
            .BeEquivalentTo(
                [
                    new RetentionExecutionContext.Tenant(tenantA),
                    new RetentionExecutionContext.Tenant(tenantB),
                    TenantlessPass,
                ],
                options => options.ComparingRecordsByValue()
            );
        entries.Ran.Should().Contain(RowDispatch);
    }

    [Fact]
    public async Task History_Pruning_Runs_As_System_And_Prunes_Every_Tenants_History()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        await CreateSchemaWithPoliciesAsync(database.ConnectionString);
        await SeedSettledRunAsync(database.ConnectionString, tenantA, DateTimeOffset.UtcNow.AddDays(-100));
        await SeedSettledRunAsync(database.ConnectionString, tenantB, DateTimeOffset.UtcNow.AddDays(-100));
        var entries = new EntryLog();
        using var host = BuildHost(
            database.ConnectionString,
            services =>
            {
                services.AddSingleton(entries);
                services.AddScoped<IRetentionExecutionScope, RecordingExecutionScope>();
            },
            new Dictionary<string, string?> { ["Cohort:HistoryPruning:SucceededRunRetention"] = "90.00:00:00" }
        );

        var pruned = await host.Services.GetRequiredService<RetentionHistoryPruner>().PruneAsync();

        pruned.Runs.Should().Be(2);
        entries.Ran.Take(2).Should().Equal(HistoryPruning, Readiness);
        entries.Ran.Skip(2).Should().NotBeEmpty().And.OnlyContain(context => context == HistoryPruning);
        (await LoadRunsAsync(database.ConnectionString)).Should().BeEmpty();
    }

    [Fact]
    public async Task Startup_Readiness_Runs_As_System_Readiness()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await CreateSchemaWithPoliciesAsync(database.ConnectionString);
        var entries = new EntryLog();
        using var host = BuildHost(
            database.ConnectionString,
            services =>
            {
                services.AddSingleton(entries);
                services.AddScoped<IRetentionExecutionScope, RecordingExecutionScope>();
            }
        );

        await host.StartAsync();
        await host.StopAsync();

        entries.Ran.First().Should().Be(Readiness);
    }

    [Fact]
    public async Task Readiness_Sees_Another_Tenants_Orphaned_Hold_When_A_Tenant_Sweep_Calls_It_First()
    {
        // Without hosted startup, a tenant's sweep is the first caller. Run in that tenant's
        // scope, the policies would hide tenant B's hold and readiness would pass; it gets its
        // own System(Readiness) scope instead.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var oldIdentity = Guid.NewGuid();
        await CreateSchemaWithPoliciesAsync(database.ConnectionString);
        await SeedAsync(database.ConnectionString, [Note(tenantA, "a-expired")], []);
        await using (var owner = CreateOwnerContext(database.ConnectionString))
        {
            await owner.Database.ExecuteSqlAsync(
                $"""
                INSERT INTO "retention_holds"
                    ("HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt")
                VALUES ({Guid.NewGuid()}, {oldIdentity}, {Guid.NewGuid().ToString()}, {tenantB}, 'litigation', {DateTimeOffset.UtcNow.AddDays(-1)}, NULL, NULL)
                """
            );
        }
        var entries = new EntryLog();
        using var host = BuildHost(
            database.ConnectionString,
            services =>
            {
                services.AddSingleton(entries);
                services.AddScoped<IRetentionExecutionScope, RecordingExecutionScope>();
            }
        );

        var sweep = () => host.Services.GetRequiredService<IRetentionSweep>().ExecuteAsync(RetentionSweepRequest.Tenanted(Tenant(tenantA), AsOf));

        (await sweep.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error => error.Contains($"'{oldIdentity}'", StringComparison.Ordinal));
        entries.Ran.Should().Equal(new RetentionExecutionContext.Tenant(tenantA), Readiness);
        (await LoadNoteBodiesAsync(database.ConnectionString)).Should().Equal("a-expired");
        (await LoadRunsAsync(database.ConnectionString)).Should().BeEmpty();
    }

    [Fact]
    public async Task Tenant_Scoped_Deletion_Of_A_Held_Tenantless_Row_Is_Refused()
    {
        // A host deleting shared, tenantless data from a tenant's request scope must see the
        // row's NULL-tenant hold; hidden by the holds policy, the row would look unheld.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenantA = Guid.NewGuid();
        var log = Log("log-held");
        await CreateSchemaWithPoliciesAsync(database.ConnectionString);
        await SeedAsync(database.ConnectionString, [], [log]);
        var holdId = await SeedTenantlessHoldAsync(database.ConnectionString, log.Id);
        using var host = BuildHost(
            database.ConnectionString,
            services =>
            {
                services.AddSingleton(new EntryLog());
                services.AddScoped<IRetentionExecutionScope, RecordingExecutionScope>();
            }
        );
        await using var scope = host.Services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<RlsDbContext>();
        db.Access = new RetentionExecutionContext.Tenant(tenantA);
        var holds = scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>();

        var outcome = await scope.ServiceProvider.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
            [new RetentionTarget(LogEntityId, log.Id.ToString(), tenantId: null)],
            ct => db.Logs.Where(row => row.Id == log.Id).ExecuteDeleteAsync(ct)
        );

        outcome.Should().Be(RetentionDeletionOutcome.Protected);
        (await holds.HasActiveHoldAsync(LogEntityId, log.Id.ToString(), tenantId: null, CancellationToken.None))
            .Should().BeTrue();
        (await holds.ListActiveAsync(CancellationToken.None)).Select(hold => hold.HoldId).Should().Equal(holdId);
        (await LoadLogPayloadsAsync(database.ConnectionString)).Should().Equal("log-held");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Deletion_Reaching_A_Tenantless_Row_Is_Refused_When_The_Holds_Policy_Hides_Tenantless_Holds(
        bool targetIsTenanted
    )
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenantA = Guid.NewGuid();
        var note = Note(tenantA, "a-note");
        var held = Log("log-held");
        var unheld = Log("log-unheld");
        await CreateSchemaWithPoliciesAsync(database.ConnectionString, tenantsReadTenantlessHolds: false);
        await SeedAsync(database.ConnectionString, [note], [held, unheld]);
        await SeedTenantlessHoldAsync(database.ConnectionString, held.Id);
        using var host = BuildHost(
            database.ConnectionString,
            services =>
            {
                services.AddSingleton(new EntryLog());
                services.AddScoped<IRetentionExecutionScope, RecordingExecutionScope>();
            }
        );
        await using var scope = host.Services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<RlsDbContext>();
        db.Access = new RetentionExecutionContext.Tenant(tenantA);

        // The listed target is unheld; the callback also removes the held tenantless row.
        RetentionTarget target = targetIsTenanted
            ? new RetentionTarget(NoteEntityId, note.Id.ToString(), tenantA)
            : new RetentionTarget(LogEntityId, unheld.Id.ToString(), tenantId: null);
        var delete = () => scope.ServiceProvider.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
            [target],
            async ct =>
            {
                await db.Notes.ExecuteDeleteAsync(ct);
                await db.Logs.ExecuteDeleteAsync(ct);
            }
        );

        await delete.Should().ThrowAsync<RetentionConfigurationException>().WithMessage("*cannot see 1 active hold*");
        (await LoadLogPayloadsAsync(database.ConnectionString)).Should().BeEquivalentTo("log-held", "log-unheld");
        (await LoadNoteBodiesAsync(database.ConnectionString)).Should().Equal("a-note");
    }

    [Fact]
    public async Task Tenant_Scope_Cannot_Create_Or_Remove_A_Tenantless_Hold()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenantA = Guid.NewGuid();
        var held = Log("log-held");
        var unheld = Log("log-unheld");
        await CreateSchemaWithPoliciesAsync(database.ConnectionString);
        await SeedAsync(database.ConnectionString, [], [held, unheld]);
        var holdId = await SeedTenantlessHoldAsync(database.ConnectionString, held.Id);
        using var host = BuildHost(
            database.ConnectionString,
            services =>
            {
                services.AddSingleton(new EntryLog());
                services.AddScoped<IRetentionExecutionScope, RecordingExecutionScope>();
            }
        );
        await using var scope = host.Services.CreateAsyncScope();
        scope.ServiceProvider.GetRequiredService<RlsDbContext>().Access = new RetentionExecutionContext.Tenant(tenantA);
        var holds = scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>();

        var create = () => holds.CreateAsync(
            new RetentionHoldRequest(Guid.NewGuid(), LogEntityId, unheld.Id.ToString(), tenantId: null, "litigation", DateTimeOffset.UtcNow),
            CancellationToken.None
        );
        var remove = () => holds.RemoveAsync(holdId, DateTimeOffset.UtcNow, CancellationToken.None);

        (await create.Should().ThrowAsync<PostgresException>())
            .Which.SqlState.Should().Be(PostgresErrorCodes.InsufficientPrivilege);
        await remove.Should().ThrowAsync<InvalidOperationException>().WithMessage("*could not be removed*");
        (await LoadActiveHoldIdsAsync(database.ConnectionString)).Should().Equal(holdId);
    }

    [Fact]
    public async Task Startup_Refuses_A_Host_Without_An_Execution_Scope()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await CreateSchemaWithPoliciesAsync(database.ConnectionString);
        using var host = BuildHost(database.ConnectionString, _ => { });

        var start = () => host.StartAsync();
        var sweep = () => host.Services.GetRequiredService<IRetentionSweep>().ExecuteAsync(RetentionSweepRequest.Tenanted(Tenant(Guid.NewGuid()), AsOf));

        (await start.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                error.Contains("No IRetentionExecutionScope has been registered", StringComparison.Ordinal)
                && error.Contains("RetentionExecutionScope.Unisolated", StringComparison.Ordinal)
            );
        // Without the hosted startup check, the first scope Cohort creates refuses the same way.
        await sweep.Should().ThrowAsync<RetentionConfigurationException>();
    }

    private static IHost BuildHost(
        string connectionString,
        Action<IServiceCollection> configureServices,
        IReadOnlyDictionary<string, string?>? settings = null
    )
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:Schedule"] = "0 0 0 1 1 *",
            }
        );
        if (settings is not null)
        {
            builder.Configuration.AddInMemoryCollection(settings);
        }

        builder.Services.AddDbContext<RlsDbContext>(options =>
            options.UseNpgsql(connectionString).AddInterceptors(RowSecurityInterceptor.Instance)
        );
        builder.Services.AddSingleton<IRetentionRuleProvider>(new RlsRuleProvider());
        builder.Services.AddCohort<RlsDbContext>();
        configureServices(builder.Services);
        return builder.Build();
    }

    private static RetentionWorker GetWorker(IHost host) =>
        host.Services.GetServices<IHostedService>().OfType<RetentionWorker>().Single();

    /// <summary>The database clock, which stamps every run, so an occurrence taken from it compares with them.</summary>
    private static async Task<DateTimeOffset> DatabaseNowAsync(string connectionString)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT statement_timestamp()";
        return new DateTimeOffset((DateTime)(await command.ExecuteScalarAsync())!);
    }

    private static TenantContext Tenant(Guid id) => new(id, "uk", new Dictionary<string, string>());

    private static RlsNote Note(Guid tenantId, string body, DateTimeOffset? createdAt = null, Guid? personId = null) =>
        new()
        {
            Id = Guid.NewGuid(),
            TenantId = tenantId,
            PersonId = personId,
            CreatedAt = createdAt ?? AsOf.AddDays(-45),
            Body = body,
        };

    private static RlsLog Log(string payload) =>
        new() { Id = Guid.NewGuid(), CreatedAt = AsOf.AddDays(-45), Payload = payload };

    private static RlsDbContext CreateOwnerContext(string connectionString) =>
        new(new DbContextOptionsBuilder<RlsDbContext>().UseNpgsql(connectionString).Options);

    /// <summary>
    /// The item 21 policies from docs/reference/schema.md, plus the host's own tables. The
    /// tenant role gets table privileges and nothing else: no sequence grants.
    /// </summary>
    private static async Task CreateSchemaWithPoliciesAsync(
        string connectionString,
        bool tenantsReadTenantlessHolds = true
    )
    {
        await using var db = CreateOwnerContext(connectionString);
        await db.Database.EnsureCreatedAsync();
        await db.Database.ExecuteSqlRawAsync(
            $$"""
            DO $$
            BEGIN
                CREATE ROLE {{TenantRole}} NOLOGIN;
            EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL;
            END
            $$;

            GRANT SELECT, INSERT, UPDATE, DELETE ON
                public.rls_notes, public.rls_logs, public.retention_holds, public.sweep_run,
                public.sweep_run_entity_summary, public.sweep_run_row_detail, public.sweep_row_handler_status
            TO {{TenantRole}};

            ALTER TABLE public.rls_notes ENABLE ROW LEVEL SECURITY;
            ALTER TABLE public.rls_notes FORCE ROW LEVEL SECURITY;
            ALTER TABLE public.rls_logs ENABLE ROW LEVEL SECURITY;
            ALTER TABLE public.rls_logs FORCE ROW LEVEL SECURITY;
            ALTER TABLE public.retention_holds ENABLE ROW LEVEL SECURITY;
            ALTER TABLE public.retention_holds FORCE ROW LEVEL SECURITY;
            ALTER TABLE public.sweep_run ENABLE ROW LEVEL SECURITY;
            ALTER TABLE public.sweep_run FORCE ROW LEVEL SECURITY;
            ALTER TABLE public.sweep_run_entity_summary ENABLE ROW LEVEL SECURITY;
            ALTER TABLE public.sweep_run_entity_summary FORCE ROW LEVEL SECURITY;
            ALTER TABLE public.sweep_run_row_detail ENABLE ROW LEVEL SECURITY;
            ALTER TABLE public.sweep_run_row_detail FORCE ROW LEVEL SECURITY;
            ALTER TABLE public.sweep_row_handler_status ENABLE ROW LEVEL SECURITY;
            ALTER TABLE public.sweep_row_handler_status FORCE ROW LEVEL SECURITY;

            -- rls_logs has no tenant column: shared data every tenant may use, invisible to a
            -- scope working for no one.
            CREATE POLICY shared_with_tenants ON public.rls_logs
                USING (NULLIF(current_setting('app.tenant_id', true), '') IS NOT NULL);
            CREATE POLICY tenant_isolation ON public.rls_notes
                USING ("TenantId" = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
            CREATE POLICY holds_read ON public.retention_holds FOR SELECT
                USING ("TenantId" IS NULL OR "TenantId" = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
            CREATE POLICY holds_write ON public.retention_holds FOR ALL
                USING ("TenantId" = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
            CREATE POLICY tenant_isolation ON public.sweep_run
                USING ("TenantId" = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
            CREATE POLICY tenant_isolation ON public.sweep_run_entity_summary
                USING ("TenantId" = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
            CREATE POLICY tenant_isolation ON public.sweep_run_row_detail
                USING ("TenantId" = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
            CREATE POLICY tenant_isolation ON public.sweep_row_handler_status
                USING (EXISTS (
                    SELECT 1 FROM public.sweep_run_row_detail detail
                    WHERE detail."Id" = sweep_row_handler_status."SweepRunRowDetailId"
                      AND detail."TenantId" = NULLIF(current_setting('app.tenant_id', true), '')::uuid
                ));
            """
        );
        if (!tenantsReadTenantlessHolds)
        {
            await db.Database.ExecuteSqlRawAsync(
                """
                ALTER POLICY holds_read ON public.retention_holds
                    USING ("TenantId" = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
                """
            );
        }
    }

    private static async Task SeedAsync(string connectionString, IEnumerable<RlsNote> notes, IEnumerable<RlsLog> logs)
    {
        await using var db = CreateOwnerContext(connectionString);
        db.Notes.AddRange(notes);
        db.Logs.AddRange(logs);
        await db.SaveChangesAsync();
    }

    private static async Task<Guid> SeedTenantlessHoldAsync(string connectionString, Guid logId)
    {
        var holdId = Guid.NewGuid();
        await using var owner = CreateOwnerContext(connectionString);
        await owner.Database.ExecuteSqlAsync(
            $"""
            INSERT INTO "retention_holds"
                ("HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt")
            VALUES ({holdId}, {LogEntityId}, {logId.ToString()}, NULL, 'litigation', {DateTimeOffset.UtcNow.AddDays(-1)}, NULL, NULL)
            """
        );
        return holdId;
    }

    private static async Task<List<Guid>> LoadActiveHoldIdsAsync(string connectionString)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """SELECT "HoldId" FROM "retention_holds" WHERE "RemovedAt" IS NULL""";
        var holdIds = new List<Guid>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            holdIds.Add(reader.GetGuid(0));
        }

        return holdIds;
    }

    private static async Task SeedSettledRunAsync(string connectionString, Guid tenantId, DateTimeOffset startedAt)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO "sweep_run"
                ("SweepId", "StartedAt", "Status", "SettledAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected")
            VALUES (@sweepId, @startedAt, @status, @startedAt, INTERVAL '0', @trigger, FALSE, @tenantId, 0)
            """;
        command.Parameters.AddWithValue("sweepId", Guid.NewGuid());
        command.Parameters.AddWithValue("startedAt", startedAt);
        command.Parameters.AddWithValue("status", (int)SweepRunStatus.Succeeded);
        command.Parameters.AddWithValue("trigger", (int)SweepTriggerKind.Manual);
        command.Parameters.AddWithValue("tenantId", tenantId);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<List<string>> LoadNoteBodiesAsync(string connectionString)
    {
        await using var db = CreateOwnerContext(connectionString);
        return await db.Notes.OrderBy(note => note.Body).Select(note => note.Body).ToListAsync();
    }

    private static async Task<List<string>> LoadLogPayloadsAsync(string connectionString)
    {
        await using var db = CreateOwnerContext(connectionString);
        return await db.Logs.OrderBy(log => log.Payload).Select(log => log.Payload).ToListAsync();
    }

    private static async Task<List<RunRow>> LoadRunsAsync(string connectionString)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """SELECT "TenantId", "Status" FROM "sweep_run" """;
        var runs = new List<RunRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            runs.Add(new RunRow(reader.GetGuid(0), (SweepRunStatus)reader.GetInt32(1)));
        }

        return runs;
    }

    private static async Task<List<(Guid TenantId, SweepRowHandlerDispatchState State)>> LoadHandlerStatesAsync(
        string connectionString
    )
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT detail."TenantId", status."State"
            FROM "sweep_row_handler_status" status
            JOIN "sweep_run_row_detail" detail ON detail."Id" = status."SweepRunRowDetailId"
            """;
        var states = new List<(Guid, SweepRowHandlerDispatchState)>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            states.Add((reader.GetGuid(0), (SweepRowHandlerDispatchState)reader.GetInt32(1)));
        }

        return states;
    }

    private static async Task<string> LoadSuperuserNameAsync(string connectionString)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT current_user";
        return (string)(await command.ExecuteScalarAsync())!;
    }

    private sealed record RunRow(Guid TenantId, SweepRunStatus Status);

    private sealed record SessionObservation(string Label, string TenantSetting, string CurrentUser, string Request = "");

    private sealed class EntryLog
    {
        private readonly ConcurrentQueue<RetentionExecutionContext> ran = new();

        public IReadOnlyList<RetentionExecutionContext> Ran => [.. ran];

        public void Add(RetentionExecutionContext context) => ran.Enqueue(context);

        public void Clear() => ran.Clear();
    }

    private sealed class HandlerObservations
    {
        private readonly ConcurrentQueue<SessionObservation> seen = new();

        public IReadOnlyList<SessionObservation> Seen => [.. seen];

        public void Add(SessionObservation observation) => seen.Enqueue(observation);
    }

    /// <summary>
    /// The host's hook keeps whose work this is in an AsyncLocal set around the work, as a host
    /// using a Sluice flow or an ambient tenant would, so a connection Cohort opens outside the
    /// work gets no access. A caller's own scope sets <see cref="RlsDbContext.Access"/> instead.
    /// </summary>
    private static readonly AsyncLocal<RetentionExecutionContext?> HostFlow = new();

    private static async Task RunInFlowAsync(RetentionExecutionContext access, Func<Task> work)
    {
        var outer = HostFlow.Value;
        HostFlow.Value = access;
        try
        {
            await work();
        }
        finally
        {
            HostFlow.Value = outer;
        }
    }

    private sealed class RecordingExecutionScope(EntryLog entries) : IRetentionExecutionScope
    {
        public Task RunAsync(RetentionExecutionContext context, Func<Task> work, CancellationToken ct)
        {
            entries.Add(context);
            return RunInFlowAsync(context, work);
        }
    }

    private sealed class WrongTenantExecutionScope(EntryLog entries, Guid wrongTenant) : IRetentionExecutionScope
    {
        public Task RunAsync(RetentionExecutionContext context, Func<Task> work, CancellationToken ct)
        {
            entries.Add(context);
            return RunInFlowAsync(
                context is RetentionExecutionContext.Tenant ? new RetentionExecutionContext.Tenant(wrongTenant) : context,
                work
            );
        }
    }

    /// <summary>
    /// Applies the scope's access whenever its connection opens. Session-level, not
    /// SET LOCAL at transaction start, because Cohort also runs ADO.NET commands outside
    /// transactions; every open re-applies the full state, so pooled sessions never leak it.
    /// </summary>
    private sealed class RowSecurityInterceptor : DbConnectionInterceptor
    {
        public static readonly RowSecurityInterceptor Instance = new();

        public override void ConnectionOpened(DbConnection connection, ConnectionEndEventData eventData)
        {
            using var command = CreateCommand(connection, eventData);
            command.ExecuteNonQuery();
        }

        public override async Task ConnectionOpenedAsync(
            DbConnection connection,
            ConnectionEndEventData eventData,
            CancellationToken cancellationToken = default
        )
        {
            await using var command = CreateCommand(connection, eventData);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        private static DbCommand CreateCommand(DbConnection connection, ConnectionEndEventData eventData)
        {
            var command = connection.CreateCommand();
            command.CommandText = (HostFlow.Value ?? ((RlsDbContext)eventData.Context!).Access) switch
            {
                RetentionExecutionContext.Tenant tenant =>
                    $"SET ROLE {TenantRole}; SELECT set_config('app.tenant_id', '{tenant.TenantId}', false)",
                RetentionExecutionContext.System =>
                    "RESET ROLE; SELECT set_config('app.tenant_id', '', false)",
                _ => $"SET ROLE {TenantRole}; SELECT set_config('app.tenant_id', '', false)",
            };
            return command;
        }
    }

    private sealed class FixedTenantSource(IReadOnlyList<Guid> tenantIds) : IRetentionTenantSource
    {
        public Task<IReadOnlyList<TenantContext>> GetTenantsAsync(CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<TenantContext>>([.. tenantIds.Select(Tenant)]);
    }

    private sealed class RlsRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == "rls-short" ? new([Strategy.Purge]) : null;

        public Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct) =>
            Task.FromResult<RetentionRule?>(
                context.Category == "rls-short" ? new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge) : null
            );
    }

    [RowHandlerId("89766ecd-99b6-4622-822a-f0fe32322b8c")]
    private sealed class SessionObservingNoteHandler(RlsDbContext db, HandlerObservations observations)
        : IRetentionHandler<RlsNote>
    {
        public async Task OnBeforeAsync(RlsNote row, RetentionBeforeContext ctx, CancellationToken ct)
        {
            ctx.Snapshot["body"] = row.Body;
            observations.Add(await ObserveAsync(db, $"before:{row.Body}", ct));
        }

        public async Task OnAfterAsync(RetentionAfterContext<RlsNote> ctx, CancellationToken ct) =>
            observations.Add(await ObserveAsync(db, $"after:{ctx.Snapshot["body"]}", ct));
    }

    [RowHandlerId("95b8bc24-1b3e-439d-bb03-337c5e82a8cc")]
    private sealed class SessionObservingLogHandler(RlsDbContext db, HandlerObservations observations)
        : IRetentionHandler<RlsLog>
    {
        public async Task OnBeforeAsync(RlsLog row, RetentionBeforeContext ctx, CancellationToken ct)
        {
            ctx.Snapshot["payload"] = row.Payload;
            observations.Add(await ObserveAsync(db, $"before:{row.Payload}", ct));
        }

        public async Task OnAfterAsync(RetentionAfterContext<RlsLog> ctx, CancellationToken ct) =>
            observations.Add(await ObserveAsync(db, $"after:{ctx.Snapshot["payload"]}", ct));
    }

    private static async Task<SessionObservation> ObserveAsync(RlsDbContext db, string label, CancellationToken ct)
    {
        var session = await db.Database
            .SqlQueryRaw<string>(
                """SELECT coalesce(current_setting('app.tenant_id', true), '') || '|' || current_user || '|' || coalesce(current_setting('app.request', true), '') AS "Value" """
            )
            .SingleAsync(ct);
        var parts = session.Split('|');
        return new SessionObservation(label, parts[0], parts[1], parts[2]);
    }

    private sealed class RlsDbContext(DbContextOptions<RlsDbContext> options) : DbContext(options)
    {
        /// <summary>Whose work this context does; set by the host's execution scope.</summary>
        public RetentionExecutionContext? Access { get; set; }

        public DbSet<RlsNote> Notes => Set<RlsNote>();

        public DbSet<RlsLog> Logs => Set<RlsLog>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<RlsNote>(entity =>
            {
                entity.ToTable("rls_notes");
                entity.HasKey(note => note.Id);
            });
            modelBuilder.Entity<RlsLog>(entity =>
            {
                entity.ToTable("rls_logs");
                entity.HasKey(log => log.Id);
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("rls-short", nameof(CreatedAt))]
    [RetentionEntityId("0b6f3c8e-2f43-4d55-9a57-6a3d0f1c7e21")]
    private sealed class RlsNote
    {
        public Guid Id { get; set; }

        public Guid TenantId { get; set; }

        [ErasureSubject("person")]
        public Guid? PersonId { get; set; }

        public DateTimeOffset CreatedAt { get; set; }

        public string Body { get; set; } = "";
    }

    [Retain("rls-short", nameof(CreatedAt))]
    [RetentionTenantless]
    [RetentionEntityId("6d1c2a9b-58e4-4f0a-b1d7-3c9e8f2a4b60")] // LogEntityId
    private sealed class RlsLog
    {
        public Guid Id { get; set; }

        public DateTimeOffset CreatedAt { get; set; }

        public string Payload { get; set; } = "";
    }
}
