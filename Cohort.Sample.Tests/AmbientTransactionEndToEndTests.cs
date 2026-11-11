using System.Transactions;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using Npgsql;

namespace Cohort.Sample.Tests;

// A caller's ambient System.Transactions transaction is treated two ways. Cohort's owned work (a
// sweep, a preview, an owned erasure, recovery) runs outside it. The ports that run on the
// caller's own context (the holds repository and readiness) treat it as the caller's transaction,
// as they treat one begun through EF: readiness checks inside it and keeps no pass from it, and a
// retrying execution strategy's context is refused up front when the call is outside the strategy.
// A test calls a port inside `new TransactionScope(TransactionScopeAsyncFlowOption.Enabled)` on
// the default connection string (Npgsql Enlist=true) and disposes the scope without Complete(), so
// anything that joined it rolls back, except the retrying-strategy tests. The EF-transaction one,
// and the `ambient: false` rows of the owned-work and warm-call ones, run inside a transaction
// begun through EF, with no scope. The ambient one completes the scope of its in-strategy call,
// which only lists.
// A cold call is the first readiness call for the model and connection string (a fresh
// TemporaryDatabase); a warm call follows one made outside any scope.
// Make-fails: remove the suppression in RetentionExecutionScopes.RunInScopeAsync (both sweeps, the
// owned erasure, and the recovery go red; with the preview's, the cold preview goes red too).
// Readiness inside a scope: put the Suppress block back around RetentionRuntimeReadinessValidator's
// model pass (the cold listings on a used context and on a wrapped connection, the orphaned-hold
// listing, and the not-kept pass go red); record the pass for an enlisted context (the not-kept
// pass goes red); begin CohortSchemaValidator's own transaction for an enlisted context (every
// cold listing inside a scope goes red, and so does the retrying test's in-strategy scope half);
// begin the statement explain's own transaction for one (the same, except the orphaned-hold
// listing, which refuses before the statements); drop the retrying check (both retrying tests go
// red on EF's message); drop PassThroughConnection.EnlistTransaction (the wrapped-connection test
// goes red); record the stored-identities pass whenever the context has no EF transaction, in
// place of only when it has no transaction at all (the stored-identities test goes red); apply the
// retrying check whatever the transaction (the owned-work test goes red, both variants); move the
// retrying check below the recorded-pass return (the warm-call test goes red, both variants, on
// EF's message).
// An_owned_erasure_inside_an_ambient_transaction_reads_the_committed_hold_and_refuses pins that an
// owned erasure's readiness reads committed state, outside the ambient transaction, so the
// caller's uncommitted end to a hold doesn't lift the refusal. Make-fail: remove both of those
// suppressions (it goes red: the erasure throws InvalidOperationException, not the declared
// refusal).
// A_cold_hold_listing_on_a_used_context_inside_an_ambient_transaction_refuses_the_committed_orphaned_hold
// pins that a cold listing on a context already used inside the scope runs the checks, and that the
// stored-identities check reads the committed hold in a session of its own (the caller's
// uncommitted end to it doesn't lift the refusal), and that the refusal repeats after the scope.
// Make-fail: skip the stored-identities check (both refusals go red).

[Collection("Integration")]
public sealed class AmbientTransactionEndToEndTests(PostgresFixture fixture)
{
    private static readonly DateTimeOffset AsOf = new(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task A_cold_sweep_inside_an_ambient_transaction_commits_and_is_not_undone_when_the_scope_is_disposed()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenant = NewTenant();
        await SeedDueRowsAsync(services, tenant.Id, subjectId: null);

        RetentionSweepResult result;
        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            result = await services.GetRequiredService<IRetentionSweep>()
                .ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, AsOf));
        }

        result.Counts.Should().ContainSingle(count => count.Strategy == Strategy.Purge).Which.Affected.Should().Be(2);
        await AssertRowsLeftAsync(services, tenant.Id, 0);
        await AssertRunSucceededAsync(services, result.SweepId);
    }

    [Fact]
    public async Task A_warm_sweep_inside_an_ambient_transaction_commits_and_is_not_undone_when_the_scope_is_disposed()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenant = NewTenant();
        await SeedDueRowsAsync(services, tenant.Id, subjectId: null);
        await services.GetRequiredService<IRetentionSweep>()
            .ExecuteAsync(RetentionSweepRequest.Tenanted(NewTenant(), AsOf));

        RetentionSweepResult result;
        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            result = await services.GetRequiredService<IRetentionSweep>()
                .ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, AsOf));
        }

        result.Counts.Should().ContainSingle(count => count.Strategy == Strategy.Purge).Which.Affected.Should().Be(2);
        await AssertRowsLeftAsync(services, tenant.Id, 0);
        await AssertRunSucceededAsync(services, result.SweepId);
    }

    [Fact]
    public async Task A_warm_owned_erasure_inside_an_ambient_transaction_commits_and_is_not_undone_when_the_scope_is_disposed()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenant = NewTenant();
        var subject = Guid.NewGuid();
        await SeedDueRowsAsync(services, tenant.Id, subject);
        await services.GetRequiredService<IRetentionSweep>()
            .ExecuteAsync(RetentionSweepRequest.Tenanted(NewTenant(), AsOf));

        ErasureResult result;
        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var scope = services.CreateAsyncScope();
            result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>()
                .ExecuteAsync(RetentionErasureRequest.Tenanted(tenant, new ErasureScope("user", subject), AsOf));
        }

        result.Complete.Should().BeTrue();
        result.Counts.Should().ContainSingle(count => count.Strategy == Strategy.Purge).Which.Affected.Should().Be(2);
        await AssertRowsLeftAsync(services, tenant.Id, 0);
        await AssertRunSucceededAsync(services, result.SweepId);
    }

    [Fact]
    public async Task A_cold_recovery_inside_an_ambient_transaction_fails_an_abandoned_run_and_is_not_undone_when_the_scope_is_disposed()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var sweepId = await SeedStartedRunAsync(services, database.ConnectionString, DateTimeOffset.UtcNow.AddDays(-1));

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await services.GetRequiredService<IRetentionRunRecovery>().RecoverAsync();
        }

        await AssertRunSettledAsync(services, sweepId, SweepRunStatus.Failed);
    }

    [Fact]
    public async Task A_cold_preview_inside_an_ambient_transaction_counts_the_due_rows_and_changes_nothing()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenant = NewTenant();
        await SeedDueRowsAsync(services, tenant.Id, subjectId: null);

        RetentionSweepResult result;
        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            result = await services.GetRequiredService<IRetentionPreview>().PreviewAsync(tenant, AsOf);
        }

        result.Counts.Should().ContainSingle(count => count.Strategy == Strategy.Purge).Which.Affected.Should().Be(2);
        await AssertRowsLeftAsync(services, tenant.Id, 2);
        await AssertRunCountAsync(services, 0);
    }

    [Fact]
    public async Task A_cold_hold_listing_returns_inside_an_ambient_transaction()
    {
        // Readiness runs on the caller's context here, not in a scope of Cohort's own.
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenant = NewTenant();
        var rowIds = await SeedDueRowsAsync(services, tenant.Id, subjectId: null);
        var holdId = Guid.NewGuid();
        await SeedActiveHoldAsync(services, holdId, rowIds[0], tenant.Id);

        IReadOnlyList<RetentionHold> holds;
        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var scope = services.CreateAsyncScope();
            holds = await scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None);
        }

        holds.Should().ContainSingle().Which.HoldId.Should().Be(holdId);
    }

    [Fact]
    public async Task A_cold_hold_listing_on_a_context_the_caller_has_used_inside_an_ambient_transaction_returns()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenant = NewTenant();
        var rowIds = await SeedDueRowsAsync(services, tenant.Id, subjectId: null);
        var holdId = Guid.NewGuid();
        await SeedActiveHoldAsync(services, holdId, rowIds[0], tenant.Id);

        IReadOnlyList<RetentionHold> holds;
        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var scope = services.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<AmbientDbContext>().Database.ExecuteSqlRawAsync("SELECT 1");
            holds = await scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None);
        }

        holds.Should().ContainSingle().Which.HoldId.Should().Be(holdId);
    }

    [Fact]
    public async Task A_cold_hold_listing_on_a_connection_wrapper_the_caller_has_used_inside_an_ambient_transaction_returns()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        var tenant = NewTenant();
        var holdId = Guid.NewGuid();
        // EnsureCreated can't run over a wrapper, so a provider of its own creates and seeds the
        // database. The wrapper's provider then has a cold readiness state of its own.
        await using (var seeding = BuildServiceProvider(database.ConnectionString))
        {
            var rowIds = await SeedDueRowsAsync(seeding, tenant.Id, subjectId: null);
            await SeedActiveHoldAsync(seeding, holdId, rowIds[0], tenant.Id);
        }

        await using var services = BuildServiceProvider(
            database.ConnectionString,
            options => options.UseNpgsql(
                new PassThroughConnection(new NpgsqlConnection(database.ConnectionString)),
                contextOwnsConnection: true
            )
        );

        IReadOnlyList<RetentionHold> holds;
        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var scope = services.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<AmbientDbContext>().Database.ExecuteSqlRawAsync("SELECT 1");
            holds = await scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None);
        }

        holds.Should().ContainSingle().Which.HoldId.Should().Be(holdId);
    }

    // The scope renames a table back inside its own transaction, so the check passes there, then
    // rolls back. A pass recorded from that scope would let the later call skip the check.
    [Fact]
    public async Task A_cold_check_passed_inside_an_ambient_transaction_is_not_kept_for_later_calls()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        await SeedDueRowsAsync(services, NewTenant().Id, subjectId: null);
        await using (var setup = services.CreateAsyncScope())
        {
            await setup.ServiceProvider.GetRequiredService<AmbientDbContext>().Database.ExecuteSqlRawAsync(
                """ALTER TABLE public."sweep_run_row_detail" RENAME TO "sweep_run_row_detail_away" """
            );
        }

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var scope = services.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<AmbientDbContext>().Database.ExecuteSqlRawAsync(
                """ALTER TABLE public."sweep_run_row_detail_away" RENAME TO "sweep_run_row_detail" """
            );
            (await scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None)).Should().BeEmpty();
        }

        await using var after = services.CreateAsyncScope();
        var listing = () => after.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
            .ListActiveAsync(CancellationToken.None);
        (await listing.Should().ThrowAsync<RetentionConfigurationException>()).Which.Errors.Should().ContainSingle(error =>
            error.Contains("missing Cohort runtime capabilities", StringComparison.Ordinal)
            && error.Contains("table '\"public\".\"sweep_run_row_detail\"'", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task A_cold_hold_listing_on_a_used_context_inside_an_ambient_transaction_refuses_the_committed_orphaned_hold()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenant = NewTenant();
        await SeedDueRowsAsync(services, tenant.Id, subjectId: null);
        var (orphanedEntityId, holdId) = await SeedOrphanedHoldAsync(services, tenant.Id);

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var scope = services.CreateAsyncScope();
            await EndHoldAsync(scope, holdId);
            // The stored-identities check reads in a session of its own, so it sees the committed
            // hold, still active, and not the caller's uncommitted end to it.
            var inScope = () => scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None);
            await AssertOrphanedHoldRefusedAsync(inScope, orphanedEntityId);
        }

        await using var after = services.CreateAsyncScope();
        var listing = () => after.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
            .ListActiveAsync(CancellationToken.None);
        await AssertOrphanedHoldRefusedAsync(listing, orphanedEntityId);
    }

    // A host that gives every context one open connection: the stored-identities read, which
    // Cohort runs in a scope of its own, reaches the caller's session and sees the caller's
    // uncommitted end to the only hold on an identity no retained entity registers. The check
    // passes inside the scope, which then rolls back, so the pass must not be remembered.
    [Fact]
    public async Task A_stored_identities_check_that_read_the_callers_uncommitted_writes_inside_an_ambient_transaction_is_not_kept()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        Guid orphanedEntityId;
        Guid holdId;
        await using (var seeding = BuildServiceProvider(database.ConnectionString))
        {
            var tenant = NewTenant();
            await SeedDueRowsAsync(seeding, tenant.Id, subjectId: null);
            (orphanedEntityId, holdId) = await SeedOrphanedHoldAsync(seeding, tenant.Id);
        }

        await using var sharedConnection = new NpgsqlConnection(database.ConnectionString);
        await using var services = BuildServiceProvider(
            database.ConnectionString,
            options => options.UseNpgsql(sharedConnection)
        );

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var scope = services.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<AmbientDbContext>();
            await db.Database.OpenConnectionAsync();
            try
            {
                await EndHoldAsync(scope, holdId);
                var inScope = () => scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                    .ListActiveAsync(CancellationToken.None);

                await inScope.Should().NotThrowAsync("the only hold on the orphaned identity is ended inside the scope");
            }
            finally
            {
                await db.Database.CloseConnectionAsync();
            }
        }

        await using var after = services.CreateAsyncScope();
        var listing = () => after.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
            .ListActiveAsync(CancellationToken.None);
        await AssertOrphanedHoldRefusedAsync(listing, orphanedEntityId);
    }

    // A retrying execution strategy refuses a call inside the caller's transaction only for the
    // ports that run on the caller's context. Owned work runs on a context of its own outside the
    // transaction, so it is not refused, whichever kind of transaction the caller has open.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task On_a_retrying_context_owned_work_inside_the_callers_transaction_is_not_refused(bool ambient)
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString, RetryingOptions(database));
        var swept = NewTenant();
        var previewed = NewTenant();
        var erased = NewTenant();
        var subject = Guid.NewGuid();
        await SeedDueRowsAsync(services, swept.Id, subjectId: null);
        await SeedDueRowsAsync(services, previewed.Id, subjectId: null);
        await SeedDueRowsAsync(services, erased.Id, subject);

        async Task RunOwnedWorkAsync(AsyncServiceScope scope)
        {
            var sweep = await scope.ServiceProvider.GetRequiredService<IRetentionSweep>()
                .ExecuteAsync(RetentionSweepRequest.Tenanted(swept, AsOf));
            sweep.Counts.Should().ContainSingle(count => count.Strategy == Strategy.Purge).Which.Affected.Should().Be(2);

            var preview = await scope.ServiceProvider.GetRequiredService<IRetentionPreview>()
                .PreviewAsync(previewed, AsOf);
            preview.Counts.Should().ContainSingle(count => count.Strategy == Strategy.Purge).Which.Affected.Should().Be(2);

            var erasure = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>()
                .ExecuteAsync(RetentionErasureRequest.Tenanted(erased, new ErasureScope("user", subject), AsOf));
            erasure.Counts.Should().ContainSingle(count => count.Strategy == Strategy.Purge).Which.Affected.Should().Be(2);
        }

        if (ambient)
        {
            using var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
            await using var scope = services.CreateAsyncScope();
            await RunOwnedWorkAsync(scope);
        }
        else
        {
            await using var scope = services.CreateAsyncScope();
            await using var transaction = await scope.ServiceProvider.GetRequiredService<AmbientDbContext>()
                .Database.BeginTransactionAsync();
            await RunOwnedWorkAsync(scope);
        }

        await AssertRowsLeftAsync(services, swept.Id, 0);
        await AssertRowsLeftAsync(services, previewed.Id, 2);
        await AssertRowsLeftAsync(services, erased.Id, 0);
    }

    [Fact]
    public async Task On_a_retrying_context_a_call_inside_an_EF_transaction_but_outside_the_strategy_is_refused()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString, RetryingOptions(database));
        var holdId = await SeedRetryingHoldAsync(services);

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<AmbientDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            var outside = () => scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None);

            (await outside.Should().ThrowAsync<InvalidOperationException>()).Which.Message.Should().Be(RetryingRefusal);
        }

        await using var inside = services.CreateAsyncScope();
        var insideDb = inside.ServiceProvider.GetRequiredService<AmbientDbContext>();
        var holds = await insideDb.Database.CreateExecutionStrategy().ExecuteAsync(async () =>
        {
            await using var transaction = await insideDb.Database.BeginTransactionAsync();
            return await inside.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None);
        });

        holds.Should().ContainSingle().Which.HoldId.Should().Be(holdId);
    }

    [Fact]
    public async Task On_a_retrying_context_a_call_inside_an_ambient_transaction_but_outside_the_strategy_is_refused()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString, RetryingOptions(database));
        var holdId = await SeedRetryingHoldAsync(services);

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var scope = services.CreateAsyncScope();
            var outside = () => scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None);

            (await outside.Should().ThrowAsync<InvalidOperationException>()).Which.Message.Should().Be(RetryingRefusal);
        }

        await using var inside = services.CreateAsyncScope();
        var holds = await inside.ServiceProvider.GetRequiredService<AmbientDbContext>().Database
            .CreateExecutionStrategy()
            .ExecuteAsync(async () =>
            {
                using var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
                var listed = await inside.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                    .ListActiveAsync(CancellationToken.None);
                transactionScope.Complete();
                return listed;
            });

        holds.Should().ContainSingle().Which.HoldId.Should().Be(holdId);
    }

    // Every started host is warm: readiness has passed and later calls return with no round trip.
    // The refusal comes before that return, so a warm call is refused in Cohort's words too.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task On_a_retrying_context_a_warm_call_inside_the_callers_transaction_but_outside_the_strategy_is_refused(
        bool ambient
    )
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString, RetryingOptions(database));
        var holdId = await SeedRetryingHoldAsync(services);

        await using (var warm = services.CreateAsyncScope())
        {
            var listed = await warm.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None);
            listed.Should().ContainSingle().Which.HoldId.Should().Be(holdId);
        }

        if (ambient)
        {
            using var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
            await using var scope = services.CreateAsyncScope();
            var outside = () => scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None);

            (await outside.Should().ThrowAsync<InvalidOperationException>()).Which.Message.Should().Be(RetryingRefusal);
        }
        else
        {
            await using var scope = services.CreateAsyncScope();
            await using var transaction = await scope.ServiceProvider.GetRequiredService<AmbientDbContext>()
                .Database.BeginTransactionAsync();
            var outside = () => scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None);

            (await outside.Should().ThrowAsync<InvalidOperationException>()).Which.Message.Should().Be(RetryingRefusal);
        }
    }

    [Fact]
    public async Task An_owned_erasure_inside_an_ambient_transaction_reads_the_committed_hold_and_refuses()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenant = NewTenant();
        var subject = Guid.NewGuid();
        await SeedDueRowsAsync(services, tenant.Id, subject);
        var (orphanedEntityId, holdId) = await SeedOrphanedHoldAsync(services, tenant.Id);
        var request = RetentionErasureRequest.Tenanted(tenant, new ErasureScope("user", subject), AsOf);

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var scope = services.CreateAsyncScope();
            await EndHoldAsync(scope, holdId);
            // The owned erasure runs readiness in a scope of its own, outside the ambient
            // transaction, so it reads the committed hold, still active.
            var inScope = () => scope.ServiceProvider.GetRequiredService<IRetentionErasureService>()
                .ExecuteAsync(request);
            await AssertOrphanedHoldRefusedAsync(inScope, orphanedEntityId);
        }

        await using var after = services.CreateAsyncScope();
        var erasure = () => after.ServiceProvider.GetRequiredService<IRetentionErasureService>()
            .ExecuteAsync(request);
        await AssertOrphanedHoldRefusedAsync(erasure, orphanedEntityId);
    }

    private static TenantContext NewTenant() => new(Guid.NewGuid(), "uk", new Dictionary<string, string>());

    private static async Task<Guid[]> SeedDueRowsAsync(ServiceProvider services, Guid tenantId, Guid? subjectId)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AmbientDbContext>();
        await db.Database.EnsureCreatedAsync();
        Guid[] ids = [Guid.NewGuid(), Guid.NewGuid()];
        db.Records.AddRange(ids.Select(id => new AmbientRecord
        {
            Id = id,
            TenantId = tenantId,
            SubjectId = subjectId,
            CreatedAt = AsOf.AddDays(-45),
        }));
        await db.SaveChangesAsync();
        return ids;
    }

    private const string RetryingRefusal =
        "Cohort was called inside your transaction on a DbContext with a retrying execution strategy (EnableRetryOnFailure), outside that strategy: wrap your transaction and the call in db.Database.CreateExecutionStrategy().ExecuteAsync(...), as EF requires of your own work in it.";

    private static Action<DbContextOptionsBuilder> RetryingOptions(TemporaryDatabase database) =>
        options => options.UseNpgsql(
            database.ConnectionString,
            npgsql => npgsql.EnableRetryOnFailure(
                maxRetryCount: 2,
                maxRetryDelay: TimeSpan.FromMilliseconds(50),
                errorCodesToAdd: null
            )
        );

    // A committed active hold on a retained row, which a retrying context's listing returns.
    private static async Task<Guid> SeedRetryingHoldAsync(ServiceProvider services)
    {
        var tenant = NewTenant();
        var rowIds = await SeedDueRowsAsync(services, tenant.Id, subjectId: null);
        var holdId = Guid.NewGuid();
        await SeedActiveHoldAsync(services, holdId, rowIds[0], tenant.Id);
        return holdId;
    }

    private static async Task SeedActiveHoldAsync(ServiceProvider services, Guid holdId, Guid rowId, Guid tenantId)
    {
        await using var setup = services.CreateAsyncScope();
        await setup.ServiceProvider.GetRequiredService<AmbientDbContext>().Database.ExecuteSqlAsync(
            $"""
            INSERT INTO "retention_holds"
                ("HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt")
            VALUES ({holdId}, {RetentionEntityIdentity.For<AmbientRecord>()}, {rowId.ToString()}, {tenantId}, 'litigation', {AsOf}, NULL, NULL)
            """
        );
    }

    // An active hold whose RetentionEntityId no entity in the model registers, committed.
    private static async Task<(Guid EntityId, Guid HoldId)> SeedOrphanedHoldAsync(ServiceProvider services, Guid tenantId)
    {
        var entityId = Guid.NewGuid();
        var holdId = Guid.NewGuid();
        await using var scope = services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<AmbientDbContext>().Database.ExecuteSqlAsync(
            $"""
            INSERT INTO "retention_holds"
                ("HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt")
            VALUES ({holdId}, {entityId}, {Guid.NewGuid().ToString()}, {tenantId}, 'litigation', {AsOf}, NULL, NULL)
            """
        );
        return (entityId, holdId);
    }

    private static async Task EndHoldAsync(AsyncServiceScope scope, Guid holdId) =>
        await scope.ServiceProvider.GetRequiredService<AmbientDbContext>().Database.ExecuteSqlAsync(
            $"""UPDATE "retention_holds" SET "RemovedAt" = statement_timestamp() WHERE "HoldId" = {holdId}"""
        );

    private static async Task AssertOrphanedHoldRefusedAsync(Func<Task> call, Guid orphanedEntityId) =>
        (await call.Should().ThrowAsync<RetentionConfigurationException>())
            .Which.Errors.Should().ContainSingle(error =>
                error.Contains($"'{orphanedEntityId}'", StringComparison.Ordinal)
                && error.Contains("is not registered by any retained entity", StringComparison.Ordinal)
            );

    private static async Task AssertRowsLeftAsync(ServiceProvider services, Guid tenantId, int expected)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AmbientDbContext>();
        (await db.Records.CountAsync(record => record.TenantId == tenantId)).Should().Be(expected);
    }

    // A run left Started at startedAt, whose owner is gone (no session holds its run lock).
    private static async Task<Guid> SeedStartedRunAsync(ServiceProvider services, string connectionString, DateTimeOffset startedAt)
    {
        await using (var setup = services.CreateAsyncScope())
        {
            await setup.ServiceProvider.GetRequiredService<AmbientDbContext>().Database.EnsureCreatedAsync();
        }

        var sweepId = Guid.NewGuid();
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO "sweep_run" ("SweepId", "StartedAt", "Status", "TriggerKind", "DryRun", "TenantId", "TotalAffected")
            VALUES (@sweepId, @startedAt, @status, @trigger, FALSE, @tenantId, 0)
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("startedAt", startedAt);
        command.Parameters.AddWithValue("status", (int)SweepRunStatus.Started);
        command.Parameters.AddWithValue("trigger", (int)SweepTriggerKind.Manual);
        command.Parameters.AddWithValue("tenantId", Guid.NewGuid());
        await command.ExecuteNonQueryAsync();
        return sweepId;
    }

    private static Task AssertRunSucceededAsync(ServiceProvider services, Guid sweepId) =>
        AssertRunSettledAsync(services, sweepId, SweepRunStatus.Succeeded);

    private static async Task AssertRunSettledAsync(ServiceProvider services, Guid sweepId, SweepRunStatus expected)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AmbientDbContext>();
        (await db.Database
            .SqlQuery<int>($"""SELECT "Status" AS "Value" FROM "sweep_run" WHERE "SweepId" = {sweepId} AND "SettledAt" IS NOT NULL""")
            .ToListAsync())
            .Should().Equal((int)expected);
    }

    private static async Task AssertRunCountAsync(ServiceProvider services, long expected)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AmbientDbContext>();
        (await db.Database.SqlQuery<long>($"""SELECT COUNT(*) AS "Value" FROM "sweep_run" """).SingleAsync())
            .Should().Be(expected);
    }

    private static ServiceProvider BuildServiceProvider(
        string connectionString,
        Action<DbContextOptionsBuilder>? useNpgsql = null
    )
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().AddInMemoryCollection().Build());
        services.AddLogging();
        services.AddDbContext<AmbientDbContext>(options =>
        {
            if (useNpgsql is null)
            {
                options.UseNpgsql(connectionString);
            }
            else
            {
                useNpgsql(options);
            }
        });
        services.AddSingleton<IRetentionRuleProvider>(new AmbientCategoryRepository());
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<AmbientDbContext>();
        return services.BuildServiceProvider(validateScopes: true);
    }

    private sealed class AmbientCategoryRepository : ITestRetentionRuleProvider
    {
        private static readonly ITestRetentionRule Resolver = new StaticTestRetentionRule(
            new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge)
        );

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct) =>
            Task.FromResult<ITestRetentionRule?>(category == "short-lived" ? Resolver : null);
    }

    private sealed class AmbientDbContext(DbContextOptions<AmbientDbContext> options) : DbContext(options)
    {
        public DbSet<AmbientRecord> Records => Set<AmbientRecord>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<AmbientRecord>().ToTable("ambient_records").HasKey(record => record.Id);
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("short-lived", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0007-000000000001")]
    private sealed class AmbientRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }

        [ErasureSubject("user")]
        public Guid? SubjectId { get; init; }

        public DateTimeOffset CreatedAt { get; init; }
    }
}
