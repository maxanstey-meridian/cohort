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

// Cohort's own work never enlists in a caller's ambient System.Transactions transaction. Each
// test calls a port inside `new TransactionScope(TransactionScopeAsyncFlowOption.Enabled)` on the
// default connection string (Npgsql Enlist=true) and disposes the scope without Complete(), so
// anything that joined it rolls back.
// A cold call is the first readiness call for the model and connection string (a fresh
// TemporaryDatabase); a warm call follows one made outside any scope.
// Make-fails: remove the suppression in RetentionExecutionScopes.RunInScopeAsync (both sweeps, the
// owned erasure, and the recovery go red), then the one around
// RetentionRuntimeReadinessValidator's model pass (both cold hold listing tests go red). With
// both removed, the cold preview goes red too.
// Two tests check holds; neither pins the recording rule, which other tests pin under an EF
// transaction.
// An_owned_erasure_inside_an_ambient_transaction_reads_the_committed_hold_and_refuses pins that an
// owned erasure's readiness reads committed state, outside the ambient transaction, so the
// caller's uncommitted end to a hold doesn't lift the refusal. Make-fail: remove both of those
// suppressions (it goes red: the erasure throws InvalidOperationException, not the declared
// refusal).
// A_cold_hold_listing_on_a_context_used_inside_an_ambient_transaction_throws_and_the_hold_still_refuses_afterwards
// pins that a cold listing on a context already used inside the scope fails with EF's ambient
// error, and that the committed hold refuses after the scope. Make-fails: remove the suppression
// around the model pass (the in-scope throw becomes Npgsql's "A transaction is already in
// progress", so the message assertion goes red); skip the stored-identities check (the refusal
// after the scope goes red).

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
        await using (var setup = services.CreateAsyncScope())
        {
            await setup.ServiceProvider.GetRequiredService<AmbientDbContext>().Database.ExecuteSqlAsync(
                $"""
                INSERT INTO "retention_holds"
                    ("HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt")
                VALUES ({holdId}, {RetentionEntityIdentity.For<AmbientRecord>()}, {rowIds[0].ToString()}, {tenant.Id}, 'litigation', {AsOf}, NULL, NULL)
                """
            );
        }

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
    public async Task A_cold_hold_listing_on_a_context_used_inside_an_ambient_transaction_throws_and_the_hold_still_refuses_afterwards()
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
            var inScope = () => scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None);
            // Expected failure, marking a known gap: a cold readiness check on a context the
            // caller has already used inside an ambient transaction throws EF's undeclared
            // exception, where the correct behaviour is Cohort's declared refusal. When the gap is
            // fixed this assertion goes red and comes off.
            await inScope.Should().ThrowAsync<InvalidOperationException>()
                .WithMessage("*This connection was used with an ambient transaction*");
        }

        await using var after = services.CreateAsyncScope();
        var listing = () => after.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>()
            .ListActiveAsync(CancellationToken.None);
        await AssertOrphanedHoldRefusedAsync(listing, orphanedEntityId);
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

    private static ServiceProvider BuildServiceProvider(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().AddInMemoryCollection().Build());
        services.AddLogging();
        services.AddDbContext<AmbientDbContext>(options => options.UseNpgsql(connectionString));
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
