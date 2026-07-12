using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class PurgeSweepStrategyTests(PostgresFixture fixture)
{
    [Fact]
    public async Task Sweep_Deletes_Only_Rows_Past_Cutoff_For_The_Target_Tenant()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<PurgeTestDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Records.AddRange(
                CreateRecord(Guid.NewGuid(), tenantA, asOf.AddDays(-45), "delete-me"),
                CreateRecord(Guid.NewGuid(), tenantA, asOf.AddDays(-5), "keep-newer"),
                CreateRecord(Guid.NewGuid(), tenantB, asOf.AddDays(-45), "keep-other-tenant")
            );
            await db.SaveChangesAsync();
        }

        var result = await services
            .GetRequiredService<IRetentionSweep>()
            .SweepAsync(new TenantContext(tenantA, "uk", new Dictionary<string, string>()), asOf);

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(PurgeCandidateRecord),
                    "short-lived",
                    tenantA,
                    Strategy.Purge,
                    1
                )
            );

        await using var verifyScope = services.CreateAsyncScope();
        var verify = verifyScope.ServiceProvider.GetRequiredService<PurgeTestDbContext>();
        var remainingRows = await verify
            .Records.OrderBy(record => record.Body)
            .Select(record => new RemainingRow(record.TenantId, record.Body))
            .ToListAsync();
        remainingRows
            .Should()
            .Equal(
                new RemainingRow(tenantA, "keep-newer"),
                new RemainingRow(tenantB, "keep-other-tenant")
            );
    }

    [Fact]
    public async Task Sweep_Uses_Mapped_Record_Id_When_Excluding_Held_Rows()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var selectedId = Guid.NewGuid();
        var heldId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<PurgeTestDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Records.AddRange(
                CreateRecord(selectedId, tenantId, asOf.AddDays(-45), "delete-me"),
                CreateRecord(heldId, tenantId, asOf.AddDays(-45), "keep-held")
            );
            await db.SaveChangesAsync();

            var holds = scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>();
            await holds.CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    RetentionEntityIdentity.For<PurgeCandidateRecord>(),
                    heldId.ToString(),
                    tenantId,
                    "mapped-id-test",
                    asOf.AddDays(-1)
                ),
                CancellationToken.None
            );
        }

        var result = await services
            .GetRequiredService<IRetentionSweep>()
            .SweepAsync(new TenantContext(tenantId, "uk", new Dictionary<string, string>()), asOf);

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(PurgeCandidateRecord),
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    1,
                    HeldCount: 1
                )
            );

        await using var verifyScope = services.CreateAsyncScope();
        var verify = verifyScope.ServiceProvider.GetRequiredService<PurgeTestDbContext>();
        var remainingIds = await verify.Records.Select(record => record.Id).ToListAsync();
        remainingIds.Should().Equal(heldId);
    }

    private static PurgeCandidateRecord CreateRecord(
        Guid id,
        Guid tenantId,
        DateTimeOffset createdAt,
        string body
    ) =>
        new()
        {
            Id = id,
            TenantId = tenantId,
            CreatedAt = createdAt,
            Body = body,
        };

    private static ServiceProvider BuildServiceProvider(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(
            new ConfigurationBuilder().AddInMemoryCollection().Build()
        );
        services.AddLogging();
        services.AddDbContext<PurgeTestDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(new PurgeCategoryRepository());
        services.AddCohort<PurgeTestDbContext>();
        return services.BuildServiceProvider(validateScopes: true);
    }

    private sealed class PurgeCategoryRepository : ITestRetentionRuleProvider
    {
        private static readonly ITestRetentionRule Resolver = new StaticTestRetentionRule(
            new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
        );

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct) =>
            Task.FromResult<ITestRetentionRule?>(category == "short-lived" ? Resolver : null);
    }

    private sealed class PurgeTestDbContext(DbContextOptions<PurgeTestDbContext> options)
        : DbContext(options)
    {
        public DbSet<PurgeCandidateRecord> Records => Set<PurgeCandidateRecord>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PurgeCandidateRecord>(entity =>
            {
                entity.ToTable("mapped_purge_candidate_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.Id).HasColumnName("record_id");
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("short-lived", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0003-000000000001")]
    private sealed class PurgeCandidateRecord
    {
        [RetentionRecordId]
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public string Body { get; init; } = "";
    }

    private sealed record RemainingRow(Guid TenantId, string Body);
}
