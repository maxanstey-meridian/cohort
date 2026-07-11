using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests;

public sealed class RestrictiveForeignKeySweepEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Sweep_Deletes_Retained_Child_Before_Parent_Under_Restrictive_Foreign_Key()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        await using var services = BuildServiceProvider(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<RestrictiveForeignKeyDbContext>();
            await db.Database.EnsureCreatedAsync();

            var parent = new ARestrictiveParent
            {
                Id = Guid.NewGuid(),
                TenantId = tenantId,
                CreatedAt = asOf.AddDays(-90),
            };
            db.Parents.Add(parent);
            db.Children.Add(
                new ZRestrictiveChild
                {
                    Id = Guid.NewGuid(),
                    ParentId = parent.Id,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-90),
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await services
            .GetRequiredService<IRetentionSweep>()
            .SweepAsync(new TenantContext(tenantId, "uk", new Dictionary<string, string>()), asOf);

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ARestrictiveParent));
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(ZRestrictiveChild));
        result.Counts.Where(count =>
                count.EntityType == typeof(ARestrictiveParent)
                || count.EntityType == typeof(ZRestrictiveChild)
            )
            .Should()
            .OnlyContain(count => count.Affected == 1);

        await using var verifyScope = services.CreateAsyncScope();
        var verify = verifyScope.ServiceProvider.GetRequiredService<RestrictiveForeignKeyDbContext>();
        (await verify.Parents.CountAsync()).Should().Be(0);
        (await verify.Children.CountAsync()).Should().Be(0);

        await verify.Database.OpenConnectionAsync();
        await using (var runCommand = verify.Database.GetDbConnection().CreateCommand())
        {
            runCommand.CommandText =
                "SELECT \"Status\", \"TotalAffected\" FROM \"sweep_run\" WHERE \"SweepId\" = @sweepId";
            var sweepId = runCommand.CreateParameter();
            sweepId.ParameterName = "sweepId";
            sweepId.Value = result.SweepId;
            runCommand.Parameters.Add(sweepId);

            await using var reader = await runCommand.ExecuteReaderAsync();
            (await reader.ReadAsync()).Should().BeTrue();
            ((SweepRunStatus)reader.GetInt32(0)).Should().Be(SweepRunStatus.Succeeded);
            reader.GetInt64(1).Should().Be(2);
        }

        await using (var summaryCommand = verify.Database.GetDbConnection().CreateCommand())
        {
            summaryCommand.CommandText = """
                SELECT "EntityType", "Category", "Strategy", "ResolvedPeriod", "Affected", "HeldCount", "SkippedCount"
                FROM "sweep_run_entity_summary"
                WHERE "SweepId" = @sweepId
                ORDER BY "EntityType"
                """;
            var sweepId = summaryCommand.CreateParameter();
            sweepId.ParameterName = "sweepId";
            sweepId.Value = result.SweepId;
            summaryCommand.Parameters.Add(sweepId);

            var summaries = new List<(string, string, Strategy, TimeSpan, long, long, long)>();
            await using var reader = await summaryCommand.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                summaries.Add(
                    (
                        reader.GetString(0),
                        reader.GetString(1),
                        (Strategy)reader.GetInt32(2),
                        reader.GetFieldValue<TimeSpan>(3),
                        reader.GetInt64(4),
                        reader.GetInt64(5),
                        reader.GetInt64(6)
                    )
                );
            }

            summaries
                .Should()
                .Equal(
                    (
                        typeof(ARestrictiveParent).FullName!,
                        "restrictive-fk",
                        Strategy.Purge,
                        TimeSpan.FromDays(30),
                        1L,
                        0L,
                        0L
                    ),
                    (
                        typeof(ZRestrictiveChild).FullName!,
                        "restrictive-fk",
                        Strategy.Purge,
                        TimeSpan.FromDays(30),
                        1L,
                        0L,
                        0L
                    )
                );
        }
    }

    private static ServiceProvider BuildServiceProvider(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(
            new ConfigurationBuilder().AddInMemoryCollection().Build()
        );
        services.AddLogging();
        services.AddDbContext<RestrictiveForeignKeyDbContext>(options =>
            options.UseNpgsql(connectionString)
        );
        services.AddSingleton<IRetentionCategoryRepository>(new PurgeCategoryRepository());
        services.AddCohort<RestrictiveForeignKeyDbContext>();
        return services.BuildServiceProvider(validateScopes: true);
    }

    private sealed class PurgeCategoryRepository : IRetentionCategoryRepository
    {
        private static readonly IRetentionRuleResolver Resolver = new StaticRetentionRuleResolver(
            new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
        );

        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct) =>
            Task.FromResult<IRetentionRuleResolver?>(category == "restrictive-fk" ? Resolver : null);
    }

    private sealed class RestrictiveForeignKeyDbContext(
        DbContextOptions<RestrictiveForeignKeyDbContext> options
    ) : DbContext(options)
    {
        public DbSet<ARestrictiveParent> Parents => Set<ARestrictiveParent>();
        public DbSet<ZRestrictiveChild> Children => Set<ZRestrictiveChild>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ARestrictiveParent>(entity =>
            {
                entity.ToTable("restrictive_fk_parents");
                entity.HasKey(parent => parent.Id);
            });
            modelBuilder.Entity<ZRestrictiveChild>(entity =>
            {
                entity.ToTable("restrictive_fk_children");
                entity.HasKey(child => child.Id);
                entity
                    .HasOne<ARestrictiveParent>()
                    .WithMany()
                    .HasForeignKey(child => child.ParentId)
                    .OnDelete(DeleteBehavior.Restrict);
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("restrictive-fk", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0002-000000000001")]
    private sealed class ARestrictiveParent
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("restrictive-fk", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0002-000000000002")]
    private sealed class ZRestrictiveChild
    {
        public Guid Id { get; init; }
        public Guid ParentId { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }
}
