using Cohort.Infrastructure.Migrations;
using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

// Narrow integration test: table adoption uses Npgsql model metadata but executes no SQL.
public sealed class CohortTableAdoptionTests
{
    [Fact]
    public void ConfigureCohortTables_Rejects_Host_Entities_Coincidentally_Mapped_To_Cohort_Table_Names()
    {
        var options = new DbContextOptionsBuilder<RogueSweepRunDbContext>()
            .UseNpgsqlMetadataModel($"rogue-sweep-run-{Guid.NewGuid()}")
            .Options;
        using var db = new RogueSweepRunDbContext(options);

        var act = () => db.Model;

        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("*sweep_run*table-name collision*");
    }

    [Fact]
    public void ConfigureCohortTables_Adopts_Host_Entities_Declaring_The_Expected_Key()
    {
        var options = new DbContextOptionsBuilder<AdoptedSweepRunDbContext>()
            .UseNpgsqlMetadataModel($"adopted-sweep-run-{Guid.NewGuid()}")
            .Options;
        using var db = new AdoptedSweepRunDbContext(options);

        var adopted = db.Model.FindEntityType(typeof(HostSweepRun));

        adopted.Should().NotBeNull();
        adopted!.GetTableName().Should().Be("sweep_run");
        adopted!
            .FindPrimaryKey()!
            .Properties.Select(property => property.Name)
            .Should()
            .Equal("SweepId");
    }

    [Fact]
    public void ConfigureCohortTables_Adopts_Entity_Summary_With_Stable_Identity_Key()
    {
        var options = new DbContextOptionsBuilder<AdoptedSummaryDbContext>()
            .UseNpgsqlMetadataModel($"adopted-summary-{Guid.NewGuid()}")
            .Options;
        using var db = new AdoptedSummaryDbContext(options);

        db.Model
            .FindEntityType(typeof(HostSweepRunEntitySummary))!
            .FindPrimaryKey()!
            .Properties.Select(property => property.Name)
            .Should()
            .Equal("SweepId", "RetentionEntityId", "Category", "TenantId", "Strategy");
    }

    [Fact]
    public void ConfigureCohortTables_Rejects_Entity_Summary_With_Obsolete_Entity_Type_Key()
    {
        var options = new DbContextOptionsBuilder<ObsoleteSummaryDbContext>()
            .UseNpgsqlMetadataModel($"obsolete-summary-{Guid.NewGuid()}")
            .Options;
        using var db = new ObsoleteSummaryDbContext(options);

        var act = () => db.Model;

        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("*sweep_run_entity_summary*table-name collision*");
    }

    private sealed class RogueSweepRunDbContext(DbContextOptions<RogueSweepRunDbContext> options)
        : DbContext(options)
    {
        public DbSet<RogueSweepRun> SweepRuns => Set<RogueSweepRun>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<RogueSweepRun>(b =>
            {
                b.ToTable("sweep_run");
                b.HasKey(run => run.Id);
            });

            modelBuilder.ConfigureCohortTables();
        }
    }

    public sealed class RogueSweepRun
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class AdoptedSweepRunDbContext(
        DbContextOptions<AdoptedSweepRunDbContext> options
    ) : DbContext(options)
    {
        public DbSet<HostSweepRun> SweepRuns => Set<HostSweepRun>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<HostSweepRun>(b =>
            {
                b.ToTable("sweep_run");
                b.HasKey(run => run.SweepId);
            });

            modelBuilder.ConfigureCohortTables();
        }
    }

    public sealed class HostSweepRun
    {
        public Guid SweepId { get; set; }
        public DateTimeOffset StartedAt { get; set; }
        public int Status { get; set; }
        public DateTimeOffset? SettledAt { get; set; }
        public TimeSpan? Duration { get; set; }
        public int TriggerKind { get; set; }
        public bool DryRun { get; set; }
        public Guid TenantId { get; set; }
        public long? TotalAffected { get; set; }
        public string? Error { get; set; }
    }

    private sealed class AdoptedSummaryDbContext(
        DbContextOptions<AdoptedSummaryDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<HostSweepRunEntitySummary>(builder =>
            {
                builder.ToTable("sweep_run_entity_summary");
                builder.HasKey(
                    summary => new
                    {
                        summary.SweepId,
                        summary.RetentionEntityId,
                        summary.Category,
                        summary.TenantId,
                        summary.Strategy,
                    }
                );
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    private sealed class ObsoleteSummaryDbContext(
        DbContextOptions<ObsoleteSummaryDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<HostSweepRunEntitySummary>(builder =>
            {
                builder.ToTable("sweep_run_entity_summary");
                builder.HasKey(
                    summary => new
                    {
                        summary.SweepId,
                        summary.EntityType,
                        summary.Category,
                        summary.TenantId,
                        summary.Strategy,
                    }
                );
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    public sealed class HostSweepRunEntitySummary
    {
        public Guid SweepId { get; set; }
        public string EntityType { get; set; } = "";
        public Guid RetentionEntityId { get; set; }
        public string Category { get; set; } = "";
        public Guid TenantId { get; set; }
        public int Strategy { get; set; }
        public DateTimeOffset At { get; set; }
        public TimeSpan ResolvedPeriod { get; set; }
        public long Affected { get; set; }
        public long HeldCount { get; set; }
        public long SkippedCount { get; set; }
        public long NullAnchorCount { get; set; }
        public string? RuleSource { get; set; }
        public string? RuleReason { get; set; }
    }
}
