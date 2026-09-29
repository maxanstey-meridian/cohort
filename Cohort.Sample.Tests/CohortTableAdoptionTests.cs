using Cohort.Infrastructure.Migrations;
using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

// Narrow integration test: table adoption uses Npgsql model metadata but executes no SQL.
public sealed class CohortTableAdoptionTests
{
    private static readonly IReadOnlyList<string> CohortTableNames =
        CohortSchemaContract.TableNames;

    [Fact]
    public void ConfigureCohortTables_Maps_All_Tables_To_The_Supplied_Schema()
    {
        const string schema = "Cohort schema \"quoted\"";
        var options = new DbContextOptionsBuilder<CustomSchemaDbContext>()
            .UseNpgsqlMetadataModel($"custom-cohort-schema-{Guid.NewGuid()}")
            .Options;
        using var db = new CustomSchemaDbContext(options);

        db.Model
            .GetEntityTypes()
            .Where(entityType => CohortTableNames.Contains(entityType.GetTableName()))
            .Should()
            .HaveCount(5)
            .And.OnlyContain(entityType => entityType.GetSchema() == schema);
    }

    [Fact]
    public void ConfigureCohortTables_Does_Not_Adopt_A_Same_Named_Table_In_Another_Schema()
    {
        var options = new DbContextOptionsBuilder<OtherSchemaCollisionDbContext>()
            .UseNpgsqlMetadataModel($"other-schema-collision-{Guid.NewGuid()}")
            .Options;
        using var db = new OtherSchemaCollisionDbContext(options);

        var mappedSweepRuns = db.Model
            .GetEntityTypes()
            .Where(entityType => entityType.GetTableName() == "sweep_run")
            .ToArray();

        mappedSweepRuns.Should().HaveCount(2);
        mappedSweepRuns.Should().ContainSingle(entityType => entityType.GetSchema() == "host");
        mappedSweepRuns.Should().ContainSingle(entityType => entityType.GetSchema() == "cohort");
    }

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

    private sealed class CustomSchemaDbContext(DbContextOptions<CustomSchemaDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables("Cohort schema \"quoted\"");
        }
    }

    private sealed class OtherSchemaCollisionDbContext(
        DbContextOptions<OtherSchemaCollisionDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<RogueSweepRun>(builder =>
            {
                builder.ToTable("sweep_run", "host");
                builder.HasKey(run => run.Id);
            });
            modelBuilder.ConfigureCohortTables("cohort");
        }
    }
}
