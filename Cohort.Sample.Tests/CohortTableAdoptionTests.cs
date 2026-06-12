using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

// Narrow integration test: table adoption happens during model building, which the
// InMemory provider fully serves — no SQL involved.
public sealed class CohortTableAdoptionTests
{
    [Fact]
    public void ConfigureCohortTables_Rejects_Host_Entities_Coincidentally_Mapped_To_Cohort_Table_Names()
    {
        var options = new DbContextOptionsBuilder<RogueSweepRunDbContext>()
            .UseInMemoryDatabase($"rogue-sweep-run-{Guid.NewGuid()}")
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
            .UseInMemoryDatabase($"adopted-sweep-run-{Guid.NewGuid()}")
            .Options;
        using var db = new AdoptedSweepRunDbContext(options);

        var adopted = db.Model.FindEntityType(typeof(HostSweepRun));

        adopted.Should().NotBeNull();
        adopted!.GetTableName().Should().Be("sweep_run");
        adopted!.FindPrimaryKey()!.Properties.Select(property => property.Name)
            .Should().Equal("SweepId");
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

    private sealed class AdoptedSweepRunDbContext(DbContextOptions<AdoptedSweepRunDbContext> options)
        : DbContext(options)
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
        public DateTimeOffset? CompletedAt { get; set; }
        public TimeSpan? Duration { get; set; }
        public int TriggerKind { get; set; }
        public bool DryRun { get; set; }
        public Guid TenantId { get; set; }
        public long? TotalAffected { get; set; }
        public DateTimeOffset? FailedAt { get; set; }
        public string? Error { get; set; }
    }
}
