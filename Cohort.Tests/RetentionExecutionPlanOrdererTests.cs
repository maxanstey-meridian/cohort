using Cohort.Application;
using Cohort.Domain;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Tests;

public sealed class RetentionExecutionPlanOrdererTests
{
    [Fact]
    public void Order_Runs_Dependent_Child_Before_Retained_Parent()
    {
        using var db = new DependencyOrderedTestDbContext(
            new DbContextOptionsBuilder<DependencyOrderedTestDbContext>()
                .UseInMemoryDatabase(nameof(Order_Runs_Dependent_Child_Before_Retained_Parent))
                .Options
        );

        var parentEntry = new RetentionEntry(
            typeof(ParentRecord),
            "parents",
            "parent",
            nameof(ParentRecord.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(ParentRecord.Id), "Id", typeof(Guid)),
            [],
            new TenantConvention(nameof(ParentRecord.TenantId), "TenantId"),
            null
        );
        var childEntry = new RetentionEntry(
            typeof(ChildRecord),
            "children",
            "child",
            nameof(ChildRecord.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(ChildRecord.Id), "Id", typeof(Guid)),
            [],
            new TenantConvention(nameof(ChildRecord.TenantId), "TenantId"),
            null
        );

        var ordered = RetentionExecutionPlanOrderer.Order(
            db,
            [parentEntry, childEntry],
            entry => entry
        );

        ordered.Select(entry => entry.EntityType).Should().Equal(typeof(ChildRecord), typeof(ParentRecord));
    }

    [Fact]
    public void Order_Preserves_Alphabetical_Fallback_When_Entities_Are_Unrelated()
    {
        using var db = new DependencyOrderedTestDbContext(
            new DbContextOptionsBuilder<DependencyOrderedTestDbContext>()
                .UseInMemoryDatabase(nameof(Order_Preserves_Alphabetical_Fallback_When_Entities_Are_Unrelated))
                .Options
        );

        var zetaEntry = new RetentionEntry(
            typeof(ZetaRecord),
            "zetas",
            "zeta",
            nameof(ZetaRecord.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(ZetaRecord.Id), "Id", typeof(Guid)),
            [],
            new TenantConvention(nameof(ZetaRecord.TenantId), "TenantId"),
            null
        );
        var alphaEntry = new RetentionEntry(
            typeof(AlphaRecord),
            "alphas",
            "alpha",
            nameof(AlphaRecord.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(AlphaRecord.Id), "Id", typeof(Guid)),
            [],
            new TenantConvention(nameof(AlphaRecord.TenantId), "TenantId"),
            null
        );

        var ordered = RetentionExecutionPlanOrderer.Order(
            db,
            [zetaEntry, alphaEntry],
            entry => entry
        );

        ordered.Select(entry => entry.EntityType).Should().Equal(typeof(AlphaRecord), typeof(ZetaRecord));
    }

    private sealed class DependencyOrderedTestDbContext(DbContextOptions<DependencyOrderedTestDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ParentRecord>(builder =>
            {
                builder.ToTable("parents");
                builder.HasKey(entity => entity.Id);
            });

            modelBuilder.Entity<ChildRecord>(builder =>
            {
                builder.ToTable("children");
                builder.HasKey(entity => entity.Id);
                builder
                    .HasOne<ParentRecord>()
                    .WithMany()
                    .HasForeignKey(entity => entity.ParentId)
                    .OnDelete(DeleteBehavior.Restrict);
            });

            modelBuilder.Entity<AlphaRecord>(builder =>
            {
                builder.ToTable("alphas");
                builder.HasKey(entity => entity.Id);
            });

            modelBuilder.Entity<ZetaRecord>(builder =>
            {
                builder.ToTable("zetas");
                builder.HasKey(entity => entity.Id);
            });
        }
    }

    private sealed class ParentRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class ChildRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public Guid ParentId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class AlphaRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class ZetaRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }
}
