using Cohort.Application;
using Cohort.Domain;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

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

    [Fact]
    public void Order_Warns_When_Foreign_Key_Cycles_Force_The_Alphabetical_Fallback()
    {
        using var db = new CyclicTestDbContext(
            new DbContextOptionsBuilder<CyclicTestDbContext>()
                .UseInMemoryDatabase(nameof(Order_Warns_When_Foreign_Key_Cycles_Force_The_Alphabetical_Fallback))
                .Options
        );

        var firstEntry = new RetentionEntry(
            typeof(CycleFirstRecord),
            "cycle_firsts",
            "cycle-first",
            nameof(CycleFirstRecord.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(CycleFirstRecord.Id), "Id", typeof(Guid)),
            [],
            new TenantConvention(nameof(CycleFirstRecord.TenantId), "TenantId"),
            null
        );
        var secondEntry = new RetentionEntry(
            typeof(CycleSecondRecord),
            "cycle_seconds",
            "cycle-second",
            nameof(CycleSecondRecord.CreatedAt),
            "CreatedAt",
            new RecordIdConvention(nameof(CycleSecondRecord.Id), "Id", typeof(Guid)),
            [],
            new TenantConvention(nameof(CycleSecondRecord.TenantId), "TenantId"),
            null
        );
        var logger = new RecordingLogger();

        var ordered = RetentionExecutionPlanOrderer.Order(
            db,
            [secondEntry, firstEntry],
            entry => entry,
            logger
        );

        ordered.Select(entry => entry.EntityType)
            .Should().Equal(typeof(CycleFirstRecord), typeof(CycleSecondRecord));
        logger.Warnings.Should().ContainSingle(message =>
            message.Contains("foreign-key cycle", StringComparison.Ordinal)
            && message.Contains(nameof(CycleFirstRecord), StringComparison.Ordinal)
            && message.Contains(nameof(CycleSecondRecord), StringComparison.Ordinal)
        );
    }

    private sealed class RecordingLogger : ILogger
    {
        public List<string> Warnings { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter
        )
        {
            if (logLevel == LogLevel.Warning)
            {
                Warnings.Add(formatter(state, exception));
            }
        }
    }

    private sealed class CyclicTestDbContext(DbContextOptions<CyclicTestDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CycleFirstRecord>(builder =>
            {
                builder.ToTable("cycle_firsts");
                builder.HasKey(entity => entity.Id);
                builder
                    .HasOne<CycleSecondRecord>()
                    .WithMany()
                    .HasForeignKey(entity => entity.SecondId)
                    .OnDelete(DeleteBehavior.Restrict);
            });

            modelBuilder.Entity<CycleSecondRecord>(builder =>
            {
                builder.ToTable("cycle_seconds");
                builder.HasKey(entity => entity.Id);
                builder
                    .HasOne<CycleFirstRecord>()
                    .WithMany()
                    .HasForeignKey(entity => entity.FirstId)
                    .OnDelete(DeleteBehavior.Restrict);
            });
        }
    }

    private sealed class CycleFirstRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public Guid SecondId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class CycleSecondRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public Guid FirstId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
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
