using Cohort.Application;
using Cohort.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Cohort.Sample.Tests;

// Narrow integration tests: the orderer reads EF model metadata but executes no SQL.
public sealed class RetentionExecutionPlanOrdererTests
{
    [Fact]
    public void Order_Runs_Dependent_Child_Before_Retained_Parent()
    {
        using var db = new DependencyOrderedTestDbContext(
            new DbContextOptionsBuilder<DependencyOrderedTestDbContext>()
                .UseNpgsqlMetadataModel(nameof(Order_Runs_Dependent_Child_Before_Retained_Parent))
                .Options
        );

        var parentEntry = CreateEntry<ParentRecord>("parents", "parent");
        var childEntry = CreateEntry<ChildRecord>("children", "child");

        var ordered = RetentionExecutionPlanOrderer.Order(
            db,
            [parentEntry, childEntry],
            entry => entry
        );

        ordered
            .Select(entry => entry.EntityType)
            .Should()
            .Equal(typeof(ChildRecord), typeof(ParentRecord));
    }

    [Fact]
    public void Order_Preserves_Alphabetical_Fallback_When_Entities_Are_Unrelated()
    {
        using var db = new DependencyOrderedTestDbContext(
            new DbContextOptionsBuilder<DependencyOrderedTestDbContext>()
                .UseNpgsqlMetadataModel(
                    nameof(Order_Preserves_Alphabetical_Fallback_When_Entities_Are_Unrelated)
                )
                .Options
        );

        var zetaEntry = CreateEntry<ZetaRecord>("zetas", "zeta");
        var alphaEntry = CreateEntry<AlphaRecord>("alphas", "alpha");

        var ordered = RetentionExecutionPlanOrderer.Order(
            db,
            [zetaEntry, alphaEntry],
            entry => entry
        );

        ordered
            .Select(entry => entry.EntityType)
            .Should()
            .Equal(typeof(AlphaRecord), typeof(ZetaRecord));
    }

    [Fact]
    public void Order_Rejects_Foreign_Key_Cycles()
    {
        using var db = new CyclicTestDbContext(
            new DbContextOptionsBuilder<CyclicTestDbContext>()
                .UseNpgsqlMetadataModel(
                    nameof(Order_Rejects_Foreign_Key_Cycles)
                )
                .Options
        );
        var firstEntry = CreateEntry<CycleFirstRecord>("cycle_firsts", "cycle-first");
        var secondEntry = CreateEntry<CycleSecondRecord>("cycle_seconds", "cycle-second");
        var logger = new RecordingLogger();

        var act = () =>
            RetentionExecutionPlanOrderer.Order(
                db,
                [secondEntry, firstEntry],
                entry => entry,
                logger
            );

        act.Should()
            .Throw<RetentionConfigurationException>()
            .Which.Errors.Should()
            .ContainSingle(error => error.Contains("foreign-key graph contains a cycle"));
        logger.Entries.Should().ContainSingle();
        logger.Entries[0].Level.Should().Be(LogLevel.Error);
        logger.Entries[0]
            .Message.Should()
            .Contain("foreign-key graph contains a cycle")
            .And.Contain(typeof(CycleFirstRecord).FullName)
            .And.Contain(typeof(CycleSecondRecord).FullName);
    }

    private static RetentionEntry CreateEntry<TEntity>(string table, string category) =>
        new(
            typeof(TEntity),
            Guid.NewGuid(),
            table,
            category,
            "CreatedAt",
            "CreatedAt",
            new RecordIdConvention("Id", "Id", typeof(Guid)),
            [],
            [],
            new TenantConvention("TenantId", "TenantId"),
            null
        );

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

    private sealed class DependencyOrderedTestDbContext(
        DbContextOptions<DependencyOrderedTestDbContext> options
    ) : DbContext(options)
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
            modelBuilder.Entity<AlphaRecord>(builder => builder.HasKey(entity => entity.Id));
            modelBuilder.Entity<ZetaRecord>(builder => builder.HasKey(entity => entity.Id));
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

    private sealed class RecordingLogger : ILogger
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter
        ) => Entries.Add((logLevel, formatter(state, exception)));
    }
}
