using Cohort.Application;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Migrations;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Cohort.Sample.Tests;

// Narrow integration tests: the orderer reads EF model metadata but executes no SQL.
public sealed class RetentionExecutionPlanOrdererTests
{
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
            new RelationalObjectName("public", table),
            new CohortStoreTables(
                new("public", CohortTableNames.RetentionHolds),
                new("public", CohortTableNames.SweepRun),
                new("public", CohortTableNames.SweepRunEntitySummary),
                new("public", CohortTableNames.SweepRunRowDetail),
                new("public", CohortTableNames.SweepRowHandlerStatus)
            ),
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
