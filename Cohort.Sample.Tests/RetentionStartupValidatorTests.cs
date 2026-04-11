using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

public sealed class RetentionStartupValidatorTests
{
    [Fact]
    public async Task ValidateAsync_Succeeds_For_Retained_Entities_With_Static_Resolvers()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseInMemoryDatabase($"startup-validator-static-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["short-lived"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
            }
        );

        var act = async () => await new RetentionStartupValidator(db, repository).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Allows_Deferred_Resolvers_At_Startup()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseInMemoryDatabase($"startup-validator-deferred-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["short-lived"] = new DeferredRuleResolver(),
            }
        );

        var act = async () => await new RetentionStartupValidator(db, repository).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Entities_Without_Retention_Or_Exemption_Metadata()
    {
        var options = new DbContextOptionsBuilder<MissingAttributeDbContext>()
            .UseInMemoryDatabase($"startup-validator-missing-attribute-{Guid.NewGuid()}")
            .Options;
        await using var db = new MissingAttributeDbContext(options);

        var act = async () =>
            await new RetentionStartupValidator(db, InMemoryCategoryRepository.Empty).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Entity {typeof(UnannotatedRecord).FullName} must declare exactly one of [Retain] or [ExemptFromRetention]."
            );
        exception.Which.Message.Should().Contain(typeof(UnannotatedRecord).FullName);
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Entities_With_Both_Retention_And_Exemption_Metadata()
    {
        var options = new DbContextOptionsBuilder<ConflictingAttributeDbContext>()
            .UseInMemoryDatabase($"startup-validator-conflicting-attribute-{Guid.NewGuid()}")
            .Options;
        await using var db = new ConflictingAttributeDbContext(options);

        var act = async () =>
            await new RetentionStartupValidator(db, InMemoryCategoryRepository.Empty).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Entity {typeof(ConflictingRecord).FullName} must declare exactly one of [Retain] or [ExemptFromRetention]."
            );
        exception.Which.Message.Should().Contain(typeof(ConflictingRecord).FullName);
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Invalid_Retention_Anchor_Metadata()
    {
        var options = new DbContextOptionsBuilder<InvalidAnchorDbContext>()
            .UseInMemoryDatabase($"startup-validator-invalid-anchor-{Guid.NewGuid()}")
            .Options;
        await using var db = new InvalidAnchorDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["invalid-anchor"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(90), Strategy.Purge)
                ),
            }
        );

        var act = async () => await new RetentionStartupValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"[Retain] on {typeof(InvalidAnchorRecord).FullName}: anchor '{nameof(InvalidAnchorRecord.Body)}' must be DateTime or DateTimeOffset (nullable allowed), got String."
            );
        exception.Which.Message.Should().Contain(nameof(InvalidAnchorRecord.Body));
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Missing_Category_Resolvers()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseInMemoryDatabase($"startup-validator-missing-category-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);

        var act = async () =>
            await new RetentionStartupValidator(db, InMemoryCategoryRepository.Empty).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Retention category 'short-lived' for entity {typeof(Note).FullName} could not be resolved."
            );
        exception.Which.Message.Should().Contain("short-lived");
    }

    [Fact]
    public async Task ValidateAsync_Aggregates_Multiple_Independent_Failures()
    {
        var options = new DbContextOptionsBuilder<AggregateFailureDbContext>()
            .UseInMemoryDatabase($"startup-validator-aggregate-{Guid.NewGuid()}")
            .Options;
        await using var db = new AggregateFailureDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["valid-category"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
            }
        );

        var act = async () => await new RetentionStartupValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().HaveCount(3);
        exception
            .Which.Errors.Should()
            .Contain(
                $"Entity {typeof(UnannotatedRecord).FullName} must declare exactly one of [Retain] or [ExemptFromRetention]."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"[Retain] on {typeof(InvalidAnchorRecord).FullName}: anchor '{nameof(InvalidAnchorRecord.Body)}' must be DateTime or DateTimeOffset (nullable allowed), got String."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'missing-category' for entity {typeof(MissingCategoryRecord).FullName} could not be resolved."
            );
        exception.Which.Message.Should().Contain(typeof(UnannotatedRecord).FullName);
        exception.Which.Message.Should().Contain(typeof(InvalidAnchorRecord).FullName);
        exception.Which.Message.Should().Contain(typeof(MissingCategoryRecord).FullName);
    }

    private sealed class InMemoryCategoryRepository(
        IReadOnlyDictionary<string, IRetentionRuleResolver> resolvers
    ) : IRetentionCategoryRepository
    {
        public static InMemoryCategoryRepository Empty { get; } = new(
            new Dictionary<string, IRetentionRuleResolver>()
        );

        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            resolvers.TryGetValue(category, out var resolver);
            return Task.FromResult(resolver);
        }
    }

    private sealed class DeferredRuleResolver : IRetentionRuleResolver
    {
        public Task<RetentionRule> ResolveAsync(RetentionResolutionContext ctx, CancellationToken ct) =>
            Task.FromResult(new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge));
    }

    private sealed class MissingAttributeDbContext(DbContextOptions<MissingAttributeDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<UnannotatedRecord>(entity =>
            {
                entity.ToTable("unannotated_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
            });
        }
    }

    private sealed class ConflictingAttributeDbContext(
        DbContextOptions<ConflictingAttributeDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ConflictingRecord>(entity =>
            {
                entity.ToTable("conflicting_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
            });
        }
    }

    private sealed class InvalidAnchorDbContext(DbContextOptions<InvalidAnchorDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<InvalidAnchorRecord>(entity =>
            {
                entity.ToTable("invalid_anchor_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.Body).HasColumnName("body");
            });
        }
    }

    private sealed class AggregateFailureDbContext(DbContextOptions<AggregateFailureDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<UnannotatedRecord>(entity =>
            {
                entity.ToTable("aggregate_unannotated_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
            });
            modelBuilder.Entity<InvalidAnchorRecord>(entity =>
            {
                entity.ToTable("aggregate_invalid_anchor_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.Body).HasColumnName("body");
            });
            modelBuilder.Entity<MissingCategoryRecord>(entity =>
            {
                entity.ToTable("aggregate_missing_category_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
            });
            modelBuilder.Entity<ValidRetainedRecord>(entity =>
            {
                entity.ToTable("aggregate_valid_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
            });
        }
    }

    private sealed class UnannotatedRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("conflict-category", nameof(CreatedAt))]
    [ExemptFromRetention("covered by statutory retention")]
    private sealed class ConflictingRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("invalid-anchor", nameof(Body))]
    private sealed class InvalidAnchorRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public string Body { get; init; } = "";
    }

    [Retain("missing-category", nameof(CreatedAt))]
    private sealed class MissingCategoryRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("valid-category", nameof(CreatedAt))]
    private sealed class ValidRetainedRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }
}
