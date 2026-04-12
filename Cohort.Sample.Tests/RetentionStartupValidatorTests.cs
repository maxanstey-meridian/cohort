using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;
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
        var repository = new GuardedSampleCategoryRepository();

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
        var repository = new DeferredSampleCategoryRepository();

        var act = async () => await new RetentionStartupValidator(db, repository).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Opaque_Deferred_Resolvers_On_Entities_Without_SoftDelete_Convention()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseInMemoryDatabase($"startup-validator-opaque-deferred-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);

        var act = async () =>
            await new RetentionStartupValidator(db, new OpaqueDeferredSampleCategoryRepository())
                .ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Retention category 'short-lived' for entity {typeof(Note).FullName} uses a deferred resolver that does not declare its possible strategies at startup. Opaque deferred resolvers must either advertise their strategies or target entities with a valid soft-delete or anonymise convention."
            );
    }

    [Fact]
    public async Task ValidateAsync_Allows_Exempt_Sample_Entities_Without_Category_Resolution()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseInMemoryDatabase($"startup-validator-exempt-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);

        var act = async () =>
            await new RetentionStartupValidator(db, new GuardedSampleCategoryRepository()).ValidateAsync();

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
        var options = new DbContextOptionsBuilder<BrokenAnnotationDbContext>()
            .UseInMemoryDatabase($"startup-validator-invalid-anchor-{Guid.NewGuid()}")
            .Options;
        await using var db = new BrokenAnnotationDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["broken-sample"] = new StaticRetentionRuleResolver(
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
                $"[Retain] on {typeof(BrokenAnnotationEntity).FullName}: anchor '{nameof(BrokenAnnotationEntity.Body)}' must be DateTime or DateTimeOffset (nullable allowed), got String."
            );
        exception.Which.Message.Should().Contain(nameof(BrokenAnnotationEntity.Body));
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
        exception.Which.Errors.Should().HaveCount(3);
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'short-lived' for entity {typeof(Note).FullName} could not be resolved."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'soft-delete' for entity {typeof(SoftDeleteRecord).FullName} could not be resolved."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'anonymise' for entity {typeof(AnonymisedContact).FullName} could not be resolved."
            );
        exception.Which.Message.Should().Contain("short-lived");
        exception.Which.Message.Should().Contain("soft-delete");
        exception.Which.Message.Should().Contain("anonymise");
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
                $"[Retain] on {typeof(BrokenAnnotationEntity).FullName}: anchor '{nameof(BrokenAnnotationEntity.Body)}' must be DateTime or DateTimeOffset (nullable allowed), got String."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'missing-category' for entity {typeof(MissingCategoryRecord).FullName} could not be resolved."
            );
        exception.Which.Message.Should().Contain(typeof(UnannotatedRecord).FullName);
        exception.Which.Message.Should().Contain(typeof(BrokenAnnotationEntity).FullName);
        exception.Which.Message.Should().Contain(typeof(MissingCategoryRecord).FullName);
    }

    [Fact]
    public async Task ValidateAsync_Aggregates_Throwing_Startup_Resolvers_With_Other_Failures()
    {
        var options = new DbContextOptionsBuilder<ThrowingResolverAggregateDbContext>()
            .UseInMemoryDatabase($"startup-validator-throwing-resolver-{Guid.NewGuid()}")
            .Options;
        await using var db = new ThrowingResolverAggregateDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["throwing-category"] = new ThrowingStartupRuleResolver("resolver exploded"),
            }
        );

        var act = async () => await new RetentionStartupValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().HaveCount(2);
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'throwing-category' for entity {typeof(ThrowingResolverRecord).FullName} failed startup validation: resolver exploded"
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Entity {typeof(UnannotatedRecord).FullName} must declare exactly one of [Retain] or [ExemptFromRetention]."
            );
        exception.Which.Message.Should().Contain("throwing-category");
        exception.Which.Message.Should().Contain(typeof(UnannotatedRecord).FullName);
    }

    [Fact]
    public async Task Startup_Service_Validates_Before_Scanning_Registry_Metadata()
    {
        var options = new DbContextOptionsBuilder<BrokenAnnotationDbContext>()
            .UseInMemoryDatabase($"startup-service-invalid-anchor-{Guid.NewGuid()}")
            .Options;
        await using var db = new BrokenAnnotationDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["broken-sample"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
            }
        );
        var startup = new SampleRetentionStartupService(
            new RetentionRegistry(db),
            new RetentionStartupValidator(db, repository),
            new RetentionSweepEngine(
                db,
                new RetentionRegistry(db),
                repository,
                [new PurgeSweepStrategy(), new SoftDeleteSweepStrategy()]
            ),
            new RetentionPreviewService(
                db,
                new RetentionRegistry(db),
                repository
            )
        );

        var act = async () => await startup.RunAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"[Retain] on {typeof(BrokenAnnotationEntity).FullName}: anchor '{nameof(BrokenAnnotationEntity.Body)}' must be DateTime or DateTimeOffset (nullable allowed), got String."
            );
    }

    [Fact]
    public async Task Startup_Service_Validates_Before_Previewing_Sweep_Candidates()
    {
        var options = new DbContextOptionsBuilder<BrokenAnnotationDbContext>()
            .UseInMemoryDatabase($"startup-service-preview-invalid-anchor-{Guid.NewGuid()}")
            .Options;
        await using var db = new BrokenAnnotationDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["broken-sample"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
            }
        );
        var startup = new SampleRetentionStartupService(
            new RetentionRegistry(db),
            new RetentionStartupValidator(db, repository),
            new RetentionSweepEngine(
                db,
                new RetentionRegistry(db),
                repository,
                [new PurgeSweepStrategy(), new SoftDeleteSweepStrategy()]
            ),
            new RetentionPreviewService(
                db,
                new RetentionRegistry(db),
                repository
            )
        );

        var act = async () =>
            await startup.RunPreviewAsync(
                new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                DateTimeOffset.UtcNow
            );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"[Retain] on {typeof(BrokenAnnotationEntity).FullName}: anchor '{nameof(BrokenAnnotationEntity.Body)}' must be DateTime or DateTimeOffset (nullable allowed), got String."
            );
    }

    [Fact]
    public async Task Startup_Service_Validates_Before_Sweeping_Retained_Entities()
    {
        var options = new DbContextOptionsBuilder<BrokenAnnotationDbContext>()
            .UseInMemoryDatabase($"startup-service-sweep-invalid-anchor-{Guid.NewGuid()}")
            .Options;
        await using var db = new BrokenAnnotationDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["broken-sample"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
            }
        );
        var startup = new SampleRetentionStartupService(
            new RetentionRegistry(db),
            new RetentionStartupValidator(db, repository),
            new RetentionSweepEngine(
                db,
                new RetentionRegistry(db),
                repository,
                [new PurgeSweepStrategy(), new SoftDeleteSweepStrategy()]
            ),
            new RetentionPreviewService(
                db,
                new RetentionRegistry(db),
                repository
            )
        );

        var act = async () =>
            await startup.RunSweepAsync(
                new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                DateTimeOffset.UtcNow
            );

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"[Retain] on {typeof(BrokenAnnotationEntity).FullName}: anchor '{nameof(BrokenAnnotationEntity.Body)}' must be DateTime or DateTimeOffset (nullable allowed), got String."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Invalid_Tenant_Metadata()
    {
        var options = new DbContextOptionsBuilder<InvalidTenantDbContext>()
            .UseInMemoryDatabase($"startup-validator-invalid-tenant-{Guid.NewGuid()}")
            .Options;
        await using var db = new InvalidTenantDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["tenant-category"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
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
                $"Tenant convention on {typeof(InvalidTenantRecord).FullName}: TenantId must be Guid or nullable Guid, got String."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_SoftDelete_Categories_Without_A_Public_Bool_IsDeleted_Property()
    {
        var options = new DbContextOptionsBuilder<InvalidSoftDeleteIsDeletedDbContext>()
            .UseInMemoryDatabase($"startup-validator-invalid-soft-delete-flag-{Guid.NewGuid()}")
            .Options;
        await using var db = new InvalidSoftDeleteIsDeletedDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["invalid-soft-delete"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
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
                $"Soft-delete convention on {typeof(InvalidSoftDeleteIsDeletedRecord).FullName}: retained SoftDelete categories require a public bool IsDeleted CLR property."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_SoftDelete_Categories_With_Invalid_DeletedAt_Types()
    {
        var options = new DbContextOptionsBuilder<InvalidSoftDeleteDeletedAtDbContext>()
            .UseInMemoryDatabase($"startup-validator-invalid-soft-delete-deleted-at-{Guid.NewGuid()}")
            .Options;
        await using var db = new InvalidSoftDeleteDeletedAtDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["invalid-soft-delete"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
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
                $"Soft-delete convention on {typeof(InvalidSoftDeleteDeletedAtRecord).FullName}: DeletedAt must be DateTime or DateTimeOffset (nullable allowed), got String."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_SoftDelete_Categories_Without_Tenant_Metadata()
    {
        var options = new DbContextOptionsBuilder<MissingSoftDeleteTenantDbContext>()
            .UseInMemoryDatabase($"startup-validator-missing-soft-delete-tenant-{Guid.NewGuid()}")
            .Options;
        await using var db = new MissingSoftDeleteTenantDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["missing-soft-delete-tenant"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
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
                $"Soft-delete convention on {typeof(MissingSoftDeleteTenantRecord).FullName}: retained SoftDelete categories require tenant metadata via a public Guid or nullable Guid TenantId property mapped by EF."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Anonymise_Categories_Without_Annotated_Fields()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseInMemoryDatabase($"startup-validator-missing-anonymise-fields-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["short-lived"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
                ["soft-delete"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
                ["anonymise"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
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
                $"Anonymise convention on {typeof(Note).FullName}: retained Anonymise categories require at least one [Anonymise]-annotated property mapped by EF."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Anonymise_Categories_With_Invalid_Method_Type_Mismatches()
    {
        var options = new DbContextOptionsBuilder<InvalidAnonymiseMethodDbContext>()
            .UseInMemoryDatabase($"startup-validator-invalid-anonymise-methods-{Guid.NewGuid()}")
            .Options;
        await using var db = new InvalidAnonymiseMethodDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["invalid-null-anonymise"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
                ["invalid-empty-string-anonymise"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
                ["invalid-fixed-literal-anonymise"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
            }
        );

        var act = async () => await new RetentionStartupValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().HaveCount(3);
        exception
            .Which.Errors.Should()
            .Contain(
                $"Anonymise convention on {typeof(InvalidNullAnonymiseRecord).FullName}: [Anonymise] member Age uses Null but Int32 is not nullable."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Anonymise convention on {typeof(InvalidEmptyStringAnonymiseRecord).FullName}: [Anonymise] member ExternalId uses EmptyString but Guid is not string."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Anonymise convention on {typeof(InvalidFixedLiteralAnonymiseRecord).FullName}: [Anonymise] member LastSeenAt uses FixedLiteral but DateTimeOffset is not string."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Anonymise_Categories_Without_Tenant_Metadata()
    {
        var options = new DbContextOptionsBuilder<MissingAnonymiseTenantDbContext>()
            .UseInMemoryDatabase($"startup-validator-missing-anonymise-tenant-{Guid.NewGuid()}")
            .Options;
        await using var db = new MissingAnonymiseTenantDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["missing-anonymise-tenant"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
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
                $"Anonymise convention on {typeof(MissingAnonymiseTenantRecord).FullName}: retained Anonymise categories require tenant metadata via a public Guid or nullable Guid TenantId property mapped by EF."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Opaque_Deferred_SoftDelete_Categories_Without_Tenant_Metadata()
    {
        var options = new DbContextOptionsBuilder<MissingSoftDeleteTenantDbContext>()
            .UseInMemoryDatabase($"startup-validator-opaque-soft-delete-missing-tenant-{Guid.NewGuid()}")
            .Options;
        await using var db = new MissingSoftDeleteTenantDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["missing-soft-delete-tenant"] = new OpaqueDeferredRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
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
                $"Retention category 'missing-soft-delete-tenant' for entity {typeof(MissingSoftDeleteTenantRecord).FullName} uses a deferred resolver that does not declare its possible strategies at startup. retained SoftDelete categories require tenant metadata via a public Guid or nullable Guid TenantId property mapped by EF."
            );
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

    private sealed class DeferredRuleResolver(RetentionRule rule) : IRetentionRuleResolver
    {
        public IReadOnlySet<Strategy>? GetPossibleStrategiesAtStartup() => new HashSet<Strategy>
        {
            rule.Strategy,
        };

        public Task<RetentionRule> ResolveAsync(RetentionResolutionContext ctx, CancellationToken ct) =>
            Task.FromResult(rule);
    }

    private sealed class OpaqueDeferredRuleResolver(RetentionRule rule) : IRetentionRuleResolver
    {
        public Task<RetentionRule> ResolveAsync(RetentionResolutionContext ctx, CancellationToken ct) =>
            Task.FromResult(rule);
    }

    private sealed class GuardedSampleCategoryRepository : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            if (category == "short-lived")
            {
                return Task.FromResult<IRetentionRuleResolver?>(
                    new StaticRetentionRuleResolver(new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge))
                );
            }

            if (category == "soft-delete")
            {
                return Task.FromResult<IRetentionRuleResolver?>(
                    new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    )
                );
            }

            if (category == "anonymise")
            {
                return Task.FromResult<IRetentionRuleResolver?>(
                    new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    )
                );
            }

            throw new InvalidOperationException(
                $"Unexpected category lookup for '{category}'. Exempt sample entities must not resolve categories."
            );
        }
    }

    private sealed class DeferredSampleCategoryRepository : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            if (category == "short-lived")
            {
                return Task.FromResult<IRetentionRuleResolver?>(
                    new DeferredRuleResolver(new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge))
                );
            }

            if (category == "soft-delete")
            {
                return Task.FromResult<IRetentionRuleResolver?>(
                    new DeferredRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    )
                );
            }

            if (category == "anonymise")
            {
                return Task.FromResult<IRetentionRuleResolver?>(
                    new DeferredRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    )
                );
            }

            throw new InvalidOperationException(
                $"Unexpected category lookup for '{category}'. Exempt sample entities must not resolve categories."
            );
        }
    }

    private sealed class ThrowingStartupRuleResolver(string message) : IRetentionRuleResolver
    {
        public Task<RetentionRule> ResolveAsync(RetentionResolutionContext ctx, CancellationToken ct) =>
            Task.FromResult(new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge));

        public RetentionRule? TryResolveAtStartup() => throw new InvalidOperationException(message);
    }

    private sealed class OpaqueDeferredSampleCategoryRepository : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            return category switch
            {
                "short-lived" => Task.FromResult<IRetentionRuleResolver?>(
                    new OpaqueDeferredRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    )
                ),
                "soft-delete" => Task.FromResult<IRetentionRuleResolver?>(
                    new OpaqueDeferredRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    )
                ),
                "anonymise" => Task.FromResult<IRetentionRuleResolver?>(
                    new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    )
                ),
                _ => throw new InvalidOperationException(
                    $"Unexpected category lookup for '{category}'."
                ),
            };
        }
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

    private sealed class BrokenAnnotationDbContext(DbContextOptions<BrokenAnnotationDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BrokenAnnotationEntity>(entity =>
            {
                entity.ToTable("broken_annotation_entities");
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
            modelBuilder.Entity<BrokenAnnotationEntity>(entity =>
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

    private sealed class ThrowingResolverAggregateDbContext(
        DbContextOptions<ThrowingResolverAggregateDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ThrowingResolverRecord>(entity =>
            {
                entity.ToTable("throwing_resolver_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
            });
            modelBuilder.Entity<UnannotatedRecord>(entity =>
            {
                entity.ToTable("throwing_resolver_unannotated_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
            });
        }
    }

    private sealed class InvalidTenantDbContext(DbContextOptions<InvalidTenantDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<InvalidTenantRecord>(entity =>
            {
                entity.ToTable("invalid_tenant_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
            });
        }
    }

    private sealed class InvalidSoftDeleteIsDeletedDbContext(
        DbContextOptions<InvalidSoftDeleteIsDeletedDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<InvalidSoftDeleteIsDeletedRecord>(entity =>
            {
                entity.ToTable("invalid_soft_delete_is_deleted_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.IsDeleted).HasColumnName("is_deleted");
            });
        }
    }

    private sealed class InvalidSoftDeleteDeletedAtDbContext(
        DbContextOptions<InvalidSoftDeleteDeletedAtDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<InvalidSoftDeleteDeletedAtRecord>(entity =>
            {
                entity.ToTable("invalid_soft_delete_deleted_at_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.IsDeleted).HasColumnName("is_deleted");
                entity.Property(record => record.DeletedAt).HasColumnName("deleted_at_utc");
            });
        }
    }

    private sealed class MissingSoftDeleteTenantDbContext(
        DbContextOptions<MissingSoftDeleteTenantDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<MissingSoftDeleteTenantRecord>(entity =>
            {
                entity.ToTable("missing_soft_delete_tenant_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.IsDeleted).HasColumnName("is_deleted");
                entity.Property(record => record.DeletedAt).HasColumnName("deleted_at_utc");
            });
        }
    }

    private sealed class InvalidAnonymiseMethodDbContext(
        DbContextOptions<InvalidAnonymiseMethodDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<InvalidNullAnonymiseRecord>(entity =>
            {
                entity.ToTable("invalid_null_anonymise_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.Age).HasColumnName("age");
            });
            modelBuilder.Entity<InvalidEmptyStringAnonymiseRecord>(entity =>
            {
                entity.ToTable("invalid_empty_string_anonymise_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.ExternalId).HasColumnName("external_id");
            });
            modelBuilder.Entity<InvalidFixedLiteralAnonymiseRecord>(entity =>
            {
                entity.ToTable("invalid_fixed_literal_anonymise_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.LastSeenAt).HasColumnName("last_seen_at");
            });
        }
    }

    private sealed class MissingAnonymiseTenantDbContext(
        DbContextOptions<MissingAnonymiseTenantDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<MissingAnonymiseTenantRecord>(entity =>
            {
                entity.ToTable("missing_anonymise_tenant_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.EmailAddress).HasColumnName("email_address");
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

    [Retain("throwing-category", nameof(CreatedAt))]
    private sealed class ThrowingResolverRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("tenant-category", nameof(CreatedAt))]
    private sealed class InvalidTenantRecord
    {
        public Guid Id { get; init; }
        public string TenantId { get; init; } = "";
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("invalid-soft-delete", nameof(InvalidSoftDeleteIsDeletedRecord.CreatedAt))]
    private sealed class InvalidSoftDeleteIsDeletedRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public string IsDeleted { get; init; } = "";
    }

    [Retain("invalid-soft-delete", nameof(InvalidSoftDeleteDeletedAtRecord.CreatedAt))]
    private sealed class InvalidSoftDeleteDeletedAtRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public bool IsDeleted { get; init; }
        public string DeletedAt { get; init; } = "";
    }

    [Retain("missing-soft-delete-tenant", nameof(MissingSoftDeleteTenantRecord.CreatedAt))]
    private sealed class MissingSoftDeleteTenantRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public bool IsDeleted { get; init; }
        public DateTimeOffset? DeletedAt { get; init; }
    }

    [Retain("invalid-null-anonymise", nameof(InvalidNullAnonymiseRecord.CreatedAt))]
    private sealed class InvalidNullAnonymiseRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.Null)]
        public int Age { get; init; }
    }

    [Retain("invalid-empty-string-anonymise", nameof(InvalidEmptyStringAnonymiseRecord.CreatedAt))]
    private sealed class InvalidEmptyStringAnonymiseRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.EmptyString)]
        public Guid ExternalId { get; init; }
    }

    [Retain("invalid-fixed-literal-anonymise", nameof(InvalidFixedLiteralAnonymiseRecord.CreatedAt))]
    private sealed class InvalidFixedLiteralAnonymiseRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.FixedLiteral, "[redacted]")]
        public DateTimeOffset LastSeenAt { get; init; }
    }

    [Retain("missing-anonymise-tenant", nameof(MissingAnonymiseTenantRecord.CreatedAt))]
    private sealed class MissingAnonymiseTenantRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.Null)]
        public string? EmailAddress { get; init; }
    }
}
