using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Sweep;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Cohort.Sample.Tests;

public sealed class RetentionStartupValidatorTests
{
    private static readonly IRetentionRuleResolver ExemptResolver = new StaticRetentionRuleResolver(
        new RetentionRule(TimeSpan.FromDays(30), Strategy.Exempt)
    );

    [Fact]
    public async Task ValidateAsync_Succeeds_For_Retained_Entities_With_Static_Resolvers()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseInMemoryDatabase($"startup-validator-static-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);
        var repository = new GuardedSampleCategoryRepository();

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

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

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Allows_Opaque_Deferred_Resolvers_Without_Declaring_Possible_Strategies()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseInMemoryDatabase($"startup-validator-opaque-deferred-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);

        var act = async () =>
            await CreateValidator(db, new OpaqueDeferredSampleCategoryRepository())
                .ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Allows_Opaque_Deferred_Resolvers_On_Entities_With_Only_Anonymise_Convention()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseInMemoryDatabase($"startup-validator-opaque-anonymise-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);

        var act = async () =>
            await CreateValidator(db, new OpaqueDeferredAnonymiseSampleCategoryRepository())
                .ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Allows_Exempt_Sample_Entities_Without_Category_Resolution()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseInMemoryDatabase($"startup-validator-exempt-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);

        var act = async () =>
            await CreateValidator(db, new GuardedSampleCategoryRepository()).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Passes_For_Unannotated_Entities_As_Implicitly_Exempt()
    {
        var options = new DbContextOptionsBuilder<MissingAttributeDbContext>()
            .UseInMemoryDatabase($"startup-validator-missing-attribute-{Guid.NewGuid()}")
            .Options;
        await using var db = new MissingAttributeDbContext(options);

        var act = async () =>
            await new RetentionStartupValidator(db, InMemoryCategoryRepository.Empty, new RetentionEntryBuilder(new CohortConventions())).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Entities_With_Both_Retention_And_Exemption_Metadata()
    {
        var options = new DbContextOptionsBuilder<ConflictingAttributeDbContext>()
            .UseInMemoryDatabase($"startup-validator-conflicting-attribute-{Guid.NewGuid()}")
            .Options;
        await using var db = new ConflictingAttributeDbContext(options);

        var act = async () =>
            await new RetentionStartupValidator(db, InMemoryCategoryRepository.Empty, new RetentionEntryBuilder(new CohortConventions())).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Entity {typeof(ConflictingRecord).FullName} must declare exactly one of [Retain] or [ExemptFromRetention], not both."
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

        var act = async () => await new RetentionStartupValidator(db, repository, new RetentionEntryBuilder(new CohortConventions())).ValidateAsync();

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
            await new RetentionStartupValidator(db, InMemoryCategoryRepository.Empty, new RetentionEntryBuilder(new CohortConventions())).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().HaveCount(7);
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
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'tenantless-purge' for entity {typeof(TenantlessLog).FullName} could not be resolved."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'tenantless-softdelete' for entity {typeof(TenantlessSoftDelete).FullName} could not be resolved."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'per-row-audit-override' for entity {typeof(PerRowAuditedLog).FullName} could not be resolved."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'tombstone-anonymise' for entity {typeof(TombstoneRecord).FullName} could not be resolved."
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

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().HaveCount(2);
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

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors.Should()
            .Contain(
                $"Retention category 'throwing-category' for entity {typeof(ThrowingResolverRecord).FullName} failed startup validation: resolver exploded"
            );
        exception.Which.Message.Should().Contain("throwing-category");
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
        var startup = CreateStartupService(db, repository);

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
        var startup = CreateStartupService(db, repository);

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
        var startup = CreateStartupService(db, repository);

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
    public async Task Startup_Service_Validates_Before_Erasing_Subject_Matched_Entities()
    {
        var options = new DbContextOptionsBuilder<BrokenAnnotationDbContext>()
            .UseInMemoryDatabase($"startup-service-erasure-invalid-anchor-{Guid.NewGuid()}")
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
        var startup = CreateStartupService(db, repository);

        var act = async () =>
            await startup.RunErasureAsync(
                new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                new ErasureScope(Guid.NewGuid()),
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
    public async Task Sweep_Engine_Validates_Before_Sweeping_Retained_Entities()
    {
        var options = new DbContextOptionsBuilder<BrokenAnnotationDbContext>()
            .UseInMemoryDatabase($"sweep-engine-invalid-anchor-{Guid.NewGuid()}")
            .Options;
        await using var db = new BrokenAnnotationDbContext(options);
        var repository = CreateBrokenAnnotationRepository();
        var engine = CreateSweepEngine(db, repository);

        var act = async () =>
            await engine.SweepAsync(
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
    public async Task Preview_Service_Validates_Before_Previewing_Sweep_Candidates()
    {
        var options = new DbContextOptionsBuilder<BrokenAnnotationDbContext>()
            .UseInMemoryDatabase($"preview-service-invalid-anchor-{Guid.NewGuid()}")
            .Options;
        await using var db = new BrokenAnnotationDbContext(options);
        var repository = CreateBrokenAnnotationRepository();
        var preview = CreatePreviewService(db, repository);

        var act = async () =>
            await preview.PreviewAsync(
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
    public async Task Erasure_Service_Validates_Before_Erasing_Subject_Matched_Entities()
    {
        var options = new DbContextOptionsBuilder<BrokenAnnotationDbContext>()
            .UseInMemoryDatabase($"erasure-service-invalid-anchor-{Guid.NewGuid()}")
            .Options;
        await using var db = new BrokenAnnotationDbContext(options);
        var repository = CreateBrokenAnnotationRepository();
        var erasure = CreateErasureService(db, repository);

        var act = async () =>
            await erasure.EraseAsync(
                new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                new ErasureScope(Guid.NewGuid()),
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

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

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

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Soft-delete convention on {typeof(InvalidSoftDeleteIsDeletedRecord).FullName}: soft-delete flag 'IsDeleted' must be a public bool CLR property."
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

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Soft-delete convention on {typeof(InvalidSoftDeleteDeletedAtRecord).FullName}: 'DeletedAt' must be DateTime or DateTimeOffset (nullable allowed), got String."
            );
    }

    [Fact]
    public async Task ValidateAsync_Allows_SoftDelete_Categories_Without_Tenant_Metadata()
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

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        await act.Should().NotThrowAsync();
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
                // Other sample entities in SampleDbContext aren't the subject of this test;
                // resolve them as Exempt so only the Anonymise-on-Note mismatch surfaces.
                ["tenantless-purge"] = ExemptResolver,
                ["tenantless-softdelete"] = ExemptResolver,
                ["per-row-audit-override"] = ExemptResolver,
                ["tombstone-anonymise"] = ExemptResolver,
            }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

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

        var act = async () => await new RetentionStartupValidator(db, repository, new RetentionEntryBuilder(new CohortConventions())).ValidateAsync();

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
    public async Task ValidateAsync_Rejects_FactoryBacked_Anonymise_Fields_With_Invalid_Factory_Types()
    {
        var options = new DbContextOptionsBuilder<InvalidFactoryTypeAnonymiseDbContext>()
            .UseInMemoryDatabase($"startup-validator-factory-backed-invalid-type-{Guid.NewGuid()}")
            .Options;
        await using var db = new InvalidFactoryTypeAnonymiseDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["invalid-factory-type-anonymise"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
            }
        );

        var act = async () =>
            await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Anonymise convention on {typeof(InvalidFactoryTypeAnonymiseRecord).FullName}: [AnonymiseWith] member ExternalId specifies factory type {typeof(NotAFactory).FullName} which does not implement {nameof(IAnonymiseValueFactory)}."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_FactoryBacked_Anonymise_Fields_That_Are_Not_Registered()
    {
        var options = new DbContextOptionsBuilder<FactoryBackedAnonymiseDbContext>()
            .UseInMemoryDatabase($"startup-validator-factory-backed-unregistered-{Guid.NewGuid()}")
            .Options;
        await using var db = new FactoryBackedAnonymiseDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["factory-backed-anonymise"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
            }
        );

        var act = async () =>
            await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Anonymise convention on {typeof(FactoryBackedAnonymiseRecord).FullName}: [AnonymiseWith] member ExternalId specifies factory type {typeof(TestAnonymiseValueFactory).FullName} but no matching {nameof(IAnonymiseValueFactory)} is registered in DI."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Null_Anonymise_On_NonNullable_Reference_Types()
    {
        var options = new DbContextOptionsBuilder<InvalidNullReferenceAnonymiseDbContext>()
            .UseInMemoryDatabase($"startup-validator-invalid-null-reference-anonymise-{Guid.NewGuid()}")
            .Options;
        await using var db = new InvalidNullReferenceAnonymiseDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["invalid-null-reference-anonymise"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
            }
        );

        var act = async () => await new RetentionStartupValidator(db, repository, new RetentionEntryBuilder(new CohortConventions())).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Anonymise convention on {typeof(InvalidNullReferenceAnonymiseRecord).FullName}: [Anonymise] member DisplayName uses Null but String is not nullable."
            );
    }

    [Fact]
    public async Task ValidateAsync_Allows_Anonymise_Categories_Without_Tenant_Metadata()
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

        var act = async () => await new RetentionStartupValidator(db, repository, new RetentionEntryBuilder(new CohortConventions())).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Allows_Opaque_Deferred_SoftDelete_Categories_Without_Tenant_Metadata()
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

        var act = async () => await new RetentionStartupValidator(db, repository, new RetentionEntryBuilder(new CohortConventions())).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    private static InMemoryCategoryRepository CreateBrokenAnnotationRepository()
    {
        return new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["broken-sample"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
            }
        );
    }

    private static SampleRetentionStartupService CreateStartupService(
        DbContext db,
        IRetentionCategoryRepository repository
    )
    {
        var registry = new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions()));
        var validator = CreateValidator(db, repository);

        return new SampleRetentionStartupService(
            registry,
            validator,
            new RetentionSweepEngine(
                db,
                registry,
                repository,
                validator,
                new NoOpRetentionAuditWriter(),
                CreateSweepStrategies(db)
            ),
            new RetentionPreviewService(
                db,
                registry,
                repository,
                validator,
                CreateSweepStrategies(db)
            ),
            new RetentionErasureService(
                db,
                registry,
                repository,
                validator,
                new NoOpRetentionAuditWriter(),
                CreateSweepStrategies(db),
                new StaticOptionsMonitor<CohortOptions>(new CohortOptions())
            )
        );
    }

    private static RetentionSweepEngine CreateSweepEngine(
        DbContext db,
        IRetentionCategoryRepository repository
    )
    {
        var registry = new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions()));
        var validator = CreateValidator(db, repository);

        return new RetentionSweepEngine(
            db,
            registry,
            repository,
            validator,
            new NoOpRetentionAuditWriter(),
            CreateSweepStrategies(db)
        );
    }

    private static IRetentionPreview CreatePreviewService(
        DbContext db,
        IRetentionCategoryRepository repository
    )
    {
        var registry = new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions()));
        var validator = CreateValidator(db, repository);

        return new RetentionPreviewService(
            db,
            registry,
            repository,
            validator,
            CreateSweepStrategies(db)
        );
    }

    private static IRetentionErasureService CreateErasureService(
        DbContext db,
        IRetentionCategoryRepository repository
    )
    {
        var registry = new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions()));
        var validator = CreateValidator(db, repository);

        return new RetentionErasureService(
            db,
            registry,
            repository,
            validator,
            new NoOpRetentionAuditWriter(),
            CreateSweepStrategies(db),
            new StaticOptionsMonitor<CohortOptions>(new CohortOptions())
        );
    }

    private static IRetentionSweepStrategy[] CreateSweepStrategies(DbContext db)
    {
        return [new PurgeSweepStrategy(), new SoftDeleteSweepStrategy(), new AnonymiseSweepStrategy(db)];
    }

    private sealed class StaticOptionsMonitor<T>(T currentValue) : IOptionsMonitor<T>
    {
        public T CurrentValue => currentValue;

        public T Get(string? name)
        {
            return currentValue;
        }

        public IDisposable? OnChange(Action<T, string?> listener)
        {
            return null;
        }
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
        public Task<RetentionRule> ResolveAsync(RetentionResolutionContext ctx, CancellationToken ct) =>
            Task.FromResult(rule);
    }

    private sealed class OpaqueDeferredRuleResolver(RetentionRule rule) : IRetentionRuleResolver
    {
        public Task<RetentionRule> ResolveAsync(RetentionResolutionContext ctx, CancellationToken ct) =>
            Task.FromResult(rule);
    }

    private static RetentionStartupValidator CreateValidator(
        DbContext db,
        IRetentionCategoryRepository repository
    )
    {
        return new RetentionStartupValidator(
            db,
            repository,
            new RetentionEntryBuilder(new CohortConventions()),
            [new GuidTombstoneFactory(), new OriginalValueTombstoneFactory()]
        );
    }

    private sealed class GuardedSampleCategoryRepository : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            if (category == "short-lived" || category == "tenantless-purge")
            {
                return Task.FromResult<IRetentionRuleResolver?>(
                    new StaticRetentionRuleResolver(new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge))
                );
            }

            if (category == "soft-delete" || category == "tenantless-softdelete")
            {
                return Task.FromResult<IRetentionRuleResolver?>(
                    new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    )
                );
            }

            if (category == "anonymise" || category == "tombstone-anonymise")
            {
                return Task.FromResult<IRetentionRuleResolver?>(
                    new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    )
                );
            }

            if (category == "per-row-audit-override")
            {
                return Task.FromResult<IRetentionRuleResolver?>(
                    new StaticRetentionRuleResolver(
                        new RetentionRule(
                            TimeSpan.FromDays(30),
                            Strategy.Purge,
                            AuditRowDetail: AuditRowDetail.SummaryOnly
                        )
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
            if (category == "short-lived" || category == "tenantless-purge" || category == "per-row-audit-override")
            {
                return Task.FromResult<IRetentionRuleResolver?>(
                    new DeferredRuleResolver(new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge))
                );
            }

            if (category == "soft-delete" || category == "tenantless-softdelete")
            {
                return Task.FromResult<IRetentionRuleResolver?>(
                    new DeferredRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    )
                );
            }

            if (category == "anonymise" || category == "tombstone-anonymise")
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
                "short-lived" or "tenantless-purge" or "per-row-audit-override" =>
                    Task.FromResult<IRetentionRuleResolver?>(
                        new OpaqueDeferredRuleResolver(
                            new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                        )
                    ),
                "soft-delete" or "tenantless-softdelete" => Task.FromResult<IRetentionRuleResolver?>(
                    new OpaqueDeferredRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    )
                ),
                "anonymise" or "tombstone-anonymise" => Task.FromResult<IRetentionRuleResolver?>(
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

    private sealed class OpaqueDeferredAnonymiseSampleCategoryRepository
        : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            return category switch
            {
                "short-lived" or "tenantless-purge" or "per-row-audit-override" =>
                    Task.FromResult<IRetentionRuleResolver?>(
                        new StaticRetentionRuleResolver(
                            new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                        )
                    ),
                "soft-delete" or "tenantless-softdelete" => Task.FromResult<IRetentionRuleResolver?>(
                    new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    )
                ),
                "anonymise" or "tombstone-anonymise" => Task.FromResult<IRetentionRuleResolver?>(
                    new OpaqueDeferredRuleResolver(
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

    private sealed class FactoryBackedAnonymiseDbContext(
        DbContextOptions<FactoryBackedAnonymiseDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<FactoryBackedAnonymiseRecord>(entity =>
            {
                entity.ToTable("factory_backed_anonymise_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.ExternalId).HasColumnName("external_id");
            });
        }
    }

    private sealed class InvalidFactoryTypeAnonymiseDbContext(
        DbContextOptions<InvalidFactoryTypeAnonymiseDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<InvalidFactoryTypeAnonymiseRecord>(entity =>
            {
                entity.ToTable("invalid_factory_type_anonymise_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.ExternalId).HasColumnName("external_id");
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

    private sealed class InvalidNullReferenceAnonymiseDbContext(
        DbContextOptions<InvalidNullReferenceAnonymiseDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<InvalidNullReferenceAnonymiseRecord>(entity =>
            {
                entity.ToTable("invalid_null_reference_anonymise_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.DisplayName).HasColumnName("display_name");
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

    [Retain("factory-backed-anonymise", nameof(FactoryBackedAnonymiseRecord.CreatedAt))]
    private sealed class FactoryBackedAnonymiseRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [AnonymiseWith(typeof(TestAnonymiseValueFactory))]
        public Guid ExternalId { get; init; }
    }

    [Retain("invalid-factory-type-anonymise", nameof(InvalidFactoryTypeAnonymiseRecord.CreatedAt))]
    private sealed class InvalidFactoryTypeAnonymiseRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [AnonymiseWith(typeof(NotAFactory))]
        public Guid ExternalId { get; init; }
    }

    [Retain("missing-anonymise-tenant", nameof(MissingAnonymiseTenantRecord.CreatedAt))]
    private sealed class MissingAnonymiseTenantRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.Null)]
        public string? EmailAddress { get; init; }
    }

    [Retain("invalid-null-reference-anonymise", nameof(InvalidNullReferenceAnonymiseRecord.CreatedAt))]
    private sealed class InvalidNullReferenceAnonymiseRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.Null)]
        public string DisplayName { get; init; } = "";
    }

    private sealed class TestAnonymiseValueFactory : IAnonymiseValueFactory
    {
        public object? Create(AnonymiseValueContext context) => Guid.Empty;
    }

    private sealed class NotAFactory;
}
