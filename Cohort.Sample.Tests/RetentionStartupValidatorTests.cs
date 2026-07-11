using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;

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
            .UseNpgsqlMetadataModel($"startup-validator-static-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);
        var repository = new GuardedSampleCategoryRepository();

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Does_Not_Repeat_A_Successful_Full_Validation_In_The_Same_Scope()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-once-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);
        var repository = new CountingCategoryRepository(new GuardedSampleCategoryRepository());
        var validator = CreateValidator(db, repository);

        await validator.ValidateAsync();
        var callsAfterFirstValidation = repository.GetAsyncCount;
        await validator.ValidateAsync();

        callsAfterFirstValidation.Should().BeGreaterThan(0);
        repository.GetAsyncCount.Should().Be(callsAfterFirstValidation);
    }

    [Fact]
    public async Task ValidateAsync_Allows_Deferred_Resolvers_At_Startup()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-deferred-{Guid.NewGuid()}")
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
            .UseNpgsqlMetadataModel($"startup-validator-opaque-deferred-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);

        var act = async () =>
            await CreateValidator(db, new OpaqueDeferredSampleCategoryRepository()).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Allows_Opaque_Deferred_Resolvers_On_Entities_With_Only_Anonymise_Convention()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-opaque-anonymise-{Guid.NewGuid()}")
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
            .UseNpgsqlMetadataModel($"startup-validator-exempt-{Guid.NewGuid()}")
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
            .UseNpgsqlMetadataModel($"startup-validator-missing-attribute-{Guid.NewGuid()}")
            .Options;
        await using var db = new MissingAttributeDbContext(options);

        var act = async () =>
            await new RetentionStartupValidator(
                db,
                InMemoryCategoryRepository.Empty,
                new RetentionEntryBuilder(new RetentionModelConventions())
            ).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Rejects_A_Retained_Entity_Without_A_Stable_Identity()
    {
        var options = new DbContextOptionsBuilder<MissingRetentionIdentityDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-missing-identity-{Guid.NewGuid()}")
            .Options;
        await using var db = new MissingRetentionIdentityDbContext(options);

        var act = async () =>
            await CreateValidator(db, IdentityCategoryRepository()).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .ContainSingle(error =>
                error.Contains("must declare [RetentionEntityId", StringComparison.Ordinal)
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Duplicate_Retention_Entity_Identities()
    {
        var options = new DbContextOptionsBuilder<DuplicateRetentionIdentityDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-duplicate-identity-{Guid.NewGuid()}")
            .Options;
        await using var db = new DuplicateRetentionIdentityDbContext(options);

        var act = async () =>
            await CreateValidator(db, IdentityCategoryRepository()).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .ContainSingle(error =>
                error.Contains("identities must be unique", StringComparison.Ordinal)
            );
    }

    private static InMemoryCategoryRepository IdentityCategoryRepository() =>
        new(new Dictionary<string, IRetentionRuleResolver> { ["identity"] = ExemptResolver });

    [Fact]
    public async Task ValidateAsync_Rejects_Entities_With_Both_Retention_And_Exemption_Metadata()
    {
        var options = new DbContextOptionsBuilder<ConflictingAttributeDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-conflicting-attribute-{Guid.NewGuid()}")
            .Options;
        await using var db = new ConflictingAttributeDbContext(options);

        var act = async () =>
            await new RetentionStartupValidator(
                db,
                InMemoryCategoryRepository.Empty,
                new RetentionEntryBuilder(new RetentionModelConventions())
            ).ValidateAsync();

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
            .UseNpgsqlMetadataModel($"startup-validator-invalid-anchor-{Guid.NewGuid()}")
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

        var act = async () =>
            await new RetentionStartupValidator(
                db,
                repository,
                new RetentionEntryBuilder(new RetentionModelConventions())
            ).ValidateAsync();

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
            .UseNpgsqlMetadataModel($"startup-validator-missing-category-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);

        var act = async () =>
            await new RetentionStartupValidator(
                db,
                InMemoryCategoryRepository.Empty,
                new RetentionEntryBuilder(new RetentionModelConventions())
            ).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .BeEquivalentTo(
                [
                    $"Retention category 'short-lived' for entity {typeof(Note).FullName} could not be resolved.",
                    $"Retention category 'blob-cleanup' for entity {typeof(BlobBackedFile).FullName} could not be resolved.",
                    $"Retention category 'soft-delete' for entity {typeof(SoftDeleteRecord).FullName} could not be resolved.",
                    $"Retention category 'anonymise' for entity {typeof(AnonymisedContact).FullName} could not be resolved.",
                    $"Retention category 'tenantless-purge' for entity {typeof(TenantlessLog).FullName} could not be resolved.",
                    $"Retention category 'tenantless-purge' for entity {typeof(ExternalNumberedLog).FullName} could not be resolved.",
                    $"Retention category 'tenantless-softdelete' for entity {typeof(TenantlessSoftDelete).FullName} could not be resolved.",
                    $"Retention category 'per-row-audit-override' for entity {typeof(PerRowAuditedLog).FullName} could not be resolved.",
                    $"Retention category 'tombstone-anonymise' for entity {typeof(TombstoneRecord).FullName} could not be resolved.",
                    $"Retention category 'nullable-anchor-purge' for entity {typeof(NullableAnchorEvent).FullName} could not be resolved.",
                ]
            );
        exception.Which.Message.Should().Contain("short-lived");
        exception.Which.Message.Should().Contain("soft-delete");
        exception.Which.Message.Should().Contain("anonymise");
    }

    [Fact]
    public async Task ValidateAsync_Aggregates_Multiple_Independent_Failures()
    {
        var options = new DbContextOptionsBuilder<AggregateFailureDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-aggregate-{Guid.NewGuid()}")
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
            .UseNpgsqlMetadataModel($"startup-validator-throwing-resolver-{Guid.NewGuid()}")
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
    public async Task ValidateAsync_Rejects_Invalid_Tenant_Metadata()
    {
        var options = new DbContextOptionsBuilder<InvalidTenantDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-invalid-tenant-{Guid.NewGuid()}")
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
                $"Tenant convention on {typeof(InvalidTenantRecord).FullName}: TenantId must be a non-nullable Guid, got String."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Nullable_Clr_Tenant_Properties()
    {
        var options = new DbContextOptionsBuilder<NullableClrTenantDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-nullable-clr-tenant-{Guid.NewGuid()}")
            .Options;
        await using var db = new NullableClrTenantDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver> { ["nullable-tenant"] = ExemptResolver }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Tenant convention on {typeof(NullableClrTenantRecord).FullName}: TenantId must be a non-nullable Guid, got Nullable`1."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Record_Id_Properties_That_Do_Not_Uniquely_Identify_Rows()
    {
        var options = new DbContextOptionsBuilder<NonUniqueRecordIdDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-non-unique-record-id-{Guid.NewGuid()}")
            .Options;
        await using var db = new NonUniqueRecordIdDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver> { ["record-id"] = ExemptResolver }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Record-id convention on {typeof(NonUniqueRecordIdRecord).FullName}: record-id property 'ExternalId' must uniquely identify rows via a single-column primary key, alternate key, or unique index."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Nullable_Record_Id_Properties()
    {
        var options = new DbContextOptionsBuilder<NullableRecordIdDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-nullable-record-id-{Guid.NewGuid()}")
            .Options;
        await using var db = new NullableRecordIdDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver> { ["record-id"] = ExemptResolver }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Record-id convention on {typeof(NullableRecordIdRecord).FullName}: record-id property 'ExternalId' must be non-nullable in CLR and EF metadata."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Record_Id_Properties_That_Are_Only_Part_Of_A_Composite_Key()
    {
        var options = new DbContextOptionsBuilder<CompositeRecordIdDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-composite-record-id-{Guid.NewGuid()}")
            .Options;
        await using var db = new CompositeRecordIdDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver> { ["record-id"] = ExemptResolver }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Record-id convention on {typeof(CompositeRecordIdRecord).FullName}: record-id property 'ExternalId' must uniquely identify rows via a single-column primary key, alternate key, or unique index."
            );
    }

    [Fact]
    public async Task ValidateAsync_Allows_Record_Id_Properties_With_Single_Column_Alternate_Keys_Or_Unique_Indexes()
    {
        var options = new DbContextOptionsBuilder<UniqueRecordIdDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-unique-record-id-{Guid.NewGuid()}")
            .Options;
        await using var db = new UniqueRecordIdDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver> { ["record-id"] = ExemptResolver }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public void Scan_Leaves_Tenant_Metadata_Null_But_Only_Records_Explicit_Tenantless_Intent_When_Marked()
    {
        var explicitOptions = new DbContextOptionsBuilder<ExplicitTenantlessSoftDeleteDbContext>()
            .UseNpgsqlMetadataModel($"registry-explicit-tenantless-{Guid.NewGuid()}")
            .Options;
        using var explicitDb = new ExplicitTenantlessSoftDeleteDbContext(explicitOptions);
        var explicitEntry = new RetentionRegistry(
            explicitDb,
            new RetentionEntryBuilder(new RetentionModelConventions())
        ).Scan()[typeof(ExplicitTenantlessSoftDeleteRecord)];

        explicitEntry.Tenant.Should().BeNull();
        explicitEntry.IsExplicitlyTenantless.Should().BeTrue();

        var missingOptions = new DbContextOptionsBuilder<MissingSoftDeleteTenantDbContext>()
            .UseNpgsqlMetadataModel($"registry-missing-tenant-{Guid.NewGuid()}")
            .Options;
        using var missingDb = new MissingSoftDeleteTenantDbContext(missingOptions);
        var missingEntry = new RetentionRegistry(
            missingDb,
            new RetentionEntryBuilder(new RetentionModelConventions())
        ).Scan()[typeof(MissingSoftDeleteTenantRecord)];

        missingEntry.Tenant.Should().BeNull();
        missingEntry.IsExplicitlyTenantless.Should().BeFalse();
    }

    [Fact]
    public async Task ValidateAsync_Rejects_SoftDelete_Categories_Without_A_Public_Bool_IsDeleted_Property()
    {
        var options = new DbContextOptionsBuilder<InvalidSoftDeleteIsDeletedDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-invalid-soft-delete-flag-{Guid.NewGuid()}")
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
            .UseNpgsqlMetadataModel(
                $"startup-validator-invalid-soft-delete-deleted-at-{Guid.NewGuid()}"
            )
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
    public async Task ValidateAsync_Rejects_Retained_Entities_Without_Tenant_Metadata_Unless_Explicitly_Tenantless()
    {
        var options = new DbContextOptionsBuilder<MissingSoftDeleteTenantDbContext>()
            .UseNpgsqlMetadataModel(
                $"startup-validator-missing-soft-delete-tenant-{Guid.NewGuid()}"
            )
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

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Tenant convention on {typeof(MissingSoftDeleteTenantRecord).FullName}: retained entities must expose a public non-nullable Guid tenant property named 'TenantId' by convention, or mark the tenant property with [RetentionTenant], unless the entity is explicitly marked with [RetentionTenantless]."
            );
    }

    [Fact]
    public async Task ValidateAsync_Allows_Explicitly_Tenantless_SoftDelete_Categories()
    {
        var options = new DbContextOptionsBuilder<ExplicitTenantlessSoftDeleteDbContext>()
            .UseNpgsqlMetadataModel(
                $"startup-validator-explicit-tenantless-soft-delete-{Guid.NewGuid()}"
            )
            .Options;
        await using var db = new ExplicitTenantlessSoftDeleteDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["explicit-tenantless-soft-delete"] = new StaticRetentionRuleResolver(
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
            .UseNpgsqlMetadataModel($"startup-validator-missing-anonymise-fields-{Guid.NewGuid()}")
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
                ["blob-cleanup"] = ExemptResolver,
                ["tenantless-purge"] = ExemptResolver,
                ["nullable-anchor-purge"] = ExemptResolver,
                ["tenantless-softdelete"] = ExemptResolver,
                ["per-row-audit-override"] = ExemptResolver,
                ["tombstone-anonymise"] = ExemptResolver,
            }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().HaveCount(2);
        exception
            .Which.Errors.Should()
            .Contain(
                $"Anonymise convention on {typeof(Note).FullName}: retained Anonymise categories require at least one [Anonymise]-annotated property mapped by EF."
            );
        exception
            .Which.Errors.Should()
            .Contain(
                $"Anonymise convention on {typeof(Note).FullName}: retained Anonymise categories require a nullable DateTimeOffset marker property (named AnonymisedAt by convention, or marked with [RetentionAnonymisedAt]). NULL marks rows not yet anonymised; without it anonymisation re-scrubs every expired row on every sweep."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Anonymise_Categories_With_Invalid_Method_Type_Mismatches()
    {
        var options = new DbContextOptionsBuilder<InvalidAnonymiseMethodDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-invalid-anonymise-methods-{Guid.NewGuid()}")
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

        var act = async () =>
            await new RetentionStartupValidator(
                db,
                repository,
                new RetentionEntryBuilder(new RetentionModelConventions())
            ).ValidateAsync();

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
            .UseNpgsqlMetadataModel(
                $"startup-validator-factory-backed-invalid-type-{Guid.NewGuid()}"
            )
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

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

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
            .UseNpgsqlMetadataModel(
                $"startup-validator-factory-backed-unregistered-{Guid.NewGuid()}"
            )
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

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

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
            .UseNpgsqlMetadataModel(
                $"startup-validator-invalid-null-reference-anonymise-{Guid.NewGuid()}"
            )
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

        var act = async () =>
            await new RetentionStartupValidator(
                db,
                repository,
                new RetentionEntryBuilder(new RetentionModelConventions())
            ).ValidateAsync();

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
    public async Task ValidateAsync_Rejects_Null_Anonymise_When_EF_Property_Is_Required()
    {
        var options = new DbContextOptionsBuilder<RequiredNullableAnonymiseDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-required-null-anonymise-{Guid.NewGuid()}")
            .Options;
        await using var db = new RequiredNullableAnonymiseDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["required-null-anonymise"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
            }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .ContainSingle()
            .Which.Should()
            .Contain("EF metadata is non-nullable");
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Anonymise_Fields_That_Overlap_Structural_Roles()
    {
        var options = new DbContextOptionsBuilder<StructuralAnonymiseDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-structural-anonymise-{Guid.NewGuid()}")
            .Options;
        await using var db = new StructuralAnonymiseDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["structural-anonymise"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
            }
        );

        var act = async () =>
            await new RetentionStartupValidator(
                db,
                repository,
                new RetentionEntryBuilder(new RetentionModelConventions()),
                [new TestAnonymiseValueFactory()]
            ).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .Contain(error => error.Contains("record ID") && error.Contains("Id"));
        exception
            .Which.Errors.Should()
            .Contain(error => error.Contains("tenant") && error.Contains("TenantId"));
        exception
            .Which.Errors.Should()
            .Contain(error => error.Contains("anchor") && error.Contains("CreatedAt"));
        exception
            .Which.Errors.Should()
            .Contain(error => error.Contains("soft-delete") && error.Contains("IsDeleted"));
        exception
            .Which.Errors.Should()
            .Contain(error => error.Contains("AnonymisedAt") && error.Contains("AnonymisedAt"));
    }

    [Fact]
    public async Task ValidateAsync_Allows_Explicitly_Tenantless_Anonymise_Categories()
    {
        var options = new DbContextOptionsBuilder<ExplicitTenantlessAnonymiseDbContext>()
            .UseNpgsqlMetadataModel(
                $"startup-validator-explicit-tenantless-anonymise-{Guid.NewGuid()}"
            )
            .Options;
        await using var db = new ExplicitTenantlessAnonymiseDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["explicit-tenantless-anonymise"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
            }
        );

        var act = async () =>
            await new RetentionStartupValidator(
                db,
                repository,
                new RetentionEntryBuilder(new RetentionModelConventions())
            ).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Allows_Opaque_Deferred_Explicitly_Tenantless_SoftDelete_Categories()
    {
        var options = new DbContextOptionsBuilder<ExplicitTenantlessSoftDeleteDbContext>()
            .UseNpgsqlMetadataModel(
                $"startup-validator-opaque-explicit-tenantless-soft-delete-{Guid.NewGuid()}"
            )
            .Options;
        await using var db = new ExplicitTenantlessSoftDeleteDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["explicit-tenantless-soft-delete"] = new OpaqueDeferredRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
            }
        );

        var act = async () =>
            await new RetentionStartupValidator(
                db,
                repository,
                new RetentionEntryBuilder(new RetentionModelConventions())
            ).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Retained_Entities_In_Inheritance_Hierarchies()
    {
        var options = new DbContextOptionsBuilder<InheritanceDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-inheritance-{Guid.NewGuid()}")
            .Options;
        await using var db = new InheritanceDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["inheritance-base"] = new StaticRetentionRuleResolver(
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
                $"[Retain] on {typeof(InheritanceBaseRecord).FullName}: entity participates in an EF inheritance hierarchy (TPH/TPT/TPC). Sweep SQL targets the mapped table without a type discriminator, so rows of sibling or derived types would be swept too. Retention on inheritance-mapped entities is not supported."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Retained_Entities_Mapped_To_NonDefault_Schemas()
    {
        var options = new DbContextOptionsBuilder<NonDefaultSchemaDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-schema-{Guid.NewGuid()}")
            .Options;
        await using var db = new NonDefaultSchemaDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["schema-category"] = new StaticRetentionRuleResolver(
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
                $"[Retain] on {typeof(NonDefaultSchemaRecord).FullName}: entity is mapped to schema 'audit'. Cohort SQL does not schema-qualify identifiers and resolves tables via the connection search_path, so entities outside the default 'public' schema are not supported."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Cascade_Delete_Paths_Into_Retained_Entities()
    {
        var options = new DbContextOptionsBuilder<CascadeDeleteDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-cascade-{Guid.NewGuid()}")
            .Options;
        await using var db = new CascadeDeleteDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["cascade-parent"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
                ["cascade-child"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(365), Strategy.Purge)
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
                $"[Retain] on {typeof(CascadeParentRecord).FullName}: purging this entity cascades (ON DELETE CASCADE) into retained entity {typeof(CascadeChildRecord).FullName}, bypassing that entity's retention window, legal holds, and audit trail. Configure the relationship with DeleteBehavior.Restrict or NoAction so dependents are retired by their own retention rules."
            );
    }

    [Fact]
    public async Task ValidateAsync_Allows_Restrict_Delete_Paths_Between_Retained_Entities()
    {
        var options = new DbContextOptionsBuilder<RestrictDeleteDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-restrict-{Guid.NewGuid()}")
            .Options;
        await using var db = new RestrictDeleteDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["cascade-parent"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
                ["restrict-child"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(365), Strategy.Purge)
                ),
            }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Duplicate_Marker_Attributes()
    {
        var options = new DbContextOptionsBuilder<DuplicateTenantMarkerDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-duplicate-marker-{Guid.NewGuid()}")
            .Options;
        await using var db = new DuplicateTenantMarkerDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["duplicate-tenant-marker"] = new StaticRetentionRuleResolver(
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
                $"Marker convention on {typeof(DuplicateTenantMarkerRecord).FullName}: [RetentionTenant] is declared on multiple properties (OrganisationId, OwnerId); exactly one is allowed."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Duplicate_AnonymisedAt_Marker_Attributes()
    {
        var options = new DbContextOptionsBuilder<DuplicateAnonymisedAtMarkerDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-duplicate-anonymised-at-{Guid.NewGuid()}")
            .Options;
        await using var db = new DuplicateAnonymisedAtMarkerDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["duplicate-anonymised-at-marker"] = new StaticRetentionRuleResolver(
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
                $"Marker convention on {typeof(DuplicateAnonymisedAtMarkerRecord).FullName}: [RetentionAnonymisedAt] is declared on multiple properties (RedactedAt, ScrubbedAt); exactly one is allowed."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Tenantless_Marker_On_Entities_With_A_Tenant_Property()
    {
        // Tenantedness is decided by the resolved tenant convention everywhere; an
        // entity declaring both would be swept per tenant with the marker silently
        // ignored.
        var options = new DbContextOptionsBuilder<ContradictoryTenantlessDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-contradictory-tenantless-{Guid.NewGuid()}")
            .Options;
        await using var db = new ContradictoryTenantlessDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["contradictory-tenantless"] = new StaticRetentionRuleResolver(
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
                $"Tenant convention on {typeof(ContradictoryTenantlessRecord).FullName}: entity is marked [RetentionTenantless] but exposes tenant property 'TenantId'. The tenant property wins and the entity would be swept per tenant, so the marker is contradictory; remove [RetentionTenantless] or the tenant property."
            );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Naive_Timestamp_Anchor_Columns()
    {
        // Npgsql model building works offline; ValidateAsync only inspects metadata.
        var options = new DbContextOptionsBuilder<NaiveTimestampDbContext>()
            .UseNpgsql("Host=localhost;Database=cohort-model-only")
            .Options;
        await using var db = new NaiveTimestampDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["naive-anchor"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
            }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception.Which.Errors[0].Should().Contain("'CreatedAt'");
        exception.Which.Errors[0].Should().Contain("timestamp without time zone");
        exception.Which.Errors[0].Should().Contain("timestamp with time zone");
    }

    [Fact]
    public async Task ValidateAsync_Allows_Timestamptz_Anchor_Columns()
    {
        var options = new DbContextOptionsBuilder<TimestamptzAnchorDbContext>()
            .UseNpgsql("Host=localhost;Database=cohort-model-only")
            .Options;
        await using var db = new TimestamptzAnchorDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["naive-anchor"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
            }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    private sealed class InMemoryCategoryRepository(
        IReadOnlyDictionary<string, IRetentionRuleResolver> resolvers
    ) : IRetentionCategoryRepository
    {
        public static InMemoryCategoryRepository Empty { get; } =
            new(new Dictionary<string, IRetentionRuleResolver>());

        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            resolvers.TryGetValue(category, out var resolver);
            return Task.FromResult(resolver);
        }
    }

    private sealed class DeferredRuleResolver(RetentionRule rule) : IRetentionRuleResolver
    {
        public Task<RetentionRule> ResolveAsync(
            RetentionResolutionContext ctx,
            CancellationToken ct
        ) => Task.FromResult(rule);
    }

    private sealed class OpaqueDeferredRuleResolver(RetentionRule rule) : IRetentionRuleResolver
    {
        public Task<RetentionRule> ResolveAsync(
            RetentionResolutionContext ctx,
            CancellationToken ct
        ) => Task.FromResult(rule);
    }

    private static RetentionStartupValidator CreateValidator(
        DbContext db,
        IRetentionCategoryRepository repository
    )
    {
        return new RetentionStartupValidator(
            db,
            repository,
            new RetentionEntryBuilder(new RetentionModelConventions()),
            [new GuidTombstoneFactory(), new OriginalValueTombstoneFactory()]
        );
    }

    private sealed class GuardedSampleCategoryRepository : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            if (
                category == "short-lived"
                || category == "blob-cleanup"
                || category == "tenantless-purge"
                || category == "nullable-anchor-purge"
            )
            {
                return Task.FromResult<IRetentionRuleResolver?>(
                    new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    )
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
            if (
                category == "short-lived"
                || category == "blob-cleanup"
                || category == "tenantless-purge"
                || category == "nullable-anchor-purge"
                || category == "per-row-audit-override"
            )
            {
                return Task.FromResult<IRetentionRuleResolver?>(
                    new DeferredRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    )
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
        public Task<RetentionRule> ResolveAsync(
            RetentionResolutionContext ctx,
            CancellationToken ct
        ) => Task.FromResult(new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge));

        public RetentionRule? TryResolveAtStartup() => throw new InvalidOperationException(message);
    }

    private sealed class OpaqueDeferredSampleCategoryRepository : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            return category switch
            {
                "short-lived"
                or "blob-cleanup"
                or "tenantless-purge"
                or "nullable-anchor-purge"
                or "per-row-audit-override" => Task.FromResult<IRetentionRuleResolver?>(
                    new OpaqueDeferredRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    )
                ),
                "soft-delete" or "tenantless-softdelete" =>
                    Task.FromResult<IRetentionRuleResolver?>(
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
                "short-lived"
                or "blob-cleanup"
                or "tenantless-purge"
                or "nullable-anchor-purge"
                or "per-row-audit-override" => Task.FromResult<IRetentionRuleResolver?>(
                    new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    )
                ),
                "soft-delete" or "tenantless-softdelete" =>
                    Task.FromResult<IRetentionRuleResolver?>(
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

    private sealed class MissingAttributeDbContext(
        DbContextOptions<MissingAttributeDbContext> options
    ) : DbContext(options)
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

    private sealed class BrokenAnnotationDbContext(
        DbContextOptions<BrokenAnnotationDbContext> options
    ) : DbContext(options)
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

    private sealed class AggregateFailureDbContext(
        DbContextOptions<AggregateFailureDbContext> options
    ) : DbContext(options)
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
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
                entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
            });
            modelBuilder.Entity<ValidRetainedRecord>(entity =>
            {
                entity.ToTable("aggregate_valid_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
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
                entity.Property(record => record.TenantId).HasColumnName("tenant_id");
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

    private sealed class NullableClrTenantDbContext(
        DbContextOptions<NullableClrTenantDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<NullableClrTenantRecord>(entity =>
            {
                entity.ToTable("nullable_clr_tenant_records");
                entity.HasKey(record => record.Id);
            });
        }
    }

    private sealed class NonUniqueRecordIdDbContext(
        DbContextOptions<NonUniqueRecordIdDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<NonUniqueRecordIdRecord>(entity =>
            {
                entity.ToTable("non_unique_record_id_records");
                entity.HasKey(record => record.InternalKey);
            });
        }
    }

    private sealed class NullableRecordIdDbContext(
        DbContextOptions<NullableRecordIdDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<NullableRecordIdRecord>(entity =>
            {
                entity.ToTable("nullable_record_id_records");
                entity.HasKey(record => record.InternalKey);
                entity.HasIndex(record => record.ExternalId).IsUnique();
            });
        }
    }

    private sealed class CompositeRecordIdDbContext(
        DbContextOptions<CompositeRecordIdDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CompositeRecordIdRecord>(entity =>
            {
                entity.ToTable("composite_record_id_records");
                entity.HasKey(record => record.InternalKey);
                entity.HasAlternateKey(record => new { record.ExternalId, record.TenantId });
            });
        }
    }

    private sealed class UniqueRecordIdDbContext(DbContextOptions<UniqueRecordIdDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<AlternateKeyRecordIdRecord>(entity =>
            {
                entity.ToTable("alternate_key_record_id_records");
                entity.HasKey(record => record.InternalKey);
                entity.HasAlternateKey(record => record.ExternalId);
            });
            modelBuilder.Entity<UniqueIndexRecordIdRecord>(entity =>
            {
                entity.ToTable("unique_index_record_id_records");
                entity.HasKey(record => record.InternalKey);
                entity.HasIndex(record => record.ExternalId).IsUnique();
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

    private sealed class ExplicitTenantlessSoftDeleteDbContext(
        DbContextOptions<ExplicitTenantlessSoftDeleteDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ExplicitTenantlessSoftDeleteRecord>(entity =>
            {
                entity.ToTable("explicit_tenantless_soft_delete_records");
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

    private sealed class ExplicitTenantlessAnonymiseDbContext(
        DbContextOptions<ExplicitTenantlessAnonymiseDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ExplicitTenantlessAnonymiseRecord>(entity =>
            {
                entity.ToTable("explicit_tenantless_anonymise_records");
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

    private sealed class RequiredNullableAnonymiseDbContext(
        DbContextOptions<RequiredNullableAnonymiseDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<RequiredNullableAnonymiseRecord>(entity =>
            {
                entity.HasKey(record => record.Id);
                entity.Property(record => record.DisplayName).IsRequired();
            });
        }
    }

    private sealed class StructuralAnonymiseDbContext(
        DbContextOptions<StructuralAnonymiseDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<StructuralAnonymiseRecord>(entity =>
            {
                entity.HasKey(record => record.Id);
            });
        }
    }

    private sealed class UnannotatedRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("conflict-category", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000001c")]
    [ExemptFromRetention("covered by statutory retention")]
    private sealed class ConflictingRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("missing-category", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000001d")]
    private sealed class MissingCategoryRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("valid-category", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000001e")]
    private sealed class ValidRetainedRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("throwing-category", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000001f")]
    private sealed class ThrowingResolverRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("tenant-category", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000020")]
    private sealed class InvalidTenantRecord
    {
        public Guid Id { get; init; }
        public string TenantId { get; init; } = "";
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("nullable-tenant", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000021")]
    private sealed class NullableClrTenantRecord
    {
        public Guid Id { get; init; }
        public Guid? TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("record-id", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000022")]
    private sealed class NonUniqueRecordIdRecord
    {
        public Guid InternalKey { get; init; }

        [RetentionRecordId]
        public Guid ExternalId { get; init; }

        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("record-id", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000023")]
    private sealed class NullableRecordIdRecord
    {
        public Guid InternalKey { get; init; }

        [RetentionRecordId]
        public Guid? ExternalId { get; init; }

        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("record-id", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000024")]
    private sealed class CompositeRecordIdRecord
    {
        public Guid InternalKey { get; init; }

        [RetentionRecordId]
        public Guid ExternalId { get; init; }

        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("record-id", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000025")]
    private sealed class AlternateKeyRecordIdRecord
    {
        public Guid InternalKey { get; init; }

        [RetentionRecordId]
        public Guid ExternalId { get; init; }

        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("record-id", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000026")]
    private sealed class UniqueIndexRecordIdRecord
    {
        public Guid InternalKey { get; init; }

        [RetentionRecordId]
        public Guid ExternalId { get; init; }

        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("invalid-soft-delete", nameof(InvalidSoftDeleteIsDeletedRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000027")]
    private sealed class InvalidSoftDeleteIsDeletedRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public string IsDeleted { get; init; } = "";
    }

    [Retain("invalid-soft-delete", nameof(InvalidSoftDeleteDeletedAtRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000028")]
    private sealed class InvalidSoftDeleteDeletedAtRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public bool IsDeleted { get; init; }
        public string DeletedAt { get; init; } = "";
    }

    [Retain("missing-soft-delete-tenant", nameof(MissingSoftDeleteTenantRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000029")]
    private sealed class MissingSoftDeleteTenantRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public bool IsDeleted { get; init; }
        public DateTimeOffset? DeletedAt { get; init; }
    }

    [Retain(
        "explicit-tenantless-soft-delete",
        nameof(ExplicitTenantlessSoftDeleteRecord.CreatedAt)
    )]
    [RetentionEntityId("00000000-0000-0000-0001-00000000002a")]
    [RetentionTenantless]
    private sealed class ExplicitTenantlessSoftDeleteRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public bool IsDeleted { get; init; }
        public DateTimeOffset? DeletedAt { get; init; }
    }

    [Retain("invalid-null-anonymise", nameof(InvalidNullAnonymiseRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000002b")]
    private sealed class InvalidNullAnonymiseRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.Null)]
        public int Age { get; init; }

        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    [Retain("invalid-empty-string-anonymise", nameof(InvalidEmptyStringAnonymiseRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000002c")]
    private sealed class InvalidEmptyStringAnonymiseRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.EmptyString)]
        public Guid ExternalId { get; init; }

        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    [Retain(
        "invalid-fixed-literal-anonymise",
        nameof(InvalidFixedLiteralAnonymiseRecord.CreatedAt)
    )]
    [RetentionEntityId("00000000-0000-0000-0001-00000000002d")]
    private sealed class InvalidFixedLiteralAnonymiseRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.FixedLiteral, "[redacted]")]
        public DateTimeOffset LastSeenAt { get; init; }

        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    [Retain("factory-backed-anonymise", nameof(FactoryBackedAnonymiseRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000002e")]
    private sealed class FactoryBackedAnonymiseRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [AnonymiseWith(typeof(TestAnonymiseValueFactory))]
        public Guid ExternalId { get; init; }

        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    [Retain("invalid-factory-type-anonymise", nameof(InvalidFactoryTypeAnonymiseRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000002f")]
    private sealed class InvalidFactoryTypeAnonymiseRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [AnonymiseWith(typeof(NotAFactory))]
        public Guid ExternalId { get; init; }

        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    [Retain("missing-anonymise-tenant", nameof(MissingAnonymiseTenantRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000030")]
    private sealed class MissingAnonymiseTenantRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.Null)]
        public string? EmailAddress { get; init; }
    }

    [Retain("explicit-tenantless-anonymise", nameof(ExplicitTenantlessAnonymiseRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000031")]
    [RetentionTenantless]
    private sealed class ExplicitTenantlessAnonymiseRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.Null)]
        public string? EmailAddress { get; init; }

        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    [Retain(
        "invalid-null-reference-anonymise",
        nameof(InvalidNullReferenceAnonymiseRecord.CreatedAt)
    )]
    [RetentionEntityId("00000000-0000-0000-0001-000000000032")]
    private sealed class InvalidNullReferenceAnonymiseRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.Null)]
        public string DisplayName { get; init; } = "";

        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    [Retain("required-null-anonymise", nameof(RequiredNullableAnonymiseRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000033")]
    private sealed class RequiredNullableAnonymiseRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [Anonymise(AnonymiseMethod.Null)]
        public string? DisplayName { get; init; }

        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    [Retain("structural-anonymise", nameof(StructuralAnonymiseRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000034")]
    private sealed class StructuralAnonymiseRecord
    {
        [AnonymiseWith(typeof(TestAnonymiseValueFactory))]
        public Guid Id { get; init; }

        [AnonymiseWith(typeof(TestAnonymiseValueFactory))]
        public Guid TenantId { get; init; }

        [AnonymiseWith(typeof(TestAnonymiseValueFactory))]
        public DateTimeOffset CreatedAt { get; init; }

        [AnonymiseWith(typeof(TestAnonymiseValueFactory))]
        public bool IsDeleted { get; init; }

        [AnonymiseWith(typeof(TestAnonymiseValueFactory))]
        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    private sealed class InheritanceDbContext(DbContextOptions<InheritanceDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<InheritanceBaseRecord>(entity =>
            {
                entity.ToTable("inheritance_records");
                entity.HasKey(record => record.Id);
            });
            modelBuilder.Entity<InheritanceDerivedRecord>();
        }
    }

    private sealed class NonDefaultSchemaDbContext(
        DbContextOptions<NonDefaultSchemaDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<NonDefaultSchemaRecord>(entity =>
            {
                entity.ToTable("non_default_schema_records", "audit");
                entity.HasKey(record => record.Id);
            });
        }
    }

    private sealed class CascadeDeleteDbContext(DbContextOptions<CascadeDeleteDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CascadeParentRecord>(entity =>
            {
                entity.ToTable("cascade_parent_records");
                entity.HasKey(record => record.Id);
            });
            modelBuilder.Entity<CascadeChildRecord>(entity =>
            {
                entity.ToTable("cascade_child_records");
                entity.HasKey(record => record.Id);
                entity
                    .HasOne<CascadeParentRecord>()
                    .WithMany()
                    .HasForeignKey(record => record.ParentId)
                    .OnDelete(DeleteBehavior.Cascade);
            });
        }
    }

    private sealed class RestrictDeleteDbContext(DbContextOptions<RestrictDeleteDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CascadeParentRecord>(entity =>
            {
                entity.ToTable("restrict_parent_records");
                entity.HasKey(record => record.Id);
            });
            modelBuilder.Entity<RestrictChildRecord>(entity =>
            {
                entity.ToTable("restrict_child_records");
                entity.HasKey(record => record.Id);
                entity
                    .HasOne<CascadeParentRecord>()
                    .WithMany()
                    .HasForeignKey(record => record.ParentId)
                    .OnDelete(DeleteBehavior.Restrict);
            });
        }
    }

    private sealed class DuplicateTenantMarkerDbContext(
        DbContextOptions<DuplicateTenantMarkerDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<DuplicateTenantMarkerRecord>(entity =>
            {
                entity.ToTable("duplicate_tenant_marker_records");
                entity.HasKey(record => record.Id);
            });
        }
    }

    private sealed class DuplicateAnonymisedAtMarkerDbContext(
        DbContextOptions<DuplicateAnonymisedAtMarkerDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<DuplicateAnonymisedAtMarkerRecord>(entity =>
            {
                entity.ToTable("duplicate_anonymised_at_marker_records");
                entity.HasKey(record => record.Id);
            });
        }
    }

    private sealed class ContradictoryTenantlessDbContext(
        DbContextOptions<ContradictoryTenantlessDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ContradictoryTenantlessRecord>(entity =>
            {
                entity.ToTable("contradictory_tenantless_records");
                entity.HasKey(record => record.Id);
            });
        }
    }

    [Retain("inheritance-base", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000035")]
    private class InheritanceBaseRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class InheritanceDerivedRecord : InheritanceBaseRecord
    {
        public string Extra { get; init; } = "";
    }

    [Retain("schema-category", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000036")]
    private sealed class NonDefaultSchemaRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("cascade-parent", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000037")]
    private sealed class CascadeParentRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("cascade-child", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000038")]
    private sealed class CascadeChildRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public Guid ParentId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("restrict-child", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000039")]
    private sealed class RestrictChildRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public Guid ParentId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("duplicate-tenant-marker", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000003a")]
    private sealed class DuplicateTenantMarkerRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [RetentionTenant]
        public Guid OrganisationId { get; init; }

        [RetentionTenant]
        public Guid OwnerId { get; init; }
    }

    [Retain("duplicate-anonymised-at-marker", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000003b")]
    private sealed class DuplicateAnonymisedAtMarkerRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [RetentionAnonymisedAt]
        public DateTimeOffset? ScrubbedAt { get; init; }

        [RetentionAnonymisedAt]
        public DateTimeOffset? RedactedAt { get; init; }
    }

    [Retain("contradictory-tenantless", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000003c")]
    [RetentionTenantless]
    private sealed class ContradictoryTenantlessRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class TestAnonymiseValueFactory : IAnonymiseValueFactory
    {
        public object? Create(AnonymiseValueContext context) => Guid.Empty;
    }

    private sealed class NotAFactory;

    [Retain("naive-anchor", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-00000000003d")]
    private sealed class NaiveTimestampRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTime CreatedAt { get; init; }
    }

    private sealed class NaiveTimestampDbContext(DbContextOptions<NaiveTimestampDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<NaiveTimestampRecord>(entity =>
            {
                entity.ToTable("naive_timestamp_records");
                entity.HasKey(record => record.Id);
                entity
                    .Property(record => record.CreatedAt)
                    .HasColumnType("timestamp without time zone");
            });
        }
    }

    private sealed class TimestamptzAnchorDbContext(
        DbContextOptions<TimestamptzAnchorDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<NaiveTimestampRecord>(entity =>
            {
                entity.ToTable("timestamptz_anchor_records");
                entity.HasKey(record => record.Id);
            });
        }
    }

    private sealed class MissingRetentionIdentityDbContext(
        DbContextOptions<MissingRetentionIdentityDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<MissingRetentionIdentityRecord>().HasKey(record => record.Id);
        }
    }

    [Retain("identity", nameof(CreatedAt))]
    [RetentionTenantless]
    private sealed class MissingRetentionIdentityRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class DuplicateRetentionIdentityDbContext(
        DbContextOptions<DuplicateRetentionIdentityDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder
                .Entity<FirstDuplicateRetentionIdentityRecord>()
                .HasKey(record => record.Id);
            modelBuilder
                .Entity<SecondDuplicateRetentionIdentityRecord>()
                .HasKey(record => record.Id);
        }
    }

    [Retain("identity", nameof(CreatedAt))]
    [RetentionEntityId("e5701795-cdba-4482-a1ea-0497d2353e78")]
    [RetentionTenantless]
    private sealed class FirstDuplicateRetentionIdentityRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("identity", nameof(CreatedAt))]
    [RetentionEntityId("e5701795-cdba-4482-a1ea-0497d2353e78")]
    [RetentionTenantless]
    private sealed class SecondDuplicateRetentionIdentityRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class CountingCategoryRepository(IRetentionCategoryRepository inner)
        : IRetentionCategoryRepository
    {
        private int getAsyncCount;

        public int GetAsyncCount => Volatile.Read(ref getAsyncCount);

        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            Interlocked.Increment(ref getAsyncCount);
            return inner.GetAsync(category, ct);
        }
    }
}
