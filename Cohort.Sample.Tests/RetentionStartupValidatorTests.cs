using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Migrations;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

public sealed class RetentionStartupValidatorTests
{
    private static readonly ITestRetentionRule ExemptResolver = new StaticTestRetentionRule(
        new RetentionRule(TimeSpan.FromDays(30), Strategy.Exempt)
    );

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

    [Fact]
    public async Task ValidateAsync_Rejects_A_Blank_Erasure_Subject_Kind()
    {
        var options = new DbContextOptionsBuilder<BlankErasureKindDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-blank-erasure-kind-{Guid.NewGuid()}")
            .Options;
        await using var db = new BlankErasureKindDbContext(options);

        var act = async () =>
            await CreateValidator(db, IdentityCategoryRepository()).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle().Which.Should().Be(
            $"Retention category 'identity' for entity {typeof(BlankErasureKindRecord).FullName} failed startup validation: Erasure subject kind cannot be blank. (Parameter 'kind')"
        );
    }

    private static InMemoryCategoryRepository IdentityCategoryRepository() =>
        new(new Dictionary<string, ITestRetentionRule> { ["identity"] = ExemptResolver });

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
                new RetentionEntryBuilder(new CohortConventions()),
                [],
                new RetentionValidationState(),
                new ErasureSubjectMetadataResolver(db)
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
            new Dictionary<string, ITestRetentionRule>
            {
                ["broken-sample"] = new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(90), Strategy.Purge)
                ),
            }
        );

        var act = async () =>
            await new RetentionStartupValidator(
                db,
                repository,
                new RetentionEntryBuilder(new CohortConventions()),
                [],
                new RetentionValidationState(),
                new ErasureSubjectMetadataResolver(db)
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
    public async Task ValidateAsync_Rejects_Missing_Category_Capabilities()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-missing-category-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);

        var act = async () =>
            await new RetentionStartupValidator(
                db,
                InMemoryCategoryRepository.Empty,
                new RetentionEntryBuilder(new CohortConventions()),
                [],
                new RetentionValidationState(),
                new ErasureSubjectMetadataResolver(db)
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
            new Dictionary<string, ITestRetentionRule>
            {
                ["valid-category"] = new StaticTestRetentionRule(
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
    public async Task ValidateAsync_Rejects_Record_Ids_Whose_Only_Unique_Index_Is_Filtered()
    {
        // A partial unique index allows duplicates outside its filter, so it proves nothing.
        var options = new DbContextOptionsBuilder<FilteredUniqueRecordIdDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-filtered-unique-record-id-{Guid.NewGuid()}")
            .Options;
        await using var db = new FilteredUniqueRecordIdDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, ITestRetentionRule> { ["record-id"] = ExemptResolver }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle().Which.Should().StartWith(
            $"Record-id convention on {typeof(NonUniqueRecordIdRecord).FullName}: record-id property 'ExternalId' must uniquely identify rows"
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
            new Dictionary<string, ITestRetentionRule> { ["record-id"] = ExemptResolver }
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
    public async Task ValidateAsync_Rejects_SoftDelete_Categories_Without_A_Public_Bool_IsDeleted_Property()
    {
        var options = new DbContextOptionsBuilder<InvalidSoftDeleteIsDeletedDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-invalid-soft-delete-flag-{Guid.NewGuid()}")
            .Options;
        await using var db = new InvalidSoftDeleteIsDeletedDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, ITestRetentionRule>
            {
                ["invalid-soft-delete"] = new StaticTestRetentionRule(
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
    public async Task ValidateAsync_Rejects_Retained_Entities_Without_Tenant_Metadata_Unless_Explicitly_Tenantless()
    {
        var options = new DbContextOptionsBuilder<MissingSoftDeleteTenantDbContext>()
            .UseNpgsqlMetadataModel(
                $"startup-validator-missing-soft-delete-tenant-{Guid.NewGuid()}"
            )
            .Options;
        await using var db = new MissingSoftDeleteTenantDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, ITestRetentionRule>
            {
                ["missing-soft-delete-tenant"] = new StaticTestRetentionRule(
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
    public async Task ValidateAsync_Rejects_Anonymise_Categories_Without_Annotated_Fields()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-missing-anonymise-fields-{Guid.NewGuid()}")
            .Options;
        await using var db = new SampleDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, ITestRetentionRule>
            {
                ["short-lived"] = new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
                ["soft-delete"] = new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
                ["anonymise"] = new StaticTestRetentionRule(
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
    public async Task ValidateAsync_Rejects_Null_Anonymise_When_EF_Property_Is_Required()
    {
        var options = new DbContextOptionsBuilder<RequiredNullableAnonymiseDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-required-null-anonymise-{Guid.NewGuid()}")
            .Options;
        await using var db = new RequiredNullableAnonymiseDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, ITestRetentionRule>
            {
                ["required-null-anonymise"] = new StaticTestRetentionRule(
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
            new Dictionary<string, ITestRetentionRule>
            {
                ["structural-anonymise"] = new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
            }
        );

        var act = async () =>
            await new RetentionStartupValidator(
                db,
                repository,
                new RetentionEntryBuilder(new CohortConventions()),
                [new TestAnonymiseValueFactory()],
                new RetentionValidationState(),
                new ErasureSubjectMetadataResolver(db)
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
    public async Task ValidateAsync_Rejects_Retained_Entities_In_Inheritance_Hierarchies()
    {
        var options = new DbContextOptionsBuilder<InheritanceDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-inheritance-{Guid.NewGuid()}")
            .Options;
        await using var db = new InheritanceDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, ITestRetentionRule>
            {
                ["inheritance-base"] = new StaticTestRetentionRule(
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
    public async Task ValidateAsync_Rejects_Cascade_Delete_Paths_Into_Retained_Entities()
    {
        var options = new DbContextOptionsBuilder<CascadeDeleteDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-cascade-{Guid.NewGuid()}")
            .Options;
        await using var db = new CascadeDeleteDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, ITestRetentionRule>
            {
                ["cascade-parent"] = new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
                ["cascade-child"] = new StaticTestRetentionRule(
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
    public async Task ValidateAsync_Rejects_Value_Converters_On_Columns_Cohort_Writes_Or_Filters_In_Sql()
    {
        // Cohort's raw SQL binds tenant ids and writes TRUE/now() directly, so a converter
        // (e.g. an is_active column exposed inverted as IsDeleted) would silently flip meaning.
        var options = new DbContextOptionsBuilder<ConvertedStructuralColumnsDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-structural-converters-{Guid.NewGuid()}")
            .Options;
        await using var db = new ConvertedStructuralColumnsDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, ITestRetentionRule>
            {
                ["structural-converters"] = new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
            }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Where(error => error.Contains("value converter", StringComparison.Ordinal))
            .Should().HaveCount(2)
            .And.Contain(error => error.Contains("'TenantId'", StringComparison.Ordinal))
            .And.Contain(error => error.Contains("'IsDeleted'", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Self_Referencing_Cascades_On_Retained_Entities()
    {
        // Purging an expired parent would cascade into children still inside their window.
        var options = new DbContextOptionsBuilder<SelfCascadeDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-self-cascade-{Guid.NewGuid()}")
            .Options;
        await using var db = new SelfCascadeDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, ITestRetentionRule>
            {
                ["self-cascade"] = new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
            }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle().Which.Should().StartWith(
            $"[Retain] on {typeof(SelfCascadeRecord).FullName}: purging this entity cascades (ON DELETE CASCADE) into retained entity {typeof(SelfCascadeRecord).FullName},"
        );
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Duplicate_Marker_Attributes()
    {
        var options = new DbContextOptionsBuilder<DuplicateTenantMarkerDbContext>()
            .UseNpgsqlMetadataModel($"startup-validator-duplicate-marker-{Guid.NewGuid()}")
            .Options;
        await using var db = new DuplicateTenantMarkerDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, ITestRetentionRule>
            {
                ["duplicate-tenant-marker"] = new StaticTestRetentionRule(
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
    public async Task ValidateAsync_Rejects_Naive_Timestamp_Anchor_Columns()
    {
        // Npgsql model building works offline; ValidateAsync only inspects metadata.
        var options = new DbContextOptionsBuilder<NaiveTimestampDbContext>()
            .UseNpgsql("Host=localhost;Database=cohort-model-only")
            .Options;
        await using var db = new NaiveTimestampDbContext(options);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, ITestRetentionRule>
            {
                ["naive-anchor"] = new StaticTestRetentionRule(
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

    [Theory]
    [InlineData("timestamptz")]
    [InlineData("timestamp(3) with time zone")]
    [InlineData("TIMESTAMPTZ(6)")]
    public async Task ValidateAsync_Accepts_Timestamptz_Aliases_For_Anchor_And_DeletedAt_Columns(
        string storeType
    )
    {
        // Service-provider caching is off so each row builds its own model for the same context type.
        var options = new DbContextOptionsBuilder<TimestamptzAliasDbContext>()
            .UseNpgsql("Host=localhost;Database=cohort-model-only")
            .EnableServiceProviderCaching(false)
            .Options;
        await using var db = new TimestamptzAliasDbContext(options, storeType);
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, ITestRetentionRule>
            {
                ["timestamptz-alias"] = new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
            }
        );

        var act = async () => await CreateValidator(db, repository).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ValidateAsync_Rejects_Record_Ids_Whose_Text_Form_Depends_On_Session_Settings()
    {
        // Cohort persists record ids as CAST(key AS text); for these store types that text
        // follows TimeZone/DateStyle/extra_float_digits, so holds and row details drift apart.
        var repository = new InMemoryCategoryRepository(
            new Dictionary<string, ITestRetentionRule> { ["session-key"] = ExemptResolver }
        );
        var timestampOptions = new DbContextOptionsBuilder<SessionDependentKeyDbContext<DateTimeOffset>>()
            .UseNpgsqlMetadataModel($"startup-validator-timestamptz-record-id-{Guid.NewGuid()}")
            .Options;
        await using var timestampDb = new SessionDependentKeyDbContext<DateTimeOffset>(timestampOptions);
        var floatOptions = new DbContextOptionsBuilder<SessionDependentKeyDbContext<double>>()
            .UseNpgsqlMetadataModel($"startup-validator-float-record-id-{Guid.NewGuid()}")
            .Options;
        await using var floatDb = new SessionDependentKeyDbContext<double>(floatOptions);

        var timestampAct = async () => await CreateValidator(timestampDb, repository).ValidateAsync();
        var floatAct = async () => await CreateValidator(floatDb, repository).ValidateAsync();

        var timestampException = await timestampAct.Should().ThrowAsync<RetentionConfigurationException>();
        timestampException.Which.Errors.Should().ContainSingle().Which.Should().StartWith(
            $"Record-id convention on {typeof(SessionDependentKeyRecord<DateTimeOffset>).FullName}: record-id property 'Id' is mapped to 'timestamp with time zone'"
        );
        var floatException = await floatAct.Should().ThrowAsync<RetentionConfigurationException>();
        floatException.Which.Errors.Should().ContainSingle().Which.Should().StartWith(
            $"Record-id convention on {typeof(SessionDependentKeyRecord<double>).FullName}: record-id property 'Id' is mapped to 'double precision'"
        );
    }

    private sealed class InMemoryCategoryRepository(
        IReadOnlyDictionary<string, ITestRetentionRule> resolvers
    ) : ITestRetentionRuleProvider
    {
        public static InMemoryCategoryRepository Empty { get; } =
            new(new Dictionary<string, ITestRetentionRule>());

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct)
        {
            resolvers.TryGetValue(category, out var resolver);
            return Task.FromResult(resolver);
        }
    }

    private sealed class DeferredRuleResolver(RetentionRule rule) : ITestRetentionRule
    {
        public Task<RetentionRule> ResolveAsync(
            RetentionResolutionContext ctx,
            CancellationToken ct
        ) => Task.FromResult(rule);
    }

    private static RetentionStartupValidator CreateValidator(
        DbContext db,
        ITestRetentionRuleProvider repository
    )
    {
        return new RetentionStartupValidator(
            db,
            repository,
            new RetentionEntryBuilder(new CohortConventions()),
            [new GuidTombstoneFactory(), new OriginalValueTombstoneFactory()],
            new RetentionValidationState(),
            new ErasureSubjectMetadataResolver(db)
        );
    }

    private sealed class ConflictingAttributeDbContext(
        DbContextOptions<ConflictingAttributeDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
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
            modelBuilder.ConfigureCohortTables();
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
            modelBuilder.ConfigureCohortTables();
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

    private sealed class NonUniqueRecordIdDbContext(
        DbContextOptions<NonUniqueRecordIdDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<NonUniqueRecordIdRecord>(entity =>
            {
                entity.ToTable("non_unique_record_id_records");
                entity.HasKey(record => record.InternalKey);
            });
        }
    }

    private sealed class FilteredUniqueRecordIdDbContext(
        DbContextOptions<FilteredUniqueRecordIdDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<NonUniqueRecordIdRecord>(entity =>
            {
                entity.ToTable("filtered_unique_record_id_records");
                entity.HasKey(record => record.InternalKey);
                entity.HasIndex(record => record.ExternalId).IsUnique().HasFilter("\"TenantId\" IS NOT NULL");
            });
        }
    }

    private sealed class InvalidSoftDeleteIsDeletedDbContext(
        DbContextOptions<InvalidSoftDeleteIsDeletedDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
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

    private sealed class MissingSoftDeleteTenantDbContext(
        DbContextOptions<MissingSoftDeleteTenantDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
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

    private sealed class RequiredNullableAnonymiseDbContext(
        DbContextOptions<RequiredNullableAnonymiseDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
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
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<StructuralAnonymiseRecord>(entity =>
            {
                entity.HasKey(record => record.Id);
            });
        }
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

    [Retain("invalid-soft-delete", nameof(InvalidSoftDeleteIsDeletedRecord.CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-000000000027")]
    private sealed class InvalidSoftDeleteIsDeletedRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public string IsDeleted { get; init; } = "";
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
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<InheritanceBaseRecord>(entity =>
            {
                entity.ToTable("inheritance_records");
                entity.HasKey(record => record.Id);
            });
            modelBuilder.Entity<InheritanceDerivedRecord>();
        }
    }

    private sealed class CascadeDeleteDbContext(DbContextOptions<CascadeDeleteDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
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

    private sealed class ConvertedStructuralColumnsDbContext(
        DbContextOptions<ConvertedStructuralColumnsDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<ConvertedStructuralColumnsRecord>(entity =>
            {
                entity.ToTable("converted_structural_columns_records");
                entity.Property(record => record.TenantId).HasConversion(value => value.ToString(), value => Guid.Parse(value));
                entity.Property(record => record.IsDeleted).HasColumnName("is_active").HasConversion(value => !value, value => !value);
            });
        }
    }

    [Retain("structural-converters", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-0000000000a5")]
    private sealed class ConvertedStructuralColumnsRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public bool IsDeleted { get; init; }
        public DateTimeOffset? DeletedAt { get; init; }
    }

    private sealed class SelfCascadeDbContext(DbContextOptions<SelfCascadeDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<SelfCascadeRecord>(entity =>
            {
                entity.ToTable("self_cascade_records");
                entity
                    .HasOne<SelfCascadeRecord>()
                    .WithMany()
                    .HasForeignKey(record => record.ParentId)
                    .OnDelete(DeleteBehavior.Cascade);
            });
        }
    }

    [Retain("self-cascade", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-0000000000a4")]
    private sealed class SelfCascadeRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public Guid? ParentId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class DuplicateTenantMarkerDbContext(
        DbContextOptions<DuplicateTenantMarkerDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<DuplicateTenantMarkerRecord>(entity =>
            {
                entity.ToTable("duplicate_tenant_marker_records");
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
            modelBuilder.ConfigureCohortTables();
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

    [Retain("timestamptz-alias", nameof(CreatedAt))]
    [RetentionEntityId("6d0e4b8c-2f7a-4c55-9b1e-3a8f0c2d7e41")]
    private sealed class TimestamptzAliasRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public bool IsDeleted { get; set; }
        public DateTimeOffset? DeletedAt { get; set; }
    }

    private sealed class TimestamptzAliasDbContext(
        DbContextOptions<TimestamptzAliasDbContext> options,
        string storeType
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<TimestamptzAliasRecord>(entity =>
            {
                entity.ToTable("timestamptz_alias_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.CreatedAt).HasColumnType(storeType);
                entity.Property(record => record.DeletedAt).HasColumnType(storeType);
            });
        }
    }

    [Retain("session-key", nameof(CreatedAt))]
    [RetentionEntityId("b3f19a6e-7c02-4d8b-a5e4-1f6c9d2b8a73")]
    private sealed class SessionDependentKeyRecord<TKey>
        where TKey : struct
    {
        public TKey Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class SessionDependentKeyDbContext<TKey>(
        DbContextOptions<SessionDependentKeyDbContext<TKey>> options
    ) : DbContext(options)
        where TKey : struct
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<SessionDependentKeyRecord<TKey>>(entity =>
            {
                entity.ToTable("session_dependent_key_records");
                entity.HasKey(record => record.Id);
            });
        }
    }

    private sealed class BlankErasureKindDbContext(
        DbContextOptions<BlankErasureKindDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<BlankErasureKindRecord>().HasKey(record => record.Id);
        }
    }

    [Retain("identity", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0001-0000000000e1")]
    private sealed class BlankErasureKindRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }

        [ErasureSubject(" ")]
        public Guid? SubjectId { get; init; }

        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class MissingRetentionIdentityDbContext(
        DbContextOptions<MissingRetentionIdentityDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
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
            modelBuilder.ConfigureCohortTables();
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

    private sealed class CountingCategoryRepository(ITestRetentionRuleProvider inner)
        : ITestRetentionRuleProvider
    {
        private int getAsyncCount;

        public int GetAsyncCount => Volatile.Read(ref getAsyncCount);

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct)
        {
            Interlocked.Increment(ref getAsyncCount);
            return inner.GetAsync(category, ct);
        }
    }
}
