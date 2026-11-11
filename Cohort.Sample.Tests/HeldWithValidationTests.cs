using System.ComponentModel.DataAnnotations.Schema;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Holds;
using Cohort.Infrastructure.Migrations;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests;

// Pattern #3-shaped: Npgsql model metadata only, no connection. Each refusal in the spec's
// Decision 5 has a test-local DbContext, so the sample's registry and plan order stay untouched.
public sealed class HeldWithValidationTests
{
    private const string AttributeShape = "[HeldWith(typeof(";

    [Fact]
    public async Task A_Chain_Of_Both_Shapes_Passes_And_Resolves_Holder_And_Covered_Paths()
    {
        await using var db = CreateContext<ChainDbContext>();

        await CreateValidator(db).ValidateAsync();

        var entries = new RetentionEntryBuilder(new CohortConventions()).BuildAll(db.Model);
        var upload = entries[typeof(ChainUpload)];
        var submission = entries[typeof(ChainSubmission)];
        var theCase = entries[typeof(ChainCase)];
        var person = entries[typeof(ChainPerson)];

        // Upload -(a)-> Submission -(b)-> Case -(a)-> Person; each prefix is its own path.
        upload.HolderPaths.Select(PathNames).Should().Equal(
            nameof(ChainSubmission),
            $"{nameof(ChainSubmission)}>{nameof(ChainCase)}",
            $"{nameof(ChainSubmission)}>{nameof(ChainCase)}>{nameof(ChainPerson)}"
        );
        submission.HolderPaths.Select(PathNames).Should().Equal(
            nameof(ChainCase),
            $"{nameof(ChainCase)}>{nameof(ChainPerson)}"
        );
        theCase.HolderPaths.Select(PathNames).Should().Equal(nameof(ChainPerson));
        person.HolderPaths.Should().BeEmpty();

        person.CoveredPaths.Select(PathNames).Should().Equal(
            nameof(ChainCase),
            $"{nameof(ChainCase)}>{nameof(ChainSubmission)}",
            $"{nameof(ChainCase)}>{nameof(ChainSubmission)}>{nameof(ChainUpload)}"
        );
        theCase.CoveredPaths.Select(PathNames).Should().Equal(
            nameof(ChainSubmission),
            $"{nameof(ChainSubmission)}>{nameof(ChainUpload)}"
        );
        submission.CoveredPaths.Select(PathNames).Should().Equal(nameof(ChainUpload));
        upload.CoveredPaths.Should().BeEmpty();

        // The scalars a renderer walks: [HeldWith] keeps the link on the near side.
        var uploadToSubmission = upload.HolderPaths[0].Hops.Should().ContainSingle().Which;
        uploadToSubmission.LinkColumn.Should().Be("SubmissionId");
        uploadToSubmission.LinkSide.Should().Be(HoldLinkSide.Near);
        uploadToSubmission.Near.EntityType.Should().Be(typeof(ChainUpload));
        uploadToSubmission.Far.EntityType.Should().Be(typeof(ChainSubmission));
        uploadToSubmission.Far.RetentionEntityId.Should().Be(submission.RetentionEntityId);
        uploadToSubmission.Far.Table.Should().Be(submission.Table);
        uploadToSubmission.Far.RecordId.Should().Be(submission.RecordId);
        uploadToSubmission.Far.Tenant.Should().Be(submission.Tenant);

        // [HeldWithReferencing] keeps it on the far side: the Case's column names the Submission.
        var submissionToCase = submission.HolderPaths[0].Hops.Should().ContainSingle().Which;
        submissionToCase.LinkColumn.Should().Be("SourceSubmissionId");
        submissionToCase.LinkSide.Should().Be(HoldLinkSide.Far);
        submissionToCase.Far.Table.Should().Be(theCase.Table);

        // Walked the other way the sides swap: from the Case down to the Submission the link
        // column (the Case's SourceSubmissionId) is on the near end, and from the Submission down
        // to the Upload (the Upload's SubmissionId) it is on the far end.
        var caseToSubmission = theCase.CoveredPaths[0].Hops.Should().ContainSingle().Which;
        caseToSubmission.LinkSide.Should().Be(HoldLinkSide.Near);
        caseToSubmission.LinkColumn.Should().Be("SourceSubmissionId");
        var submissionToUpload = submission.CoveredPaths[0].Hops.Should().ContainSingle().Which;
        submissionToUpload.LinkSide.Should().Be(HoldLinkSide.Far);
        submissionToUpload.LinkColumn.Should().Be("SubmissionId");
        person.CoveredPaths[0].Hops.Should().ContainSingle().Which.LinkColumn.Should().Be("PersonId");

        // ChainCase.PersonId is a PersonKey with a value converter to Guid: the validation above
        // passing is the proof that the match is on the provider type; this pins the converter.
        var link = db.Model.FindEntityType(typeof(ChainCase))!.FindProperty(nameof(ChainCase.PersonId))!;
        link.ClrType.Should().Be(typeof(PersonKey));
        link.GetValueConverter()!.ProviderClrType.Should().Be(typeof(Guid));
    }

    [Fact]
    public async Task A_Link_Spelled_As_Another_Alias_Of_The_Record_Ids_Store_Type_Passes()
    {
        // int4 is integer, and varchar(36) is HasMaxLength(36)'s character varying(36): one type
        // each, so only the provider CLR types are compared here and readiness compares the rest.
        await using var db = CreateContext<StoreTypeSpellingDbContext>();

        var act = async () => await CreateValidator(db).ValidateAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task A_Declaration_On_An_Entity_That_Is_Not_Retained_Is_Refused()
    {
        var errors = await RefusalsAsync<NotRetainedDeclaringDbContext>();

        errors.Should().HaveCount(2);
        errors.Should().ContainSingle(error =>
            error.Contains($"{AttributeShape}{typeof(HeldTarget).FullName}), \"TargetId\")] on {typeof(ExemptDeclaring).FullName}", StringComparison.Ordinal)
            && error.Contains("isn't a [Retain] entity", StringComparison.Ordinal)
        );
        errors.Should().ContainSingle(error =>
            error.Contains($"on {typeof(UnannotatedDeclaring).FullName}", StringComparison.Ordinal)
            && error.Contains("isn't a [Retain] entity", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task A_Related_Type_That_Is_Not_A_Retained_Entity_In_The_Model_Is_Refused()
    {
        var errors = await RefusalsAsync<NotRetainedRelatedDbContext>();

        errors.Should().HaveCount(3);
        errors.Should().ContainSingle(error =>
            error.Contains($"{AttributeShape}{typeof(ExemptRelated).FullName}), \"RelatedId\")] on {typeof(LinksToExempt).FullName}", StringComparison.Ordinal)
            && error.Contains($"{typeof(ExemptRelated).FullName} isn't a [Retain] entity in the DbContext model", StringComparison.Ordinal)
        );
        errors.Should().ContainSingle(error =>
            error.Contains($"on {typeof(LinksToUnannotated).FullName}", StringComparison.Ordinal)
            && error.Contains($"{typeof(UnannotatedRelated).FullName} isn't a [Retain] entity in the DbContext model", StringComparison.Ordinal)
        );
        // Retained by attribute, but never mapped in this model.
        errors.Should().ContainSingle(error =>
            error.Contains($"on {typeof(LinksToUnmapped).FullName}", StringComparison.Ordinal)
            && error.Contains($"{typeof(UnmappedRetained).FullName} isn't a [Retain] entity in the DbContext model", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task A_Link_Member_That_Is_Not_A_Mapped_Property_On_The_Right_Side_Is_Refused()
    {
        var errors = await RefusalsAsync<UnmappedMemberDbContext>();

        errors.Should().HaveCount(3);
        errors.Should().ContainSingle(error =>
            error.Contains($"{AttributeShape}{typeof(HeldTarget).FullName}), \"Missing\")] on {typeof(MissingMemberLink).FullName}", StringComparison.Ordinal)
            && error.Contains($"link member 'Missing' isn't a mapped public property with a table column on {typeof(MissingMemberLink).FullName}", StringComparison.Ordinal)
        );
        errors.Should().ContainSingle(error =>
            error.Contains($"\"{nameof(NotMappedLink.Ignored)}\")] on {typeof(NotMappedLink).FullName}", StringComparison.Ordinal)
            && error.Contains("isn't a mapped public property", StringComparison.Ordinal)
        );
        // The Submission exists on the declaring entity, but [HeldWithReferencing] reads it on the related one.
        errors.Should().ContainSingle(error =>
            error.Contains($"[HeldWithReferencing(typeof({typeof(HeldTarget).FullName}), \"{nameof(WrongSideLink.CaseId)}\")] on {typeof(WrongSideLink).FullName}", StringComparison.Ordinal)
            && error.Contains($"on {typeof(HeldTarget).FullName}, which holds the link for [HeldWithReferencing]", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task A_Link_Whose_Type_Differs_From_The_Target_Record_Id_Is_Refused()
    {
        var errors = await RefusalsAsync<LinkTypeMismatchDbContext>();

        var error = errors.Should().ContainSingle().Which;
        error.Should().Contain($"\"{nameof(StringLink.CaseId)}\")] on {typeof(StringLink).FullName}");
        error.Should().Contain($"link member {typeof(StringLink).FullName}.{nameof(StringLink.CaseId)} is String");
        error.Should().Contain($"record id {typeof(HeldTarget).FullName}.Id it must equal is Guid");
    }

    [Fact]
    public async Task A_Blank_Link_Member_Is_Refused_With_The_Declaring_Type()
    {
        var errors = await RefusalsAsync<BlankLinkMemberDbContext>();

        var error = errors.Should().ContainSingle().Which;
        error.Should().Contain($"[HeldWith] or [HeldWithReferencing] on {typeof(BlankLinkMember).FullName} is invalid");
        error.Should().Contain("Link member cannot be blank");
    }

    [Fact]
    public async Task A_Type_Mapped_More_Than_Once_Has_Its_Declarations_Read_Once()
    {
        // An owned type used by two owners is two entity types with one CLR type: its one
        // declaration must be refused once for what it is, not once more as a duplicate.
        var errors = await RefusalsAsync<OwnedTwiceDbContext>();

        var error = errors.Should().ContainSingle().Which;
        error.Should().Contain($"on {typeof(OwnedDeclaring).FullName}");
        error.Should().Contain("isn't a [Retain] entity");
    }

    [Fact]
    public async Task A_Relation_Between_A_Tenanted_And_A_Tenantless_Entity_Is_Refused()
    {
        var errors = await RefusalsAsync<TenantMismatchDbContext>();

        var error = errors.Should().ContainSingle().Which;
        error.Should().Contain($"{AttributeShape}{typeof(HeldTarget).FullName}), \"TargetId\")] on {typeof(TenantlessLink).FullName}");
        error.Should().Contain($"{typeof(HeldTarget).FullName} is tenanted but {typeof(TenantlessLink).FullName} is tenantless");
    }

    [Fact]
    public async Task The_Same_Relation_Declared_Twice_Is_Refused()
    {
        var errors = await RefusalsAsync<DuplicateRelationDbContext>();

        var error = errors.Should().ContainSingle().Which;
        error.Should().Contain($"{AttributeShape}{typeof(HeldTarget).FullName}), \"TargetId\")] on {typeof(TwiceDeclared).FullName}");
        error.Should().Contain("declared more than once");
    }

    [Fact]
    public async Task A_Cycle_Is_Refused_With_Its_Members_And_Declarations()
    {
        var errors = await RefusalsAsync<CycleDbContext>();

        var error = errors.Should().ContainSingle().Which;
        error.Should().Contain($"retained entities {typeof(CycleA).FullName}, {typeof(CycleB).FullName} form a cycle");
        error.Should().Contain($"\"{nameof(CycleA.BId)}\")] on {typeof(CycleA).FullName}");
        error.Should().Contain($"\"{nameof(CycleB.AId)}\")] on {typeof(CycleB).FullName}");
    }

    [Fact]
    public async Task A_Self_Relation_Is_Refused_As_A_Cycle()
    {
        var errors = await RefusalsAsync<SelfRelationDbContext>();

        var error = errors.Should().ContainSingle().Which;
        error.Should().Contain($"retained entities {typeof(SelfRelated).FullName} form a cycle");
        error.Should().Contain($"\"{nameof(SelfRelated.ParentId)}\")] on {typeof(SelfRelated).FullName}");
    }

    [Fact]
    public async Task A_Runtime_Scan_Throws_The_Errors_Startup_Validation_Reports()
    {
        // RetentionTargetResolver scans before readiness runs, so BuildAll can't hand back a
        // registry with a bad declaration; it must say the same thing the validator does.
        await using var db = CreateContext<DuplicateRelationDbContext>();
        var validate = async () => await CreateValidator(db).ValidateAsync();
        var validatorErrors = (await validate.Should().ThrowAsync<RetentionConfigurationException>()).Which.Errors;

        var scan = () => new RetentionEntryBuilder(new CohortConventions()).BuildAll(db.Model);

        scan.Should().Throw<RetentionConfigurationException>().Which.Errors.Should().Equal(validatorErrors);
    }

    private static string PathNames(HoldPath path) =>
        string.Join('>', path.Hops.Select(hop => hop.Far.EntityType.Name));

    private static TContext CreateContext<TContext>()
        where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseNpgsqlMetadataModel($"held-with-validation-{Guid.NewGuid()}")
            .Options;
        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }

    private static async Task<IReadOnlyList<string>> RefusalsAsync<TContext>()
        where TContext : DbContext
    {
        await using var db = CreateContext<TContext>();
        var validate = async () => await CreateValidator(db).ValidateAsync();
        var exception = await validate.Should().ThrowAsync<RetentionConfigurationException>();
        return exception.Which.Errors;
    }

    private static RetentionStartupValidator CreateValidator(DbContext db) =>
        new(
            db,
            new PurgeCategoryProvider(),
            new RetentionEntryBuilder(new CohortConventions()),
            [],
            new RetentionValidationState(),
            new RetentionDependentsServiceRegistrations(new ServiceCollection())
        );

    private sealed class PurgeCategoryProvider : ITestRetentionRuleProvider
    {
        private static readonly ITestRetentionRule Resolver = new StaticTestRetentionRule(
            new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge)
        );

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct) =>
            Task.FromResult<ITestRetentionRule?>(category == "held-with" ? Resolver : null);
    }

    // ─── A valid chain: Upload (a) Submission (b) Case (a) Person ───────────────────────────

    private sealed class ChainDbContext(DbContextOptions<ChainDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<ChainPerson>();
            modelBuilder
                .Entity<ChainCase>()
                .Property(record => record.PersonId)
                .HasConversion(key => key.Value, value => new PersonKey(value));
            modelBuilder.Entity<ChainSubmission>();
            modelBuilder.Entity<ChainUpload>();
        }
    }

    private readonly record struct PersonKey(Guid Value);

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000001")]
    private sealed class ChainPerson
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000002")]
    [HeldWith(typeof(ChainPerson), nameof(PersonId))]
    private sealed class ChainCase
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public PersonKey PersonId { get; init; }
        public Guid? SourceSubmissionId { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000003")]
    [HeldWithReferencing(typeof(ChainCase), nameof(ChainCase.SourceSubmissionId))]
    private sealed class ChainSubmission
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000004")]
    [HeldWith(typeof(ChainSubmission), nameof(SubmissionId))]
    private sealed class ChainUpload
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid SubmissionId { get; init; }
    }

    // ─── A shared retained target and entities that refuse for one reason each ──────────────

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000010")]
    private sealed class HeldTarget
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class NotRetainedDeclaringDbContext(DbContextOptions<NotRetainedDeclaringDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<HeldTarget>();
            modelBuilder.Entity<ExemptDeclaring>();
            modelBuilder.Entity<UnannotatedDeclaring>();
        }
    }

    [ExemptFromRetention("test")]
    [HeldWith(typeof(HeldTarget), nameof(TargetId))]
    private sealed class ExemptDeclaring
    {
        public Guid Id { get; init; }
        public Guid TargetId { get; init; }
    }

    [HeldWith(typeof(HeldTarget), nameof(TargetId))]
    private sealed class UnannotatedDeclaring
    {
        public Guid Id { get; init; }
        public Guid TargetId { get; init; }
    }

    private sealed class NotRetainedRelatedDbContext(DbContextOptions<NotRetainedRelatedDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<ExemptRelated>();
            modelBuilder.Entity<UnannotatedRelated>();
            modelBuilder.Entity<LinksToExempt>();
            modelBuilder.Entity<LinksToUnannotated>();
            modelBuilder.Entity<LinksToUnmapped>();
        }
    }

    [ExemptFromRetention("test")]
    private sealed class ExemptRelated
    {
        public Guid Id { get; init; }
    }

    private sealed class UnannotatedRelated
    {
        public Guid Id { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000011")]
    private sealed class UnmappedRetained
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000012")]
    [HeldWith(typeof(ExemptRelated), "RelatedId")]
    private sealed class LinksToExempt
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid RelatedId { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000013")]
    [HeldWith(typeof(UnannotatedRelated), "RelatedId")]
    private sealed class LinksToUnannotated
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid RelatedId { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000014")]
    [HeldWith(typeof(UnmappedRetained), "RelatedId")]
    private sealed class LinksToUnmapped
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid RelatedId { get; init; }
    }

    private sealed class UnmappedMemberDbContext(DbContextOptions<UnmappedMemberDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<HeldTarget>();
            modelBuilder.Entity<MissingMemberLink>();
            modelBuilder.Entity<NotMappedLink>();
            modelBuilder.Entity<WrongSideLink>();
        }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000020")]
    [HeldWith(typeof(HeldTarget), "Missing")]
    private sealed class MissingMemberLink
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000021")]
    [HeldWith(typeof(HeldTarget), nameof(Ignored))]
    private sealed class NotMappedLink
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }

        [NotMapped]
        public Guid Ignored { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000022")]
    [HeldWithReferencing(typeof(HeldTarget), nameof(CaseId))]
    private sealed class WrongSideLink
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid CaseId { get; init; }
    }

    private sealed class LinkTypeMismatchDbContext(DbContextOptions<LinkTypeMismatchDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<HeldTarget>();
            modelBuilder.Entity<StringLink>();
        }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000031")]
    [HeldWith(typeof(HeldTarget), nameof(CaseId))]
    private sealed class StringLink
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public string CaseId { get; init; } = "";
    }

    private sealed class StoreTypeSpellingDbContext(DbContextOptions<StoreTypeSpellingDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<IntegerTarget>();
            modelBuilder.Entity<Int4Link>().Property(record => record.TargetId).HasColumnType("int4");
            modelBuilder.Entity<BoundedTarget>().Property(record => record.Id).HasMaxLength(36);
            modelBuilder.Entity<VarcharLink>().Property(record => record.TargetId).HasColumnType("varchar(36)");
        }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000033")]
    private sealed class IntegerTarget
    {
        public int Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000034")]
    [HeldWith(typeof(IntegerTarget), nameof(TargetId))]
    private sealed class Int4Link
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public int TargetId { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000035")]
    private sealed class BoundedTarget
    {
        public string Id { get; init; } = "";
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000036")]
    [HeldWith(typeof(BoundedTarget), nameof(TargetId))]
    private sealed class VarcharLink
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public string TargetId { get; init; } = "";
    }

    private sealed class BlankLinkMemberDbContext(DbContextOptions<BlankLinkMemberDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<HeldTarget>();
            modelBuilder.Entity<BlankLinkMember>();
        }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000037")]
    [HeldWith(typeof(HeldTarget), "")]
    private sealed class BlankLinkMember
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class OwnedTwiceDbContext(DbContextOptions<OwnedTwiceDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<HeldTarget>();
            modelBuilder.Entity<OwnerOne>().OwnsOne(owner => owner.Declaring);
            modelBuilder.Entity<OwnerTwo>().OwnsOne(owner => owner.Declaring);
        }
    }

    [HeldWith(typeof(HeldTarget), nameof(TargetId))]
    private sealed class OwnedDeclaring
    {
        public Guid TargetId { get; init; }
    }

    private sealed class OwnerOne
    {
        public Guid Id { get; init; }
        public OwnedDeclaring? Declaring { get; init; }
    }

    private sealed class OwnerTwo
    {
        public Guid Id { get; init; }
        public OwnedDeclaring? Declaring { get; init; }
    }

    private sealed class TenantMismatchDbContext(DbContextOptions<TenantMismatchDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<HeldTarget>();
            modelBuilder.Entity<TenantlessLink>();
        }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000040")]
    [RetentionTenantless]
    [HeldWith(typeof(HeldTarget), nameof(TargetId))]
    private sealed class TenantlessLink
    {
        public Guid Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid TargetId { get; init; }
    }

    private sealed class DuplicateRelationDbContext(DbContextOptions<DuplicateRelationDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<HeldTarget>();
            modelBuilder.Entity<TwiceDeclared>();
        }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000050")]
    [HeldWith(typeof(HeldTarget), nameof(TargetId))]
    [HeldWith(typeof(HeldTarget), nameof(TargetId))]
    private sealed class TwiceDeclared
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid TargetId { get; init; }
    }

    private sealed class CycleDbContext(DbContextOptions<CycleDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<CycleA>();
            modelBuilder.Entity<CycleB>();
        }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000060")]
    [HeldWith(typeof(CycleB), nameof(BId))]
    private sealed class CycleA
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid BId { get; init; }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000061")]
    [HeldWith(typeof(CycleA), nameof(AId))]
    private sealed class CycleB
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid AId { get; init; }
    }

    private sealed class SelfRelationDbContext(DbContextOptions<SelfRelationDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder.Entity<SelfRelated>();
        }
    }

    [Retain("held-with", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000070")]
    [HeldWith(typeof(SelfRelated), nameof(ParentId))]
    private sealed class SelfRelated
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public Guid ParentId { get; init; }
    }
}
