using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

// ─── EXEMPLAR #2 — narrow integration test ──────────────────────────────────
//
// Pattern: narrow integration (the middle ground). Use ONLY when the code under
// test crosses one boundary that EF Core's InMemory provider fully serves
// (reflection over `Model.GetEntityTypes()` and friends, no SQL).
//
// Appropriate when ALL of:
//   (a) the thing under test has no SQL path
//   (b) InMemory's metadata story is enough
//
// If there is ANY SQL involved → skip this pattern, write EXEMPLAR #3.
// If in doubt → don't, write EXEMPLAR #3.
//
// This file lives in Cohort.Sample.Tests (not Cohort.Tests) because it
// references SampleDbContext. The project-reference graph is the architectural
// guardrail — `Cohort.Tests` cannot accidentally drift into integration land.
// ────────────────────────────────────────────────────────────────────────────

public sealed class RegistryScanTests
{
    [Fact]
    public void Scan_Reads_Retain_Attribute_From_Sample_DbContext_Model()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseInMemoryDatabase($"registry-scan-{Guid.NewGuid()}")
            .Options;
        using var db = new SampleDbContext(options);

        var entries = new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions())).Scan();

        // Positive — the one annotated entity is found, with the right shape
        entries
            .Should()
            .Contain(kvp =>
                kvp.Key == typeof(Note)
                && kvp.Value.Category == "short-lived"
                && kvp.Value.TableName == "notes"
                && kvp.Value.AnchorMember == nameof(Note.CreatedAt)
                && kvp.Value.EntityType == typeof(Note)
            );
        entries
            .Should()
            .Contain(kvp =>
                kvp.Key == typeof(SoftDeleteRecord)
                && kvp.Value.Category == "soft-delete"
                && kvp.Value.TableName == "soft_delete_records"
                && kvp.Value.AnchorMember == nameof(SoftDeleteRecord.CreatedAt)
                && kvp.Value.EntityType == typeof(SoftDeleteRecord)
            );
        entries
            .Should()
            .Contain(kvp =>
                kvp.Key == typeof(AnonymisedContact)
                && kvp.Value.Category == "anonymise"
                && kvp.Value.TableName == "anonymised_contacts"
                && kvp.Value.AnchorMember == nameof(AnonymisedContact.CreatedAt)
                && kvp.Value.EntityType == typeof(AnonymisedContact)
            );

        // Negative — nothing else sneaks in
        entries.Values.Should().NotContain(e => e.Category == "long-lived");
        // SampleDbContext has 6 retained entities: the three original categories plus
        // TenantlessLog/TenantlessSoftDelete/PerRowAuditedLog added for negative-coverage tests.
        entries.Should().HaveCount(6);
    }

    [Fact]
    public void Scan_Derives_Anchor_And_Anonymise_Columns_From_Ef_Model_Metadata()
    {
        var options = new DbContextOptionsBuilder<RegistryMetadataDbContext>()
            .UseInMemoryDatabase($"registry-metadata-{Guid.NewGuid()}")
            .Options;
        using var db = new RegistryMetadataDbContext(options);

        var entry = new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions())).Scan()[typeof(RetentionReadyRecord)];

        entry.TableName.Should().Be("retention_ready_records");
        entry.AnchorMember.Should().Be(nameof(RetentionReadyRecord.RetainedAt));
        entry.AnchorColumn.Should().Be("retained_at_utc");
        entry.RecordId.Should().Be(
            new RecordIdConvention(nameof(RetentionReadyRecord.Id), "record_id", typeof(Guid))
        );
        entry.Tenant.Should().Be(
            new TenantConvention(nameof(RetentionReadyRecord.TenantId), "tenant_uuid")
        );
        entry.AnonymiseFields.Should().ContainSingle();
        entry.AnonymiseFields[0].Should().Be(
            new AnonymiseLiteralField(
                nameof(RetentionReadyRecord.EmailAddress),
                "email_address",
                AnonymiseMethod.FixedLiteral,
                "[redacted]"
            )
        );
    }

    [Fact]
    public void Scan_Discovers_Factory_Backed_Anonymise_Metadata_And_Column_Mapping()
    {
        var options = new DbContextOptionsBuilder<FactoryBackedRegistryMetadataDbContext>()
            .UseInMemoryDatabase($"registry-factory-anonymise-{Guid.NewGuid()}")
            .Options;
        using var db = new FactoryBackedRegistryMetadataDbContext(options);

        var entry = new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions())).Scan()[typeof(FactoryBackedRetentionReadyRecord)];

        entry.AnonymiseFields.Should().ContainSingle();
        entry.AnonymiseFields[0].Should().Be(
            new AnonymiseFactoryField(
                nameof(FactoryBackedRetentionReadyRecord.ExternalId),
                "external_identifier",
                typeof(TestAnonymiseValueFactory)
            )
        );
    }

    [Fact]
    public void Scan_Rejects_Properties_With_Both_Literal_And_Factory_Anonymise_Metadata()
    {
        var options = new DbContextOptionsBuilder<ConflictingAnonymiseMetadataDbContext>()
            .UseInMemoryDatabase($"registry-conflicting-anonymise-{Guid.NewGuid()}")
            .Options;
        using var db = new ConflictingAnonymiseMetadataDbContext(options);
        var registry = new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions()));

        var act = () => registry.Scan();

        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage(
                $"[Anonymise] and [AnonymiseWith] on *{nameof(ConflictingAnonymiseMetadataRecord)}.{nameof(ConflictingAnonymiseMetadataRecord.ExternalId)}: exactly one is allowed per property."
            );
    }

    [Fact]
    public void Scan_Captures_Soft_Delete_Convention_Metadata()
    {
        var options = new DbContextOptionsBuilder<RegistryMetadataDbContext>()
            .UseInMemoryDatabase($"registry-soft-delete-{Guid.NewGuid()}")
            .Options;
        using var db = new RegistryMetadataDbContext(options);

        var entry = new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions())).Scan()[typeof(RetentionReadyRecord)];

        entry.SoftDelete.Should().NotBeNull();
        entry.SoftDelete!.IsDeletedMember.Should().Be(nameof(RetentionReadyRecord.IsDeleted));
        entry.SoftDelete.IsDeletedColumn.Should().Be("is_deleted");
        entry.SoftDelete.DeletedAtMember.Should().Be(nameof(RetentionReadyRecord.DeletedAt));
        entry.SoftDelete.DeletedAtColumn.Should().Be("deleted_at_utc");
    }

    [Fact]
    public void Scan_Rejects_Unmapped_Deleted_At_Soft_Delete_Members()
    {
        var options = new DbContextOptionsBuilder<UnmappedDeletedAtDbContext>()
            .UseInMemoryDatabase($"registry-unmapped-deleted-at-{Guid.NewGuid()}")
            .Options;
        using var db = new UnmappedDeletedAtDbContext(options);

        var act = () => new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions())).Scan();

        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage(
                $"Soft-delete convention on *{nameof(SoftDeleteEntityWithUnmappedDeletedAt)}: DeletedAt is not mapped by EF."
            );
    }

    [Fact]
    public void Scan_Caches_The_Immutable_Lookup_Per_Registry_Instance()
    {
        var options = new DbContextOptionsBuilder<RegistryMetadataDbContext>()
            .UseInMemoryDatabase($"registry-cache-{Guid.NewGuid()}")
            .Options;
        using var db = new RegistryMetadataDbContext(options);
        var registry = new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions()));

        var firstScan = registry.Scan();
        var secondScan = registry.Scan();

        secondScan.Should().BeSameAs(firstScan);
        secondScan.Should().ContainKey(typeof(RetentionReadyRecord));
    }

    // Regression: the consumer uses `new` to shadow an inherited generic `Id` property
    // (e.g. ASP.NET Identity's `IdentityUser<TKey>.Id`). Prior to the reflection-safe
    // resolver, `Type.GetProperty("Id", ...)` would throw `AmbiguousMatchException` because
    // reflection sees both the base and the derived property and cannot pick one. The registry
    // must resolve the most-derived match and scan cleanly.
    [Fact]
    public void Scan_Handles_Entity_With_Shadowed_Generic_Id_Property_Without_Crashing()
    {
        var options = new DbContextOptionsBuilder<ShadowedIdDbContext>()
            .UseInMemoryDatabase($"shadowed-id-{Guid.NewGuid()}")
            .Options;
        using var db = new ShadowedIdDbContext(options);

        var registry = new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions()));

        var act = () => registry.Scan();

        var entries = act.Should().NotThrow().Subject;
        entries.Should().ContainKey(typeof(ShadowedIdRecord));
        var entry = entries[typeof(ShadowedIdRecord)];
        entry.RecordId.RecordIdMember.Should().Be(nameof(ShadowedIdRecord.Id));
        entry.RecordId.RecordIdType.Should().Be<Guid>();
    }

    private abstract class IdentityLikeBase<TKey>
        where TKey : IEquatable<TKey>
    {
        public virtual TKey Id { get; set; } = default!;
    }

    [Retain("long-lived", nameof(ShadowedIdRecord.RetainedAt))]
    private sealed class ShadowedIdRecord : IdentityLikeBase<Guid>
    {
        public new Guid Id { get => base.Id; set => base.Id = value; }
        public DateTimeOffset RetainedAt { get; init; }
    }

    private sealed class ShadowedIdDbContext(DbContextOptions<ShadowedIdDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ShadowedIdRecord>(entity =>
            {
                entity.ToTable("shadowed_id_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.RetainedAt);
            });
        }
    }

    [Retain("long-lived", nameof(RetentionReadyRecord.RetainedAt))]
    private sealed class RetentionReadyRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset RetainedAt { get; init; }

        [Anonymise(AnonymiseMethod.FixedLiteral, "[redacted]")]
        public string EmailAddress { get; init; } = "";

        public bool IsDeleted { get; init; }
        public DateTimeOffset? DeletedAt { get; init; }
    }

    [Retain("long-lived", nameof(FactoryBackedRetentionReadyRecord.RetainedAt))]
    private sealed class FactoryBackedRetentionReadyRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset RetainedAt { get; init; }

        [AnonymiseWith(typeof(TestAnonymiseValueFactory))]
        public Guid ExternalId { get; init; }
    }

    [Retain("long-lived", nameof(ConflictingAnonymiseMetadataRecord.RetainedAt))]
    private sealed class ConflictingAnonymiseMetadataRecord
    {
        public Guid Id { get; init; }
        public DateTimeOffset RetainedAt { get; init; }

        [Anonymise(AnonymiseMethod.FixedLiteral, "[redacted]")]
        [AnonymiseWith(typeof(TestAnonymiseValueFactory))]
        public string ExternalId { get; init; } = "";
    }

    [Retain("long-lived", nameof(SoftDeleteEntityWithUnmappedDeletedAt.RetainedAt))]
    private sealed class SoftDeleteEntityWithUnmappedDeletedAt
    {
        public Guid Id { get; init; }
        public DateTimeOffset RetainedAt { get; init; }
        public bool IsDeleted { get; init; }
        public DateTimeOffset? DeletedAt { get; init; }
    }

    private sealed class RegistryMetadataDbContext(DbContextOptions<RegistryMetadataDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<RetentionReadyRecord>(entity =>
            {
                entity.ToTable("retention_ready_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.Id).HasColumnName("record_id");
                entity.Property(record => record.TenantId).HasColumnName("tenant_uuid");
                entity.Property(record => record.RetainedAt).HasColumnName("retained_at_utc");
                entity.Property(record => record.EmailAddress).HasColumnName("email_address");
                entity.Property(record => record.IsDeleted).HasColumnName("is_deleted");
                entity.Property(record => record.DeletedAt).HasColumnName("deleted_at_utc");
            });
        }
    }

    private sealed class FactoryBackedRegistryMetadataDbContext(
        DbContextOptions<FactoryBackedRegistryMetadataDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<FactoryBackedRetentionReadyRecord>(entity =>
            {
                entity.ToTable("factory_backed_retention_ready_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.RetainedAt).HasColumnName("retained_at_utc");
                entity.Property(record => record.ExternalId).HasColumnName("external_identifier");
            });
        }
    }

    private sealed class ConflictingAnonymiseMetadataDbContext(
        DbContextOptions<ConflictingAnonymiseMetadataDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ConflictingAnonymiseMetadataRecord>(entity =>
            {
                entity.ToTable("conflicting_anonymise_metadata_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.RetainedAt).HasColumnName("retained_at_utc");
                entity.Property(record => record.ExternalId).HasColumnName("external_identifier");
            });
        }
    }

    private sealed class UnmappedDeletedAtDbContext(DbContextOptions<UnmappedDeletedAtDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SoftDeleteEntityWithUnmappedDeletedAt>(entity =>
            {
                entity.ToTable("retention_ready_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.RetainedAt).HasColumnName("retained_at_utc");
                entity.Property(record => record.IsDeleted).HasColumnName("is_deleted");
                entity.Ignore(record => record.DeletedAt);
            });
        }
    }

    private sealed class TestAnonymiseValueFactory : IAnonymiseValueFactory
    {
        public object? Create(AnonymiseValueContext context) => Guid.Empty;
    }
}
