using Cohort.Application;
using Cohort.Domain;
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

        var entries = new RetentionRegistry(db).Scan();

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
        entries.Should().HaveCount(3);
    }

    [Fact]
    public void Scan_Derives_Anchor_And_Anonymise_Columns_From_Ef_Model_Metadata()
    {
        var options = new DbContextOptionsBuilder<RegistryMetadataDbContext>()
            .UseInMemoryDatabase($"registry-metadata-{Guid.NewGuid()}")
            .Options;
        using var db = new RegistryMetadataDbContext(options);

        var entry = new RetentionRegistry(db).Scan()[typeof(RetentionReadyRecord)];

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
            new AnonymiseField(
                nameof(RetentionReadyRecord.EmailAddress),
                "email_address",
                AnonymiseMethod.FixedLiteral,
                "[redacted]"
            )
        );
    }

    [Fact]
    public void Scan_Captures_Soft_Delete_Convention_Metadata()
    {
        var options = new DbContextOptionsBuilder<RegistryMetadataDbContext>()
            .UseInMemoryDatabase($"registry-soft-delete-{Guid.NewGuid()}")
            .Options;
        using var db = new RegistryMetadataDbContext(options);

        var entry = new RetentionRegistry(db).Scan()[typeof(RetentionReadyRecord)];

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

        var act = () => new RetentionRegistry(db).Scan();

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
        var registry = new RetentionRegistry(db);

        var firstScan = registry.Scan();
        var secondScan = registry.Scan();

        secondScan.Should().BeSameAs(firstScan);
        secondScan.Should().ContainKey(typeof(RetentionReadyRecord));
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
}
