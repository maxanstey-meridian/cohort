using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

// Proves the CohortConventions priority chain: attribute > global config > built-in default.
// Uses InMemory EF because there's no SQL path under test — only reflection + convention
// resolution. No Postgres fixture required.

public sealed class CohortConventionsEndToEndTests
{
    [Fact]
    public void Global_Config_TenantPropertyName_Resolves_Unattributed_Property()
    {
        var options = new DbContextOptionsBuilder<OrganisationTenantDbContext>()
            .UseInMemoryDatabase($"conventions-tenant-{Guid.NewGuid()}")
            .Options;
        using var db = new OrganisationTenantDbContext(options);

        var builder = new RetentionEntryBuilder(new CohortConventions { TenantPropertyName = "OrganisationId" });
        var entry = new RetentionRegistry(db, builder).Scan()[typeof(OrganisationTenantRecord)];

        entry.Tenant.Should().NotBeNull();
        entry.Tenant!.TenantMember.Should().Be(nameof(OrganisationTenantRecord.OrganisationId));
        entry.Tenant.TenantColumn.Should().Be("OrganisationId");
    }

    [Fact]
    public void Attribute_Overrides_Global_Config_Tenant_Property()
    {
        var options = new DbContextOptionsBuilder<AttributeWinsDbContext>()
            .UseInMemoryDatabase($"conventions-attribute-wins-{Guid.NewGuid()}")
            .Options;
        using var db = new AttributeWinsDbContext(options);

        // Global config says "look for OrganisationId", but the attribute points at WorkspaceId.
        var builder = new RetentionEntryBuilder(new CohortConventions { TenantPropertyName = "OrganisationId" });
        var entry = new RetentionRegistry(db, builder).Scan()[typeof(AttributeOverrideRecord)];

        entry.Tenant.Should().NotBeNull();
        entry.Tenant!.TenantMember.Should().Be(nameof(AttributeOverrideRecord.WorkspaceId));
    }

    [Fact]
    public void Default_Convention_Resolves_TenantId_When_No_Attribute_And_No_Config_Override()
    {
        var options = new DbContextOptionsBuilder<DefaultTenantDbContext>()
            .UseInMemoryDatabase($"conventions-default-{Guid.NewGuid()}")
            .Options;
        using var db = new DefaultTenantDbContext(options);

        var builder = new RetentionEntryBuilder(new CohortConventions());
        var entry = new RetentionRegistry(db, builder).Scan()[typeof(DefaultTenantRecord)];

        entry.Tenant.Should().NotBeNull();
        entry.Tenant!.TenantMember.Should().Be(nameof(DefaultTenantRecord.TenantId));
    }

    [Fact]
    public void Global_Config_RecordIdPropertyName_Resolves_Unattributed_Property()
    {
        var options = new DbContextOptionsBuilder<OrganisationIdDbContext>()
            .UseInMemoryDatabase($"conventions-record-id-{Guid.NewGuid()}")
            .Options;
        using var db = new OrganisationIdDbContext(options);

        var builder = new RetentionEntryBuilder(new CohortConventions
        {
            RecordIdPropertyName = "RecordKey",
            TenantPropertyName = "OrganisationId",
        });
        var entry = new RetentionRegistry(db, builder).Scan()[typeof(OrganisationScopedRecord)];

        entry.RecordId.RecordIdMember.Should().Be(nameof(OrganisationScopedRecord.RecordKey));
    }

    [Retain("conventions", nameof(RetainedAt))]
    private sealed class OrganisationTenantRecord
    {
        public Guid Id { get; init; }
        public Guid OrganisationId { get; init; }
        public DateTimeOffset RetainedAt { get; init; }
    }

    [Retain("conventions", nameof(RetainedAt))]
    private sealed class OrganisationScopedRecord
    {
        public Guid RecordKey { get; init; }
        public Guid OrganisationId { get; init; }
        public DateTimeOffset RetainedAt { get; init; }
    }

    [Retain("conventions", nameof(RetainedAt))]
    private sealed class AttributeOverrideRecord
    {
        public Guid Id { get; init; }
        public Guid OrganisationId { get; init; }

        [RetentionTenant]
        public Guid WorkspaceId { get; init; }

        public DateTimeOffset RetainedAt { get; init; }
    }

    [Retain("conventions", nameof(RetainedAt))]
    private sealed class DefaultTenantRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset RetainedAt { get; init; }
    }

    private sealed class OrganisationTenantDbContext(DbContextOptions<OrganisationTenantDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<OrganisationTenantRecord>(entity =>
            {
                entity.ToTable("organisation_tenant_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.OrganisationId);
                entity.Property(record => record.RetainedAt);
            });
        }
    }

    private sealed class OrganisationIdDbContext(DbContextOptions<OrganisationIdDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<OrganisationScopedRecord>(entity =>
            {
                entity.ToTable("organisation_scoped_records");
                entity.HasKey(record => record.RecordKey);
                entity.Property(record => record.OrganisationId);
                entity.Property(record => record.RetainedAt);
            });
        }
    }

    private sealed class AttributeWinsDbContext(DbContextOptions<AttributeWinsDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<AttributeOverrideRecord>(entity =>
            {
                entity.ToTable("attribute_override_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.OrganisationId);
                entity.Property(record => record.WorkspaceId);
                entity.Property(record => record.RetainedAt);
            });
        }
    }

    private sealed class DefaultTenantDbContext(DbContextOptions<DefaultTenantDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<DefaultTenantRecord>(entity =>
            {
                entity.ToTable("default_tenant_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.TenantId);
                entity.Property(record => record.RetainedAt);
            });
        }
    }
}
