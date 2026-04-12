using Cohort.Sample.Entities;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample;

public sealed class SampleDbContext(DbContextOptions<SampleDbContext> options) : DbContext(options)
{
    public DbSet<Note> Notes => Set<Note>();
    public DbSet<ExemptDocument> ExemptDocuments => Set<ExemptDocument>();
    public DbSet<SoftDeleteRecord> SoftDeleteRecords => Set<SoftDeleteRecord>();
    public DbSet<AnonymisedContact> AnonymisedContacts => Set<AnonymisedContact>();
    public DbSet<HeldRecord> HeldRecords => Set<HeldRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Note>(b =>
        {
            b.ToTable("notes");
            b.HasKey(n => n.Id);
            b.Property(n => n.TenantId);
            b.Property(n => n.CreatedAt).IsRequired();
            b.Property(n => n.Body).IsRequired();
        });

        modelBuilder.Entity<ExemptDocument>(b =>
        {
            b.ToTable("exempt_documents");
            b.HasKey(document => document.Id);
            b.Property(document => document.CreatedAt).IsRequired();
            b.Property(document => document.Title).IsRequired();
        });

        modelBuilder.Entity<SoftDeleteRecord>(b =>
        {
            b.ToTable("soft_delete_records");
            b.HasKey(record => record.Id);
            b.Property(record => record.TenantId);
            b.Property(record => record.CreatedAt).IsRequired();
            b.Property(record => record.Body).IsRequired();
            b.Property(record => record.IsDeleted).IsRequired();
            b.Property(record => record.DeletedAt);
        });

        modelBuilder.Entity<AnonymisedContact>(b =>
        {
            b.ToTable("anonymised_contacts");
            b.HasKey(contact => contact.Id);
            b.Property(contact => contact.TenantId);
            b.Property(contact => contact.CreatedAt).IsRequired();
            b.Property(contact => contact.EmailAddress);
            b.Property(contact => contact.GivenName).IsRequired();
            b.Property(contact => contact.Surname).IsRequired();
            b.Property(contact => contact.Notes).IsRequired();
        });

        modelBuilder.Entity<HeldRecord>(b =>
        {
            b.ToTable("retention_holds");
            b.HasKey(record => record.HoldId);
            b.Property(record => record.TableName).IsRequired();
            b.Property(record => record.RecordId).IsRequired();
            b.Property(record => record.TenantId).IsRequired();
            b.Property(record => record.Reason).IsRequired();
            b.Property(record => record.CreatedAt).IsRequired();
            b.Property(record => record.ExpiresAt);
            b.Property(record => record.RemovedAt);
            b.HasIndex(record => new { record.TableName, record.TenantId, record.RecordId });
        });

        modelBuilder.ConfigureCohortTables();
    }
}
