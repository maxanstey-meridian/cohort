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
    public DbSet<ErasureSubjectRecord> ErasureSubjectRecords => Set<ErasureSubjectRecord>();
    public DbSet<HeldRecord> HeldRecords => Set<HeldRecord>();
    public DbSet<TenantlessLog> TenantlessLogs => Set<TenantlessLog>();
    public DbSet<TenantlessSoftDelete> TenantlessSoftDeletes => Set<TenantlessSoftDelete>();
    public DbSet<PerRowAuditedLog> PerRowAuditedLogs => Set<PerRowAuditedLog>();
    public DbSet<TombstoneRecord> TombstoneRecords => Set<TombstoneRecord>();
    public DbSet<BlobBackedFile> BlobBackedFiles => Set<BlobBackedFile>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Note>(b =>
        {
            b.ToTable("notes");
            b.HasKey(n => n.Id);
            b.Property(n => n.TenantId);
            b.Property(n => n.SubjectId);
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
            b.Property(record => record.SubjectId);
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
            b.Property(contact => contact.SubjectId);
            b.Property(contact => contact.CreatedAt).IsRequired();
            b.Property(contact => contact.EmailAddress);
            b.Property(contact => contact.GivenName).IsRequired();
            b.Property(contact => contact.Surname).IsRequired();
            b.Property(contact => contact.Notes).IsRequired();
        });

        modelBuilder.Entity<ErasureSubjectRecord>(b =>
        {
            b.ToTable("erasure_subject_records");
            b.HasKey(record => record.Id);
            b.Property(record => record.TenantId).IsRequired();
            b.Property(record => record.SubjectId);
            b.Property(record => record.CreatedAt).IsRequired();
            b.Property(record => record.Body).IsRequired();
        });

        modelBuilder.Entity<TenantlessLog>(b =>
        {
            b.ToTable("tenantless_logs");
            b.HasKey(log => log.Id);
            b.Property(log => log.CreatedAt).IsRequired();
            b.Property(log => log.Payload).IsRequired();
        });

        modelBuilder.Entity<TenantlessSoftDelete>(b =>
        {
            b.ToTable("tenantless_soft_deletes");
            b.HasKey(record => record.Id);
            b.Property(record => record.CreatedAt).IsRequired();
            b.Property(record => record.Payload).IsRequired();
            b.Property(record => record.IsDeleted).IsRequired();
            b.Property(record => record.DeletedAt);
        });

        modelBuilder.Entity<PerRowAuditedLog>(b =>
        {
            b.ToTable("per_row_audited_logs");
            b.HasKey(log => log.Id);
            b.Property(log => log.TenantId).IsRequired();
            b.Property(log => log.CreatedAt).IsRequired();
            b.Property(log => log.Payload).IsRequired();
        });

        modelBuilder.Entity<TombstoneRecord>(b =>
        {
            b.ToTable("tombstone_records");
            b.HasKey(record => record.Id);
            b.Property(record => record.TenantId).IsRequired();
            b.Property(record => record.SubjectId);
            b.Property(record => record.CreatedAt).IsRequired();
            b.Property(record => record.ExternalId).IsRequired();
            b.Property(record => record.DisplayName).IsRequired();
            b.Property(record => record.ContactEmail);
            b.Property(record => record.Notes).IsRequired();
        });

        modelBuilder.Entity<BlobBackedFile>(b =>
        {
            b.ToTable("blob_backed_files");
            b.HasKey(file => file.Id);
            b.Property(file => file.TenantId).IsRequired();
            b.Property(file => file.CreatedAt).IsRequired();
            b.Property(file => file.StoragePath).IsRequired();
            b.Property(file => file.OriginalFileName).IsRequired();
            b.Property(file => file.ContentType).IsRequired();
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
