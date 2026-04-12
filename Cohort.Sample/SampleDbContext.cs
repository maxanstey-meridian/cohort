using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample;

public sealed class SampleDbContext(DbContextOptions<SampleDbContext> options) : DbContext(options)
{
    public DbSet<Note> Notes => Set<Note>();
    public DbSet<ExemptDocument> ExemptDocuments => Set<ExemptDocument>();
    public DbSet<SoftDeleteRecord> SoftDeleteRecords => Set<SoftDeleteRecord>();

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
    }
}
