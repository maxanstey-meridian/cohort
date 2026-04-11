using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample;

public sealed class SampleDbContext(DbContextOptions<SampleDbContext> options) : DbContext(options)
{
    public DbSet<Note> Notes => Set<Note>();
    public DbSet<ExemptDocument> ExemptDocuments => Set<ExemptDocument>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Note>(b =>
        {
            b.ToTable("notes");
            b.HasKey(n => n.Id);
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
    }
}
