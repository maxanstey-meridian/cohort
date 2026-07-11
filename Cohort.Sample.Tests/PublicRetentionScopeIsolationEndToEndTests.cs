using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests;

public sealed class PublicRetentionScopeIsolationEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task RetentionSweep_Does_Not_Allow_Caller_Tracked_Entity_To_Overwrite_SoftDelete()
    {
        var tenantId = Guid.NewGuid();
        var recordId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);

        await Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            var record = new SoftDeleteRecord
            {
                Id = recordId,
                TenantId = tenantId,
                CreatedAt = asOf.AddDays(-60),
                Body = "before sweep",
            };
            db.SoftDeleteRecords.Add(record);
            await db.SaveChangesAsync();

            var sweep = services.GetRequiredService<IRetentionSweep>();
            await sweep.SweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                asOf
            );

            record.IsDeleted.Should().BeFalse();
            record.DeletedAt.Should().BeNull();
            await db.Entry(record).ReloadAsync();
            record.Body = "caller update after sweep";
            await db.SaveChangesAsync();
        });

        await using var verify = Host.CreateDbContext();
        var persisted = await verify.SoftDeleteRecords.SingleAsync(record => record.Id == recordId);
        persisted.Body.Should().Be("caller update after sweep");
        persisted.IsDeleted.Should().BeTrue();
        persisted.DeletedAt.Should().Be(asOf);
    }

    [Fact]
    public async Task RetentionErasureService_Does_Not_Allow_Caller_Tracked_Entity_To_Overwrite_Anonymisation()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var contactId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);

        await Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            var contact = new AnonymisedContact
            {
                Id = contactId,
                TenantId = tenantId,
                SubjectId = subjectId,
                CreatedAt = asOf.AddDays(-60),
                EmailAddress = "person@example.com",
                GivenName = "Before",
                Surname = "Erasure",
                Notes = "before erasure",
            };
            db.AnonymisedContacts.Add(contact);
            await db.SaveChangesAsync();

            var erasure = services.GetRequiredService<IRetentionErasureService>();
            await erasure.EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
                asOf
            );

            contact.EmailAddress.Should().Be("person@example.com");
            contact.GivenName.Should().Be("Before");
            contact.Surname.Should().Be("Erasure");
            contact.AnonymisedAt.Should().BeNull();
            await db.Entry(contact).ReloadAsync();
            contact.Notes = "caller update after erasure";
            await db.SaveChangesAsync();
        });

        await using var verify = Host.CreateDbContext();
        var persisted = await verify.AnonymisedContacts.SingleAsync(contact => contact.Id == contactId);
        persisted.EmailAddress.Should().BeNull();
        persisted.GivenName.Should().BeEmpty();
        persisted.Surname.Should().Be("[redacted]");
        persisted.Notes.Should().Be("caller update after erasure");
        persisted.AnonymisedAt.Should().Be(asOf);
    }

    [Fact]
    public async Task RetentionPreview_Does_Not_Join_The_Callers_Uncommitted_DbContext_Transaction()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);

        await Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            await using var transaction = await db.Database.BeginTransactionAsync();
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = Guid.NewGuid(),
                    CreatedAt = asOf.AddDays(-120),
                    Body = "uncommitted",
                }
            );
            await db.SaveChangesAsync();

            var result = await services
                .GetRequiredService<IRetentionPreview>()
                .PreviewAsync(
                    new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                    asOf
                );

            result.Counts.Single(count => count.EntityType == typeof(Note)).Affected.Should().Be(0);
            await transaction.RollbackAsync();
        });
    }
}
