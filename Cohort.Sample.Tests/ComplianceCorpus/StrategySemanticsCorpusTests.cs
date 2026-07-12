using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests.ComplianceCorpus;

public sealed class StrategySemanticsCorpusTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Public_sweep_applies_each_strategy_only_to_eligible_rows()
    {
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var boundaryNoteId = Guid.NewGuid();
        var freshNoteId = Guid.NewGuid();
        var softDeleteId = Guid.NewGuid();
        var boundarySoftDeleteId = Guid.NewGuid();
        var freshSoftDeleteId = Guid.NewGuid();
        var alreadyDeletedId = Guid.NewGuid();
        var contactId = Guid.NewGuid();
        var boundaryContactId = Guid.NewGuid();
        var freshContactId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                CreateNote(noteId, tenantId, now.AddDays(-60), "eligible purge"),
                CreateNote(boundaryNoteId, tenantId, now.AddDays(-30), "boundary purge"),
                CreateNote(freshNoteId, tenantId, now.AddDays(-29), "fresh purge")
            );
            db.SoftDeleteRecords.AddRange(
                CreateSoftDelete(softDeleteId, tenantId, now.AddDays(-60), "eligible soft delete"),
                CreateSoftDelete(boundarySoftDeleteId, tenantId, now.AddDays(-30), "boundary soft delete"),
                CreateSoftDelete(freshSoftDeleteId, tenantId, now.AddDays(-29), "fresh soft delete"),
                new SoftDeleteRecord
                {
                    Id = alreadyDeletedId,
                    TenantId = tenantId,
                    CreatedAt = now.AddDays(-60),
                    Body = "already deleted",
                    IsDeleted = true,
                    DeletedAt = now.AddDays(-10),
                }
            );
            db.AnonymisedContacts.AddRange(
                CreateContact(contactId, tenantId, now.AddDays(-60), "eligible@example.org"),
                CreateContact(boundaryContactId, tenantId, now.AddDays(-30), "boundary@example.org"),
                CreateContact(freshContactId, tenantId, now.AddDays(-29), "fresh@example.org")
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now
        );

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Single(count => count.EntityType == typeof(Note)).Affected.Should().Be(1);
        result.Counts.Single(count => count.EntityType == typeof(SoftDeleteRecord)).Affected.Should().Be(1);
        result.Counts.Single(count => count.EntityType == typeof(AnonymisedContact)).Affected.Should().Be(1);
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeFalse();
        (await verify.Notes.AnyAsync(note => note.Id == boundaryNoteId)).Should().BeTrue();
        (await verify.Notes.AnyAsync(note => note.Id == freshNoteId)).Should().BeTrue();
        var deleted = await verify.SoftDeleteRecords.SingleAsync(row => row.Id == softDeleteId);
        deleted.IsDeleted.Should().BeTrue();
        deleted.DeletedAt.Should().Be(now);
        deleted.Body.Should().Be("eligible soft delete");
        deleted.CreatedAt.Should().Be(now.AddDays(-60));
        deleted.TenantId.Should().Be(tenantId);
        var alreadyDeleted = await verify.SoftDeleteRecords.SingleAsync(row => row.Id == alreadyDeletedId);
        alreadyDeleted.Body.Should().Be("already deleted");
        alreadyDeleted.IsDeleted.Should().BeTrue();
        alreadyDeleted.DeletedAt.Should().Be(now.AddDays(-10));
        var protectedSoftDeletes = await verify.SoftDeleteRecords
            .Where(row => row.Id == boundarySoftDeleteId || row.Id == freshSoftDeleteId)
            .ToListAsync();
        protectedSoftDeletes.Select(row => row.Id).Should().BeEquivalentTo([boundarySoftDeleteId, freshSoftDeleteId]);
        protectedSoftDeletes.Should().OnlyContain(row => !row.IsDeleted && row.DeletedAt == null);
        var anonymised = await verify.AnonymisedContacts.SingleAsync(row => row.Id == contactId);
        anonymised.EmailAddress.Should().BeNull();
        anonymised.GivenName.Should().BeEmpty();
        anonymised.Surname.Should().Be("[redacted]");
        anonymised.Notes.Should().Be("preserve");
        anonymised.AnonymisedAt.Should().Be(now);
        var protectedContacts = await verify.AnonymisedContacts
            .Where(row => row.Id == boundaryContactId || row.Id == freshContactId)
            .ToListAsync();
        protectedContacts.Select(row => row.Id).Should().BeEquivalentTo([boundaryContactId, freshContactId]);
        protectedContacts.Should().OnlyContain(row =>
            row.AnonymisedAt == null
            && row.EmailAddress != null
            && row.GivenName == "Person"
            && row.Surname == "Name"
            && row.Notes == "preserve"
        );

        var exemptId = Guid.NewGuid();
        await using (var seedExempt = Host.CreateDbContext())
        {
            seedExempt.Notes.Add(new Note
            {
                Id = exemptId,
                TenantId = tenantId,
                CreatedAt = now.AddYears(-1),
                Body = "runtime exempt",
            });
            await seedExempt.SaveChangesAsync();
        }

        using var exemptHost = new CohortTestHost(ConnectionString, new ExemptCategoryRepository());
        var exemptResult = await exemptHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now
        );
        exemptResult.Counts.Should().Contain(count =>
            count.EntityType == typeof(Note)
            && count.Strategy == Strategy.Exempt
            && count.Affected == 0
        );
        await using var verifyExempt = Host.CreateDbContext();
        (await verifyExempt.Notes.AnyAsync(note => note.Id == exemptId)).Should().BeTrue();
    }

    private static Note CreateNote(
        Guid id,
        Guid tenantId,
        DateTimeOffset createdAt,
        string body
    ) => new() { Id = id, TenantId = tenantId, CreatedAt = createdAt, Body = body };

    private static SoftDeleteRecord CreateSoftDelete(
        Guid id,
        Guid tenantId,
        DateTimeOffset createdAt,
        string body
    ) => new() { Id = id, TenantId = tenantId, CreatedAt = createdAt, Body = body };

    private static AnonymisedContact CreateContact(
        Guid id,
        Guid tenantId,
        DateTimeOffset createdAt,
        string email
    ) => new()
    {
        Id = id,
        TenantId = tenantId,
        CreatedAt = createdAt,
        EmailAddress = email,
        GivenName = "Person",
        Surname = "Name",
        Notes = "preserve",
    };

    private sealed class ExemptCategoryRepository : ITestRetentionRuleProvider
    {
        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct)
        {
            return Task.FromResult<ITestRetentionRule?>(
                new StaticTestRetentionRule(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Exempt)
                )
            );
        }
    }
}
