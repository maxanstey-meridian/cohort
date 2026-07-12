using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests.ComplianceCorpus;

public sealed class HoldsCorpusTests(PostgresFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Active_holds_block_mutation_while_expired_holds_do_not()
    {
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var activeId = Guid.NewGuid();
        var expiredId = Guid.NewGuid();
        var removedId = Guid.NewGuid();
        var softDeleteId = Guid.NewGuid();
        var unheldSoftDeleteId = Guid.NewGuid();
        var contactId = Guid.NewGuid();
        var unheldContactId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = activeId, TenantId = tenantId, CreatedAt = now.AddDays(-60), Body = "active hold" },
                new Note { Id = expiredId, TenantId = tenantId, CreatedAt = now.AddDays(-60), Body = "expired hold" },
                new Note { Id = removedId, TenantId = tenantId, CreatedAt = now.AddDays(-60), Body = "removed hold" }
            );
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord { Id = softDeleteId, TenantId = tenantId, CreatedAt = now.AddDays(-60), Body = "held soft delete" },
                new SoftDeleteRecord { Id = unheldSoftDeleteId, TenantId = tenantId, CreatedAt = now.AddDays(-60), Body = "unheld soft delete" }
            );
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact { Id = contactId, TenantId = tenantId, CreatedAt = now.AddDays(-60), EmailAddress = "held@example.org", GivenName = "Held", Surname = "Contact", Notes = "held anonymisation" },
                new AnonymisedContact { Id = unheldContactId, TenantId = tenantId, CreatedAt = now.AddDays(-60), EmailAddress = "unheld@example.org", GivenName = "Unheld", Surname = "Contact", Notes = "positive control" }
            );
            await db.SaveChangesAsync();
        }

        var removedHoldId = Guid.NewGuid();
        await Host.RunWithServicesAsync(async services =>
        {
            var holds = services.GetRequiredService<IRetentionHoldsRepository>();
            await holds.CreateAsync(
                new RetentionHoldRequest(Guid.NewGuid(), Note.RetentionIdentity, activeId.ToString(), tenantId, "case-specific restriction", now.AddDays(-1)),
                CancellationToken.None
            );
            await holds.CreateAsync(
                new RetentionHoldRequest(removedHoldId, Note.RetentionIdentity, removedId.ToString(), tenantId, "removed restriction", now.AddDays(-1)),
                CancellationToken.None
            );
            await holds.RemoveAsync(removedHoldId, now.AddHours(-1), CancellationToken.None);
            await holds.CreateAsync(
                new RetentionHoldRequest(Guid.NewGuid(), Note.RetentionIdentity, expiredId.ToString(), tenantId, "expired restriction", now.AddDays(-2), now.AddDays(-1)),
                CancellationToken.None
            );
            await holds.CreateAsync(
                new RetentionHoldRequest(Guid.NewGuid(), Guid.Parse("6107ff39-bf33-413c-889e-6347c909ba15"), softDeleteId.ToString(), tenantId, "soft-delete restriction", now.AddDays(-1)),
                CancellationToken.None
            );
            await holds.CreateAsync(
                new RetentionHoldRequest(Guid.NewGuid(), Guid.Parse("fd4a533e-e6a9-44ea-948e-cbf881f35e57"), contactId.ToString(), tenantId, "anonymisation restriction", now.AddDays(-1)),
                CancellationToken.None
            );
        });

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now
        );

        result.Counts.Should().Contain(count =>
            count.EntityType == typeof(Note) && count.Affected == 2 && count.HeldCount == 1
        );
        result.Counts.Should().Contain(count =>
            count.EntityType == typeof(SoftDeleteRecord) && count.Affected == 1 && count.HeldCount == 1
        );
        result.Counts.Should().Contain(count =>
            count.EntityType == typeof(AnonymisedContact) && count.Affected == 1 && count.HeldCount == 1
        );
        await using var verify = Host.CreateDbContext();
        var heldNote = await verify.Notes.SingleAsync(note => note.Id == activeId);
        heldNote.TenantId.Should().Be(tenantId);
        heldNote.CreatedAt.Should().Be(now.AddDays(-60));
        heldNote.Body.Should().Be("active hold");
        (await verify.Notes.AnyAsync(note => note.Id == expiredId)).Should().BeFalse();
        (await verify.Notes.AnyAsync(note => note.Id == removedId)).Should().BeFalse();
        var heldSoftDelete = await verify.SoftDeleteRecords.SingleAsync(row => row.Id == softDeleteId);
        heldSoftDelete.Body.Should().Be("held soft delete");
        heldSoftDelete.CreatedAt.Should().Be(now.AddDays(-60));
        heldSoftDelete.TenantId.Should().Be(tenantId);
        heldSoftDelete.IsDeleted.Should().BeFalse();
        heldSoftDelete.DeletedAt.Should().BeNull();
        (await verify.SoftDeleteRecords.SingleAsync(row => row.Id == unheldSoftDeleteId)).IsDeleted.Should().BeTrue();
        var contact = await verify.AnonymisedContacts.SingleAsync(row => row.Id == contactId);
        contact.EmailAddress.Should().Be("held@example.org");
        contact.GivenName.Should().Be("Held");
        contact.Surname.Should().Be("Contact");
        contact.Notes.Should().Be("held anonymisation");
        contact.CreatedAt.Should().Be(now.AddDays(-60));
        contact.TenantId.Should().Be(tenantId);
        contact.AnonymisedAt.Should().BeNull();
        var unheldContact = await verify.AnonymisedContacts.SingleAsync(row => row.Id == unheldContactId);
        unheldContact.EmailAddress.Should().BeNull();
        unheldContact.AnonymisedAt.Should().Be(now);

        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT "EntityType", "Affected", "HeldCount"
            FROM "sweep_run_entity_summary"
            WHERE "SweepId" = @sweepId AND "HeldCount" > 0
            ORDER BY "EntityType"
            """;
        command.Parameters.AddWithValue("sweepId", result.SweepId);
        await using var reader = await command.ExecuteReaderAsync();
        var summaries = new List<(string EntityType, long Affected, long Held)>();
        while (await reader.ReadAsync())
        {
            summaries.Add((reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2)));
        }
        summaries.Should().BeEquivalentTo(
        [
            (typeof(Note).FullName!, 2L, 1L),
            (typeof(SoftDeleteRecord).FullName!, 1L, 1L),
            (typeof(AnonymisedContact).FullName!, 1L, 1L),
        ]);

        await AssertSubjectErasureHoldAndControlAsync();
    }

    [Fact]
    public async Task Hold_creation_rejects_a_nonexistent_canonical_record_id()
    {
        var holdId = Guid.NewGuid();
        var recordId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();

        var act = () => Host.RunWithServicesAsync(async services =>
        {
            var holds = services.GetRequiredService<IRetentionHoldsRepository>();
            await holds.CreateAsync(
                new RetentionHoldRequest(
                    holdId,
                    Note.RetentionIdentity,
                    recordId.ToString("D"),
                    tenantId,
                    "target existence check",
                    DateTimeOffset.UtcNow
                ),
                CancellationToken.None
            );
        });

        await act.Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage($"*record '{recordId:D}' does not exist*");

        await using var verify = Host.CreateDbContext();
        (await verify.HeldRecords.AnyAsync(hold => hold.HoldId == holdId)).Should().BeFalse();
    }

    private async Task AssertSubjectErasureHoldAndControlAsync()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var heldId = Guid.NewGuid();
        var unheldId = Guid.NewGuid();
        var holdId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = heldId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = now.AddDays(-1),
                    Body = "held subject erasure",
                },
                new Note
                {
                    Id = unheldId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = now.AddDays(-1),
                    Body = "unheld subject erasure",
                }
            );
            await db.SaveChangesAsync();
        }

        await Host.RunWithServicesAsync(async services =>
        {
            await services.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                new RetentionHoldRequest(
                    holdId,
                    Note.RetentionIdentity,
                    heldId.ToString(),
                    tenantId,
                    "subject erasure restriction",
                    now.AddDays(-1)
                ),
                CancellationToken.None
            );
        });

        var result = await Host.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            now
        );

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Sum(count => count.Affected).Should().Be(1);
        result.Counts.Sum(count => count.HeldCount).Should().Be(1);
        result.Counts.Should().ContainSingle(count =>
            count.EntityType == typeof(Note)
            && count.Affected == 1
            && count.HeldCount == 1
            && count.SkippedCount == 0
            && count.NullAnchorCount == 0
        );

        await using var verify = Host.CreateDbContext();
        var held = await verify.Notes.SingleAsync(note => note.Id == heldId);
        (held.Id, held.TenantId, held.SubjectId, held.CreatedAt, held.Body).Should().Be(
            (heldId, tenantId, subjectId, now.AddDays(-1), "held subject erasure")
        );
        (await verify.Notes.AnyAsync(note => note.Id == unheldId)).Should().BeFalse();
        var hold = await verify.HeldRecords.SingleAsync(row => row.HoldId == holdId);
        hold.RetentionEntityId.Should().Be(Note.RetentionIdentity);
        hold.RecordId.Should().Be(heldId.ToString());
        hold.TenantId.Should().Be(tenantId);
        hold.Reason.Should().Be("subject erasure restriction");
        hold.CreatedAt.Should().Be(now.AddDays(-1));
        hold.ExpiresAt.Should().BeNull();
        hold.RemovedAt.Should().BeNull();
    }
}
