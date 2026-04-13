using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests;

// Proves a single sweep processes multiple retained entities independently — holding
// one entity type does not prevent the other from being processed, and the audit run
// records both summaries. The existing AuditWriterEndToEndTests covers multi-entity
// sweeps happily processing, but no test has verified that a *hold* on entity A still
// allows entity B to be swept to completion.

public sealed class MultiEntitySweepEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Sweep_Path_Processes_Unheld_Entity_Even_When_Other_Entity_Is_Fully_Held()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var heldNoteId = Guid.NewGuid();
        var unheldSoftDeleteId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = heldNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "held-note-must-survive",
                }
            );
            db.SoftDeleteRecords.Add(
                new SoftDeleteRecord
                {
                    Id = unheldSoftDeleteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "unheld-record-must-be-soft-deleted",
                    IsDeleted = false,
                }
            );
            await db.SaveChangesAsync();
        }

        await Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            await repository.CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    "notes",
                    heldNoteId.ToString(),
                    tenantId,
                    "multi-entity hold",
                    asOf.AddDays(-10)
                ),
                CancellationToken.None
            );
        });

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 0),
            because: "the only expired Note is held and must not be purged"
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(SoftDeleteRecord), "soft-delete", tenantId, Strategy.SoftDelete, 1),
            because: "the Note hold must not cascade to the SoftDeleteRecord sweep"
        );

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.Select(note => note.Body).ToListAsync())
            .Should().Equal("held-note-must-survive");

        var records = await verify.SoftDeleteRecords.OrderBy(record => record.Body).ToListAsync();
        records.Should().ContainSingle();
        records[0].IsDeleted.Should().BeTrue();
        records[0].Id.Should().Be(unheldSoftDeleteId);
    }
}
