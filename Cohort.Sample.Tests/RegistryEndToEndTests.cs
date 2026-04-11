using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

// ─── EXEMPLAR #3 — end-to-end test ──────────────────────────────────────────
//
// Pattern: end-to-end test. THIS IS THE PATTERN.
//
// Feed real data in the front. Run the real code path. Assert what comes out
// the back. Use this whenever the code under test touches a port (DbContext,
// IOptions with real config binding, IHostedService, file/HTTP I/O).
//
// Copy this file. Rename it. Edit the seed and assertions.
//
// Do NOT abstract.
// Do NOT share a base class beyond IntegrationTestBase.
// Do NOT add mocks — NSubstitute is intentionally absent from this project.
//
// When you add a new port `IFoo`, the same PR adds an end-to-end test here that
// exercises the REAL implementation against PostgresFixture. Non-negotiable.
// See CLAUDE.md.
// ────────────────────────────────────────────────────────────────────────────

public sealed class RegistryEndToEndTests(PostgresFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Registry_Scan_Finds_Annotated_Entity_Against_Real_Postgres()
    {
        // Arrange — seed two real rows through the real DbContext
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = DateTimeOffset.UtcNow.AddDays(-10),
                    Body = "one",
                }
            );
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = DateTimeOffset.UtcNow.AddDays(-1),
                    Body = "two",
                }
            );
            await db.SaveChangesAsync();
        }

        // Act — run the registry against the same real DbContext
        IReadOnlyDictionary<Type, RetentionEntry> entries;
        await using (var db = Host.CreateDbContext())
        {
            entries = new RetentionRegistry(db).Scan();
        }

        // Assert — positive AND negative
        entries
            .Should()
            .ContainSingle(kvp =>
                kvp.Key == typeof(Note)
                && kvp.Value.Category == "short-lived"
                && kvp.Value.AnchorMember == nameof(Note.CreatedAt)
                && kvp.Value.EntityType == typeof(Note)
            );
        entries.Values.Should().NotContain(e => e.Category == "long-lived");

        // Sanity — the rows we seeded actually landed in Postgres, not just in
        // the EF model. Catches the "test passed but the writes were silently
        // dropped" failure mode.
        await using var verify = Host.CreateDbContext();
        var count = await verify.Notes.CountAsync();
        count.Should().Be(2);
    }
}
