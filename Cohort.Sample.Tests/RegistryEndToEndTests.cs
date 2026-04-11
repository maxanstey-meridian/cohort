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
    public async Task Startup_Path_Validates_And_Returns_Only_Retained_Entities_Against_Real_Postgres()
    {
        // Arrange — seed retained and exempt rows through the real DbContext
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = Guid.NewGuid(),
                    CreatedAt = DateTimeOffset.UtcNow.AddDays(-10),
                    Body = "one",
                }
            );
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = Guid.NewGuid(),
                    CreatedAt = DateTimeOffset.UtcNow.AddDays(-1),
                    Body = "two",
                }
            );
            db.ExemptDocuments.Add(
                new ExemptDocument
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = DateTimeOffset.UtcNow.AddDays(-30),
                    Title = "archived",
                }
            );
            await db.SaveChangesAsync();
        }

        // Act — run the real startup path against the same real DbContext
        IReadOnlyDictionary<Type, RetentionEntry> entries;
        entries = await Host.RunStartupAsync();

        // Assert — the retained sample entity is present and the exempt sample
        // fixture is ignored by the registry surface.
        entries
            .Should()
            .ContainSingle(kvp =>
                kvp.Key == typeof(Note)
                && kvp.Value.Category == "short-lived"
                && kvp.Value.AnchorMember == nameof(Note.CreatedAt)
                && kvp.Value.Tenant != null
                && kvp.Value.EntityType == typeof(Note)
            );
        entries.Values.Should().NotContain(e => e.Category == "long-lived");
        entries.Should().NotContainKey(typeof(ExemptDocument));

        // Sanity — the sample host model contains the exempt entity and the row
        // we seeded actually landed in Postgres. Catches the "registry excludes
        // it because the host forgot to map it" failure mode.
        await using var verify = Host.CreateDbContext();
        verify.Model.FindEntityType(typeof(ExemptDocument)).Should().NotBeNull();
        var count = await verify.Notes.CountAsync();
        count.Should().Be(2);
        var exemptCount = await verify.ExemptDocuments.CountAsync();
        exemptCount.Should().Be(1);
    }
}
