using Cohort.Application;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

// ─── EXEMPLAR #2 — narrow integration test ──────────────────────────────────
//
// Pattern: narrow integration (the middle ground). Use ONLY when the code under
// test crosses one boundary that EF Core's InMemory provider fully serves
// (reflection over `Model.GetEntityTypes()` and friends, no SQL).
//
// Appropriate when ALL of:
//   (a) the thing under test has no SQL path
//   (b) InMemory's metadata story is enough
//
// If there is ANY SQL involved → skip this pattern, write EXEMPLAR #3.
// If in doubt → don't, write EXEMPLAR #3.
//
// This file lives in Cohort.Sample.Tests (not Cohort.Tests) because it
// references SampleDbContext. The project-reference graph is the architectural
// guardrail — `Cohort.Tests` cannot accidentally drift into integration land.
// ────────────────────────────────────────────────────────────────────────────

public sealed class RegistryScanTests
{
    [Fact]
    public void Scan_Reads_Retain_Attribute_From_Sample_DbContext_Model()
    {
        var options = new DbContextOptionsBuilder<SampleDbContext>()
            .UseInMemoryDatabase($"registry-scan-{Guid.NewGuid()}")
            .Options;
        using var db = new SampleDbContext(options);

        var entries = new RetentionRegistry(db).Scan();

        // Positive — the one annotated entity is found, with the right shape
        entries
            .Should()
            .ContainSingle(e =>
                e.Category == "short-lived"
                && e.AnchorMember == nameof(Note.CreatedAt)
                && e.EntityType == typeof(Note)
            );

        // Negative — nothing else sneaks in
        entries.Should().NotContain(e => e.Category == "long-lived");
        entries.Should().HaveCount(1);
    }
}
