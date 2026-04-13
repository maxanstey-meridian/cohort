using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

// Mirrors AnonymiseSweepEndToEndTests.Sweep_Path_Does_Not_Anonymise_Rows_Exactly_On_The_Cutoff for the
// other two strategies. Proves each strategy's cutoff SQL matches anonymise's semantics: rows inside
// the retention period and rows exactly on the cutoff survive; only rows strictly older than the
// cutoff are processed.

public sealed class BoundaryPrecisionEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Purge_Sweep_Does_Not_Delete_Rows_Inside_Or_Exactly_On_The_Cutoff()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-29),
                    Body = "purge-inside-retention",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-30),
                    Body = "purge-exact-cutoff",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-31),
                    Body = "purge-outside-retention",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(Note),
                "short-lived",
                tenantId,
                Strategy.Purge,
                1
            )
        );

        await using var verify = Host.CreateDbContext();
        var remaining = await verify.Notes
            .OrderBy(note => note.Body)
            .Select(note => note.Body)
            .ToListAsync();
        remaining.Should().Equal("purge-exact-cutoff", "purge-inside-retention");
    }

    [Fact]
    public async Task Purge_Sweep_Is_Idempotent_And_Second_Run_Reports_Zero_Affected()
    {
        // Anonymise + SoftDelete have idempotency tests; Purge previously relied on implicit
        // "deleted rows can't be re-deleted." This test proves a Purge sweep against an already-
        // empty target produces 0 affected, does not throw, and writes a clean audit record.
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "purge-idempotent-target",
                }
            );
            await db.SaveChangesAsync();
        }

        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());

        var first = await Host.RunSweepAsync(tenant, asOf);
        var second = await Host.RunSweepAsync(tenant, asOf);

        first.Counts.Should().Contain(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
        );
        second.Counts.Should().Contain(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 0)
        );
        second.SweepId.Should().NotBe(first.SweepId);

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.CountAsync()).Should().Be(0);
    }

    [Fact]
    public async Task SoftDelete_Sweep_Does_Not_Flag_Rows_Inside_Or_Exactly_On_The_Cutoff()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-29),
                    Body = "softdelete-inside-retention",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-30),
                    Body = "softdelete-exact-cutoff",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-31),
                    Body = "softdelete-outside-retention",
                    IsDeleted = false,
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(SoftDeleteRecord),
                "soft-delete",
                tenantId,
                Strategy.SoftDelete,
                1
            )
        );

        await using var verify = Host.CreateDbContext();
        var records = await verify.SoftDeleteRecords
            .OrderBy(record => record.Body)
            .Select(record => new { record.Body, record.IsDeleted })
            .ToListAsync();

        records.Should().Equal(
            new { Body = "softdelete-exact-cutoff", IsDeleted = false },
            new { Body = "softdelete-inside-retention", IsDeleted = false },
            new { Body = "softdelete-outside-retention", IsDeleted = true }
        );
    }
}
