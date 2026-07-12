using Cohort.Domain;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests.ComplianceCorpus;

public sealed class StorageLimitationCorpusTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Scheduled_sweep_applies_strict_resolved_cutoff_and_reports_null_anchors()
    {
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var expiredId = Guid.NewGuid();
        var boundaryId = Guid.NewGuid();
        var freshId = Guid.NewGuid();
        var nullAnchorId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = expiredId, TenantId = tenantId, CreatedAt = now.AddDays(-31), Body = "expired" },
                new Note { Id = boundaryId, TenantId = tenantId, CreatedAt = now.AddDays(-30), Body = "boundary" },
                new Note { Id = freshId, TenantId = tenantId, CreatedAt = now.AddDays(-1), Body = "fresh" }
            );
            db.NullableAnchorEvents.Add(new NullableAnchorEvent
            {
                Id = nullAnchorId,
                TenantId = tenantId,
                OccurredAt = null,
                Payload = "unknown age",
            });
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now
        );

        result.Counts.Should().Contain(count =>
            count.EntityType == typeof(Note) && count.Affected == 1
        );
        result.Counts.Should().Contain(count =>
            count.EntityType == typeof(NullableAnchorEvent) && count.NullAnchorCount == 1
        );
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == expiredId)).Should().BeFalse();
        (await verify.Notes.AnyAsync(note => note.Id == boundaryId)).Should().BeTrue();
        (await verify.Notes.AnyAsync(note => note.Id == freshId)).Should().BeTrue();
        (await verify.NullableAnchorEvents.AnyAsync(row => row.Id == nullAnchorId)).Should().BeTrue();

        var zeroRetentionId = Guid.NewGuid();
        await using (var seedZeroRetention = Host.CreateDbContext())
        {
            seedZeroRetention.Notes.Add(new Note
            {
                Id = zeroRetentionId,
                TenantId = tenantId,
                CreatedAt = now.AddTicks(-1),
                Body = "zero retention",
            });
            await seedZeroRetention.SaveChangesAsync();
        }

        using var zeroRetentionHost = new CohortTestHost(
            ConnectionString,
            new ZeroRetentionCategoryRepository()
        );
        await zeroRetentionHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now
        );
        await using var verifyZeroRetention = Host.CreateDbContext();
        (await verifyZeroRetention.Notes.AnyAsync(note => note.Id == zeroRetentionId))
            .Should()
            .BeFalse();
    }

    private sealed class ZeroRetentionCategoryRepository : ITestRetentionRuleProvider
    {
        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct)
        {
            var strategy = category == "short-lived" ? Strategy.Purge : Strategy.Exempt;
            return Task.FromResult<ITestRetentionRule?>(
                new StaticTestRetentionRule(new RetentionRule(TimeSpan.Zero, strategy))
            );
        }
    }
}
