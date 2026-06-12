using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests;

public sealed class NullAnchorEndToEndTests(PostgresFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Sweep_Reports_Null_Anchor_Rows_And_Leaves_Them_Untouched()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.NullableAnchorEvents.AddRange(
                new NullableAnchorEvent
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    OccurredAt = asOf.AddDays(-120),
                    Payload = "null-anchor-expired",
                },
                new NullableAnchorEvent
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    OccurredAt = null,
                    Payload = "null-anchor-invisible",
                },
                new NullableAnchorEvent
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    OccurredAt = null,
                    Payload = "null-anchor-invisible-too",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        // NULL anchors never match a cutoff: the rows survive, and the summary says so
        // instead of letting them accumulate invisibly.
        result.Counts.Should().Contain(count =>
            count.EntityType == typeof(NullableAnchorEvent)
            && count.Category == "nullable-anchor-purge"
            && count.Affected == 1
            && count.NullAnchorCount == 2
        );

        await using var verify = Host.CreateDbContext();
        var remaining = await verify.NullableAnchorEvents
            .OrderBy(record => record.Payload)
            .Select(record => record.Payload)
            .ToListAsync();
        remaining.Should().Equal("null-anchor-invisible", "null-anchor-invisible-too");
    }

    [Fact]
    public async Task DryRun_Reports_Null_Anchor_Rows_In_Its_Audited_Summary()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.NullableAnchorEvents.Add(
                new NullableAnchorEvent
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    OccurredAt = null,
                    Payload = "null-anchor-dry-run",
                }
            );
            await db.SaveChangesAsync();
        }

        RetentionSweepResult? result = null;
        await Host.RunWithServicesAsync(async serviceProvider =>
        {
            var engine = serviceProvider.GetRequiredService<RetentionSweepEngine>();
            result = await engine.DryRunAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                asOf,
                SweepTriggerKind.Manual
            );
        });

        result.Should().NotBeNull();
        result!.Counts.Should().Contain(count =>
            count.EntityType == typeof(NullableAnchorEvent)
            && count.Affected == 0
            && count.NullAnchorCount == 1
        );

        await using var verify = Host.CreateDbContext();
        (await verify.NullableAnchorEvents.CountAsync()).Should().Be(1);
    }
}
