using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests;

public sealed class TenantlessEntityEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Sweep_Path_Purges_Only_Expired_Rows_On_Explicitly_Tenantless_Entity()
    {
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var oldId = Guid.NewGuid();
        var youngId = Guid.NewGuid();
        var entries = await Host.RunStartupAsync();
        var tenantlessEntry = entries[typeof(TenantlessLog)];

        tenantlessEntry.Tenant.Should().BeNull();
        tenantlessEntry.IsExplicitlyTenantless.Should().BeTrue();

        await using (var db = Host.CreateDbContext())
        {
            db.TenantlessLogs.AddRange(
                new TenantlessLog
                {
                    Id = oldId,
                    CreatedAt = asOf.AddDays(-120),
                    Payload = "tenantless-purge-me",
                },
                new TenantlessLog
                {
                    Id = youngId,
                    CreatedAt = asOf.AddDays(-5),
                    Payload = "tenantless-keep-newer",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(count =>
            count.EntityType == typeof(TenantlessLog)
            && count.Category == "tenantless-purge"
            && count.Strategy == Strategy.Purge
            && count.Affected == 1
        );

        await using var verify = Host.CreateDbContext();
        var remaining = await verify.TenantlessLogs
            .OrderBy(log => log.Payload)
            .Select(log => log.Payload)
            .ToListAsync();
        remaining.Should().Equal("tenantless-keep-newer");
    }

    [Fact]
    public async Task Sweep_Path_Soft_Deletes_Only_Expired_Rows_On_Explicitly_Tenantless_Entity()
    {
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var entries = await Host.RunStartupAsync();
        var tenantlessEntry = entries[typeof(TenantlessSoftDelete)];

        tenantlessEntry.Tenant.Should().BeNull();
        tenantlessEntry.IsExplicitlyTenantless.Should().BeTrue();

        await using (var db = Host.CreateDbContext())
        {
            db.TenantlessSoftDeletes.AddRange(
                new TenantlessSoftDelete
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = asOf.AddDays(-120),
                    Payload = "tenantless-softdelete-me",
                    IsDeleted = false,
                },
                new TenantlessSoftDelete
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = asOf.AddDays(-5),
                    Payload = "tenantless-keep-newer",
                    IsDeleted = false,
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(count =>
            count.EntityType == typeof(TenantlessSoftDelete)
            && count.Category == "tenantless-softdelete"
            && count.Strategy == Strategy.SoftDelete
            && count.Affected == 1
        );

        await using var verify = Host.CreateDbContext();
        var rows = await verify.TenantlessSoftDeletes
            .OrderBy(record => record.Payload)
            .ToListAsync();
        rows.Should().HaveCount(2);
        rows.Single(record => record.Payload == "tenantless-softdelete-me").IsDeleted.Should().BeTrue();
        rows.Single(record => record.Payload == "tenantless-keep-newer").IsDeleted.Should().BeFalse();
    }

    [Fact]
    public async Task Sweep_Path_Respects_Active_Holds_On_Explicitly_Tenantless_Entity_Matched_By_Table_And_RecordId_Only()
    {
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var heldId = Guid.NewGuid();
        var unheldId = Guid.NewGuid();
        var holdMarker = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.TenantlessLogs.AddRange(
                new TenantlessLog
                {
                    Id = heldId,
                    CreatedAt = asOf.AddDays(-120),
                    Payload = "tenantless-held",
                },
                new TenantlessLog
                {
                    Id = unheldId,
                    CreatedAt = asOf.AddDays(-120),
                    Payload = "tenantless-unheld",
                }
            );
            await db.SaveChangesAsync();
        }

        await CreateHoldAsync(
            new RetentionHoldRequest(
                holdMarker,
                "tenantless_logs",
                heldId.ToString(),
                Guid.Empty,
                "tenantless legal hold",
                asOf.AddDays(-10)
            )
        );

        var firstResult = await Host.RunSweepAsync(
            new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
            asOf
        );

        firstResult.Counts.Should().Contain(count =>
            count.EntityType == typeof(TenantlessLog)
            && count.Category == "tenantless-purge"
            && count.Strategy == Strategy.Purge
            && count.Affected == 1
        );

        await using (var verify = Host.CreateDbContext())
        {
            var remaining = await verify.TenantlessLogs
                .Select(log => log.Payload)
                .ToListAsync();
            remaining.Should().Equal("tenantless-held");
        }

        await RemoveHoldAsync(holdMarker, asOf.AddMinutes(-5));

        var secondResult = await Host.RunSweepAsync(
            new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
            asOf
        );

        secondResult.Counts.Should().Contain(count =>
            count.EntityType == typeof(TenantlessLog)
            && count.Affected == 1
        );

        await using var verifyAfter = Host.CreateDbContext();
        (await verifyAfter.TenantlessLogs.CountAsync()).Should().Be(0);
    }

    private Task CreateHoldAsync(RetentionHoldRequest request)
    {
        return Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            await repository.CreateAsync(request, CancellationToken.None);
        });
    }

    private Task RemoveHoldAsync(Guid holdId, DateTimeOffset removedAt)
    {
        return Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            await repository.RemoveAsync(holdId, removedAt, CancellationToken.None);
        });
    }
}
