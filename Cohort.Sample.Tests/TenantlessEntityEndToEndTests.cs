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
    public async Task Public_Sweep_Port_Derives_Tenantless_Identity_From_Requested_Scope()
    {
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        await using (var db = Host.CreateDbContext())
        {
            db.TenantlessLogs.Add(
                new TenantlessLog
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = asOf.AddDays(-120),
                    Payload = "public-tenantless-sweep",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunWithServicesAsync(services =>
            services
                .GetRequiredService<IRetentionSweep>()
                .ExecuteAsync(
                    RetentionSweepRequest.Tenantless(asOf)
                )
        );

        result
            .Counts.Should()
            .ContainSingle(count => count.EntityType == typeof(TenantlessLog))
            .Which.TenantId.Should()
            .Be(Guid.Empty);
        await using var verify = Host.CreateDbContext();
        (await verify.TenantlessLogs.CountAsync()).Should().Be(0);
    }

    [Fact]
    public async Task Preview_Scopes_Tenantless_Entities_Without_Attributing_Them_To_A_Tenant()
    {
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        await using (var db = Host.CreateDbContext())
        {
            db.TenantlessLogs.Add(
                new TenantlessLog
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = asOf.AddDays(-120),
                    Payload = "public-tenantless-preview",
                }
            );
            await db.SaveChangesAsync();
        }

        await Host.RunWithServicesAsync(async services =>
        {
            var preview = services.GetRequiredService<IRetentionPreview>();

            var tenanted = await preview.PreviewAsync(tenant, asOf);
            var tenantless = await preview.ExecuteAsync(
                RetentionPreviewRequest.Tenantless(asOf)
            );

            tenanted.Counts.Should().NotContain(count => count.EntityType == typeof(TenantlessLog));
            tenantless
                .Counts.Should()
                .ContainSingle(count => count.EntityType == typeof(TenantlessLog))
                .Which.Should()
                .Match<EntitySweepCount>(count => count.Affected == 1 && count.TenantId == Guid.Empty);
        });
    }
}
