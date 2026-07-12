using Cohort.Domain;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests.ComplianceCorpus;

public sealed class AnonymisationCorpusTests(PostgresFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Anonymisation_changes_only_declared_fields_once_with_factory_semantics()
    {
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var recordId = Guid.NewGuid();
        var originalExternalId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.TombstoneRecords.Add(new TombstoneRecord
            {
                Id = recordId,
                TenantId = tenantId,
                CreatedAt = now.AddDays(-60),
                ExternalId = originalExternalId,
                DisplayName = "Original",
                ContactEmail = "person@example.org",
                Notes = "must survive",
            });
            await db.SaveChangesAsync();
        }

        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());
        var first = await Host.RunSweepAsync(tenant, now);
        var factoryContexts = await Host.RunWithServicesAsync(services => Task.FromResult(
            services.GetRequiredService<GuidTombstoneFactory>().Contexts.ToArray()
        ));
        var originalValueContexts = await Host.RunWithServicesAsync(services => Task.FromResult(
            services.GetRequiredService<OriginalValueTombstoneFactory>().Contexts.ToArray()
        ));
        var second = await Host.RunSweepAsync(tenant, now);

        first.Counts.Should().Contain(count =>
            count.EntityType == typeof(TombstoneRecord) && count.Affected == 1
        );
        second.Counts.Should().Contain(count =>
            count.EntityType == typeof(TombstoneRecord) && count.Affected == 0
        );
        factoryContexts.Should().ContainSingle();
        factoryContexts[0].EntityType.Should().Be(typeof(TombstoneRecord));
        factoryContexts[0].MemberName.Should().Be(nameof(TombstoneRecord.ExternalId));
        factoryContexts[0].OriginalValue.Should().BeNull();
        factoryContexts[0].Now.Should().Be(now);
        factoryContexts[0].TenantId.Should().Be(tenantId);
        originalValueContexts.Should().ContainSingle();
        originalValueContexts[0].EntityType.Should().Be(typeof(TombstoneRecord));
        originalValueContexts[0].MemberName.Should().Be(nameof(TombstoneRecord.DisplayName));
        originalValueContexts[0].OriginalValue.Should().Be("Original");
        originalValueContexts[0].Now.Should().Be(now);
        originalValueContexts[0].TenantId.Should().Be(tenantId);
        await Host.RunWithServicesAsync(services =>
        {
            services.GetRequiredService<GuidTombstoneFactory>().Contexts.Should().HaveCount(1);
            services.GetRequiredService<OriginalValueTombstoneFactory>().Contexts.Should().HaveCount(1);
            return Task.CompletedTask;
        });
        await using var verify = Host.CreateDbContext();
        var record = await verify.TombstoneRecords.SingleAsync(row => row.Id == recordId);
        record.ExternalId.Should().Be(GuidTombstoneFactory.TombstoneValue);
        record.DisplayName.Should().Be("Original-tombstone");
        record.ContactEmail.Should().BeNull();
        record.Notes.Should().Be("must survive");
        record.AnonymisedAt.Should().Be(now);
    }
}
