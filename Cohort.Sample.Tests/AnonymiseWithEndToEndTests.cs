using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests;

public sealed class AnonymiseWithEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Sweep_Path_Applies_Literal_And_FactoryBacked_Anonymisation_On_TombstoneRecord()
    {
        var tenantId = Guid.NewGuid();
        var otherTenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.TombstoneRecords.AddRange(
                new TombstoneRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = Guid.NewGuid(),
                    CreatedAt = asOf.AddDays(-120),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "alpha",
                    ContactEmail = "alpha@example.com",
                    Notes = "expired-first",
                },
                new TombstoneRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = Guid.NewGuid(),
                    CreatedAt = asOf.AddDays(-90),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "beta",
                    ContactEmail = "beta@example.com",
                    Notes = "expired-second",
                },
                new TombstoneRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = Guid.NewGuid(),
                    CreatedAt = asOf.AddDays(-5),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "fresh",
                    ContactEmail = "fresh@example.com",
                    Notes = "fresh",
                },
                new TombstoneRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = otherTenantId,
                    SubjectId = Guid.NewGuid(),
                    CreatedAt = asOf.AddDays(-120),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "other-tenant",
                    ContactEmail = "other@example.com",
                    Notes = "other-tenant",
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
                typeof(TombstoneRecord),
                "tombstone-anonymise",
                tenantId,
                Strategy.Anonymise,
                2
            )
        );

        await using (var verify = Host.CreateDbContext())
        {
            var records = await verify.TombstoneRecords.OrderBy(record => record.Notes).ToListAsync();

            records.Single(record => record.Notes == "expired-first").ExternalId.Should().Be(GuidTombstoneFactory.TombstoneValue);
            records.Single(record => record.Notes == "expired-first").DisplayName.Should().Be("alpha-tombstone");
            records.Single(record => record.Notes == "expired-first").ContactEmail.Should().BeNull();

            records.Single(record => record.Notes == "expired-second").ExternalId.Should().Be(GuidTombstoneFactory.TombstoneValue);
            records.Single(record => record.Notes == "expired-second").DisplayName.Should().Be("beta-tombstone");
            records.Single(record => record.Notes == "expired-second").ContactEmail.Should().BeNull();

            records.Single(record => record.Notes == "fresh").DisplayName.Should().Be("fresh");
            records.Single(record => record.Notes == "fresh").ContactEmail.Should().Be("fresh@example.com");

            records.Single(record => record.Notes == "other-tenant").DisplayName.Should().Be("other-tenant");
            records.Single(record => record.Notes == "other-tenant").ContactEmail.Should().Be("other@example.com");
        }

        await Host.RunWithServicesAsync(
            serviceProvider =>
            {
                var guidFactory = serviceProvider.GetRequiredService<GuidTombstoneFactory>();
                var originalValueFactory = serviceProvider.GetRequiredService<OriginalValueTombstoneFactory>();

                guidFactory.Contexts.Should().ContainSingle();
                guidFactory.Contexts[0].OriginalValue.Should().BeNull();
                guidFactory.Contexts[0].EntityType.Should().Be(typeof(TombstoneRecord));
                guidFactory.Contexts[0].MemberName.Should().Be(nameof(TombstoneRecord.ExternalId));
                guidFactory.Contexts[0].TenantId.Should().Be(tenantId);

                originalValueFactory.Contexts.Should().HaveCount(2);
                originalValueFactory.Contexts.Select(context => context.OriginalValue)
                    .Should()
                    .BeEquivalentTo(new object?[] { "alpha", "beta" });
                originalValueFactory.Contexts.Select(context => context.MemberName)
                    .Should()
                    .OnlyContain(memberName => memberName == nameof(TombstoneRecord.DisplayName));
                originalValueFactory.Contexts.Select(context => context.TenantId)
                    .Should()
                    .OnlyContain(currentTenantId => currentTenantId == tenantId);

                return Task.CompletedTask;
            }
        );
    }

    [Fact]
    public async Task Erasure_Path_Applies_FactoryBacked_Anonymisation_On_TombstoneRecord()
    {
        var tenantId = Guid.NewGuid();
        var otherTenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var otherSubjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.TombstoneRecords.AddRange(
                new TombstoneRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "erasure-alpha",
                    ContactEmail = "erasure-alpha@example.com",
                    Notes = "subject-first",
                },
                new TombstoneRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "erasure-beta",
                    ContactEmail = "erasure-beta@example.com",
                    Notes = "subject-second",
                },
                new TombstoneRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = otherSubjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "other-subject",
                    ContactEmail = "other-subject@example.com",
                    Notes = "other-subject",
                },
                new TombstoneRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = otherTenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "other-tenant",
                    ContactEmail = "other-tenant@example.com",
                    Notes = "other-tenant",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(TombstoneRecord),
                "tombstone-anonymise",
                tenantId,
                Strategy.Anonymise,
                2
            )
        );

        await using (var verify = Host.CreateDbContext())
        {
            var records = await verify.TombstoneRecords.OrderBy(record => record.Notes).ToListAsync();

            records.Single(record => record.Notes == "subject-first").ExternalId.Should().Be(GuidTombstoneFactory.TombstoneValue);
            records.Single(record => record.Notes == "subject-first").DisplayName.Should().Be("erasure-alpha-tombstone");
            records.Single(record => record.Notes == "subject-first").ContactEmail.Should().BeNull();

            records.Single(record => record.Notes == "subject-second").ExternalId.Should().Be(GuidTombstoneFactory.TombstoneValue);
            records.Single(record => record.Notes == "subject-second").DisplayName.Should().Be("erasure-beta-tombstone");
            records.Single(record => record.Notes == "subject-second").ContactEmail.Should().BeNull();

            records.Single(record => record.Notes == "other-subject").DisplayName.Should().Be("other-subject");
            records.Single(record => record.Notes == "other-subject").ContactEmail.Should().Be("other-subject@example.com");

            records.Single(record => record.Notes == "other-tenant").DisplayName.Should().Be("other-tenant");
            records.Single(record => record.Notes == "other-tenant").ContactEmail.Should().Be("other-tenant@example.com");
        }

        await Host.RunWithServicesAsync(
            serviceProvider =>
            {
                var originalValueFactory = serviceProvider.GetRequiredService<OriginalValueTombstoneFactory>();

                originalValueFactory.Contexts.Should().HaveCount(2);
                originalValueFactory.Contexts.Select(context => context.OriginalValue)
                    .Should()
                    .BeEquivalentTo(new object?[] { "erasure-alpha", "erasure-beta" });

                return Task.CompletedTask;
            }
        );
    }

    private static DateTimeOffset EligibleErasureCreatedAt(DateTimeOffset asOf)
    {
        return asOf.AddDays(-45);
    }
}
