using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

public sealed class AnonymiseSweepEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Sweep_Path_Anonymises_Only_Expired_Rows_For_The_Target_Tenant_And_Leaves_Unmarked_Columns_Unchanged()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "expired@example.com",
                    GivenName = "Alice",
                    Surname = "Smith",
                    Notes = "keep-expired-notes",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-5),
                    EmailAddress = "newer@example.com",
                    GivenName = "Bob",
                    Surname = "Jones",
                    Notes = "keep-newer-notes",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "other-tenant@example.com",
                    GivenName = "Cara",
                    Surname = "Mills",
                    Notes = "keep-other-tenant-notes",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(AnonymisedContact),
                "anonymise",
                tenantA,
                Strategy.Anonymise,
                1
            )
        );

        await using var verify = Host.CreateDbContext();
        var contacts = await verify
            .AnonymisedContacts.OrderBy(contact => contact.Notes)
            .Select(contact => new
            {
                contact.EmailAddress,
                contact.GivenName,
                contact.Surname,
                contact.Notes,
            })
            .ToListAsync();

        contacts.Should().Equal(
            new
            {
                EmailAddress = (string?)null,
                GivenName = string.Empty,
                Surname = "[redacted]",
                Notes = "keep-expired-notes",
            },
            new
            {
                EmailAddress = (string?)"newer@example.com",
                GivenName = "Bob",
                Surname = "Jones",
                Notes = "keep-newer-notes",
            },
            new
            {
                EmailAddress = (string?)"other-tenant@example.com",
                GivenName = "Cara",
                Surname = "Mills",
                Notes = "keep-other-tenant-notes",
            }
        );
    }

    [Fact]
    public async Task Startup_Path_Fails_When_An_Anonymise_Category_Has_No_Annotated_Fields()
    {
        await using var db = Host.CreateDbContext();
        var connectionString = db.Database.GetConnectionString()!;

        using var host = new CohortTestHost(
            connectionString,
            new StaticCategoryRepository(
                new Dictionary<string, IRetentionRuleResolver>
                {
                    ["short-lived"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                    ["soft-delete"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    ),
                    ["anonymise"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                }
            )
        );

        var act = () => host.RunStartupAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle();
        exception
            .Which.Errors[0]
            .Should()
            .Be(
                $"Anonymise convention on {typeof(Note).FullName}: retained Anonymise categories require at least one [Anonymise]-annotated property mapped by EF."
            );
    }

    [Fact]
    public async Task Sweep_Path_Can_Run_Twice_Without_Reintroducing_Scrubbed_Data()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "repeat@example.com",
                    GivenName = "Repeat",
                    Surname = "Target",
                    Notes = "repeat-notes",
                }
            );
            await db.SaveChangesAsync();
        }

        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());

        var first = await Host.RunSweepAsync(tenant, asOf);
        var second = await Host.RunSweepAsync(tenant, asOf);

        first.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(AnonymisedContact),
                "anonymise",
                tenantId,
                Strategy.Anonymise,
                1
            )
        );
        second.Counts.Should().Contain(
            count =>
                count.EntityType == typeof(AnonymisedContact)
                && count.Category == "anonymise"
                && count.TenantId == tenantId
                && count.Strategy == Strategy.Anonymise
        );

        await using var verify = Host.CreateDbContext();
        var contact = await verify.AnonymisedContacts.SingleAsync();

        contact.EmailAddress.Should().BeNull();
        contact.GivenName.Should().BeEmpty();
        contact.Surname.Should().Be("[redacted]");
        contact.Notes.Should().Be("repeat-notes");
    }

    private sealed class StaticCategoryRepository(
        IReadOnlyDictionary<string, IRetentionRuleResolver> resolvers
    ) : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            resolvers.TryGetValue(category, out var resolver);
            return Task.FromResult(resolver);
        }
    }
}
