using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

public sealed class RetentionPreviewEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Shared_Host_Preview_Path_Returns_Candidate_Counts_Without_Deleting_Rows()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "preview-delete-me",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-5),
                    Body = "preview-keep-newer",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "preview-keep-other-tenant",
                }
            );
            db.ExemptDocuments.Add(
                new ExemptDocument
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = asOf.AddDays(-400),
                    Title = "preview-exempt-document",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunPreviewAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().HaveCount(3);
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(Note),
                "short-lived",
                tenantA,
                Strategy.Purge,
                1
            )
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(SoftDeleteRecord),
                "soft-delete",
                tenantA,
                Strategy.SoftDelete,
                0
            )
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(AnonymisedContact),
                "anonymise",
                tenantA,
                Strategy.Anonymise,
                0
            )
        );

        await using var verify = Host.CreateDbContext();
        var noteBodies = await verify.Notes.OrderBy(note => note.Body).Select(note => note.Body).ToListAsync();
        noteBodies.Should().Equal(
            "preview-delete-me",
            "preview-keep-newer",
            "preview-keep-other-tenant"
        );
        var exemptTitles = await verify
            .ExemptDocuments.OrderBy(document => document.Title)
            .Select(document => document.Title)
            .ToListAsync();
        exemptTitles.Should().Equal("preview-exempt-document");
    }

    [Fact]
    public async Task Preview_Path_Returns_Zero_Counts_For_Exempt_Runtime_Rules_Without_Deleting_Rows()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-180),
                    Body = "preview-exempt-note",
                }
            );
            await db.SaveChangesAsync();
        }

        using var previewHost = new CohortTestHost(
            GetConnectionString(),
            new StaticCategoryRepository(
                new Dictionary<string, IRetentionRuleResolver>
                {
                    ["short-lived"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Exempt)
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

        var result = await previewHost.RunPreviewAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().HaveCount(3);
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(Note),
                "short-lived",
                tenantId,
                Strategy.Exempt,
                0
            )
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(SoftDeleteRecord),
                "soft-delete",
                tenantId,
                Strategy.SoftDelete,
                0
            )
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(AnonymisedContact),
                "anonymise",
                tenantId,
                Strategy.Anonymise,
                0
            )
        );

        await using var verify = Host.CreateDbContext();
        var noteBodies = await verify.Notes.Select(note => note.Body).ToListAsync();
        noteBodies.Should().Equal("preview-exempt-note");
    }

    [Fact]
    public async Task Preview_Path_Uses_The_Greater_Of_Period_And_Legal_Min_Without_Deleting_Rows()
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
                    CreatedAt = asOf.AddDays(-45),
                    Body = "preview-keep-legal-min",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "preview-count-legal-min",
                }
            );
            await db.SaveChangesAsync();
        }

        using var previewHost = new CohortTestHost(
            GetConnectionString(),
            new StaticCategoryRepository(
                new Dictionary<string, IRetentionRuleResolver>
                {
                    ["short-lived"] = new StaticRetentionRuleResolver(
                        new RetentionRule(
                            TimeSpan.FromDays(30),
                            Strategy.Purge,
                            TimeSpan.FromDays(90)
                        )
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

        var result = await previewHost.RunPreviewAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().HaveCount(3);
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(Note),
                "short-lived",
                tenantId,
                Strategy.Purge,
                1
            )
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(SoftDeleteRecord),
                "soft-delete",
                tenantId,
                Strategy.SoftDelete,
                0
            )
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(AnonymisedContact),
                "anonymise",
                tenantId,
                Strategy.Anonymise,
                0
            )
        );

        await using var verify = Host.CreateDbContext();
        var noteBodies = await verify.Notes.OrderBy(note => note.Body).Select(note => note.Body).ToListAsync();
        noteBodies.Should().Equal("preview-count-legal-min", "preview-keep-legal-min");
    }

    [Fact]
    public async Task Preview_Path_Ignores_Legacy_Notes_With_Null_TenantId_For_Targeted_Runs()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "preview-count-target-tenant",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "preview-ignore-other-tenant",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = null,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "preview-ignore-null-tenant",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunPreviewAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().HaveCount(3);
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(Note),
                "short-lived",
                tenantA,
                Strategy.Purge,
                1
            )
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(SoftDeleteRecord),
                "soft-delete",
                tenantA,
                Strategy.SoftDelete,
                0
            )
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(AnonymisedContact),
                "anonymise",
                tenantA,
                Strategy.Anonymise,
                0
            )
        );

        await using var verify = Host.CreateDbContext();
        var noteBodies = await verify.Notes.OrderBy(note => note.Body).Select(note => note.Body).ToListAsync();
        noteBodies.Should().Equal(
            "preview-count-target-tenant",
            "preview-ignore-null-tenant",
            "preview-ignore-other-tenant"
        );
    }

    [Fact]
    public async Task Preview_Path_Counts_Anonymise_Candidates_Without_Modifying_Rows()
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
                    CreatedAt = asOf.AddDays(-45),
                    EmailAddress = "preview-expired@example.com",
                    GivenName = "Expired",
                    Surname = "Candidate",
                    Notes = "preview-count-target-tenant",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-5),
                    EmailAddress = "preview-current@example.com",
                    GivenName = "Current",
                    Surname = "Candidate",
                    Notes = "preview-ignore-current-row",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-45),
                    EmailAddress = "preview-other-tenant@example.com",
                    GivenName = "Other",
                    Surname = "Tenant",
                    Notes = "preview-ignore-other-tenant",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunPreviewAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().HaveCount(3);
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(Note),
                "short-lived",
                tenantA,
                Strategy.Purge,
                0
            )
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(SoftDeleteRecord),
                "soft-delete",
                tenantA,
                Strategy.SoftDelete,
                0
            )
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
                EmailAddress = (string?)"preview-expired@example.com",
                GivenName = "Expired",
                Surname = "Candidate",
                Notes = "preview-count-target-tenant",
            },
            new
            {
                EmailAddress = (string?)"preview-current@example.com",
                GivenName = "Current",
                Surname = "Candidate",
                Notes = "preview-ignore-current-row",
            },
            new
            {
                EmailAddress = (string?)"preview-other-tenant@example.com",
                GivenName = "Other",
                Surname = "Tenant",
                Notes = "preview-ignore-other-tenant",
            }
        );
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

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }
}
