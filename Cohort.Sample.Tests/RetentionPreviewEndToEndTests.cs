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

        result.Counts.Should().ContainSingle();
        result.Counts[0].Should().Be(
            new EntitySweepCount(
                typeof(Note),
                "short-lived",
                tenantA,
                Strategy.Purge,
                1
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
                }
            )
        );

        var result = await previewHost.RunPreviewAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().ContainSingle();
        result.Counts[0].Should().Be(
            new EntitySweepCount(
                typeof(Note),
                "short-lived",
                tenantId,
                Strategy.Exempt,
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
                }
            )
        );

        var result = await previewHost.RunPreviewAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().ContainSingle();
        result.Counts[0].Should().Be(
            new EntitySweepCount(
                typeof(Note),
                "short-lived",
                tenantId,
                Strategy.Purge,
                1
            )
        );

        await using var verify = Host.CreateDbContext();
        var noteBodies = await verify.Notes.OrderBy(note => note.Body).Select(note => note.Body).ToListAsync();
        noteBodies.Should().Equal("preview-count-legal-min", "preview-keep-legal-min");
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
