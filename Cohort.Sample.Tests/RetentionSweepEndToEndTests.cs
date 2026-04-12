using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

public sealed class RetentionSweepEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Sweep_Path_Deletes_Only_Rows_Older_Than_The_Resolved_Cutoff_For_The_Target_Tenant_And_Leaves_Exempt_Documents_Untouched()
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
                    Body = "sweep-delete-me",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "sweep-keep-legal-min",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "sweep-keep-other-tenant",
                }
            );
            db.ExemptDocuments.Add(
                new ExemptDocument
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = asOf.AddDays(-400),
                    Title = "sweep-exempt-document",
                }
            );
            await db.SaveChangesAsync();
        }

        using var sweepHost = new CohortTestHost(
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
                }
            )
        );

        var result = await sweepHost.RunSweepAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().HaveCount(2);
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

        await using var verify = Host.CreateDbContext();
        var noteBodies = await verify.Notes.OrderBy(note => note.Body).Select(note => note.Body).ToListAsync();
        noteBodies.Should().Equal("sweep-keep-legal-min", "sweep-keep-other-tenant");
        var exemptTitles = await verify
            .ExemptDocuments.OrderBy(document => document.Title)
            .Select(document => document.Title)
            .ToListAsync();
        exemptTitles.Should().Equal("sweep-exempt-document");
    }

    [Fact]
    public async Task Sweep_Path_Ignores_Legacy_Notes_With_Null_TenantId_For_Targeted_Runs()
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
                    Body = "sweep-delete-target-tenant",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "sweep-keep-other-tenant",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = null,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "sweep-keep-null-tenant",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().HaveCount(2);
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

        await using var verify = Host.CreateDbContext();
        var noteBodies = await verify.Notes.OrderBy(note => note.Body).Select(note => note.Body).ToListAsync();
        noteBodies.Should().Equal("sweep-keep-null-tenant", "sweep-keep-other-tenant");
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
