using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

public sealed class RetentionSweepEngineEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Shared_Host_Sweep_Path_Deletes_Only_Expired_Notes_For_The_Target_Tenant()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "delete-me",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "keep-legal-min",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "keep-other-tenant",
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
                }
            )
        );

        var result = await sweepHost.RunSweepAsync(
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
        var remaining = verify.Notes.OrderBy(note => note.Body).Select(note => note.Body).ToArray();
        remaining.Should().Equal("keep-legal-min", "keep-other-tenant");
    }

    [Fact]
    public async Task Shared_Host_Sweep_Path_Records_Exempt_Counts_Without_Deleting_Notes()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "keep-me",
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
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Exempt)
                    ),
                }
            )
        );

        var result = await sweepHost.RunSweepAsync(
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
        var remainingBodies = verify.Notes.Select(note => note.Body).ToArray();
        remainingBodies.Should().Equal("keep-me");
    }

    [Fact]
    public async Task SweepAsync_Resolves_Runtime_Rules_Before_Opening_A_Transaction()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

        await using var db = Host.CreateDbContext();
        db.Notes.Add(
            new Note
            {
                Id = Guid.NewGuid(),
                TenantId = tenantId,
                CreatedAt = asOf.AddDays(-120),
                Body = "delete-after-resolve",
            }
        );
        await db.SaveChangesAsync();

        var resolver = new TransactionAssertingResolver(
            db,
            new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
        );
        var engine = new RetentionSweepEngine(
            db,
            new RetentionRegistry(db),
            new StaticCategoryRepository(
                new Dictionary<string, IRetentionRuleResolver> { ["short-lived"] = resolver }
            ),
            new PurgeSweepStrategy()
        );

        var result = await engine.SweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        resolver.SawNoTransactionDuringResolve.Should().BeTrue();
        result.Counts.Should().ContainSingle();
        result.Counts[0].Affected.Should().Be(1);
    }

    [Fact]
    public async Task Shared_Host_Sweep_Path_Rejects_Unsupported_Runtime_Strategies()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "must-remain",
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
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    ),
                }
            )
        );

        var act = () => sweepHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await act
            .Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("*Milestone A sweep engine*");

        await using var verify = Host.CreateDbContext();
        var remainingBodies = verify.Notes.Select(note => note.Body).ToArray();
        remainingBodies.Should().Equal("must-remain");
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

    private sealed class TransactionAssertingResolver(
        SampleDbContext db,
        RetentionRule rule
    ) : IRetentionRuleResolver
    {
        public bool SawNoTransactionDuringResolve { get; private set; }

        public Task<RetentionRule> ResolveAsync(
            RetentionResolutionContext ctx,
            CancellationToken ct
        )
        {
            SawNoTransactionDuringResolve = db.Database.CurrentTransaction is null;
            return Task.FromResult(rule);
        }

        public RetentionRule? TryResolveAtStartup()
        {
            return rule;
        }
    }

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }
}
