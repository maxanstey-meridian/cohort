using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

public sealed class RetentionResolverEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Sweep_Path_Uses_The_Injected_Custom_Resolver()
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
                    CreatedAt = asOf.AddDays(-45),
                    Body = "custom-resolver-note",
                }
            );
            await db.SaveChangesAsync();
        }

        using var sweepHost = new CohortTestHost(
            GetConnectionString(),
            new TenantAwareCategoryRepository()
        );

        var result = await sweepHost.RunSweepAsync(
            new TenantContext(
                tenantId,
                "uk",
                new Dictionary<string, string> { ["profile"] = "lenient" }
            ),
            asOf
        );

        result.Counts.Should().HaveCount(2);
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(Note),
                "short-lived",
                tenantId,
                Strategy.Purge,
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

        await using var verify = Host.CreateDbContext();
        var noteBodies = await verify.Notes.Select(note => note.Body).ToListAsync();
        noteBodies.Should().Equal("custom-resolver-note");
    }

    [Fact]
    public async Task Preview_Path_Propagates_Retention_Alias_Cycle_Exception()
    {
        using var previewHost = new CohortTestHost(GetConnectionString(), new AliasCategoryRepository());

        var act = () =>
            previewHost.RunPreviewAsync(
                new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero)
            );

        var exception = await act.Should().ThrowAsync<RetentionAliasCycleException>();
        exception.Which.Message.Should().Contain("policy-a");
        exception.Which.Message.Should().Contain("policy-b");
    }

    private sealed class TenantAwareCategoryRepository : IRetentionCategoryRepository
    {
        private readonly IRetentionRuleResolver resolver = new TenantAwareResolver();

        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            return Task.FromResult<IRetentionRuleResolver?>(
                category switch
                {
                    "short-lived" => resolver,
                    "soft-delete" => new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    ),
                    _ => null,
                }
            );
        }
    }

    private sealed class TenantAwareResolver : IRetentionRuleResolver
    {
        public Task<RetentionRule> ResolveAsync(RetentionResolutionContext ctx, CancellationToken ct)
        {
            var isLenient =
                ctx.Tenant.Tags.TryGetValue("profile", out var profile)
                && StringComparer.Ordinal.Equals(profile, "lenient");

            return Task.FromResult(
                new RetentionRule(
                    isLenient ? TimeSpan.FromDays(60) : TimeSpan.FromDays(30),
                    Strategy.Purge
                )
            );
        }
    }

    private sealed class AliasCategoryRepository : IRetentionCategoryRepository
    {
        private readonly IReadOnlyDictionary<string, IRetentionRuleResolver> resolvers;

        public AliasCategoryRepository()
        {
            resolvers = new Dictionary<string, IRetentionRuleResolver>
            {
                ["short-lived"] = new AliasResolver(this, "policy-a"),
                ["soft-delete"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
                ["policy-a"] = new AliasResolver(this, "policy-b"),
                ["policy-b"] = new AliasResolver(this, "policy-a"),
            };
        }

        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            resolvers.TryGetValue(category, out var resolver);
            return Task.FromResult(resolver);
        }
    }

    private sealed class AliasResolver(
        AliasCategoryRepository categoryRepository,
        string nextCategory
    ) : IRetentionRuleResolver
    {
        public async Task<RetentionRule> ResolveAsync(
            RetentionResolutionContext ctx,
            CancellationToken ct
        )
        {
            if (ctx.AliasPath.Contains(nextCategory, StringComparer.Ordinal))
            {
                throw new RetentionAliasCycleException(
                    $"Retention alias cycle detected: {string.Join(" -> ", [.. ctx.AliasPath, ctx.Category, nextCategory])}"
                );
            }

            var resolver = await categoryRepository.GetAsync(nextCategory, ct);
            if (resolver is null)
            {
                throw new InvalidOperationException(
                    $"Retention category '{nextCategory}' could not be resolved."
                );
            }

            return await resolver.ResolveAsync(
                new RetentionResolutionContext(
                    nextCategory,
                    ctx.Tenant,
                    ctx.Now,
                    [.. ctx.AliasPath, ctx.Category]
                ),
                ct
            );
        }
    }

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }
}
