using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

public sealed class RetentionRuleProviderEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Sweep_Path_Uses_The_Injected_Dynamic_Provider()
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
            new TenantAwareRuleProvider()
        );

        var result = await sweepHost.RunSweepAsync(
            new TenantContext(
                tenantId,
                "uk",
                new Dictionary<string, string> { ["profile"] = "lenient" }
            ),
            asOf
        );

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 0)
            );
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantId,
                    Strategy.SoftDelete,
                    0
                )
            );
        result
            .Counts.Should()
            .Contain(
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
        noteBodies.Should().Equal("custom-resolver-note");
    }

    [Fact]
    public async Task Preview_Path_Propagates_Retention_Alias_Cycle_Exception()
    {
        using var previewHost = new CohortTestHost(
            GetConnectionString(),
            new AliasRuleProvider()
        );

        var act = () =>
            previewHost.RunPreviewAsync(
                new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero)
            );

        var exception = await act.Should().ThrowAsync<RetentionAliasCycleException>();
        exception.Which.Message.Should().Contain("policy-a");
        exception.Which.Message.Should().Contain("policy-b");
    }

    [Fact]
    public async Task Preview_Path_Rejects_A_Strategy_Not_Declared_By_The_Provider()
    {
        using var previewHost = new CohortTestHost(
            GetConnectionString(),
            new UndeclaredStrategyRuleProvider()
        );

        var act = () =>
            previewHost.RunPreviewAsync(
                new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero)
            );

        await act.Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("*resolved strategy 'SoftDelete'*not declared*");
    }

    [Fact]
    public async Task Preview_Path_Uses_The_Capabilities_Validated_At_Startup()
    {
        var provider = new MutableCapabilityRuleProvider();
        using var previewHost = new CohortTestHost(GetConnectionString(), provider);
        await previewHost.ValidateAndScanAsync();
        provider.ChangeShortLivedStrategyToSoftDelete();

        var act = () =>
            previewHost.RunPreviewAsync(
                new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero)
            );

        await act.Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("*resolved strategy 'SoftDelete'*startup-validated capabilities*");
    }

    [Fact]
    public async Task Preview_Path_Uses_The_Startup_Snapshot_When_Provider_Capabilities_Disappear()
    {
        using var previewHost = new CohortTestHost(
            GetConnectionString(),
            new DisappearingCategoryRuleProvider()
        );

        var tenantId = Guid.NewGuid();

        var result = await previewHost.RunPreviewAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero)
        );

        result.Counts.Should().Contain(count =>
            count.EntityType == typeof(Note)
            && count.Category == "short-lived"
            && count.Strategy == Strategy.Purge
        );
    }

    [Fact]
    public async Task Preview_Path_Rejects_An_Unresolved_Runtime_Rule()
    {
        using var previewHost = new CohortTestHost(
            GetConnectionString(),
            new UnresolvedRuleProvider()
        );

        var act = () =>
            previewHost.RunPreviewAsync(
                new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>()),
                new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero)
            );

        await act.Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("*category 'short-lived' could not be resolved at runtime*");
    }

    private sealed class TenantAwareRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category switch
            {
                "short-lived" => new([Strategy.Purge]),
                "soft-delete" => new([Strategy.SoftDelete]),
                "anonymise" => new([Strategy.Anonymise]),
                _ => new([Strategy.Exempt]),
            };

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            var isLenient =
                context.Tenant.Tags.TryGetValue("profile", out var profile)
                && StringComparer.Ordinal.Equals(profile, "lenient");

            var strategy = context.Category switch
            {
                "short-lived" => Strategy.Purge,
                "soft-delete" => Strategy.SoftDelete,
                "anonymise" => Strategy.Anonymise,
                _ => Strategy.Exempt,
            };
            return Task.FromResult<RetentionRule?>(
                new RetentionRule(
                    isLenient ? TimeSpan.FromDays(60) : TimeSpan.FromDays(30),
                    strategy
                )
            );
        }
    }

    private sealed class AliasRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category switch
            {
                "short-lived" or "policy-a" or "policy-b" => new([Strategy.Purge]),
                "soft-delete" => new([Strategy.SoftDelete]),
                "anonymise" => new([Strategy.Anonymise]),
                _ => new([Strategy.Exempt]),
            };

        public async Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            var nextCategory = context.Category switch
            {
                "short-lived" => "policy-a",
                "policy-a" => "policy-b",
                "policy-b" => "policy-a",
                _ => null,
            };
            if (nextCategory is null)
            {
                var strategy = GetCapabilities(context.Category)!.Strategies.Single();
                return new RetentionRule(TimeSpan.FromDays(30), strategy);
            }

            if (context.AliasPath.Contains(nextCategory, StringComparer.Ordinal))
            {
                throw new RetentionAliasCycleException(
                    $"Retention alias cycle detected: {string.Join(" -> ", [.. context.AliasPath, context.Category, nextCategory])}"
                );
            }

            return await ResolveAsync(
                new RetentionResolutionContext(
                    nextCategory,
                    context.Tenant,
                    context.Now,
                    [.. context.AliasPath, context.Category]
                ),
                ct
            );
        }
    }

    private sealed class UndeclaredStrategyRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            new([Strategy.Purge]);

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        ) => Task.FromResult<RetentionRule?>(
            new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
        );
    }

    private sealed class DisappearingCategoryRuleProvider : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();
        private int shortLivedCapabilityRequests;

        public RetentionCategoryCapabilities? GetCapabilities(string category)
        {
            if (
                category == "short-lived"
                && Interlocked.Increment(ref shortLivedCapabilityRequests) > 1
            )
            {
                return null;
            }

            return inner.GetCapabilities(category);
        }

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        ) => inner.ResolveAsync(context, ct);
    }

    private sealed class MutableCapabilityRuleProvider : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();
        private bool changed;

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            changed && category == "short-lived"
                ? new([Strategy.SoftDelete])
                : inner.GetCapabilities(category);

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        ) =>
            changed && context.Category == "short-lived"
                ? Task.FromResult<RetentionRule?>(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                )
                : inner.ResolveAsync(context, ct);

        public void ChangeShortLivedStrategyToSoftDelete()
        {
            changed = true;
        }
    }

    private sealed class UnresolvedRuleProvider : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            inner.GetCapabilities(category);

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        ) =>
            context.Category == "short-lived"
                ? Task.FromResult<RetentionRule?>(null)
                : inner.ResolveAsync(context, ct);
    }

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }
}
