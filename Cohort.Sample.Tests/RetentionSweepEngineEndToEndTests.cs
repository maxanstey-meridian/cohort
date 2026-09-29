using System.Data;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Cohort.Sample.Tests;

public sealed class RetentionSweepEngineEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task SweepAsync_Resolves_Runtime_Rules_Before_Opening_A_Transaction()
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
                    Body = "delete-after-resolve",
                }
            );
            await db.SaveChangesAsync();
        }

        TransactionAssertingResolver? resolver = null;
        using var sweepHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.RemoveAll<IRetentionRuleProvider>();
                services.AddScoped<IRetentionRuleProvider>(provider =>
                {
                    resolver = new TransactionAssertingResolver(
                        provider.GetRequiredService<SampleDbContext>(),
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    );
                    return new StaticCategoryRepository(
                        new Dictionary<string, ITestRetentionRule>
                        {
                            ["short-lived"] = resolver,
                            ["soft-delete"] = new StaticTestRetentionRule(
                                new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                            ),
                            ["anonymise"] = new StaticTestRetentionRule(
                                new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                            ),
                        }
                    );
                });
            }
        );

        var result = await sweepHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        resolver.Should().NotBeNull();
        resolver!.SawNoTransactionDuringResolve.Should().BeTrue();
        result
            .Counts.Should()
            .Contain(count =>
                count.EntityType == typeof(Note)
                && count.Category == "short-lived"
                && count.Affected == 1
            );
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Body == "delete-after-resolve"))
            .Should()
            .BeFalse();
        await AssertAuditOutcomeAsync(result.SweepId, tenantId, expectedAffected: 1);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Sweep_And_DryRun_Preserve_Connection_Ownership(
        bool dryRun,
        bool preOpened
    )
    {
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var asOf = new DateTimeOffset(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

        await Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            var engine = services.GetRequiredService<RetentionSweepEngine>();

            if (preOpened)
            {
                await db.Database.OpenConnectionAsync();
            }

            if (dryRun)
            {
                await engine.DryRunAsync(
                    tenant,
                    asOf,
                    SweepTriggerKind.Manual,
                    SweepEntityScope.TenantedOnly
                );
            }
            else
            {
                await engine.SweepAsync(
                    tenant,
                    asOf,
                    SweepTriggerKind.Manual,
                    SweepEntityScope.TenantedOnly
                );
            }

            db.Database.GetDbConnection().State.Should().Be(
                preOpened ? ConnectionState.Open : ConnectionState.Closed
            );
        });
    }

    [Fact]
    public async Task SweepAsync_Retires_The_Whole_Backlog_When_BatchSize_Is_Smaller_Than_The_Backlog()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            for (var i = 0; i < 3; i++)
            {
                db.Notes.Add(
                    new Note
                    {
                        Id = Guid.NewGuid(),
                        TenantId = tenantId,
                        CreatedAt = asOf.AddDays(-120),
                        Body = $"batch-delete-{i}",
                    }
                );
            }

            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "batch-keep-fresh",
                }
            );
            await db.SaveChangesAsync();
        }

        using var sweepHost = new CohortTestHost(
            GetConnectionString(),
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["short-lived"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    ),
                }
            ),
            new Dictionary<string, string?>
            {
                [$"{Cohort.Hosting.CohortOptions.SectionName}:SweepBatchSize"] = "1",
            }
        );

        var result = await sweepHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 3)
            );
        result.EntityFailures.Should().BeEmpty();

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.Select(note => note.Body).ToListAsync())
            .Should()
            .Equal("batch-keep-fresh");
    }

    private sealed class OpaqueSoftDeleteRuleResolver : ITestRetentionRule
    {
        public Task<RetentionRule> ResolveAsync(
            RetentionResolutionContext ctx,
            CancellationToken ct
        )
        {
            return Task.FromResult(new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete));
        }
    }

    private sealed class StaticCategoryRepository(
        IReadOnlyDictionary<string, ITestRetentionRule> resolvers
    ) : ITestRetentionRuleProvider
    {
        private static readonly ITestRetentionRule ExemptFallback =
            new StaticTestRetentionRule(
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Exempt)
            );

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct)
        {
            return resolvers.TryGetValue(category, out var resolver)
                ? Task.FromResult<ITestRetentionRule?>(resolver)
                : Task.FromResult<ITestRetentionRule?>(ExemptFallback);
        }
    }

    private async Task AssertAuditOutcomeAsync(Guid sweepId, Guid tenantId, long expectedAffected)
    {
        await using var connection = new Npgsql.NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT \"SettledAt\", \"TotalAffected\" FROM \"sweep_run\" WHERE \"SweepId\" = @sweepId AND \"TenantId\" = @tenantId";
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("tenantId", tenantId);

        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.IsDBNull(0).Should().BeFalse();
        reader.GetInt64(1).Should().Be(expectedAffected);
    }

    private sealed class TransactionAssertingResolver(SampleDbContext db, RetentionRule rule)
        : ITestRetentionRule
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
