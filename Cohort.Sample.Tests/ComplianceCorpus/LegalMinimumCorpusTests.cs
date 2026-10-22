using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

using Npgsql;

namespace Cohort.Sample.Tests.ComplianceCorpus;

public sealed class LegalMinimumCorpusTests(PostgresFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Ordinary_sweep_uses_the_greater_of_period_and_legal_minimum()
    {
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var eligibleId = Guid.NewGuid();
        var boundaryId = Guid.NewGuid();
        var periodOnlyId = Guid.NewGuid();
        var nullAnchorId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = eligibleId, TenantId = tenantId, CreatedAt = now.AddDays(-91), Body = "eligible" },
                new Note { Id = boundaryId, TenantId = tenantId, CreatedAt = now.AddDays(-90), Body = "legal boundary" },
                new Note { Id = periodOnlyId, TenantId = tenantId, CreatedAt = now.AddDays(-45), Body = "past period" }
            );
            db.NullableAnchorEvents.Add(new NullableAnchorEvent
            {
                Id = nullAnchorId,
                TenantId = tenantId,
                OccurredAt = null,
                Payload = "cannot establish minimum age",
            });
            await db.SaveChangesAsync();
        }

        var repository = new LegalMinimumCategoryRepository();
        using var host = new CohortTestHost(ConnectionString, repository);
        var result = await host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now
        );

        result.Counts.Should().Contain(count =>
            count.EntityType == typeof(Note) && count.Affected == 1
        );
        result.Counts.Should().Contain(count =>
            count.EntityType == typeof(NullableAnchorEvent)
            && count.Affected == 0
            && count.NullAnchorCount == 1
        );
        repository.ObservedContexts.Should().Contain(context =>
            context.Category == "short-lived"
            && context.Tenant.Id == tenantId
            && context.Now == now
        );
        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == eligibleId)).Should().BeFalse();
            (await verify.Notes.AnyAsync(note => note.Id == boundaryId)).Should().BeTrue();
            (await verify.Notes.AnyAsync(note => note.Id == periodOnlyId)).Should().BeTrue();
            (await verify.NullableAnchorEvents.AnyAsync(row => row.Id == nullAnchorId)).Should().BeTrue();
        }

        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT "Category", "RetentionEntityId", "TenantId", "Strategy", "ResolvedMonths", "ResolvedDays", "Affected", "NullAnchorCount"
            FROM "sweep_run_entity_summary"
            WHERE "SweepId" = @sweepId AND "Category" = 'nullable-anchor-purge'
            """;
        command.Parameters.AddWithValue("sweepId", result.SweepId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetString(0).Should().Be("nullable-anchor-purge");
        reader.GetGuid(1).Should().Be(Guid.Parse("314fd4f7-f771-4b94-ab6e-7fc0a09a6ef5"));
        reader.GetGuid(2).Should().Be(tenantId);
        reader.GetInt32(3).Should().Be((int)Strategy.Purge);
        reader.GetInt32(4).Should().Be(0);
        reader.GetInt32(5).Should().Be(90);
        reader.GetInt64(6).Should().Be(0);
        reader.GetInt64(7).Should().Be(1);
    }

    [Fact]
    public async Task Tenant_and_logical_time_change_the_resolved_rule_outcome_and_audited_period()
    {
        var longTenantId = Guid.NewGuid();
        var shortTenantId = Guid.NewGuid();
        var clockTenantId = Guid.NewGuid();
        var tenantNow = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var earlyNow = new DateTimeOffset(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);
        var laterNow = new DateTimeOffset(2026, 7, 1, 12, 0, 0, TimeSpan.Zero);
        var longRecordId = Guid.NewGuid();
        var shortRecordId = Guid.NewGuid();
        var clockRecordId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = longRecordId,
                    TenantId = longTenantId,
                    CreatedAt = tenantNow.AddDays(-45),
                    Body = "tenant-specific long retention",
                },
                new Note
                {
                    Id = shortRecordId,
                    TenantId = shortTenantId,
                    CreatedAt = tenantNow.AddDays(-45),
                    Body = "tenant-specific short retention",
                },
                new Note
                {
                    Id = clockRecordId,
                    TenantId = clockTenantId,
                    CreatedAt = new DateTimeOffset(2026, 5, 15, 12, 0, 0, TimeSpan.Zero),
                    Body = "logical-time-specific retention",
                }
            );
            await db.SaveChangesAsync();
        }

        var provider = new ContextualRuleProvider();
        using var host = new CohortTestHost(ConnectionString, provider);
        var longResult = await host.RunSweepAsync(
            CreateTenant(longTenantId, "long"),
            tenantNow
        );
        var shortResult = await host.RunSweepAsync(
            CreateTenant(shortTenantId, "short"),
            tenantNow
        );
        var earlyResult = await host.RunSweepAsync(
            CreateTenant(clockTenantId, "clock"),
            earlyNow
        );

        await using (var verifyProtected = Host.CreateDbContext())
        {
            (await verifyProtected.Notes.AnyAsync(row => row.Id == longRecordId)).Should().BeTrue();
            (await verifyProtected.Notes.AnyAsync(row => row.Id == shortRecordId)).Should().BeFalse();
            (await verifyProtected.Notes.AnyAsync(row => row.Id == clockRecordId)).Should().BeTrue();
        }

        var laterResult = await host.RunSweepAsync(
            CreateTenant(clockTenantId, "clock"),
            laterNow
        );

        await using (var verifyLater = Host.CreateDbContext())
        {
            (await verifyLater.Notes.AnyAsync(row => row.Id == clockRecordId)).Should().BeFalse();
        }
        (await ReadResolvedPeriodAsync(longResult.SweepId)).Should().Be((0, 90));
        (await ReadResolvedPeriodAsync(shortResult.SweepId)).Should().Be((0, 30));
        (await ReadResolvedPeriodAsync(earlyResult.SweepId)).Should().Be((0, 60));
        (await ReadResolvedPeriodAsync(laterResult.SweepId)).Should().Be((0, 30));
        provider.ObservedContexts.Should().Contain(context =>
            context.Tenant.Id == clockTenantId && context.Now == earlyNow
        );
        provider.ObservedContexts.Should().Contain(context =>
            context.Tenant.Id == clockTenantId && context.Now == laterNow
        );
    }

    [Fact]
    public async Task A_legal_minimum_in_years_keeps_a_row_through_its_anniversary_in_the_host_time_zone()
    {
        var tenantId = Guid.NewGuid();
        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());
        // Seven years from 1 Mar 2027 cross two leap days: as 2,555 days these rows went on 27 Feb 2034.
        var marchDayBeforeId = Guid.NewGuid();
        var marchAnniversaryId = Guid.NewGuid();
        // 00:30 BST on 2 Jul 2027, still 1 Jul in UTC.
        var julyAnniversaryId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = marchDayBeforeId, TenantId = tenantId, CreatedAt = new DateTimeOffset(2027, 2, 28, 23, 30, 0, TimeSpan.Zero), Body = "28 Feb 2027" },
                new Note { Id = marchAnniversaryId, TenantId = tenantId, CreatedAt = new DateTimeOffset(2027, 3, 1, 0, 30, 0, TimeSpan.Zero), Body = "1 Mar 2027" },
                new Note { Id = julyAnniversaryId, TenantId = tenantId, CreatedAt = new DateTimeOffset(2027, 7, 1, 23, 30, 0, TimeSpan.Zero), Body = "2 Jul 2027 in London" }
            );
            await db.SaveChangesAsync();
        }

        using var host = new CohortTestHost(
            ConnectionString,
            new CalendarLegalMinimumRepository(),
            new Dictionary<string, string?> { ["Cohort:TimeZone"] = "Europe/London" }
        );

        // Late on 1 Mar 2034: the seventh anniversary of 1 Mar 2027, not of 28 Feb.
        var march = await host.RunSweepAsync(tenant, new DateTimeOffset(2034, 3, 1, 23, 30, 0, TimeSpan.Zero));
        // 2 Jul 2034 in London keeps the row anchored on 2 Jul 2027 there.
        await host.RunSweepAsync(tenant, new DateTimeOffset(2034, 7, 2, 12, 0, 0, TimeSpan.Zero));

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == marchDayBeforeId)).Should().BeFalse();
            (await verify.Notes.AnyAsync(note => note.Id == marchAnniversaryId)).Should().BeFalse();
            (await verify.Notes.AnyAsync(note => note.Id == julyAnniversaryId)).Should().BeTrue();
        }

        await host.RunSweepAsync(tenant, new DateTimeOffset(2034, 7, 3, 12, 0, 0, TimeSpan.Zero));

        await using (var verifyNextDay = Host.CreateDbContext())
        {
            (await verifyNextDay.Notes.AnyAsync(note => note.Id == julyAnniversaryId)).Should().BeFalse();
        }
        march.Counts.Should().Contain(count => count.EntityType == typeof(Note) && count.Affected == 1);
        // The legal minimum set the cutoff, and it is stored as 84 months, not as days.
        (await ReadResolvedPeriodAsync(march.SweepId)).Should().Be((84, 0));
    }

    private static TenantContext CreateTenant(Guid tenantId, string profile) =>
        new(tenantId, "uk", new Dictionary<string, string> { ["profile"] = profile });

    private async Task<(int Months, int Days)> ReadResolvedPeriodAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT \"ResolvedMonths\", \"ResolvedDays\" FROM \"sweep_run_entity_summary\" WHERE \"SweepId\" = @sweepId AND \"Category\" = 'short-lived'";
        command.Parameters.AddWithValue("sweepId", sweepId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        return (reader.GetInt32(0), reader.GetInt32(1));
    }

    private sealed class LegalMinimumCategoryRepository : ITestRetentionRuleProvider
    {
        public List<RetentionResolutionContext> ObservedContexts { get; } = [];

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct)
        {
            ITestRetentionRule resolver = category switch
            {
                "short-lived" or "nullable-anchor-purge" => new ContextRecordingResolver(
                    ObservedContexts
                ),
                _ => new StaticTestRetentionRule(
                    new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Exempt)
                ),
            };
            return Task.FromResult<ITestRetentionRule?>(resolver);
        }
    }

    private sealed class CalendarLegalMinimumRepository : ITestRetentionRuleProvider
    {
        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct) =>
            Task.FromResult<ITestRetentionRule?>(
                new StaticTestRetentionRule(
                    category == "short-lived"
                        ? new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge, new RetentionPeriod(Years: 7))
                        : new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Exempt)
                )
            );
    }

    private sealed class ContextRecordingResolver(List<RetentionResolutionContext> observedContexts)
        : ITestRetentionRule
    {
        public Task<RetentionRule> ResolveAsync(
            RetentionResolutionContext ctx,
            CancellationToken ct
        )
        {
            observedContexts.Add(ctx);
            return Task.FromResult(
                new RetentionRule(
                    new RetentionPeriod(Days: 30),
                    Strategy.Purge,
                    new RetentionPeriod(Days: 90)
                )
            );
        }
    }

    private sealed class ContextualRuleProvider : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider _inner = new();

        public List<RetentionResolutionContext> ObservedContexts { get; } = [];

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            _inner.GetCapabilities(category);

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            ObservedContexts.Add(context);
            if (context.Category != "short-lived")
            {
                return _inner.ResolveAsync(context, ct);
            }

            context.Tenant.Tags.TryGetValue("profile", out var profile);
            var period = profile switch
            {
                "long" => new RetentionPeriod(Days: 90),
                "clock" when context.Now < new DateTimeOffset(2026, 7, 1, 0, 0, 0, TimeSpan.Zero) =>
                    new RetentionPeriod(Days: 60),
                _ => new RetentionPeriod(Days: 30),
            };
            return Task.FromResult<RetentionRule?>(new RetentionRule(period, Strategy.Purge));
        }
    }
}
