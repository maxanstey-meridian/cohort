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
                        new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge)
                    );
                    return new StaticCategoryRepository(
                        new Dictionary<string, ITestRetentionRule>
                        {
                            ["short-lived"] = resolver,
                            ["soft-delete"] = new StaticTestRetentionRule(
                                new RetentionRule(new RetentionPeriod(Days: 30), Strategy.SoftDelete)
                            ),
                            ["anonymise"] = new StaticTestRetentionRule(
                                new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Anonymise)
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
                await engine.RunAsync(
                    tenant,
                    asOf,
                    SweepTriggerKind.Manual,
                    SweepEntityScope.TenantedOnly,
                    dryRun: true
                );
            }
            else
            {
                await engine.RunAsync(
                    tenant,
                    asOf,
                    SweepTriggerKind.Manual,
                    SweepEntityScope.TenantedOnly,
                    dryRun: false
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
                        new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge)
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

    [Fact]
    public async Task A_Real_Sweep_Refuses_A_Time_Later_Than_The_Database_Clock_Before_Any_Run()
    {
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var noteId = Guid.NewGuid();
        var databaseNow = await LoadDatabaseNowAsync();
        await using (var db = Host.CreateDbContext())
        {
            // 30-day purge: not due for another ten days by the database's clock.
            db.Notes.Add(new Note { Id = noteId, TenantId = tenant.Id, CreatedAt = databaseNow.AddDays(-20), Body = "not-yet-due" });
            await db.SaveChangesAsync();
        }

        var lookAhead = DateTimeOffset.UtcNow.AddDays(15);
        var sweep = () => Host.RunSweepAsync(tenant, lookAhead);

        (await sweep.Should().ThrowAsync<ArgumentOutOfRangeException>())
            .Which.Message.Should().StartWith("A run that changes rows can't use a time later than the database's");
        (await CountRunsAsync(tenant.Id)).Should().Be(0);

        // A dry run may look ahead: it counts the note and changes nothing.
        var dryRun = await Host.RunWithServicesAsync(services =>
            services.GetRequiredService<IRetentionSweep>().ExecuteAsync(
                RetentionSweepRequest.Tenanted(tenant, lookAhead, dryRun: true)
            )
        );

        dryRun.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
            .Which.Affected.Should().Be(1);
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
    }

    [Fact]
    public async Task A_Sweep_Without_A_Time_Uses_The_Database_Clock()
    {
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var databaseNow = await LoadDatabaseNowAsync();
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = Guid.NewGuid(), TenantId = tenant.Id, CreatedAt = databaseNow.AddDays(-40), Body = "due" },
                new Note { Id = Guid.NewGuid(), TenantId = tenant.Id, CreatedAt = databaseNow.AddDays(-20), Body = "not-yet-due" }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(tenant);

        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note))
            .Which.Affected.Should().Be(1);
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.Where(note => note.TenantId == tenant.Id).Select(note => note.Body).ToListAsync())
            .Should().Equal("not-yet-due");
    }

    [Fact]
    public async Task Every_Time_A_Sweep_Records_Or_Reports_Comes_From_The_Database_Clock()
    {
        // An explicit earlier time sets the cutoff only: what the run records is when it ran.
        var tenant = new TenantContext(Guid.NewGuid(), "uk", new Dictionary<string, string>());
        var asOf = new DateTimeOffset(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = Guid.NewGuid(), TenantId = tenant.Id, CreatedAt = asOf.AddDays(-120), Body = "first-batch" },
                new Note { Id = Guid.NewGuid(), TenantId = tenant.Id, CreatedAt = asOf.AddDays(-120), Body = "second-batch" }
            );
            await db.SaveChangesAsync();
        }

        var observer = new RecordingObserver();
        using var sweepHost = new CohortTestHost(
            GetConnectionString(),
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["short-lived"] = new StaticTestRetentionRule(
                        new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge, AuditRowDetail: AuditRowDetail.PerRow)
                    ),
                }
            ),
            new Dictionary<string, string?>
            {
                [$"{Cohort.Hosting.CohortOptions.SectionName}:SweepBatchSize"] = "1",
            },
            services => services.AddSingleton<IRetentionAuditObserver>(observer)
        );

        var before = await LoadDatabaseNowAsync();
        var result = await sweepHost.RunSweepAsync(tenant, asOf);
        var after = await LoadDatabaseNowAsync();

        var events = observer.Events.Where(evt => evt.SweepId == result.SweepId).ToList();
        events.Should().OnlyContain(evt => evt.At >= before && evt.At <= after);
        var started = events.OfType<SweepEvent.Started>().Single();
        var completed = events.OfType<SweepEvent.Completed>().Single();
        result.StartedAt.Should().Be(started.At);
        result.CompletedAt.Should().Be(completed.At);
        completed.Duration.Should().Be(completed.At - started.At);
        var rowDetails = events.OfType<SweepEvent.RowDetail>().ToList();
        rowDetails.Should().HaveCount(2);
        rowDetails[1].At.Should().BeOnOrAfter(rowDetails[0].At);

        // The ledger stores exactly what observers were told. Where the host's clock is finer
        // than PostgreSQL's microseconds, a host-clock time would not round-trip unchanged.
        await using var verify = Host.CreateDbContext();
        var run = await verify.Database
            .SqlQuery<StoredRunTimes>($"""SELECT "StartedAt", "SettledAt", "Duration" FROM "sweep_run" WHERE "SweepId" = {result.SweepId}""")
            .SingleAsync();
        run.Should().Be(new StoredRunTimes(started.At, completed.At, completed.Duration));
        (await verify.Database
            .SqlQuery<DateTimeOffset>($"""SELECT "At" AS "Value" FROM "sweep_run_entity_summary" WHERE "SweepId" = {result.SweepId} AND "RetentionEntityId" = {Note.RetentionIdentity}""")
            .SingleAsync()).Should().Be(events.OfType<SweepEvent.EntitySummary>().Single(summary => summary.EntityType == typeof(Note)).At);
        (await verify.Database
            .SqlQuery<DateTimeOffset>($"""SELECT "At" AS "Value" FROM "sweep_run_row_detail" WHERE "SweepId" = {result.SweepId} ORDER BY "At" """)
            .ToListAsync()).Should().Equal(rowDetails.Select(rowDetail => rowDetail.At));
    }

    private sealed record StoredRunTimes(DateTimeOffset StartedAt, DateTimeOffset? SettledAt, TimeSpan? Duration);

    private sealed class RecordingObserver : IRetentionAuditObserver
    {
        private readonly System.Collections.Concurrent.ConcurrentQueue<SweepEvent> events = new();

        public IReadOnlyList<SweepEvent> Events => [.. events];

        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            events.Enqueue(evt);
            return Task.CompletedTask;
        }
    }

    private async Task<DateTimeOffset> LoadDatabaseNowAsync()
    {
        await using var db = Host.CreateDbContext();
        return await db.Database.SqlQuery<DateTimeOffset>($"""SELECT statement_timestamp() AS "Value" """).SingleAsync();
    }

    private async Task<long> CountRunsAsync(Guid tenantId)
    {
        await using var db = Host.CreateDbContext();
        return await db.Database
            .SqlQuery<long>($"""SELECT COUNT(*) AS "Value" FROM "sweep_run" WHERE "TenantId" = {tenantId}""")
            .SingleAsync();
    }

    private sealed class StaticCategoryRepository(
        IReadOnlyDictionary<string, ITestRetentionRule> resolvers
    ) : ITestRetentionRuleProvider
    {
        private static readonly ITestRetentionRule ExemptFallback =
            new StaticTestRetentionRule(
                new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Exempt)
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
