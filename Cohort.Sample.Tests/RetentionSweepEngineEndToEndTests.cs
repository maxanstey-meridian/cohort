using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Sweep;
using Cohort.Sample.Entities;

using System.Data.Common;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

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
                    ["soft-delete"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    ),
                    ["anonymise"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                }
            )
        );

        var result = await sweepHost.RunSweepAsync(
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
                    ["soft-delete"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    ),
                    ["anonymise"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                }
            )
        );

        var result = await sweepHost.RunSweepAsync(
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
        var repository = new StaticCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["short-lived"] = resolver,
                ["soft-delete"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
                ["anonymise"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
            }
        );
        var engine = new RetentionSweepEngine(
            db,
            new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions())),
            repository,
            new RetentionStartupValidator(db, repository, new RetentionEntryBuilder(new CohortConventions())),
            new NoOpRetentionAuditWriter(),
            [new PurgeSweepStrategy(), new SoftDeleteSweepStrategy(), new AnonymiseSweepStrategy()]
        );

        var result = await engine.SweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        resolver.SawNoTransactionDuringResolve.Should().BeTrue();
        result.Counts.Should().Contain(
            count =>
                count.EntityType == typeof(Note)
                && count.Category == "short-lived"
                && count.Affected == 1
        );
    }

    [Fact]
    public async Task SweepAsync_Passes_The_Active_Db_Transaction_To_The_Strategy()
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
                Body = "track-transaction",
            }
        );
        await db.SaveChangesAsync();

        var strategy = new TransactionCapturingSweepStrategy(db);
        var repository = new StaticCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["short-lived"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
                ["soft-delete"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
                ["anonymise"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
            }
        );
        var engine = new RetentionSweepEngine(
            db,
            new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions())),
            repository,
            new RetentionStartupValidator(db, repository, new RetentionEntryBuilder(new CohortConventions())),
            new NoOpRetentionAuditWriter(),
            [strategy, new SoftDeleteSweepStrategy(), new AnonymiseSweepStrategy()]
        );

        var result = await engine.SweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        strategy.ReceivedTransaction.Should().NotBeNull();
        strategy.ReceivedTransaction.Should().BeSameAs(strategy.CurrentEfTransactionAtExecution);
        result.Counts.Should().Contain(
            count =>
                count.EntityType == typeof(Note)
                && count.Category == "short-lived"
                && count.Affected == 0
        );
    }

    [Fact]
    public async Task Sweep_Engine_Rejects_Runtime_Strategies_Without_A_Registered_Sweep_Implementation()
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
                Body = "must-remain",
            }
        );
        await db.SaveChangesAsync();

        var repository = new StaticCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["short-lived"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
                ["soft-delete"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
                ["anonymise"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
            }
        );
        var engine = new RetentionSweepEngine(
            db,
            new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions())),
            repository,
            new RetentionStartupValidator(db, repository, new RetentionEntryBuilder(new CohortConventions())),
            new NoOpRetentionAuditWriter(),
            [new PurgeSweepStrategy(), new SoftDeleteSweepStrategy()]
        );

        var act = () => engine.SweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await act
            .Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("*not registered for sweep execution*");

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

    private sealed class TransactionCapturingSweepStrategy(
        SampleDbContext db
    ) : IRetentionSweepStrategy
    {
        public Strategy HandlesStrategy => Strategy.Purge;

        public DbTransaction? ReceivedTransaction { get; private set; }
        public DbTransaction? CurrentEfTransactionAtExecution { get; private set; }

        public Task<int> PreviewAsync(
            RetentionEntry entry,
            RetentionRule rule,
            RetentionResolutionContext ctx,
            DbConnection conn,
            CancellationToken ct
        )
        {
            throw new NotSupportedException();
        }

        public Task<SweepExecutionResult> SweepAsync(
            RetentionEntry entry,
            RetentionRule rule,
            RetentionResolutionContext ctx,
            DbConnection conn,
            DbTransaction transaction,
            CancellationToken ct
        )
        {
            ReceivedTransaction = transaction;
            CurrentEfTransactionAtExecution = db.Database.CurrentTransaction?.GetDbTransaction();
            return Task.FromResult<SweepExecutionResult>(new([], 0));
        }

        public Task<SweepExecutionResult> EraseAsync(
            RetentionEntry entry,
            RetentionRule rule,
            ErasureSubjectMatch match,
            TenantContext tenant,
            DateTimeOffset now,
            DbConnection conn,
            DbTransaction transaction,
            CancellationToken ct
        )
        {
            throw new NotSupportedException();
        }
    }

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }
}
