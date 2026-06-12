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

        result.Counts.Should().HaveCount(8);
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

        result.Counts.Should().HaveCount(8);
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
            new RetentionStartupValidator(
                db,
                repository,
                new RetentionEntryBuilder(new CohortConventions()),
                CreateSampleFactories()
            ),
            new NoOpRetentionAuditWriter(),
            [
                new PurgeSweepStrategy(),
                new SoftDeleteSweepStrategy(),
                new AnonymiseSweepStrategy(db, CreateSampleFactories())
            ]
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
                new Dictionary<string, IRetentionRuleResolver>
                {
                    ["short-lived"] = new StaticRetentionRuleResolver(
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

        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 3)
        );
        result.EntityFailures.Should().BeEmpty();

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.Select(note => note.Body).ToListAsync())
            .Should()
            .Equal("batch-keep-fresh");
    }

    [Fact]
    public async Task SweepAsync_Records_Entity_Failures_And_Continues_With_Remaining_Entities()
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
                    Body = "survives-other-entity-failure",
                }
            );
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "kept@example.org",
                    GivenName = "Kept",
                    Surname = "Untouched",
                    Notes = "entity whose category misresolves at runtime",
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
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    ),
                    // Opaque deferred resolver: passes startup validation, then resolves
                    // SoftDelete for an entity with no IsDeleted member — a sweep-time failure.
                    ["anonymise"] = new OpaqueSoftDeleteRuleResolver(),
                }
            )
        );

        var result = await sweepHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.EntityFailures.Should().ContainSingle(failure =>
            failure.Contains(nameof(AnonymisedContact))
        );
        result.Counts.Should().Contain(
            count => count.EntityType == typeof(Note) && count.Affected == 1
        );
        result.Counts.Should().NotContain(count => count.EntityType == typeof(AnonymisedContact));

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Body == "survives-other-entity-failure"))
                .Should()
                .BeFalse();
            (await verify.AnonymisedContacts.AnyAsync(contact => contact.TenantId == tenantId))
                .Should()
                .BeTrue();
        }

        var run = await LoadSweepRunFailureAsync(result.SweepId);
        run.CompletedAt.Should().NotBeNull();
        run.FailedAt.Should().NotBeNull();
        run.Error.Should().Contain(nameof(AnonymisedContact));
    }

    private async Task<(DateTimeOffset? CompletedAt, DateTimeOffset? FailedAt, string? Error)>
        LoadSweepRunFailureAsync(Guid sweepId)
    {
        await using var connection = new Npgsql.NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT "CompletedAt", "FailedAt", "Error"
            FROM "sweep_run"
            WHERE "SweepId" = @sweepId
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);

        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        return (
            reader.IsDBNull(0) ? null : reader.GetFieldValue<DateTimeOffset>(0),
            reader.IsDBNull(1) ? null : reader.GetFieldValue<DateTimeOffset>(1),
            reader.IsDBNull(2) ? null : reader.GetString(2)
        );
    }

    private sealed class OpaqueSoftDeleteRuleResolver : IRetentionRuleResolver
    {
        public Task<RetentionRule> ResolveAsync(
            RetentionResolutionContext ctx,
            CancellationToken ct
        )
        {
            return Task.FromResult(new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete));
        }
    }

    [Fact]
    public async Task SweepAsync_Refuses_To_Mutate_When_DryRun_Is_Enabled()
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
                Body = "dry-run-guarded",
            }
        );
        await db.SaveChangesAsync();

        var repository = new StaticCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["short-lived"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
            }
        );
        var engine = new RetentionSweepEngine(
            db,
            new RetentionRegistry(db, new RetentionEntryBuilder(new CohortConventions())),
            repository,
            new RetentionStartupValidator(
                db,
                repository,
                new RetentionEntryBuilder(new CohortConventions()),
                CreateSampleFactories()
            ),
            new NoOpRetentionAuditWriter(),
            [
                new PurgeSweepStrategy(),
                new SoftDeleteSweepStrategy(),
                new AnonymiseSweepStrategy(db, CreateSampleFactories())
            ],
            new StaticEngineOptionsMonitor(new CohortOptions { DryRun = true })
        );

        var act = async () =>
            await engine.SweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                asOf
            );

        await act
            .Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("*DryRun*refuses*");
        (await db.Notes.AnyAsync(note => note.Body == "dry-run-guarded")).Should().BeTrue();
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
            new RetentionStartupValidator(
                db,
                repository,
                new RetentionEntryBuilder(new CohortConventions()),
                CreateSampleFactories()
            ),
            new NoOpRetentionAuditWriter(),
            [
                strategy,
                new SoftDeleteSweepStrategy(),
                new AnonymiseSweepStrategy(db, CreateSampleFactories())
            ]
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
            new RetentionStartupValidator(
                db,
                repository,
                new RetentionEntryBuilder(new CohortConventions()),
                CreateSampleFactories()
            ),
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
        private static readonly IRetentionRuleResolver ExemptFallback = new StaticRetentionRuleResolver(
            new RetentionRule(TimeSpan.FromDays(30), Strategy.Exempt)
        );

        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            return resolvers.TryGetValue(category, out var resolver)
                ? Task.FromResult<IRetentionRuleResolver?>(resolver)
                : Task.FromResult<IRetentionRuleResolver?>(ExemptFallback);
        }
    }

    private static IAnonymiseValueFactory[] CreateSampleFactories()
    {
        return [new GuidTombstoneFactory(), new OriginalValueTombstoneFactory()];
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
            CancellationToken ct,
            SweepMutationContext? execution = null
        )
        {
            ReceivedTransaction = transaction;
            CurrentEfTransactionAtExecution = db.Database.CurrentTransaction?.GetDbTransaction();
            return Task.FromResult<SweepExecutionResult>(new([], 0));
        }

        public Task<int> CountHeldAsync(
            RetentionEntry entry,
            RetentionRule rule,
            RetentionResolutionContext ctx,
            DbConnection conn,
            CancellationToken ct
        )
        {
            return Task.FromResult(0);
        }



        public Task<int> CountHeldForEraseAsync(
            RetentionEntry entry,
            RetentionRule rule,
            ErasureSubjectPredicate predicate,
            TenantContext tenant,
            DateTimeOffset now,
            DbConnection conn,
            CancellationToken ct
        )
        {
            return Task.FromResult(0);
        }

        public Task<int> PreviewEraseAsync(
            RetentionEntry entry,
            RetentionRule rule,
            ErasureSubjectPredicate predicate,
            TenantContext tenant,
            DateTimeOffset now,
            DbConnection conn,
            CancellationToken ct
        )
        {
            throw new NotSupportedException();
        }

        public Task<SweepExecutionResult> EraseAsync(
            RetentionEntry entry,
            RetentionRule rule,
            ErasureSubjectPredicate predicate,
            TenantContext tenant,
            DateTimeOffset now,
            DbConnection conn,
            DbTransaction transaction,
            CancellationToken ct,
            SweepMutationContext? execution = null
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

    private sealed class StaticEngineOptionsMonitor(CohortOptions currentValue)
        : Microsoft.Extensions.Options.IOptionsMonitor<CohortOptions>
    {
        public CohortOptions CurrentValue => currentValue;

        public CohortOptions Get(string? name)
        {
            return currentValue;
        }

        public IDisposable? OnChange(Action<CohortOptions, string?> listener)
        {
            return null;
        }
    }
}
