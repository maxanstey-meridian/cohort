using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests;

// ─── End-to-end test (copied from PurgeSweepStrategyTests) ──────────────────
//
// Readiness asks PostgreSQL to EXPLAIN every host-table statement a run can send. That is only
// worth anything if the statements readiness builds are the statements a run sends, so this
// proves it from the server's side: with pg_stat_statements on, a run's statement and
// readiness's EXPLAIN of the same text both appear in the view (the EXPLAIN as "EXPLAIN <the
// same text>"), and every host-table statement a real run sent must have its EXPLAIN. When a
// statement is missing from readiness's set, the failure names its text.
//
// Each run group gets its own fresh database, seeded first, then pg_stat_statements is reset for
// that database, readiness runs, and the operations run: batch size 1 with several due rows
// (the cursor statements), dry runs and live sweeps, previews, erasures with and without a legal
// minimum, holds, a deletion check, the worker's tenant-coverage scan, and hosts with a row
// handler and with a dependents step (the row-by-row statements). Host SQL a step runs starts
// "/* host */" and is excluded: it is the host's, not Cohort's.
// ────────────────────────────────────────────────────────────────────────────

[Collection("Integration")]
public sealed class ReadinessStatementCoverageTests(PostgresFixture fixture)
{
    private const string HostComment = "/* host */";
    private const string Explain = "EXPLAIN ";
    private const string CanonicalText = "SELECT CAST(";
    private static readonly DateTimeOffset AsOf = DateTimeOffset.UtcNow;

    [Fact]
    public async Task Every_sample_statement_the_plain_runs_send_is_explained_by_readiness()
    {
        await using var database = await MigratedSampleDatabaseAsync();
        var tenants = new CoverageTenants();
        using var host = new CohortTestHost(
            database.ConnectionString,
            new CoverageRuleProvider(tenants),
            BatchSizeOne
        );
        await SeedAsync(database, tenants);

        await StartRecordingAsync(database);
        await host.RunWithServicesAsync(services =>
            services
                .GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None)
        );

        // Sweeps and previews: S1 to S7, with the cursor on the second batch of each entity.
        (await SweepAsync(host, tenants.Swept, dryRun: true))
            .EntityFailures.Should()
            .BeEmpty();
        await host.RunPreviewAsync(Tenant(tenants.Swept), AsOf);
        (await SweepAsync(host, tenants.Swept, dryRun: false)).EntityFailures.Should().BeEmpty();
        (
            await host.RunWithServicesAsync(services =>
                services
                    .GetRequiredService<IRetentionSweep>()
                    .ExecuteAsync(RetentionSweepRequest.Tenantless(AsOf), CancellationToken.None)
            )
        )
            .EntityFailures.Should()
            .BeEmpty();

        // Erasure: each kind, with a legal minimum (a cutoff) and without (none), the tenantless
        // entities, and an Exempt rule, which only counts the subject's rows.
        foreach (var kind in new[] { "user", "person" })
        {
            await EraseAsync(host, tenants.WithLegalMinimum, kind, tenants.Subject);
            await EraseAsync(host, tenants.WithoutLegalMinimum, kind, tenants.Subject);
        }

        await EraseAsync(host, tenants.Exempted, "user", tenants.Subject);
        (
            await host.RunWithServicesAsync(services =>
                services
                    .GetRequiredService<IRetentionErasureService>()
                    .ExecuteAsync(
                        RetentionErasureRequest.Tenantless(
                            new ErasureScope("user", tenants.Subject),
                            AsOf
                        ),
                        CancellationToken.None
                    )
            )
        )
            .EntityFailures.Should()
            .BeEmpty();

        // Holds, and the deletion check.
        await host.RunWithServicesAsync(async services =>
        {
            var holds = services.GetRequiredService<IRetentionHoldsRepository>();
            var held = await FirstNoteIdAsync(database, tenants.Held);
            var free = await FirstNoteIdAsync(database, tenants.Swept);
            var holdId = Guid.NewGuid();
            await holds.CreateAsync(
                new RetentionHoldRequest(
                    holdId,
                    Note.RetentionIdentity,
                    held.ToString(),
                    tenants.Held,
                    "coverage",
                    AsOf
                ),
                CancellationToken.None
            );
            await holds.ListActiveAsync(CancellationToken.None);
            (
                await holds.HasActiveHoldAsync(
                    Note.RetentionIdentity,
                    held.ToString(),
                    tenants.Held,
                    CancellationToken.None
                )
            )
                .Should()
                .BeTrue();
            (
                await holds.ListCoveringAsync(
                    Note.RetentionIdentity,
                    held.ToString(),
                    tenants.Held,
                    CancellationToken.None
                )
            )
                .Should()
                .ContainSingle();
            (
                await holds.ListCoveringSubjectAsync(
                    "user",
                    tenants.Subject,
                    tenants.Held,
                    CancellationToken.None
                )
            )
                .Should()
                .ContainSingle();
            await holds.ListCoveringSubjectAsync(
                "user",
                tenants.Subject,
                null,
                CancellationToken.None
            );

            var deletion = services.GetRequiredService<IRetentionDeletion>();
            (
                await deletion.ExecuteAsync(
                    [new RetentionTarget(Note.RetentionIdentity, held.ToString(), tenants.Held)],
                    _ => Task.CompletedTask
                )
            )
                .Should()
                .Be(RetentionDeletionOutcome.Protected);
            (
                await deletion.ExecuteAsync(
                    [new RetentionTarget(Note.RetentionIdentity, free.ToString(), tenants.Swept)],
                    _ => Task.CompletedTask
                )
            )
                .Should()
                .Be(RetentionDeletionOutcome.Executed);
            await holds.RemoveAsync(holdId, AsOf, CancellationToken.None);
        });

        // The worker's tenant-coverage scan.
        await host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            await db.Database.OpenConnectionAsync();
            await RetentionTenantCoverage.FindGapsAsync(
                db,
                services.GetRequiredService<RetentionRegistry>(),
                services
                    .GetRequiredService<RetentionRuntimeReadinessValidator>()
                    .ValidatedCapabilities,
                [tenants.Swept],
                CancellationToken.None
            );
        });

        await AssertEveryRunStatementIsExplainedAsync<SampleDbContext>(database);
    }

    [Fact]
    public async Task Every_sample_statement_a_row_handler_hosts_sends_is_explained_by_readiness()
    {
        await using var database = await MigratedSampleDatabaseAsync();
        var tenants = new CoverageTenants();
        using var host = new CohortTestHost(
            database.ConnectionString,
            new CoverageRuleProvider(tenants),
            BatchSizeOne,
            services => services.AddRowHandler<Note, NoOpNoteHandler>()
        );
        await SeedAsync(database, tenants);

        await StartRecordingAsync(database);
        await host.RunWithServicesAsync(services =>
            services
                .GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None)
        );

        // A handler takes a Purge entity down the row-by-row path.
        (await SweepAsync(host, tenants.Swept, dryRun: false))
            .EntityFailures.Should()
            .BeEmpty();
        await EraseAsync(host, tenants.WithoutLegalMinimum, "user", tenants.Subject);

        await AssertEveryRunStatementIsExplainedAsync<SampleDbContext>(database);
    }

    [Fact]
    public async Task Every_sample_statement_a_dependents_step_hosts_sends_is_explained_by_readiness()
    {
        await using var database = await MigratedSampleDatabaseAsync();
        var tenants = new CoverageTenants();
        using var host = new CohortTestHost(
            database.ConnectionString,
            new CoverageRuleProvider(tenants),
            BatchSizeOne,
            services => services.AddRetentionDependents<Note, NoteStep>()
        );
        await SeedAsync(database, tenants);
        // A child of the second note the sweep reaches: the foreign key refuses its delete, which
        // a step's entity does not exclude beforehand (the step may retire the child).
        await ExecuteAsync(
            database,
            """
            CREATE TABLE public.note_children ("NoteId" uuid NOT NULL REFERENCES public.notes ("Id"));
            """
        );
        await ExecuteAsync(
            database,
            $"""
            INSERT INTO public.note_children ("NoteId")
            SELECT "Id" FROM public.notes WHERE "TenantId" = '{tenants.Swept}'
            ORDER BY "CreatedAt" OFFSET 1 LIMIT 1
            """
        );

        await StartRecordingAsync(database);
        await host.RunWithServicesAsync(services =>
            services
                .GetRequiredService<IRetentionHoldsRepository>()
                .ListActiveAsync(CancellationToken.None)
        );

        // The step moved the first note out of scope and the key refused the second's delete.
        (await SweepAsync(host, tenants.Swept, dryRun: false))
            .EntityFailures.Should()
            .ContainSingle(failure =>
                failure.Contains("reason=rows-skipped", StringComparison.Ordinal)
                && failure.Contains(typeof(Note).FullName!, StringComparison.Ordinal)
            );

        await AssertEveryRunStatementIsExplainedAsync<SampleDbContext>(database);
    }

    [Fact]
    public async Task Every_statement_the_hold_relation_model_sends_is_explained_by_readiness()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = HeldWithReadinessEndToEndTests.BuildServices(
            database.ConnectionString,
            configuration: BatchSizeOne
        );
        var tenant = Guid.NewGuid();
        await EnsureCreatedAsync(services);
        await SeedHeldWithAsync(database, tenant);

        await StartRecordingAsync(database);
        await using (var scope = services.CreateAsyncScope())
        {
            var provider = scope.ServiceProvider;
            var holds = provider.GetRequiredService<IRetentionHoldsRepository>();
            await holds.ListActiveAsync(CancellationToken.None);

            // A hold on a case covers its links and the submission it references.
            await holds.CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    CaseIdentity,
                    HeldWithIds.Case.ToString(),
                    tenant,
                    "coverage",
                    AsOf
                ),
                CancellationToken.None
            );
            (
                await holds.HasActiveHoldAsync(
                    LinkIdentity,
                    HeldWithIds.Link.ToString(),
                    tenant,
                    CancellationToken.None
                )
            )
                .Should()
                .BeTrue();
            (
                await holds.ListCoveringAsync(
                    SubmissionIdentity,
                    HeldWithIds.Submission.ToString(),
                    tenant,
                    CancellationToken.None
                )
            )
                .Should()
                .ContainSingle();
            (
                await provider
                    .GetRequiredService<IRetentionDeletion>()
                    .ExecuteAsync(
                        [new RetentionTarget(LinkIdentity, HeldWithIds.Link.ToString(), tenant)],
                        _ => Task.CompletedTask
                    )
            )
                .Should()
                .Be(RetentionDeletionOutcome.Protected);

            (
                await provider
                    .GetRequiredService<IRetentionSweep>()
                    .ExecuteAsync(
                        RetentionSweepRequest.Tenanted(Tenant(tenant), AsOf),
                        CancellationToken.None
                    )
            )
                .EntityFailures.Should()
                .BeEmpty();
            var heldWithDb =
                provider.GetRequiredService<HeldWithReadinessEndToEndTests.HeldWithReadinessDbContext>();
            await heldWithDb.Database.OpenConnectionAsync();
            await RetentionTenantCoverage.FindGapsAsync(
                heldWithDb,
                provider.GetRequiredService<RetentionRegistry>(),
                provider
                    .GetRequiredService<RetentionRuntimeReadinessValidator>()
                    .ValidatedCapabilities,
                [Guid.NewGuid()],
                CancellationToken.None
            );
        }

        await AssertEveryRunStatementIsExplainedAsync<HeldWithReadinessEndToEndTests.HeldWithReadinessDbContext>(
            database
        );
    }

    [Fact]
    public async Task Every_statement_the_hold_relation_model_sends_around_a_dependents_step_is_explained_by_readiness()
    {
        await using var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var services = HeldWithReadinessEndToEndTests.BuildServices(
            database.ConnectionString,
            configure: collection =>
                collection.AddRetentionDependents<
                    HeldWithReadinessEndToEndTests.ReadinessNote,
                    LinkDeletingNoteStep
                >(),
            configuration: BatchSizeOne
        );
        var tenant = Guid.NewGuid();
        await EnsureCreatedAsync(services);
        await SeedHeldWithAsync(database, tenant);

        await StartRecordingAsync(database);
        await using (var scope = services.CreateAsyncScope())
        {
            var provider = scope.ServiceProvider;
            var holds = provider.GetRequiredService<IRetentionHoldsRepository>();
            await holds.CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    CaseIdentity,
                    HeldWithIds.Case.ToString(),
                    tenant,
                    "coverage",
                    AsOf
                ),
                CancellationToken.None
            );

            // The step deletes a link the case's hold covers. The guard that reads the held rows
            // before and after it finds the link gone, rereads to see why, and refuses the
            // step's work.
            var result = await provider
                .GetRequiredService<IRetentionSweep>()
                .ExecuteAsync(
                    RetentionSweepRequest.Tenanted(Tenant(tenant), AsOf),
                    CancellationToken.None
                );

            result
                .EntityFailures.Should()
                .ContainSingle(failure =>
                    failure.Contains("reason=rows-skipped", StringComparison.Ordinal)
                    && failure.Contains(
                        typeof(HeldWithReadinessEndToEndTests.ReadinessNote).FullName!,
                        StringComparison.Ordinal
                    )
                );
        }

        await AssertEveryRunStatementIsExplainedAsync<HeldWithReadinessEndToEndTests.HeldWithReadinessDbContext>(
            database
        );
    }

    // ── the proof ───────────────────────────────────────────────────────────

    private async Task AssertEveryRunStatementIsExplainedAsync<TContext>(TemporaryDatabase database)
        where TContext : DbContext
    {
        var hostTables = HostTables<TContext>(database);
        var recorded = await RecordedStatementsAsync(database);

        var explained = recorded
            .Where(query => query.StartsWith(Explain, StringComparison.Ordinal))
            .Select(query => query[Explain.Length..])
            .ToHashSet(StringComparer.Ordinal);
        var sent = recorded
            .Where(query =>
                !query.StartsWith(Explain, StringComparison.Ordinal)
                && !query.StartsWith(HostComment, StringComparison.Ordinal)
                && (
                    hostTables.Any(table => query.Contains(table, StringComparison.Ordinal))
                    // A record key's canonical text (S11, S18) reads no table, but it casts to the
                    // model's store type, which the database may not know.
                    || query.StartsWith(CanonicalText, StringComparison.Ordinal)
                )
            )
            .Distinct(StringComparer.Ordinal)
            .ToList();

        sent.Should()
            .NotBeEmpty("the runs must have sent host-table statements for this to prove anything");
        sent.Where(query => !explained.Contains(query))
            .Should()
            .BeEmpty(
                "readiness must EXPLAIN every host-table statement a run sends; these were sent and never explained"
            );
    }

    // The quoted, schema-qualified names of the model's own tables (not Cohort's), as Cohort's
    // statements spell them.
    private static List<string> HostTables<TContext>(TemporaryDatabase database)
        where TContext : DbContext
    {
        using var db = (TContext)
            Activator.CreateInstance(
                typeof(TContext),
                new DbContextOptionsBuilder<TContext>().UseNpgsql(database.ConnectionString).Options
            )!;
        var cohort = CohortStoreTables.FromModel(db.Model);
        var cohortTables = new[]
        {
            cohort.RetentionHolds,
            cohort.SweepRun,
            cohort.SweepRunEntitySummary,
            cohort.SweepRunRowDetail,
            cohort.SweepRowHandlerStatus,
        };
        return
        [
            .. db
                .Model.GetEntityTypes()
                .Where(entityType => entityType.GetTableName() is not null)
                .Select(entityType => new RelationalObjectName(
                    entityType.GetSchema() ?? "public",
                    entityType.GetTableName()!
                ))
                .Where(table => !cohortTables.Contains(table))
                .Select(PostgreSqlIdentifier.Format),
        ];
    }

    private static async Task StartRecordingAsync(TemporaryDatabase database)
    {
        await ExecuteAsync(database, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        await ExecuteAsync(
            database,
            "SELECT pg_stat_statements_reset(0, (SELECT oid FROM pg_database WHERE datname = current_database()), 0)"
        );
    }

    private static async Task<List<string>> RecordedStatementsAsync(TemporaryDatabase database)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT query FROM pg_stat_statements
            WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            """,
            connection
        );
        var statements = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            statements.Add(reader.GetString(0));
        }

        return statements;
    }

    // ── the sample model ────────────────────────────────────────────────────

    private static readonly Dictionary<string, string?> BatchSizeOne = new()
    {
        [$"{CohortOptions.SectionName}:SweepBatchSize"] = "1",
    };

    private async Task<TemporaryDatabase> MigratedSampleDatabaseAsync()
    {
        var database = await TemporaryDatabase.CreateAsync(fixture.ConnectionString);
        await using var db = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>()
                .UseNpgsql(database.ConnectionString)
                .Options
        );
        await db.Database.MigrateAsync();
        return database;
    }

    // Three due rows of each entity that carries an erasure subject, per tenant, so a batch size
    // of 1 reaches every cursor, and one tenantless set.
    private static async Task SeedAsync(TemporaryDatabase database, CoverageTenants tenants)
    {
        await using var db = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>()
                .UseNpgsql(database.ConnectionString)
                .Options
        );
        foreach (var tenantId in tenants.All)
        {
            // A recent note no run reaches: the hold and the deletion check use it.
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = tenants.Subject,
                    CreatedAt = AsOf,
                    Body = "recent",
                }
            );
            for (var index = 0; index < 3; index++)
            {
                var created = AsOf.AddDays(-400 - index);
                db.Notes.Add(
                    new Note
                    {
                        Id = Guid.NewGuid(),
                        TenantId = tenantId,
                        SubjectId = tenants.Subject,
                        PersonId = tenants.Subject,
                        CreatedAt = created,
                        Body = "note",
                    }
                );
                db.SoftDeleteRecords.Add(
                    new SoftDeleteRecord
                    {
                        Id = Guid.NewGuid(),
                        TenantId = tenantId,
                        SubjectId = tenants.Subject,
                        CreatedAt = created,
                        Body = "record",
                    }
                );
                db.AnonymisedContacts.Add(
                    new AnonymisedContact
                    {
                        Id = Guid.NewGuid(),
                        TenantId = tenantId,
                        SubjectId = tenants.Subject,
                        CreatedAt = created,
                        EmailAddress = "a@example.com",
                        GivenName = "Given",
                        Surname = "Surname",
                        Notes = "notes",
                    }
                );
                db.TombstoneRecords.Add(
                    new TombstoneRecord
                    {
                        Id = Guid.NewGuid(),
                        TenantId = tenantId,
                        SubjectId = tenants.Subject,
                        CreatedAt = created,
                        ExternalId = Guid.NewGuid(),
                        DisplayName = "Display",
                        ContactEmail = "c@example.com",
                        Notes = "notes",
                    }
                );
            }
        }

        for (var index = 0; index < 3; index++)
        {
            db.TenantlessLogs.Add(
                new TenantlessLog
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = AsOf.AddDays(-400 - index),
                    Payload = "log",
                    SubjectId = tenants.Subject,
                }
            );
        }

        await db.SaveChangesAsync();
    }

    private static async Task<Guid> FirstNoteIdAsync(TemporaryDatabase database, Guid tenantId)
    {
        await using var db = new SampleDbContext(
            new DbContextOptionsBuilder<SampleDbContext>()
                .UseNpgsql(database.ConnectionString)
                .Options
        );
        return await db
            .Notes.Where(note => note.TenantId == tenantId)
            .OrderByDescending(note => note.CreatedAt)
            .Select(note => note.Id)
            .FirstAsync();
    }

    private static TenantContext Tenant(Guid id) => new(id, "uk", new Dictionary<string, string>());

    private static Task<RetentionSweepResult> SweepAsync(
        CohortTestHost host,
        Guid tenantId,
        bool dryRun
    ) =>
        host.RunWithServicesAsync(services =>
            services
                .GetRequiredService<IRetentionSweep>()
                .ExecuteAsync(
                    RetentionSweepRequest.Tenanted(Tenant(tenantId), AsOf, dryRun),
                    CancellationToken.None
                )
        );

    private static async Task EraseAsync(
        CohortTestHost host,
        Guid tenantId,
        string kind,
        Guid subject
    )
    {
        var result = await host.RunErasureAsync(
            Tenant(tenantId),
            new ErasureScope(kind, subject, allowSoftDeleteAsErasure: true),
            AsOf
        );
        result.EntityFailures.Should().BeEmpty();
    }

    // Every tenant the plain runs use. Three kinds of rule differ by tenant: the one with a legal
    // minimum (an erasure has a cutoff), the one without (it has none), and the one whose
    // tombstone category resolves to Exempt (an erasure only counts the subject's rows).
    private sealed class CoverageTenants
    {
        public Guid Subject { get; } = Guid.NewGuid();
        public Guid Swept { get; } = Guid.NewGuid();
        public Guid WithLegalMinimum { get; } = Guid.NewGuid();
        public Guid WithoutLegalMinimum { get; } = Guid.NewGuid();
        public Guid Exempted { get; } = Guid.NewGuid();
        public Guid Held { get; } = Guid.NewGuid();

        public IEnumerable<Guid> All =>
            [Swept, WithLegalMinimum, WithoutLegalMinimum, Exempted, Held];
    }

    private sealed class CoverageRuleProvider(CoverageTenants tenants) : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            category == "tombstone-anonymise"
                ? new RetentionCategoryCapabilities([Strategy.Anonymise, Strategy.Exempt])
                : inner.GetCapabilities(category);

        public async Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            var rule = await inner.ResolveAsync(context, ct);
            if (rule is null)
            {
                return null;
            }

            if (context.Category == "tombstone-anonymise" && context.Tenant.Id == tenants.Exempted)
            {
                return new RetentionRule(rule.Period, Strategy.Exempt);
            }

            return context.Tenant.Id == tenants.WithLegalMinimum
                ? new RetentionRule(rule.Period, rule.Strategy, new RetentionPeriod(Days: 1))
                : rule;
        }
    }

    [RowHandlerId("b8e1c4a7-3d26-4f59-9a0e-7c5d2e8f1b34")]
    private sealed class NoOpNoteHandler : IRetentionHandler<Note>;

    // On the first note it reaches, moves the note out of scope (it is no longer due); the
    // others it leaves alone. Its SQL starts with the host comment.
    [RowHandlerId("5d9a2e6c-1b73-4a84-8c0f-3e7b9d1a4f26")]
    private sealed class NoteStep(SampleDbContext db) : IRetentionDependents<Note>
    {
        private static int calls;

        public async Task RetireAsync(
            Note row,
            RetentionDependentsContext ctx,
            CancellationToken ct
        )
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                await db.Database.ExecuteSqlAsync(
                    $"/* host */ UPDATE public.notes SET \"CreatedAt\" = now() WHERE \"Id\" = {row.Id}",
                    ct
                );
            }
        }
    }

    // ── the hold relation model ─────────────────────────────────────────────

    private static readonly Guid CaseIdentity = new("00000000-0000-0000-0005-000000000001");
    private static readonly Guid LinkIdentity = new("00000000-0000-0000-0005-000000000002");
    private static readonly Guid SubmissionIdentity = new("00000000-0000-0000-0005-000000000003");

    private static class HeldWithIds
    {
        public static readonly Guid Case = Guid.NewGuid();
        public static readonly Guid Link = Guid.NewGuid();
        public static readonly Guid Submission = Guid.NewGuid();
    }

    private static async Task EnsureCreatedAsync(ServiceProvider services)
    {
        await using var scope = services.CreateAsyncScope();
        await scope
            .ServiceProvider.GetRequiredService<HeldWithReadinessEndToEndTests.HeldWithReadinessDbContext>()
            .Database.EnsureCreatedAsync();
    }

    // The case under the hold, its link and the submission it references, none due; and three due
    // rows of every entity beside them, so the sweep reaches every cursor and the step has a
    // note to run on.
    private static async Task SeedHeldWithAsync(TemporaryDatabase database, Guid tenant)
    {
        var recent = AsOf.AddDays(-1);
        await ExecuteAsync(
            database,
            $"""
            INSERT INTO public.held_with_submissions ("Id", "TenantId", "CreatedAt")
            VALUES ('{HeldWithIds.Submission}', '{tenant}', '{recent:O}');
            INSERT INTO public.held_with_cases ("Id", "TenantId", "CreatedAt", "SourceSubmissionId")
            VALUES ('{HeldWithIds.Case}', '{tenant}', '{recent:O}', '{HeldWithIds.Submission}');
            INSERT INTO public.held_with_links ("Id", "TenantId", "CreatedAt", "CaseId")
            VALUES ('{HeldWithIds.Link}', '{tenant}', '{recent:O}', '{HeldWithIds.Case}');
            """
        );
        for (var index = 0; index < 3; index++)
        {
            var due = AsOf.AddDays(-400 - index);
            var note = $"note-{index}";
            await ExecuteAsync(
                database,
                $"""
                INSERT INTO public.held_with_submissions ("Id", "TenantId", "CreatedAt")
                VALUES (gen_random_uuid(), '{tenant}', '{due:O}');
                INSERT INTO public.held_with_cases ("Id", "TenantId", "CreatedAt", "SourceSubmissionId")
                VALUES (gen_random_uuid(), '{tenant}', '{due:O}', NULL);
                INSERT INTO public.held_with_links ("Id", "TenantId", "CreatedAt", "CaseId")
                VALUES (gen_random_uuid(), '{tenant}', '{due:O}', NULL);
                INSERT INTO public.held_with_notes ("Id", "TenantId", "CreatedAt")
                VALUES ('{note}', '{tenant}', '{due:O}');
                INSERT INTO public.held_with_note_links ("Id", "TenantId", "CreatedAt", "NoteId")
                VALUES (gen_random_uuid(), '{tenant}', '{due:O}', '{note}');
                """
            );
        }
    }

    // Deletes every link when it retires a note: a host's step reaching a row a covered hold path
    // protects. Its SQL starts with the host comment.
    [RowHandlerId("e3a7c1d9-5f48-4b02-a6d1-8c4e0b2f7a95")]
    private sealed class LinkDeletingNoteStep(
        HeldWithReadinessEndToEndTests.HeldWithReadinessDbContext db
    ) : IRetentionDependents<HeldWithReadinessEndToEndTests.ReadinessNote>
    {
        public Task RetireAsync(
            HeldWithReadinessEndToEndTests.ReadinessNote row,
            RetentionDependentsContext ctx,
            CancellationToken ct
        ) => db.Database.ExecuteSqlRawAsync("/* host */ DELETE FROM public.held_with_links", ct);
    }

    private static async Task ExecuteAsync(TemporaryDatabase database, string sql)
    {
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
}
