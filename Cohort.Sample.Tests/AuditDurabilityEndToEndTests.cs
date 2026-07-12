using System.Collections.Concurrent;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class AuditDurabilityEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Rule_Resolution_Failure_Is_Sanitized_And_Does_Not_Block_Unrelated_Entities(
        bool erasure
    )
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var contactId = Guid.NewGuid();
        var provider = new FailingOnceRuleProvider("rule resolution contained private data");

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-60),
                    Body = "failed category",
                }
            );
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = contactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-60),
                    EmailAddress = "unrelated@example.org",
                    GivenName = "Unrelated",
                    Surname = "Entity",
                    Notes = "must still run",
                }
            );
            await db.SaveChangesAsync();
        }

        using var host = new CohortTestHost(ConnectionString, provider);

        var result = erasure
            ? ToAuditResult(
                await host.RunErasureAsync(
                    new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                    new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
                    asOf
                )
            )
            : ToAuditResult(
                await host.RunSweepAsync(
                    new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                    asOf
                )
            );

        result.EntityFailures.Should().ContainSingle().Which.Should().MatchRegex(
            "^type=System\\.InvalidOperationException;code=hresult:0x80131509;diagnosticId=[0-9a-f]{32}$"
        );
        result.EntityFailures.Single().Should().NotContain("private data");
        result.Counts.Should().Contain(count =>
            count.EntityType == typeof(AnonymisedContact) && count.Affected == 1
        );
        (await LoadLatestRunRegardlessOfModeAsync(tenantId)).Status.Should().Be(
            SweepRunStatus.PartiallyFailed
        );
    }

    [Fact]
    public async Task Dry_Run_Cancellation_Marks_Run_Cancelled_Without_Mutating_Source_Rows()
    {
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        var functionName = $"delay_dry_run_summary_{noteId:N}";
        var triggerName = $"delay_dry_run_summary_{noteId:N}";
        var (lockKey1, lockKey2) = AdvisoryLockKeys(noteId);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "dry-run-cancelled",
                }
            );
            await db.SaveChangesAsync();
        }

        await ExecuteAsync(
            $"""
            CREATE FUNCTION "{functionName}"() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW."TenantId" = '{tenantId}'::uuid
                   AND NEW."Category" = 'short-lived' THEN
                    PERFORM pg_advisory_xact_lock({lockKey1}, {lockKey2});
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER "{triggerName}"
            BEFORE INSERT ON "sweep_run_entity_summary"
            FOR EACH ROW EXECUTE FUNCTION "{functionName}"();
            """
        );

        try
        {
            using var host = new CohortTestHost(ConnectionString);
            using var cancellation = new CancellationTokenSource();
            await using var lockConnection = await HoldAdvisoryLockAsync(lockKey1, lockKey2);

            var runTask = RunDryRunAsync(host, tenantId, asOf, cancellation.Token);
            await WaitForAdvisoryLockWaiterAsync(lockKey1, lockKey2);
            cancellation.Cancel();
            await lockConnection.CloseAsync();

            Func<Task> act = async () => await runTask.WaitAsync(TimeSpan.FromSeconds(10));
            await act.Should().ThrowAsync<OperationCanceledException>();

            var run = await LoadLatestRunAsync(tenantId);
            run.Status.Should().Be(SweepRunStatus.Cancelled);
            run.SettledAt.Should().NotBeNull();
            run.Error.Should().NotBeNullOrWhiteSpace();

            await using var verify = Host.CreateDbContext();
            (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
        }
        finally
        {
            await ExecuteAsync(
                $"DROP TRIGGER IF EXISTS \"{triggerName}\" ON \"sweep_run_entity_summary\"; DROP FUNCTION IF EXISTS \"{functionName}\"();"
            );
        }
    }

    private sealed class FailingOnceRuleProvider(string message) : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();
        private int failed;

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            inner.GetCapabilities(category);

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            if (
                context.Category == "short-lived"
                && Interlocked.CompareExchange(ref failed, 1, 0) == 0
            )
            {
                throw new InvalidOperationException(message);
            }

            return inner.ResolveAsync(context, ct);
        }
    }

    [Fact]
    public async Task Dry_Run_Entity_Failure_Marks_Run_PartiallyFailed_And_Preserves_Successful_Summaries()
    {
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var contactId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        var functionName = $"fail_dry_run_summary_{contactId:N}";
        var triggerName = $"fail_dry_run_summary_{contactId:N}";

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "dry-run-successful-summary",
                }
            );
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = contactId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "dry-run-failure@example.org",
                    GivenName = "Dry",
                    Surname = "Run",
                    Notes = "must remain unchanged",
                }
            );
            await db.SaveChangesAsync();
        }

        await ExecuteAsync(
            $"""
            CREATE FUNCTION "{functionName}"() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW."TenantId" = '{tenantId}'::uuid
                   AND NEW."Category" = 'anonymise' THEN
                    RAISE EXCEPTION 'dry run entity summary exploded';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER "{triggerName}"
            BEFORE INSERT ON "sweep_run_entity_summary"
            FOR EACH ROW EXECUTE FUNCTION "{functionName}"();
            """
        );

        try
        {
            using var host = new CohortTestHost(ConnectionString);

            var result = await RunDryRunAsync(host, tenantId, asOf);

            result
                .EntityFailures.Should()
                .ContainSingle()
                .Which.Should()
                .MatchRegex(
                    "^type=Npgsql\\.PostgresException;code=sqlstate:P0001;diagnosticId=[0-9a-f]{32}$"
                );
            result
                .Counts.Should()
                .Contain(count => count.EntityType == typeof(Note) && count.Affected == 1);
            result.Counts.Should().NotContain(count => count.EntityType == typeof(AnonymisedContact));

            var run = await LoadLatestRunAsync(tenantId);
            run.Status.Should().Be(SweepRunStatus.PartiallyFailed);
            run.SettledAt.Should().NotBeNull();
            run.Error.Should().Be(result.EntityFailures.Single());
            run.Error.Should().NotContain("dry run entity summary exploded");
            run.TotalAffected.Should().Be(1);

            var summaryEntityTypes = await LoadSummaryEntityTypesAsync(result.SweepId);
            summaryEntityTypes.Should().Equal(typeof(Note).FullName!);
            summaryEntityTypes.Should().NotContain(typeof(AnonymisedContact).FullName!);

            await using var verify = Host.CreateDbContext();
            (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
            var contact = await verify.AnonymisedContacts.SingleAsync(row => row.Id == contactId);
            contact.EmailAddress.Should().Be("dry-run-failure@example.org");
            contact.Notes.Should().Be("must remain unchanged");
        }
        finally
        {
            await ExecuteAsync(
                $"DROP TRIGGER IF EXISTS \"{triggerName}\" ON \"sweep_run_entity_summary\"; DROP FUNCTION IF EXISTS \"{functionName}\"();"
            );
        }
    }

    [Fact]
    public async Task Erasure_Dry_Run_Observer_Failure_Does_Not_Alter_Result_Or_Authoritative_Audit()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "erasure-dry-run-successful-summary",
                }
            );
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "unaudited@example.org",
                    GivenName = "Unaudited",
                    Surname = "Entity",
                }
            );
            await db.SaveChangesAsync();
        }

        using var host = new CohortTestHost(
            ConnectionString,
            configurationOverrides: new Dictionary<string, string?>
            {
                ["Cohort:DryRun"] = "True",
            },
            configureServices: services =>
                services.AddSingleton<IRetentionAuditObserver>(
                    new ThrowOnEntitySummaryAuditObserver(typeof(AnonymisedContact))
                )
        );

        var result = await RunErasureAsync(host, tenantId, subjectId, asOf);

        result.DryRun.Should().BeTrue();
        result.EntityFailures.Should().BeEmpty();
        result
            .Counts.Should()
            .Contain(count => count.EntityType == typeof(Note) && count.Affected == 1);
        result
            .Counts.Should()
            .Contain(count => count.EntityType == typeof(AnonymisedContact) && count.Affected == 1);

        var run = await LoadLatestRunAsync(tenantId);
        run.Status.Should().Be(SweepRunStatus.Succeeded);
        run.TotalAffected.Should().Be(2);

        var summaryEntityTypes = await LoadSummaryEntityTypesAsync(result.SweepId);
        summaryEntityTypes.Should().Contain(typeof(Note).FullName!);
        summaryEntityTypes.Should().Contain(typeof(AnonymisedContact).FullName!);
    }

    [Theory]
    [InlineData(CancellationRunKind.Sweep)]
    [InlineData(CancellationRunKind.DryRun)]
    [InlineData(CancellationRunKind.Erasure)]
    public async Task Cancellation_After_Final_Entity_Summary_Settles_Cancelled_And_Propagates(
        CancellationRunKind runKind
    )
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.TombstoneRecords.Add(
                new TombstoneRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-120),
                    ExternalId = Guid.NewGuid(),
                    DisplayName = "cancel-before-terminal",
                }
            );
            await db.SaveChangesAsync();
        }

        using var cancellation = new CancellationTokenSource();
        CancelAfterEntitySummaryAuditObserver? cancellingObserver = null;
        using var host = new CohortTestHost(
            ConnectionString,
            configurationOverrides: runKind == CancellationRunKind.DryRun
                ? null
                : new Dictionary<string, string?> { ["Cohort:DryRun"] = "False" },
            configureServices: services =>
                services.AddSingleton<IRetentionAuditObserver>(sp =>
                {
                    cancellingObserver = new CancelAfterEntitySummaryAuditObserver(
                        cancellation,
                        typeof(TombstoneRecord)
                    );
                    return cancellingObserver;
                })
        );

        Func<Task> act = runKind switch
        {
            CancellationRunKind.Sweep => async () =>
                await host.RunSweepAsync(
                    new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                    asOf,
                    cancellation.Token
                ),
            CancellationRunKind.DryRun => async () =>
                await RunDryRunAsync(host, tenantId, asOf, cancellation.Token),
            CancellationRunKind.Erasure => async () =>
                await host.RunErasureAsync(
                    new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                    new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
                    asOf,
                    cancellation.Token
                ),
            _ => throw new ArgumentOutOfRangeException(nameof(runKind)),
        };

        await act.Should().ThrowAsync<OperationCanceledException>();
        cancellingObserver.Should().NotBeNull();
        cancellingObserver!.Cancelled.Should().BeTrue();

        var run = await LoadLatestRunRegardlessOfModeAsync(tenantId);
        run.Status.Should().Be(SweepRunStatus.Cancelled);
        run.SettledAt.Should().NotBeNull();
        run.Error.Should().NotBeNullOrWhiteSpace();
        run.TotalAffected.Should().Be(1);
    }

    [Fact]
    public async Task Dry_Run_Completion_Failure_Persists_Accumulated_Affected_Total()
    {
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        var functionName = $"fail_dry_run_completion_{noteId:N}";
        var triggerName = $"fail_dry_run_completion_{noteId:N}";

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "dry-run-durable-total",
                }
            );
            await db.SaveChangesAsync();
        }

        await ExecuteAsync(
            $"""
            CREATE FUNCTION "{functionName}"() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW."TenantId" = '{tenantId}'::uuid
                   AND OLD."Status" = 0
                   AND NEW."Status" IN (1, 2) THEN
                    RAISE EXCEPTION 'dry run completion exploded';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER "{triggerName}"
            BEFORE UPDATE ON "sweep_run"
            FOR EACH ROW EXECUTE FUNCTION "{functionName}"();
            """
        );

        try
        {
            using var host = new CohortTestHost(ConnectionString);

            Func<Task> act = async () => await RunDryRunAsync(host, tenantId, asOf);
            var exception = await act.Should().ThrowAsync<PostgresException>();
            exception.Which.MessageText.Should().Be("dry run completion exploded");

            var run = await LoadLatestRunAsync(tenantId);
            run.Status.Should().Be(SweepRunStatus.Failed);
            run.SettledAt.Should().NotBeNull();
            run.Error.Should().MatchRegex(
                "^type=Npgsql\\.PostgresException;code=sqlstate:P0001;diagnosticId=[0-9a-f]{32}$"
            );
            run.Error.Should().NotContain("dry run completion exploded");
            run.TotalAffected.Should().Be(1);

            var summaryEntityTypes = await LoadSummaryEntityTypesAsync(run.SweepId);
            summaryEntityTypes.Should().Contain(typeof(Note).FullName!);

            await using var verify = Host.CreateDbContext();
            (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
        }
        finally
        {
            await ExecuteAsync(
                $"DROP TRIGGER IF EXISTS \"{triggerName}\" ON \"sweep_run\"; DROP FUNCTION IF EXISTS \"{functionName}\"();"
            );
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Committed_Mutation_Remains_In_Authoritative_Totals_When_Entity_Settlement_Fails(
        bool erasure
    )
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        var functionName = $"fail_summary_{noteId:N}";
        var triggerName = $"fail_summary_{noteId:N}";

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "durable-progress",
                }
            );
            await db.SaveChangesAsync();
        }

        await ExecuteAsync(
            $"""
            CREATE FUNCTION "{functionName}"() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW."TenantId" = '{tenantId}'::uuid THEN
                    RAISE EXCEPTION 'entity settlement exploded';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER "{triggerName}"
            BEFORE UPDATE ON "sweep_run_entity_summary"
            FOR EACH ROW EXECUTE FUNCTION "{functionName}"();
            """
        );

        try
        {
            var emittedEvents = new ConcurrentQueue<SweepEvent>();
            using var host = new CohortTestHost(
                ConnectionString,
                configurationOverrides: erasure
                    ? new Dictionary<string, string?> { ["Cohort:DryRun"] = "False" }
                    : null,
                configureServices: services =>
                    services.AddSingleton<IRetentionAuditObserver>(
                        new RecordingAuditObserver(emittedEvents)
                    )
            );

            var result = erasure
                ? ToAuditResult(await RunErasureAsync(host, tenantId, subjectId, asOf))
                : ToAuditResult(
                    await host.RunSweepAsync(
                        new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                        asOf
                    )
                );

            result
                .EntityFailures.Should()
                .AllSatisfy(failure =>
                {
                    failure.Should().MatchRegex(
                        "^type=Npgsql\\.PostgresException;code=sqlstate:P0001;diagnosticId=[0-9a-f]{32}$"
                    );
                    failure.Should().NotContain("entity settlement exploded");
                });
            result.Counts.Single(count => count.EntityType == typeof(Note)).Affected.Should().Be(1);
            emittedEvents
                .OfType<SweepEvent.PartiallyFailed>()
                .Should()
                .ContainSingle(evt => evt.TotalAffected == 1);
            await AssertTotalsAsync(result.SweepId, tenantId, expectedAffected: 1);
        }
        finally
        {
            await ExecuteAsync(
                $"DROP TRIGGER IF EXISTS \"{triggerName}\" ON \"sweep_run_entity_summary\"; DROP FUNCTION IF EXISTS \"{functionName}\"();"
            );
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Completion_Failure_Marks_Started_Run_Failed_And_Preserves_Original_Exception(
        bool erasure
    )
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var marker = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        var functionName = $"fail_completion_{marker:N}";
        var triggerName = $"fail_completion_{marker:N}";

        await ExecuteAsync(
            $"""
            CREATE FUNCTION "{functionName}"() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW."TenantId" = '{tenantId}'::uuid
                   AND OLD."Status" = 0
                   AND NEW."Status" IN (1, 2) THEN
                    RAISE EXCEPTION 'completion exploded';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER "{triggerName}"
            BEFORE UPDATE ON "sweep_run"
            FOR EACH ROW EXECUTE FUNCTION "{functionName}"();
            """
        );

        try
        {
            using var host = new CohortTestHost(
                ConnectionString,
                configurationOverrides: erasure
                    ? new Dictionary<string, string?> { ["Cohort:DryRun"] = "False" }
                    : null
            );

            Func<Task> act = erasure
                ? async () => await RunErasureAsync(host, tenantId, subjectId, asOf)
                : async () =>
                    await host.RunSweepAsync(
                        new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                        asOf
                    );

            var exception = await act.Should().ThrowAsync<PostgresException>();
            exception.Which.SqlState.Should().Be(PostgresErrorCodes.RaiseException);
            exception.Which.MessageText.Should().Be("completion exploded");

            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                "SELECT \"Status\", \"SettledAt\", \"Error\" FROM \"sweep_run\" WHERE \"TenantId\" = @tenantId ORDER BY \"StartedAt\" DESC LIMIT 1";
            command.Parameters.AddWithValue("tenantId", tenantId);
            await using var reader = await command.ExecuteReaderAsync();
            (await reader.ReadAsync()).Should().BeTrue();
            reader.GetInt32(0).Should().Be((int)SweepRunStatus.Failed);
            reader.IsDBNull(1).Should().BeFalse();
            reader.GetString(2).Should().MatchRegex(
                "^type=Npgsql\\.PostgresException;code=sqlstate:P0001;diagnosticId=[0-9a-f]{32}$"
            );
            reader.GetString(2).Should().NotContain(exception.Which.MessageText);
        }
        finally
        {
            await ExecuteAsync(
                $"DROP TRIGGER IF EXISTS \"{triggerName}\" ON \"sweep_run\"; DROP FUNCTION IF EXISTS \"{functionName}\"();"
            );
        }
    }

    [Theory]
    [InlineData(false, Strategy.Purge)]
    [InlineData(false, Strategy.SoftDelete)]
    [InlineData(false, Strategy.Anonymise)]
    [InlineData(true, Strategy.Purge)]
    [InlineData(true, Strategy.SoftDelete)]
    [InlineData(true, Strategy.Anonymise)]
    public async Task Cancellation_During_Mutation_Rolls_Back_Entity_And_Audit_Progress(
        bool erasure,
        Strategy strategy
    )
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        var functionName = $"delay_mutation_{noteId:N}";
        var triggerName = $"delay_mutation_{noteId:N}";
        var (lockKey1, lockKey2) = AdvisoryLockKeys(noteId);
        var (tableName, operation) = strategy switch
        {
            Strategy.Purge => ("notes", "DELETE"),
            Strategy.SoftDelete => ("soft_delete_records", "UPDATE"),
            Strategy.Anonymise => ("anonymised_contacts", "UPDATE"),
            _ => throw new ArgumentOutOfRangeException(nameof(strategy)),
        };

        await using (var db = Host.CreateDbContext())
        {
            switch (strategy)
            {
                case Strategy.Purge:
                    db.Notes.Add(
                        new Note
                        {
                            Id = noteId,
                            TenantId = tenantId,
                            SubjectId = subjectId,
                            CreatedAt = asOf.AddDays(-120),
                            Body = "cancel-after-mutation",
                        }
                    );
                    break;
                case Strategy.SoftDelete:
                    db.SoftDeleteRecords.Add(
                        new SoftDeleteRecord
                        {
                            Id = noteId,
                            TenantId = tenantId,
                            SubjectId = subjectId,
                            CreatedAt = asOf.AddDays(-120),
                            Body = "cancel-after-mutation",
                        }
                    );
                    break;
                case Strategy.Anonymise:
                    db.AnonymisedContacts.Add(
                        new AnonymisedContact
                        {
                            Id = noteId,
                            TenantId = tenantId,
                            SubjectId = subjectId,
                            CreatedAt = asOf.AddDays(-120),
                            EmailAddress = "cancelled@example.org",
                            GivenName = "Cancel",
                            Surname = "Mutation",
                            Notes = "must remain unchanged",
                        }
                    );
                    break;
            }
            await db.SaveChangesAsync();
        }

        await ExecuteAsync(
            $"""
            CREATE FUNCTION "{functionName}"() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF OLD."TenantId" = '{tenantId}'::uuid THEN
                    PERFORM pg_advisory_xact_lock({lockKey1}, {lockKey2});
                END IF;
                RETURN OLD;
            END $$;
            CREATE TRIGGER "{triggerName}"
            AFTER {operation} ON "{tableName}"
            FOR EACH ROW EXECUTE FUNCTION "{functionName}"();
            """
        );

        try
        {
            var emittedEvents = new ConcurrentQueue<SweepEvent>();
            using var host = new CohortTestHost(
                ConnectionString,
                configurationOverrides: erasure
                    ? new Dictionary<string, string?> { ["Cohort:DryRun"] = "False" }
                    : null,
                configureServices: services =>
                    services.AddSingleton<IRetentionAuditObserver>(
                        new RecordingAuditObserver(emittedEvents)
                    )
            );
            using var cancellation = new CancellationTokenSource();
            await using var lockConnection = await HoldAdvisoryLockAsync(lockKey1, lockKey2);

            Task runTask = erasure
                ? host.RunErasureAsync(
                        new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                        new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
                        asOf,
                        cancellation.Token
                    )
                : host.RunSweepAsync(
                        new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                        asOf,
                        cancellation.Token
                    );

            await WaitForAdvisoryLockWaiterAsync(lockKey1, lockKey2);
            cancellation.Cancel();
            await lockConnection.CloseAsync();

            Func<Task> act = async () => await runTask;
            await act.Should().ThrowAsync<OperationCanceledException>();

            await using (var verify = Host.CreateDbContext())
            {
                switch (strategy)
                {
                    case Strategy.Purge:
                        (await verify.Notes.AnyAsync(row => row.Id == noteId)).Should().BeTrue();
                        break;
                    case Strategy.SoftDelete:
                        var record = await verify.SoftDeleteRecords.SingleAsync(row =>
                            row.Id == noteId
                        );
                        record.IsDeleted.Should().BeFalse();
                        record.DeletedAt.Should().BeNull();
                        break;
                    case Strategy.Anonymise:
                        var contact = await verify.AnonymisedContacts.SingleAsync(row =>
                            row.Id == noteId
                        );
                        contact.EmailAddress.Should().Be("cancelled@example.org");
                        contact.GivenName.Should().Be("Cancel");
                        contact.Surname.Should().Be("Mutation");
                        contact.AnonymisedAt.Should().BeNull();
                        break;
                }
            }

            emittedEvents
                .OfType<SweepEvent.EntityProgress>()
                .Should()
                .NotContain(progress => progress.Affected > 0);
            emittedEvents.OfType<SweepEvent.RowDetail>().Should().BeEmpty();
            await AssertCancelledWithoutProgressAsync(tenantId);
        }
        finally
        {
            await ExecuteAsync(
                $"DROP TRIGGER IF EXISTS \"{triggerName}\" ON \"{tableName}\"; DROP FUNCTION IF EXISTS \"{functionName}\"();"
            );
        }
    }

    private async Task AssertCancelledWithoutProgressAsync(Guid tenantId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT run."Status", run."SettledAt", run."Error", run."TotalAffected",
                   COUNT(detail."SweepId"), COALESCE(SUM(summary."Affected"), 0)
            FROM "sweep_run" run
            LEFT JOIN "sweep_run_row_detail" detail ON detail."SweepId" = run."SweepId"
            LEFT JOIN "sweep_run_entity_summary" summary ON summary."SweepId" = run."SweepId"
            WHERE run."SweepId" = (
                SELECT "SweepId"
                FROM "sweep_run"
                WHERE "TenantId" = @tenantId
                ORDER BY "StartedAt" DESC
                LIMIT 1
            )
            GROUP BY run."SweepId"
            """;
        command.Parameters.AddWithValue("tenantId", tenantId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt32(0).Should().Be((int)SweepRunStatus.Cancelled);
        reader.IsDBNull(1).Should().BeFalse();
        reader.GetString(2).Should().NotBeNullOrWhiteSpace();
        reader.GetInt64(3).Should().Be(0);
        reader.GetInt64(4).Should().Be(0);
        reader.GetInt64(5).Should().Be(0);
    }

    private static (int Key1, int Key2) AdvisoryLockKeys(Guid marker)
    {
        var bytes = marker.ToByteArray();
        return (
            BitConverter.ToInt32(bytes, 0) & int.MaxValue,
            BitConverter.ToInt32(bytes, 4) & int.MaxValue
        );
    }

    private async Task<NpgsqlConnection> HoldAdvisoryLockAsync(int key1, int key2)
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT pg_advisory_lock(@key1, @key2)";
        command.Parameters.AddWithValue("key1", key1);
        command.Parameters.AddWithValue("key2", key2);
        await command.ExecuteNonQueryAsync();
        return connection;
    }

    private async Task WaitForAdvisoryLockWaiterAsync(int key1, int key2)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync(timeout.Token);

        while (true)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks
                    WHERE locktype = 'advisory'
                      AND classid = @key1::oid
                      AND objid = @key2::oid
                      AND objsubid = 2
                      AND NOT granted
                )
                """;
            command.Parameters.AddWithValue("key1", key1);
            command.Parameters.AddWithValue("key2", key2);

            if ((bool)(await command.ExecuteScalarAsync(timeout.Token))!)
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(20), timeout.Token);
        }
    }

    private async Task<ErasureResult> RunErasureAsync(
        CohortTestHost host,
        Guid tenantId,
        Guid subjectId,
        DateTimeOffset asOf
    )
    {
        return await host.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );
    }

    private static async Task<RetentionSweepResult> RunDryRunAsync(
        CohortTestHost host,
        Guid tenantId,
        DateTimeOffset asOf,
        CancellationToken ct = default
    )
    {
        return await host.RunWithServicesAsync(services =>
            services
                .GetRequiredService<RetentionSweepEngine>()
                .DryRunAsync(
                    new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                    asOf,
                    SweepTriggerKind.Manual,
                    SweepEntityScope.TenantedOnly,
                    ct: ct
                )
        );
    }

    private async Task<(
        Guid SweepId,
        SweepRunStatus Status,
        DateTimeOffset? SettledAt,
        string? Error,
        long TotalAffected
    )> LoadLatestRunAsync(Guid tenantId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT "SweepId", "Status", "SettledAt", "Error", "TotalAffected"
            FROM "sweep_run"
            WHERE "TenantId" = @tenantId AND "DryRun" = TRUE
            ORDER BY "StartedAt" DESC
            LIMIT 1
            """;
        command.Parameters.AddWithValue("tenantId", tenantId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        return (
            reader.GetGuid(0),
            (SweepRunStatus)reader.GetInt32(1),
            reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTimeOffset>(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.GetInt64(4)
        );
    }

    private async Task<(
        SweepRunStatus Status,
        DateTimeOffset? SettledAt,
        string? Error,
        long TotalAffected
    )> LoadLatestRunRegardlessOfModeAsync(Guid tenantId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT "Status", "SettledAt", "Error", "TotalAffected"
            FROM "sweep_run"
            WHERE "TenantId" = @tenantId
            ORDER BY "StartedAt" DESC
            LIMIT 1
            """;
        command.Parameters.AddWithValue("tenantId", tenantId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        return (
            (SweepRunStatus)reader.GetInt32(0),
            reader.IsDBNull(1) ? null : reader.GetFieldValue<DateTimeOffset>(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.GetInt64(3)
        );
    }

    private static (
        Guid SweepId,
        IReadOnlyList<EntitySweepCount> Counts,
        IReadOnlyList<string> EntityFailures
    ) ToAuditResult(ErasureResult result) => (result.SweepId, result.Counts, result.EntityFailures);

    private static (
        Guid SweepId,
        IReadOnlyList<EntitySweepCount> Counts,
        IReadOnlyList<string> EntityFailures
    ) ToAuditResult(RetentionSweepResult result) =>
        (result.SweepId, result.Counts, result.EntityFailures);

    private async Task AssertTotalsAsync(Guid sweepId, Guid tenantId, long expectedAffected)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT run."TotalAffected", summary."Affected"
            FROM "sweep_run" run
            JOIN "sweep_run_entity_summary" summary ON summary."SweepId" = run."SweepId"
            WHERE run."SweepId" = @sweepId
              AND summary."TenantId" = @tenantId
              AND summary."EntityType" LIKE '%Note'
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        command.Parameters.AddWithValue("tenantId", tenantId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt64(0).Should().Be(expectedAffected);
        reader.GetInt64(1).Should().Be(expectedAffected);
    }

    private async Task<IReadOnlyList<string>> LoadSummaryEntityTypesAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT "EntityType"
            FROM "sweep_run_entity_summary"
            WHERE "SweepId" = @sweepId
              AND "Affected" > 0
            ORDER BY "EntityType"
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);

        var entityTypes = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            entityTypes.Add(reader.GetString(0));
        }

        return entityTypes;
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private sealed class RecordingAuditObserver(ConcurrentQueue<SweepEvent> events)
        : IRetentionAuditObserver
    {
        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            events.Enqueue(evt);
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowOnEntitySummaryAuditObserver(Type entityType)
        : IRetentionAuditObserver
    {
        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            if (evt is SweepEvent.EntitySummary summary && summary.EntityType == entityType)
            {
                throw new InvalidOperationException("entity summary exploded");
            }

            return Task.CompletedTask;
        }
    }

    private sealed class CancelAfterEntitySummaryAuditObserver(
        CancellationTokenSource cancellation,
        Type entityType
    ) : IRetentionAuditObserver
    {
        public bool Cancelled { get; private set; }

        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            if (evt is SweepEvent.EntitySummary summary && summary.EntityType == entityType)
            {
                cancellation.Cancel();
                Cancelled = true;
            }

            return Task.CompletedTask;
        }
    }

    public enum CancellationRunKind
    {
        Sweep,
        DryRun,
        Erasure,
    }
}
