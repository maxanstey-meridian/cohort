using System.Collections.Concurrent;
using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests;

public sealed class AuditWriterEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Sweep_Engine_Streams_Started_EntitySummary_RowDetail_And_Completed_To_The_Live_Audit_Writer_In_Order()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var deletedNoteId = Guid.NewGuid();
        var auditWriter = new RecordingAuditWriter();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = deletedNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "streamed-note",
                }
            );
            await db.SaveChangesAsync();
        }

        await using var services = BuildRecordingAuditProvider(
            GetConnectionString(),
            new StaticCategoryRepository(
                new Dictionary<string, IRetentionRuleResolver>
                {
                    ["short-lived"] = new StaticRetentionRuleResolver(
                        new RetentionRule(
                            TimeSpan.FromDays(30),
                            Strategy.Purge,
                            AuditRowDetail: AuditRowDetail.PerRow
                        )
                    ),
                    ["soft-delete"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    ),
                    ["anonymise"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                }
            ),
            auditWriter
        );

        RetentionSweepResult result;
        await using (var scope = services.CreateAsyncScope())
        {
            var startup = scope.ServiceProvider.GetRequiredService<SampleRetentionStartupService>();
            result = await startup.RunSweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                asOf
            );
        }

        // Expected shape: Started + one EntitySummary per retained sample entity (8) + one RowDetail
        // for the purged note + Completed. Entity iteration order is model-dependent so we assert
        // containment rather than positional ordering, except for Started/Completed bookends.
        auditWriter.Events[0].Should().BeOfType<SweepEvent.Started>();
        auditWriter.Events[^1].Should().BeOfType<SweepEvent.Completed>();

        var started = (SweepEvent.Started)auditWriter.Events[0];
        started.Trigger.Should().Be(SweepTriggerKind.Scheduled);
        started.DryRun.Should().BeFalse();
        started.TenantId.Should().Be(tenantId);

        var summaries = auditWriter.Events.OfType<SweepEvent.EntitySummary>().ToList();
        summaries.Should().HaveCount(8);
        summaries.Should().Contain(
            s => s.SweepId == started.SweepId
                && s.EntityType == typeof(AnonymisedContact)
                && s.Category == "anonymise"
                && s.TenantId == tenantId
                && s.Strategy == Strategy.Anonymise
                && s.Affected == 0
                && s.HeldCount == 0
                && s.SkippedCount == 0
        );
        summaries.Should().Contain(
            s => s.SweepId == started.SweepId
                && s.EntityType == typeof(Note)
                && s.Category == "short-lived"
                && s.TenantId == tenantId
                && s.Strategy == Strategy.Purge
                && s.Affected == 1
                && s.HeldCount == 0
                && s.SkippedCount == 0
        );
        summaries.Should().Contain(
            s => s.SweepId == started.SweepId
                && s.EntityType == typeof(SoftDeleteRecord)
                && s.Category == "soft-delete"
                && s.TenantId == tenantId
                && s.Strategy == Strategy.SoftDelete
                && s.Affected == 0
                && s.HeldCount == 0
                && s.SkippedCount == 0
        );

        var rowDetails = auditWriter.Events.OfType<SweepEvent.RowDetail>().ToList();
        rowDetails.Should().ContainSingle();
        rowDetails[0].Should().Be(
            new SweepEvent.RowDetail(
                started.SweepId,
                rowDetails[0].At,
                typeof(Note),
                deletedNoteId.ToString(),
                "short-lived",
                Strategy.Purge,
                tenantId
            )
        );

        var completed = (SweepEvent.Completed)auditWriter.Events[^1];
        completed.SweepId.Should().Be(started.SweepId);
        completed.TotalAffected.Should().Be(1);

        result.SweepId.Should().Be(started.SweepId);
        result.StartedAt.Should().Be(started.At);
        result.CompletedAt.Should().Be(completed.At);
        result.Counts.Should().BeEquivalentTo(
            auditWriter.Events
                .OfType<SweepEvent.EntitySummary>()
                .Select(summary =>
                    new EntitySweepCount(
                        summary.EntityType,
                        summary.Category,
                        summary.TenantId,
                        summary.Strategy,
                        summary.Affected
                    )
                )
        );
    }

    [Fact]
    public async Task Sweep_Path_Persists_Run_Summary_And_OptIn_Row_Detail_Events_And_Returns_Result_From_The_Same_Aggregate()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var deletedNoteId = Guid.NewGuid();
        var heldNoteId = Guid.NewGuid();
        var softDeleteId = Guid.NewGuid();
        var heldSoftDeleteId = Guid.NewGuid();
        var anonymisedContactId = Guid.NewGuid();
        var heldAnonymisedContactId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = deletedNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "purge-me",
                },
                new Note
                {
                    Id = heldNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "held-note",
                }
            );
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord
                {
                    Id = softDeleteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "soft-delete-me",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = heldSoftDeleteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "held-soft-delete",
                    IsDeleted = false,
                }
            );
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = anonymisedContactId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "contact@example.com",
                    GivenName = "Jane",
                    Surname = "Doe",
                    Notes = "keep this",
                },
                new AnonymisedContact
                {
                    Id = heldAnonymisedContactId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "held@example.com",
                    GivenName = "Held",
                    Surname = "Person",
                    Notes = "held",
                }
            );
            await db.SaveChangesAsync();
        }

        await CreateHoldAsync("notes", heldNoteId, tenantId, asOf);
        await CreateHoldAsync("soft_delete_records", heldSoftDeleteId, tenantId, asOf);
        await CreateHoldAsync("anonymised_contacts", heldAnonymisedContactId, tenantId, asOf);

        using var auditHost = new CohortTestHost(
            GetConnectionString(),
            new StaticCategoryRepository(
                new Dictionary<string, IRetentionRuleResolver>
                {
                    ["short-lived"] = new StaticRetentionRuleResolver(
                        new RetentionRule(
                            TimeSpan.FromDays(30),
                            Strategy.Purge,
                            AuditRowDetail: AuditRowDetail.PerRow
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

        var result = await auditHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(SoftDeleteRecord),
                "soft-delete",
                tenantId,
                Strategy.SoftDelete,
                1
            )
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(AnonymisedContact),
                "anonymise",
                tenantId,
                Strategy.Anonymise,
                1
            )
        );

        var run = await LoadRunAsync(result.SweepId);
        var summaries = await LoadSummariesAsync(result.SweepId);
        var rowDetails = await LoadRowDetailsAsync(result.SweepId);

        run.SweepId.Should().Be(result.SweepId);
        run.StartedAt.Should().Be(result.StartedAt);
        run.CompletedAt.Should().Be(result.CompletedAt);
        run.Trigger.Should().Be(SweepTriggerKind.Scheduled);
        run.DryRun.Should().BeFalse();
        run.TenantId.Should().Be(tenantId);
        run.TotalAffected.Should().Be(3);
        run.Duration.Should().NotBeNull();
        run.Duration.Should().BePositive();

        // 3 original retained entities + blob-backed fixture + 3 tenantless/per-row sample additions
        // + tombstone entity (all later additions are Exempt under this test's restricted dict).
        summaries.Should().HaveCount(8);
        summaries.Should().Contain(
            new SweepRunEntitySummaryRow(
                result.SweepId,
                typeof(Note).FullName!,
                "short-lived",
                tenantId,
                Strategy.Purge,
                TimeSpan.FromDays(30),
                1,
                1,
                0
            )
        );
        summaries.Should().Contain(
            new SweepRunEntitySummaryRow(
                result.SweepId,
                typeof(SoftDeleteRecord).FullName!,
                "soft-delete",
                tenantId,
                Strategy.SoftDelete,
                TimeSpan.FromDays(30),
                1,
                1,
                0
            )
        );
        summaries.Should().Contain(
            new SweepRunEntitySummaryRow(
                result.SweepId,
                typeof(AnonymisedContact).FullName!,
                "anonymise",
                tenantId,
                Strategy.Anonymise,
                TimeSpan.FromDays(30),
                1,
                1,
                0
            )
        );

        rowDetails.Should().ContainSingle();
        rowDetails[0].Should().Be(
            new SweepRunRowDetailRow(
                result.SweepId,
                typeof(Note).FullName!,
                deletedNoteId.ToString(),
                "short-lived",
                Strategy.Purge,
                tenantId
            )
        );

        result.Counts.Should().BeEquivalentTo(
            summaries.Select(summary =>
                new EntitySweepCount(
                    ResolveEntityType(summary.EntityType),
                    summary.Category,
                    summary.TenantId,
                    summary.Strategy,
                    summary.Affected
                )
            )
        );

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.Select(note => note.Body).ToListAsync()).Should().Equal("held-note");

        var softDeleteRecords = await verify.SoftDeleteRecords.OrderBy(record => record.Body).ToListAsync();
        softDeleteRecords.Should().HaveCount(2);
        softDeleteRecords.Single(record => record.Id == softDeleteId).IsDeleted.Should().BeTrue();
        softDeleteRecords.Single(record => record.Id == heldSoftDeleteId).IsDeleted.Should().BeFalse();

        var contacts = await verify.AnonymisedContacts.OrderBy(contact => contact.Id).ToListAsync();
        contacts.Should().HaveCount(2);
        contacts.Single(contact => contact.Id == anonymisedContactId).EmailAddress.Should().BeNull();
        contacts.Single(contact => contact.Id == anonymisedContactId).GivenName.Should().BeEmpty();
        contacts.Single(contact => contact.Id == anonymisedContactId).Surname.Should().Be("[redacted]");
        contacts.Single(contact => contact.Id == anonymisedContactId).Notes.Should().Be("keep this");
        contacts.Single(contact => contact.Id == heldAnonymisedContactId)
            .EmailAddress.Should()
            .Be("held@example.com");
    }

    [Fact]
    public async Task Sweep_Audit_Persists_The_Effective_Resolved_Period_When_Legal_Min_Exceeds_The_Base_Period()
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
                    CreatedAt = asOf.AddDays(-120),
                    Body = "audit-effective-period-note",
                }
            );
            await db.SaveChangesAsync();
        }

        using var auditHost = new CohortTestHost(
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

        var result = await auditHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );
        var summaries = await LoadSummariesAsync(result.SweepId);

        summaries.Should().Contain(
            new SweepRunEntitySummaryRow(
                result.SweepId,
                typeof(Note).FullName!,
                "short-lived",
                tenantId,
                Strategy.Purge,
                TimeSpan.FromDays(90),
                1,
                0
            )
        );
    }

    [Fact]
    public async Task Entity_Level_AuditRowDetail_PerRow_Overrides_Category_Level_SummaryOnly()
    {
        // [Retain] on PerRowAuditedLog sets AuditRowDetail = PerRow. The sample category
        // repository resolves "per-row-audit-override" to a rule with AuditRowDetail.SummaryOnly.
        // The effective setting must come from the entity attribute, so per-row events should
        // fire even though the rule says summary-only. Also prove that entities in OTHER
        // categories (short-lived, whose rule is the default SummaryOnly) do NOT get per-row
        // events from this same sweep.
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var perRowId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var auditWriter = new RecordingAuditWriter();

        await using (var db = Host.CreateDbContext())
        {
            db.PerRowAuditedLogs.Add(
                new PerRowAuditedLog
                {
                    Id = perRowId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Payload = "per-row-audited",
                }
            );
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "not-per-row-audited",
                }
            );
            await db.SaveChangesAsync();
        }

        await using var services = BuildRecordingAuditProvider(
            GetConnectionString(),
            new SampleCategoryRepository(),
            auditWriter
        );

        await using (var scope = services.CreateAsyncScope())
        {
            var startup = scope.ServiceProvider.GetRequiredService<SampleRetentionStartupService>();
            await startup.RunSweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                asOf
            );
        }

        var rowDetails = auditWriter.Events.OfType<SweepEvent.RowDetail>().ToList();

        rowDetails.Should().ContainSingle(
            detail => detail.EntityType == typeof(PerRowAuditedLog) && detail.EntityId == perRowId.ToString(),
            because: "entity-level AuditRowDetail.PerRow overrides the category rule's SummaryOnly"
        );
        rowDetails.Should().NotContain(
            detail => detail.EntityType == typeof(Note),
            because: "Note's category rule is SummaryOnly and its [Retain] does not override to PerRow"
        );
    }

    private async Task CreateHoldAsync(
        string tableName,
        Guid recordId,
        Guid tenantId,
        DateTimeOffset asOf
    )
    {
        await Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            await repository.CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    tableName,
                    recordId.ToString(),
                    tenantId,
                    "audit-hold",
                    asOf.AddDays(-1)
                ),
                CancellationToken.None
            );
        });
    }

    private async Task<SweepRunRow> LoadRunAsync(Guid sweepId)
    {
        await using var db = Host.CreateDbContext();
        await using var command = await CreateCommandAsync(db, sweepId);
        command.CommandText =
            """
            SELECT "SweepId", "StartedAt", "CompletedAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected"
            FROM "sweep_run"
            WHERE "SweepId" = @sweepId
            """;

        await using var reader = await command.ExecuteReaderAsync();
        reader.Read().Should().BeTrue();

        return new SweepRunRow(
            reader.GetGuid(0),
            reader.GetFieldValue<DateTimeOffset>(1),
            reader.GetFieldValue<DateTimeOffset>(2),
            reader.IsDBNull(3) ? null : reader.GetFieldValue<TimeSpan>(3),
            (SweepTriggerKind)reader.GetInt32(4),
            reader.GetBoolean(5),
            reader.GetGuid(6),
            reader.GetInt32(7)
        );
    }

    private async Task<IReadOnlyList<SweepRunEntitySummaryRow>> LoadSummariesAsync(Guid sweepId)
    {
        await using var db = Host.CreateDbContext();
        await using var command = await CreateCommandAsync(db, sweepId);
        command.CommandText =
            """
            SELECT "SweepId", "EntityType", "Category", "TenantId", "Strategy", "ResolvedPeriod", "Affected", "HeldCount", "SkippedCount"
            FROM "sweep_run_entity_summary"
            WHERE "SweepId" = @sweepId
            ORDER BY "EntityType"
            """;

        var rows = new List<SweepRunEntitySummaryRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(
                new SweepRunEntitySummaryRow(
                    reader.GetGuid(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetGuid(3),
                    (Strategy)reader.GetInt32(4),
                    reader.GetFieldValue<TimeSpan>(5),
                    reader.GetInt32(6),
                    reader.GetInt32(7),
                    reader.GetInt32(8)
                )
            );
        }

        return rows;
    }

    private async Task<IReadOnlyList<SweepRunRowDetailRow>> LoadRowDetailsAsync(Guid sweepId)
    {
        await using var db = Host.CreateDbContext();
        await using var command = await CreateCommandAsync(db, sweepId);
        command.CommandText =
            """
            SELECT "SweepId", "EntityType", "EntityId", "Category", "Strategy", "TenantId"
            FROM "sweep_run_row_detail"
            WHERE "SweepId" = @sweepId
            ORDER BY "EntityType", "EntityId"
            """;

        var rows = new List<SweepRunRowDetailRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(
                new SweepRunRowDetailRow(
                    reader.GetGuid(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    (Strategy)reader.GetInt32(4),
                    reader.GetGuid(5)
                )
            );
        }

        return rows;
    }

    private static async Task<DbCommand> CreateCommandAsync(SampleDbContext db, Guid sweepId)
    {
        await db.Database.OpenConnectionAsync();
        var command = db.Database.GetDbConnection().CreateCommand();
        var parameter = command.CreateParameter();
        parameter.ParameterName = "sweepId";
        parameter.Value = sweepId;
        command.Parameters.Add(parameter);
        return command;
    }

    private static Type ResolveEntityType(string entityType)
    {
        var resolved = AppDomain
            .CurrentDomain.GetAssemblies()
            .Select(assembly => assembly.GetType(entityType, throwOnError: false, ignoreCase: false))
            .FirstOrDefault(type => type is not null);

        return resolved
            ?? throw new InvalidOperationException($"Could not resolve entity type '{entityType}'.");
    }

    private static ServiceProvider BuildRecordingAuditProvider(
        string connectionString,
        IRetentionCategoryRepository categoryRepository,
        RecordingAuditWriter auditWriter
    )
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection().Build();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging();
        services.AddDbContext<SampleDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton(categoryRepository);
        services.AddSingleton(auditWriter);
        services.AddScoped<IRetentionAuditWriter>(sp => sp.GetRequiredService<RecordingAuditWriter>());
        services.AddSingleton<GuidTombstoneFactory>();
        services.AddSingleton<OriginalValueTombstoneFactory>();
        services.AddSingleton<IAnonymiseValueFactory>(sp => sp.GetRequiredService<GuidTombstoneFactory>());
        services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<OriginalValueTombstoneFactory>()
        );
        services.AddCohort<SampleDbContext>();
        services.AddScoped<SampleRetentionStartupService>();

        return services.BuildServiceProvider(validateScopes: true);
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

    private sealed class RecordingAuditWriter : IRetentionAuditWriter
    {
        private readonly ConcurrentQueue<SweepEvent> events = new();

        public IReadOnlyList<SweepEvent> Events => events.ToArray();

        public Task WriteAsync(SweepEvent evt, CancellationToken ct)
        {
            events.Enqueue(evt);
            return Task.CompletedTask;
        }
    }

    private sealed record SweepRunRow(
        Guid SweepId,
        DateTimeOffset StartedAt,
        DateTimeOffset CompletedAt,
        TimeSpan? Duration,
        SweepTriggerKind Trigger,
        bool DryRun,
        Guid TenantId,
        int TotalAffected
    );

    private sealed record SweepRunEntitySummaryRow(
        Guid SweepId,
        string EntityType,
        string Category,
        Guid TenantId,
        Strategy Strategy,
        TimeSpan ResolvedPeriod,
        int Affected,
        int HeldCount,
        int SkippedCount = 0
    );

    private sealed record SweepRunRowDetailRow(
        Guid SweepId,
        string EntityType,
        string EntityId,
        string Category,
        Strategy Strategy,
        Guid TenantId
    );

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }
}
