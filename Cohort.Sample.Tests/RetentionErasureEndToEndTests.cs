using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests;

public sealed class RetentionErasureEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Erasure_Path_Matches_Subject_Across_Entities_Respects_Holds_And_Persists_Auditable_Counts()
    {
        var tenantId = Guid.NewGuid();
        var otherTenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var otherSubjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var heldNoteId = Guid.NewGuid();
        var softDeleteId = Guid.NewGuid();
        var heldSoftDeleteId = Guid.NewGuid();
        var anonymisedContactId = Guid.NewGuid();
        var heldAnonymisedContactId = Guid.NewGuid();
        var exemptErasureSubjectRecordId = Guid.NewGuid();
        var inputScope = new ErasureScope(subjectId);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "erase-note",
                },
                new Note
                {
                    Id = heldNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "held-note",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = otherSubjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "other-subject-note",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = otherTenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "other-tenant-note",
                }
            );
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord
                {
                    Id = softDeleteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "erase-soft-delete",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = heldSoftDeleteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "held-soft-delete",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = otherSubjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "other-subject-soft-delete",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = otherTenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "other-tenant-soft-delete",
                    IsDeleted = false,
                }
            );
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = anonymisedContactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    EmailAddress = "subject@example.com",
                    GivenName = "Target",
                    Surname = "Contact",
                    Notes = "keep-notes",
                },
                new AnonymisedContact
                {
                    Id = heldAnonymisedContactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    EmailAddress = "held@example.com",
                    GivenName = "Held",
                    Surname = "Contact",
                    Notes = "held-notes",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = otherSubjectId,
                    CreatedAt = asOf.AddDays(-1),
                    EmailAddress = "other@example.com",
                    GivenName = "Other",
                    Surname = "Subject",
                    Notes = "other-notes",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = otherTenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    EmailAddress = "tenant@example.com",
                    GivenName = "Other",
                    Surname = "Tenant",
                    Notes = "tenant-notes",
                }
            );
            db.ErasureSubjectRecords.AddRange(
                new ErasureSubjectRecord
                {
                    Id = exemptErasureSubjectRecordId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "exempt-erasure-subject-record",
                },
                new ErasureSubjectRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = otherSubjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "other-exempt-erasure-subject-record",
                }
            );
            await db.SaveChangesAsync();
        }

        await CreateHoldAsync("notes", heldNoteId, tenantId, asOf);
        await CreateHoldAsync("soft_delete_records", heldSoftDeleteId, tenantId, asOf);
        await CreateHoldAsync("anonymised_contacts", heldAnonymisedContactId, tenantId, asOf);

        using var erasureHost = new CohortTestHost(
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

        var result = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            inputScope,
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

        run.Trigger.Should().Be(SweepTriggerKind.Erasure);
        run.TotalAffected.Should().Be(3);
        run.TenantId.Should().Be(tenantId);
        result.Scope.Should().Be(inputScope);
        result.StartedAt.Should().Be(run.StartedAt);
        result.CompletedAt.Should().Be(run.CompletedAt);
        result.CompletedAt.Should().BeOnOrAfter(result.StartedAt);
        summaries.Should().Contain(
            new SweepRunEntitySummaryRow(
                result.SweepId,
                typeof(Note).FullName!,
                "short-lived",
                tenantId,
                Strategy.Purge,
                TimeSpan.FromDays(30),
                1,
                1
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
                1
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
                1
            )
        );
        rowDetails.Should().ContainSingle();
        rowDetails[0].Should().Be(
            new SweepRunRowDetailRow(
                result.SweepId,
                typeof(Note).FullName!,
                noteId,
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
        (await verify.Notes.OrderBy(note => note.Body).Select(note => note.Body).ToListAsync())
            .Should()
            .Equal("held-note", "other-subject-note", "other-tenant-note");

        var softDeleteRecords = await verify.SoftDeleteRecords.OrderBy(record => record.Body).ToListAsync();
        softDeleteRecords.Single(record => record.Id == softDeleteId).IsDeleted.Should().BeTrue();
        softDeleteRecords.Single(record => record.Id == heldSoftDeleteId).IsDeleted.Should().BeFalse();
        softDeleteRecords.Single(record => record.Body == "other-subject-soft-delete").IsDeleted.Should().BeFalse();
        softDeleteRecords.Single(record => record.Body == "other-tenant-soft-delete").IsDeleted.Should().BeFalse();

        var contacts = await verify.AnonymisedContacts.OrderBy(contact => contact.EmailAddress).ToListAsync();
        contacts.Single(contact => contact.Id == anonymisedContactId).EmailAddress.Should().BeNull();
        contacts.Single(contact => contact.Id == anonymisedContactId).GivenName.Should().BeEmpty();
        contacts.Single(contact => contact.Id == anonymisedContactId).Surname.Should().Be("[redacted]");
        contacts.Single(contact => contact.Id == anonymisedContactId).Notes.Should().Be("keep-notes");
        contacts.Single(contact => contact.Id == heldAnonymisedContactId)
            .EmailAddress.Should()
            .Be("held@example.com");
        contacts.Single(contact => contact.EmailAddress == "other@example.com").GivenName.Should().Be("Other");
        contacts.Single(contact => contact.EmailAddress == "tenant@example.com").GivenName.Should().Be("Other");
        verify.ErasureSubjectRecords.Single(record => record.Id == exemptErasureSubjectRecordId)
            .Body.Should()
            .Be("exempt-erasure-subject-record");
        verify.ErasureSubjectRecords.Should().HaveCount(2);
    }

    [Fact]
    public async Task Erasure_Path_Fails_When_The_Scope_Subject_Cannot_Be_Expressed_Against_The_Marked_Property()
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
                    SubjectId = Guid.NewGuid(),
                    CreatedAt = asOf,
                    Body = "mismatch",
                }
            );
            await db.SaveChangesAsync();
        }

        var act = () =>
            Host.RunErasureAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope("not-a-guid"),
                asOf
            );

        await act
            .Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("*SubjectId*expects Guid*");
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
                    recordId,
                    tenantId,
                    "erasure-hold",
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
            SELECT "SweepId", "EntityType", "Category", "TenantId", "Strategy", "ResolvedPeriod", "Affected", "HeldCount"
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
                    reader.GetInt32(7)
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
                    reader.GetGuid(2),
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
        int HeldCount
    );

    private sealed record SweepRunRowDetailRow(
        Guid SweepId,
        string EntityType,
        Guid EntityId,
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
