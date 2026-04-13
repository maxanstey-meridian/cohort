using System.Data.Common;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class RetentionErasureEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Erase_LivePath_StillMutates_AndWritesDryRunFalse()
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
            CreateErasureCategoryRepository(),
            CreateCohortSettings(dryRun: false)
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
        run.DryRun.Should().BeFalse();
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
                noteId.ToString(),
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
    public async Task Erase_DryRun_ReturnsCounts_DoesNotMutate()
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
            CreateErasureCategoryRepository(),
            CreateCohortSettings(dryRun: true)
        );

        var result = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId),
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
        var rowDetails = await LoadRowDetailsAsync(result.SweepId);

        run.Trigger.Should().Be(SweepTriggerKind.Erasure);
        run.DryRun.Should().BeTrue();
        run.TotalAffected.Should().Be(3);
        rowDetails.Should().BeEmpty();

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.OrderBy(note => note.Body).Select(note => note.Body).ToListAsync())
            .Should()
            .Equal("erase-note", "held-note", "other-subject-note", "other-tenant-note");

        var softDeleteRecords = await verify.SoftDeleteRecords.OrderBy(record => record.Body).ToListAsync();
        softDeleteRecords.Single(record => record.Id == softDeleteId).IsDeleted.Should().BeFalse();
        softDeleteRecords.Single(record => record.Id == heldSoftDeleteId).IsDeleted.Should().BeFalse();
        softDeleteRecords.Single(record => record.Body == "other-subject-soft-delete").IsDeleted.Should().BeFalse();
        softDeleteRecords.Single(record => record.Body == "other-tenant-soft-delete").IsDeleted.Should().BeFalse();

        var contacts = await verify.AnonymisedContacts.OrderBy(contact => contact.EmailAddress).ToListAsync();
        contacts.Single(contact => contact.Id == anonymisedContactId).EmailAddress.Should().Be("subject@example.com");
        contacts.Single(contact => contact.Id == anonymisedContactId).GivenName.Should().Be("Target");
        contacts.Single(contact => contact.Id == anonymisedContactId).Surname.Should().Be("Contact");
        contacts.Single(contact => contact.Id == anonymisedContactId).Notes.Should().Be("keep-notes");
        contacts.Single(contact => contact.Id == heldAnonymisedContactId)
            .EmailAddress.Should()
            .Be("held@example.com");
        verify.ErasureSubjectRecords.Single(record => record.Id == exemptErasureSubjectRecordId)
            .Body.Should()
            .Be("exempt-erasure-subject-record");
        verify.ErasureSubjectRecords.Should().HaveCount(2);
    }

    [Fact]
    public async Task Erase_DryRun_AuditEventReflectsFlag()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "dry-run-audit-note",
                }
            );
            await db.SaveChangesAsync();
        }

        using var erasureHost = new CohortTestHost(
            GetConnectionString(),
            CreateErasureCategoryRepository(),
            CreateCohortSettings(dryRun: true)
        );

        var result = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId),
            asOf
        );

        var run = await LoadRunAsync(result.SweepId);
        run.DryRun.Should().BeTrue();
    }

    [Fact]
    public async Task Erase_DryRun_DoesNotLockMatchingRows()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "lock-check-note",
                }
            );
            await db.SaveChangesAsync();
        }

        await using var summaryLockConnection = new NpgsqlConnection(GetConnectionString());
        await summaryLockConnection.OpenAsync();
        await using var summaryLockTransaction = await summaryLockConnection.BeginTransactionAsync();
        await using (var lockCommand = summaryLockConnection.CreateCommand())
        {
            lockCommand.Transaction = summaryLockTransaction;
            lockCommand.CommandText = """LOCK TABLE "sweep_run_entity_summary" IN ACCESS EXCLUSIVE MODE""";
            await lockCommand.ExecuteNonQueryAsync();
        }

        using var erasureHost = new CohortTestHost(
            GetConnectionString(),
            CreateErasureCategoryRepository(),
            CreateCohortSettings(dryRun: true)
        );

        var erasureTask = erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId),
            asOf
        );

        await WaitForSummaryInsertLockAsync(GetConnectionString());

        await using (var updateConnection = new NpgsqlConnection(GetConnectionString()))
        {
            await updateConnection.OpenAsync();
            await using var updateTransaction = await updateConnection.BeginTransactionAsync();
            await using var timeoutCommand = updateConnection.CreateCommand();
            timeoutCommand.Transaction = updateTransaction;
            timeoutCommand.CommandText = """SET LOCAL lock_timeout = '250ms'""";
            await timeoutCommand.ExecuteNonQueryAsync();

            await using var updateCommand = updateConnection.CreateCommand();
            updateCommand.Transaction = updateTransaction;
            updateCommand.CommandText =
                """
                UPDATE "notes"
                SET "Body" = @body
                WHERE "Id" = @id
                """;
            updateCommand.Parameters.Add(new NpgsqlParameter("body", "lock-check-note-updated"));
            updateCommand.Parameters.Add(new NpgsqlParameter("id", noteId));

            var affected = await updateCommand.ExecuteNonQueryAsync();
            affected.Should().Be(1);
            await updateTransaction.CommitAsync();
        }

        await summaryLockTransaction.CommitAsync();

        var result = await erasureTask;
        var run = await LoadRunAsync(result.SweepId);

        run.DryRun.Should().BeTrue();
        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
        );

        await using var verify = Host.CreateDbContext();
        var note = await verify.Notes.SingleAsync(record => record.Id == noteId);
        note.Body.Should().Be("lock-check-note-updated");
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

    [Fact]
    public async Task Erasure_Path_Matches_Using_The_Marked_Clr_Property_Instead_Of_A_Hardcoded_SubjectId_Name()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildAliasSubjectServiceProvider(database.ConnectionString);

        var tenantId = Guid.NewGuid();
        var otherTenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var otherSubjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var matchingId = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<AliasSubjectDbContext>();
            await db.Database.EnsureCreatedAsync();

            db.AliasSubjectFixtureRecords.AddRange(
                new AliasSubjectFixtureRecord
                {
                    Id = matchingId,
                    TenantId = tenantId,
                    CustomerReference = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "alias-match",
                },
                new AliasSubjectFixtureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CustomerReference = otherSubjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "alias-other-subject",
                },
                new AliasSubjectFixtureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = otherTenantId,
                    CustomerReference = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "alias-other-tenant",
                }
            );
            await db.SaveChangesAsync();
        }

        ErasureResult result;
        await using (var scope = services.CreateAsyncScope())
        {
            var erasureService = scope.ServiceProvider.GetRequiredService<IRetentionErasureService>();
            result = await erasureService.EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope(subjectId),
                asOf
            );
        }

        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(AliasSubjectFixtureRecord),
                "short-lived",
                tenantId,
                Strategy.Purge,
                1
            )
        );

        await using (var scope = services.CreateAsyncScope())
        {
            var verify = scope.ServiceProvider.GetRequiredService<AliasSubjectDbContext>();
            (await verify.AliasSubjectFixtureRecords.Select(record => record.Body)
                    .OrderBy(body => body)
                    .ToListAsync())
                .Should()
                .Equal("alias-other-subject", "alias-other-tenant");
            (await verify.AliasSubjectFixtureRecords.AnyAsync(record => record.Id == matchingId))
                .Should()
                .BeFalse();
        }
    }

    [Fact]
    public async Task Erase_Path_Executes_SetBased_And_PerRow_FactoryBacked_Anonymise_Fields()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildFactoryBackedErasureServiceProvider(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var otherTenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var otherSubjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var heldPerRowId = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<FactoryBackedErasureDbContext>();
            await db.Database.EnsureCreatedAsync();

            db.SetBasedFactoryErasureRecords.AddRange(
                new SetBasedFactoryErasureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    ExternalId = Guid.NewGuid(),
                    Notes = "set-based-first",
                },
                new SetBasedFactoryErasureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    ExternalId = Guid.NewGuid(),
                    Notes = "set-based-second",
                },
                new SetBasedFactoryErasureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = otherSubjectId,
                    CreatedAt = asOf.AddDays(-1),
                    ExternalId = Guid.NewGuid(),
                    Notes = "set-based-other-subject",
                },
                new SetBasedFactoryErasureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = otherTenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    ExternalId = Guid.NewGuid(),
                    Notes = "set-based-other-tenant",
                }
            );

            db.PerRowFactoryErasureRecords.AddRange(
                new PerRowFactoryErasureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    ExternalId = "alpha",
                    DisplayName = "first",
                    Notes = "per-row-first",
                },
                new PerRowFactoryErasureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    ExternalId = "beta",
                    DisplayName = "second",
                    Notes = "per-row-second",
                },
                new PerRowFactoryErasureRecord
                {
                    Id = heldPerRowId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    ExternalId = "held",
                    DisplayName = "held",
                    Notes = "per-row-held",
                },
                new PerRowFactoryErasureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = otherSubjectId,
                    CreatedAt = asOf.AddDays(-1),
                    ExternalId = "other-subject",
                    DisplayName = "other-subject",
                    Notes = "per-row-other-subject",
                }
            );

            await db.SaveChangesAsync();
        }

        await using (var scope = services.CreateAsyncScope())
        {
            var repository = scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>();
            await repository.CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    "per_row_factory_erasure_records",
                    heldPerRowId.ToString(),
                    tenantId,
                    "factory-erasure-hold",
                    asOf.AddDays(-1)
                ),
                CancellationToken.None
            );
        }

        ErasureResult result;
        await using (var scope = services.CreateAsyncScope())
        {
            var erasureService = scope.ServiceProvider.GetRequiredService<IRetentionErasureService>();
            result = await erasureService.EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope(subjectId),
                asOf
            );
        }

        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(SetBasedFactoryErasureRecord),
                "factory-backed-set-based-erasure",
                tenantId,
                Strategy.Anonymise,
                2
            )
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(PerRowFactoryErasureRecord),
                "factory-backed-per-row-erasure",
                tenantId,
                Strategy.Anonymise,
                2
            )
        );

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<FactoryBackedErasureDbContext>();
            var setBasedRecords = await db.SetBasedFactoryErasureRecords.OrderBy(record => record.Notes).ToListAsync();
            var perRowRecords = await db.PerRowFactoryErasureRecords.OrderBy(record => record.Notes).ToListAsync();
            var setBasedFactory = scope.ServiceProvider.GetRequiredService<FactorySetBasedGuidFactory>();
            var originalFactory = scope.ServiceProvider.GetRequiredService<FactoryOriginalValueEchoFactory>();
            var perRowSetBasedFactory = scope.ServiceProvider.GetRequiredService<FactorySetBasedStringFactory>();

            setBasedRecords.Single(record => record.Notes == "set-based-first").ExternalId.Should().Be(FactorySetBasedGuidFactory.ScrubbedValue);
            setBasedRecords.Single(record => record.Notes == "set-based-second").ExternalId.Should().Be(FactorySetBasedGuidFactory.ScrubbedValue);
            setBasedRecords.Single(record => record.Notes == "set-based-other-subject").ExternalId.Should().NotBe(FactorySetBasedGuidFactory.ScrubbedValue);
            setBasedRecords.Single(record => record.Notes == "set-based-other-tenant").ExternalId.Should().NotBe(FactorySetBasedGuidFactory.ScrubbedValue);

            perRowRecords.Single(record => record.ExternalId == "alpha-scrubbed").DisplayName.Should().Be(FactorySetBasedStringFactory.ScrubbedValue);
            perRowRecords.Single(record => record.ExternalId == "beta-scrubbed").DisplayName.Should().Be(FactorySetBasedStringFactory.ScrubbedValue);
            perRowRecords.Single(record => record.Notes == "per-row-held").ExternalId.Should().Be("held");
            perRowRecords.Single(record => record.Notes == "per-row-held").DisplayName.Should().Be("held");
            perRowRecords.Single(record => record.Notes == "per-row-other-subject").ExternalId.Should().Be("other-subject");

            setBasedFactory.Contexts.Should().ContainSingle();
            setBasedFactory.Contexts[0].OriginalValue.Should().BeNull();

            originalFactory.Contexts.Should().HaveCount(2);
            originalFactory.Contexts.Select(context => context.OriginalValue).Should().BeEquivalentTo(new object?[] { "alpha", "beta" });
            originalFactory.Contexts.Should().OnlyContain(context => context.TenantId == tenantId);

            perRowSetBasedFactory.Contexts.Should().ContainSingle();
            perRowSetBasedFactory.Contexts[0].OriginalValue.Should().BeNull();
        }
    }

    [Fact]
    public async Task Erasure_Audit_Persists_The_Effective_Resolved_Period_When_Legal_Min_Exceeds_The_Base_Period()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "erasure-effective-period-note",
                }
            );
            await db.SaveChangesAsync();
        }

        using var erasureHost = new CohortTestHost(
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

        var result = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId),
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
                    "erasure-hold",
                    asOf.AddDays(-1)
                ),
                CancellationToken.None
            );
        });
    }

    private static async Task WaitForSummaryInsertLockAsync(string connectionString)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        while (DateTime.UtcNow < deadline)
        {
            await using var command = connection.CreateCommand();
            command.CommandText =
                """
                SELECT COUNT(*)
                FROM pg_stat_activity
                WHERE wait_event_type = 'Lock'
                  AND query ILIKE '%sweep_run_entity_summary%'
                """;

            var blockedCount = Convert.ToInt32(await command.ExecuteScalarAsync());
            if (blockedCount > 0)
            {
                return;
            }

            await Task.Delay(50);
        }

        throw new TimeoutException("Timed out waiting for the dry-run erasure session to block on sweep_run_entity_summary.");
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

    private static IRetentionCategoryRepository CreateErasureCategoryRepository()
    {
        return new StaticCategoryRepository(
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
        );
    }

    private static IReadOnlyDictionary<string, string?> CreateCohortSettings(bool dryRun)
    {
        return new Dictionary<string, string?>
        {
            [$"{CohortOptions.SectionName}:DryRun"] = dryRun.ToString(),
        };
    }

    private static ServiceProvider BuildFactoryBackedErasureServiceProvider(string connectionString)
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection().Build();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging();
        services.AddDbContext<FactoryBackedErasureDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionCategoryRepository>(
            new StaticCategoryRepository(
                new Dictionary<string, IRetentionRuleResolver>
                {
                    ["factory-backed-set-based-erasure"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                    ["factory-backed-per-row-erasure"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                }
            )
        );
        services.AddSingleton<FactorySetBasedGuidFactory>();
        services.AddSingleton<FactorySetBasedStringFactory>();
        services.AddSingleton<FactoryOriginalValueEchoFactory>();
        services.AddSingleton<IAnonymiseValueFactory>(sp => sp.GetRequiredService<FactorySetBasedGuidFactory>());
        services.AddSingleton<IAnonymiseValueFactory>(sp => sp.GetRequiredService<FactorySetBasedStringFactory>());
        services.AddSingleton<IAnonymiseValueFactory>(sp => sp.GetRequiredService<FactoryOriginalValueEchoFactory>());
        services.AddCohort<FactoryBackedErasureDbContext>();

        return services.BuildServiceProvider(validateScopes: true);
    }

    private static ServiceProvider BuildAliasSubjectServiceProvider(string connectionString)
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection().Build();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging();
        services.AddDbContext<AliasSubjectDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionCategoryRepository>(
            new StaticCategoryRepository(
                new Dictionary<string, IRetentionRuleResolver>
                {
                    ["short-lived"] = new StaticRetentionRuleResolver(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    ),
                }
            )
        );
        services.AddCohort<AliasSubjectDbContext>();

        return services.BuildServiceProvider(validateScopes: true);
    }
}

internal sealed class FactoryBackedErasureDbContext(
    DbContextOptions<FactoryBackedErasureDbContext> options
) : DbContext(options)
{
    public DbSet<SetBasedFactoryErasureRecord> SetBasedFactoryErasureRecords => Set<SetBasedFactoryErasureRecord>();
    public DbSet<PerRowFactoryErasureRecord> PerRowFactoryErasureRecords => Set<PerRowFactoryErasureRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SetBasedFactoryErasureRecord>(entity =>
        {
            entity.ToTable("set_based_factory_erasure_records");
            entity.HasKey(record => record.Id);
            entity.Property(record => record.TenantId).HasColumnName("tenant_id");
            entity.Property(record => record.SubjectId).HasColumnName("subject_id");
            entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
            entity.Property(record => record.ExternalId).HasColumnName("external_id");
            entity.Property(record => record.Notes).HasColumnName("notes");
        });

        modelBuilder.Entity<PerRowFactoryErasureRecord>(entity =>
        {
            entity.ToTable("per_row_factory_erasure_records");
            entity.HasKey(record => record.Id);
            entity.Property(record => record.TenantId).HasColumnName("tenant_id");
            entity.Property(record => record.SubjectId).HasColumnName("subject_id");
            entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
            entity.Property(record => record.ExternalId).HasColumnName("external_id");
            entity.Property(record => record.DisplayName).HasColumnName("display_name");
            entity.Property(record => record.Notes).HasColumnName("notes");
        });

        modelBuilder.ConfigureCohortTables();
    }
}

[Retain("factory-backed-set-based-erasure", nameof(SetBasedFactoryErasureRecord.CreatedAt))]
internal sealed class SetBasedFactoryErasureRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject]
    public Guid? SubjectId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    [AnonymiseWith(typeof(FactorySetBasedGuidFactory))]
    public Guid ExternalId { get; set; }

    public string Notes { get; set; } = "";
}

[Retain("factory-backed-per-row-erasure", nameof(PerRowFactoryErasureRecord.CreatedAt))]
internal sealed class PerRowFactoryErasureRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject]
    public Guid? SubjectId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    [AnonymiseWith(typeof(FactoryOriginalValueEchoFactory))]
    public string ExternalId { get; set; } = "";

    [AnonymiseWith(typeof(FactorySetBasedStringFactory))]
    public string DisplayName { get; set; } = "";

    public string Notes { get; set; } = "";
}

internal sealed class FactorySetBasedGuidFactory : IAnonymiseValueFactory
{
    public static readonly Guid ScrubbedValue = Guid.Parse("33333333-3333-3333-3333-333333333333");
    public List<AnonymiseValueContext> Contexts { get; } = [];

    public object? Create(AnonymiseValueContext context)
    {
        Contexts.Add(context);
        return ScrubbedValue;
    }
}

internal sealed class FactorySetBasedStringFactory : IAnonymiseValueFactory
{
    public const string ScrubbedValue = "erasure-factory-scrubbed";
    public List<AnonymiseValueContext> Contexts { get; } = [];

    public object? Create(AnonymiseValueContext context)
    {
        Contexts.Add(context);
        return ScrubbedValue;
    }
}

internal sealed class FactoryOriginalValueEchoFactory : IAnonymiseValueFactory
{
    public bool RequiresOriginalValue => true;
    public List<AnonymiseValueContext> Contexts { get; } = [];

    public object? Create(AnonymiseValueContext context)
    {
        Contexts.Add(context);
        return $"{context.OriginalValue}-scrubbed";
    }
}

internal sealed class AliasSubjectDbContext(DbContextOptions<AliasSubjectDbContext> options)
    : DbContext(options)
{
    public DbSet<AliasSubjectFixtureRecord> AliasSubjectFixtureRecords => Set<AliasSubjectFixtureRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AliasSubjectFixtureRecord>(builder =>
        {
            builder.ToTable("alias_subject_fixture_records");
            builder.HasKey(record => record.Id);
            builder.Property(record => record.TenantId).IsRequired();
            builder.Property(record => record.CustomerReference).HasColumnName("external_subject_key");
            builder.Property(record => record.CreatedAt).IsRequired();
            builder.Property(record => record.Body).IsRequired();
        });

        modelBuilder.ConfigureCohortTables();
    }
}

[Retain("short-lived", nameof(CreatedAt))]
internal sealed class AliasSubjectFixtureRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject]
    public Guid? CustomerReference { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
    public string Body { get; set; } = "";
}

internal sealed class TemporaryDatabase(string connectionString, string databaseName) : IAsyncDisposable
{
    public string ConnectionString => connectionString;

    public static async Task<TemporaryDatabase> CreateAsync(string baseConnectionString)
    {
        var databaseName = $"cohort_erasure_{Guid.NewGuid():N}";
        var adminConnectionString = CreateAdminConnectionString(baseConnectionString);

        await using var connection = new NpgsqlConnection(adminConnectionString);
        await connection.OpenAsync();

        await using (var command = connection.CreateCommand())
        {
            command.CommandText = $"CREATE DATABASE \"{databaseName}\"";
            await command.ExecuteNonQueryAsync();
        }

        var builder = new NpgsqlConnectionStringBuilder(baseConnectionString)
        {
            Database = databaseName,
        };

        return new TemporaryDatabase(builder.ConnectionString, databaseName);
    }

    public async ValueTask DisposeAsync()
    {
        var adminConnectionString = CreateAdminConnectionString(connectionString);

        await using var connection = new NpgsqlConnection(adminConnectionString);
        await connection.OpenAsync();

        await using (var terminate = connection.CreateCommand())
        {
            terminate.CommandText =
                $"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{databaseName}'
                  AND pid <> pg_backend_pid()
                """;
            await terminate.ExecuteNonQueryAsync();
        }

        await using var drop = connection.CreateCommand();
        drop.CommandText = $"DROP DATABASE IF EXISTS \"{databaseName}\"";
        await drop.ExecuteNonQueryAsync();
    }

    private static string CreateAdminConnectionString(string originalConnectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(originalConnectionString)
        {
            Database = "postgres",
        };

        return builder.ConnectionString;
    }
}
