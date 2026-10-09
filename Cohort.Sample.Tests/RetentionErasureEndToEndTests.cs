using System.Data;
using System.Data.Common;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure;
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
    public async Task Tenant_Erasure_Leaves_Tenantless_Subject_Data_Untouched_And_Unaudited()
    {
        var tenantId = Guid.NewGuid();
        var subject = Guid.NewGuid();
        var recordId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.TenantlessLogs.Add(
                new TenantlessLog
                {
                    Id = recordId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Payload = "tenantless-erasure-payload",
                    SubjectId = subject,
                }
            );
            await db.SaveChangesAsync();
        }

        using var erasureHost = new CohortTestHost(
            GetConnectionString(),
            CreateErasureCategoryRepository()
        );

        var result = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("user", subject, allowSoftDeleteAsErasure: true),
            asOf
        );

        result.Counts.Should().NotContain(count => count.EntityType == typeof(TenantlessLog));
        (await LoadSummariesAsync(result.SweepId))
            .Should()
            .NotContain(summary => summary.EntityType == typeof(TenantlessLog).FullName);
        (await LoadRowDetailsAsync(result.SweepId))
            .Should()
            .NotContain(detail => detail.EntityType == typeof(TenantlessLog).FullName);

        await using var verify = Host.CreateDbContext();
        var tenantless = await verify.TenantlessLogs.SingleAsync(log => log.Id == recordId);
        tenantless.Payload.Should().Be("tenantless-erasure-payload");
        tenantless.SubjectId.Should().Be(subject);
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
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "erase-note",
                },
                new Note
                {
                    Id = heldNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "held-note",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = otherSubjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "other-subject-note",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = otherTenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "other-tenant-note",
                }
            );
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord
                {
                    Id = softDeleteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "erase-soft-delete",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = heldSoftDeleteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "held-soft-delete",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = otherSubjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "other-subject-soft-delete",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = otherTenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
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
                    CreatedAt = EligibleErasureCreatedAt(asOf),
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
                    CreatedAt = EligibleErasureCreatedAt(asOf),
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
                    CreatedAt = EligibleErasureCreatedAt(asOf),
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
                    CreatedAt = EligibleErasureCreatedAt(asOf),
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
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "exempt-erasure-subject-record",
                },
                new ErasureSubjectRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = otherSubjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
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
            CreateErasureCategoryRepository()
        );

        var result = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true, dryRun: true),
            asOf
        );

        // Dry runs measure held rows the same way live erasure does.
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(Note),
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    1,
                    HeldCount: 1
                )
            );
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantId,
                    Strategy.SoftDelete,
                    1,
                    HeldCount: 1
                )
            );
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(AnonymisedContact),
                    "anonymise",
                    tenantId,
                    Strategy.Anonymise,
                    1,
                    HeldCount: 1
                )
            );

        var run = await LoadRunAsync(result.SweepId);
        var rowDetails = await LoadRowDetailsAsync(result.SweepId);

        run.Trigger.Should().Be(SweepTriggerKind.Erasure);
        run.DryRun.Should().BeTrue();
        result.DryRun.Should().BeTrue();
        run.TotalAffected.Should().Be(3);
        rowDetails.Should().BeEmpty();

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.OrderBy(note => note.Body).Select(note => note.Body).ToListAsync())
            .Should()
            .Equal("erase-note", "held-note", "other-subject-note", "other-tenant-note");

        var softDeleteRecords = await verify
            .SoftDeleteRecords.OrderBy(record => record.Body)
            .ToListAsync();
        softDeleteRecords.Single(record => record.Id == softDeleteId).IsDeleted.Should().BeFalse();
        softDeleteRecords
            .Single(record => record.Id == heldSoftDeleteId)
            .IsDeleted.Should()
            .BeFalse();
        softDeleteRecords
            .Single(record => record.Body == "other-subject-soft-delete")
            .IsDeleted.Should()
            .BeFalse();
        softDeleteRecords
            .Single(record => record.Body == "other-tenant-soft-delete")
            .IsDeleted.Should()
            .BeFalse();

        var contacts = await verify
            .AnonymisedContacts.OrderBy(contact => contact.EmailAddress)
            .ToListAsync();
        contacts
            .Single(contact => contact.Id == anonymisedContactId)
            .EmailAddress.Should()
            .Be("subject@example.com");
        contacts
            .Single(contact => contact.Id == anonymisedContactId)
            .GivenName.Should()
            .Be("Target");
        contacts
            .Single(contact => contact.Id == anonymisedContactId)
            .Surname.Should()
            .Be("Contact");
        contacts
            .Single(contact => contact.Id == anonymisedContactId)
            .Notes.Should()
            .Be("keep-notes");
        contacts
            .Single(contact => contact.Id == heldAnonymisedContactId)
            .EmailAddress.Should()
            .Be("held@example.com");
        verify
            .ErasureSubjectRecords.Single(record => record.Id == exemptErasureSubjectRecordId)
            .Body.Should()
            .Be("exempt-erasure-subject-record");
        verify.ErasureSubjectRecords.Should().HaveCount(2);
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
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "lock-check-note",
                }
            );
            await db.SaveChangesAsync();
        }

        using var erasureHost = new CohortTestHost(
            GetConnectionString(),
            CreateErasureCategoryRepository()
        );
        await erasureHost.RunPreviewAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await using var summaryLockConnection = new NpgsqlConnection(GetConnectionString());
        await summaryLockConnection.OpenAsync();
        await using var summaryLockTransaction =
            await summaryLockConnection.BeginTransactionAsync();
        await using (var lockCommand = summaryLockConnection.CreateCommand())
        {
            lockCommand.Transaction = summaryLockTransaction;
            lockCommand.CommandText =
                """LOCK TABLE "sweep_run_entity_summary" IN ACCESS EXCLUSIVE MODE""";
            await lockCommand.ExecuteNonQueryAsync();
        }

        var erasureTask = erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true, dryRun: true),
            asOf
        );

        await WaitForSummaryInsertLockAsync(
            GetConnectionString(),
            summaryLockConnection.ProcessID
        );

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
            updateCommand.CommandText = """
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
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
            );

        await using var verify = Host.CreateDbContext();
        var note = await verify.Notes.SingleAsync(record => record.Id == noteId);
        note.Body.Should().Be("lock-check-note-updated");
    }

    [Fact]
    public async Task Erasure_Final_Mutation_Revalidates_Hold_Subject_Tenant_And_LegalMin_After_Lock_Wait()
    {
        var tenantId = Guid.NewGuid();
        var replacementTenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var replacementSubjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var subjectChangedNoteId = Guid.NewGuid();
        var heldNoteId = Guid.NewGuid();
        var tenantChangedSoftDeleteId = Guid.NewGuid();
        var anchorChangedContactId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = subjectChangedNoteId, TenantId = tenantId, SubjectId = subjectId, CreatedAt = asOf.AddDays(-120), Body = "subject-race" },
                new Note { Id = heldNoteId, TenantId = tenantId, SubjectId = subjectId, CreatedAt = asOf.AddDays(-120), Body = "hold-race" }
            );
            db.SoftDeleteRecords.Add(
                new SoftDeleteRecord { Id = tenantChangedSoftDeleteId, TenantId = tenantId, SubjectId = subjectId, CreatedAt = asOf.AddDays(-120), Body = "tenant-race" }
            );
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = anchorChangedContactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "anchor-race@example.com",
                    GivenName = "Anchor",
                    Surname = "Race",
                    Notes = "must survive",
                }
            );
            await db.SaveChangesAsync();
        }

        var legalMin = TimeSpan.FromDays(90);
        using var erasureHost = new CohortTestHost(
            GetConnectionString(),
            CreateErasureCategoryRepository(
                shortLivedRule: new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge, legalMin),
                softDeleteRule: new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete, legalMin),
                anonymiseRule: new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise, legalMin)
            )
        );
        await using var blocker = new NpgsqlConnection(GetConnectionString());
        await blocker.OpenAsync();
        await using var blockerTransaction = await blocker.BeginTransactionAsync();
        await using (var lockCommand = blocker.CreateCommand())
        {
            lockCommand.Transaction = blockerTransaction;
            lockCommand.CommandText = """
                SELECT "Id" FROM "notes" WHERE "Id" = ANY(@noteIds) FOR UPDATE;
                SELECT "Id" FROM "soft_delete_records" WHERE "Id" = @softDeleteId FOR UPDATE;
                SELECT "Id" FROM "anonymised_contacts" WHERE "Id" = @contactId FOR UPDATE;
                """;
            lockCommand.Parameters.AddWithValue("noteIds", new[] { subjectChangedNoteId, heldNoteId });
            lockCommand.Parameters.AddWithValue("softDeleteId", tenantChangedSoftDeleteId);
            lockCommand.Parameters.AddWithValue("contactId", anchorChangedContactId);
            await lockCommand.ExecuteNonQueryAsync();
        }

        var erasure = erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );
        await WaitForBlockedRowMutationAsync(blocker.ProcessID);

        await using (var changeEligibility = blocker.CreateCommand())
        {
            changeEligibility.Transaction = blockerTransaction;
            changeEligibility.CommandText = """
                UPDATE "notes" SET "SubjectId" = @replacementSubjectId WHERE "Id" = @subjectChangedNoteId;
                UPDATE "soft_delete_records" SET "TenantId" = @replacementTenantId WHERE "Id" = @softDeleteId;
                UPDATE "anonymised_contacts" SET "CreatedAt" = @boundary WHERE "Id" = @contactId;
                INSERT INTO "retention_holds"
                    ("HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt")
                VALUES
                    (@holdId, @retentionEntityId, @recordId, @tenantId, @reason, @createdAt, NULL, NULL);
                """;
            changeEligibility.Parameters.AddWithValue("replacementSubjectId", replacementSubjectId);
            changeEligibility.Parameters.AddWithValue("subjectChangedNoteId", subjectChangedNoteId);
            changeEligibility.Parameters.AddWithValue("replacementTenantId", replacementTenantId);
            changeEligibility.Parameters.AddWithValue("softDeleteId", tenantChangedSoftDeleteId);
            changeEligibility.Parameters.AddWithValue("boundary", asOf - legalMin);
            changeEligibility.Parameters.AddWithValue("contactId", anchorChangedContactId);
            changeEligibility.Parameters.AddWithValue("holdId", Guid.NewGuid());
            changeEligibility.Parameters.AddWithValue("retentionEntityId", RetentionEntityIdentity.For<Note>());
            changeEligibility.Parameters.AddWithValue("recordId", heldNoteId.ToString());
            changeEligibility.Parameters.AddWithValue("tenantId", tenantId);
            changeEligibility.Parameters.AddWithValue("reason", "concurrent erasure hold");
            changeEligibility.Parameters.AddWithValue("createdAt", asOf);
            await changeEligibility.ExecuteNonQueryAsync();
        }
        await blockerTransaction.CommitAsync();

        var result = await erasure.WaitAsync(TimeSpan.FromSeconds(10));

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Where(count => count.Strategy != Strategy.Exempt)
            .Should()
            .OnlyContain(count => count.Affected == 0);
        result.Counts.Should().Contain(count => count.EntityType == typeof(Note) && count.HeldCount == 1);
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.CountAsync(note => note.Id == subjectChangedNoteId || note.Id == heldNoteId))
            .Should()
            .Be(2);
        (await verify.SoftDeleteRecords.SingleAsync(record => record.Id == tenantChangedSoftDeleteId))
            .IsDeleted.Should()
            .BeFalse();
        var contact = await verify.AnonymisedContacts.SingleAsync(record => record.Id == anchorChangedContactId);
        contact.EmailAddress.Should().Be("anchor-race@example.com");
        contact.AnonymisedAt.Should().BeNull();
    }

    [Fact]
    public async Task Erasure_Refuses_A_Subject_Of_The_Wrong_Type_Before_Any_Run()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        var act = () => Host.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("user", "not-a-guid", allowSoftDeleteAsErasure: true),
            asOf
        );

        var exception = await act.Should().ThrowAsync<InvalidOperationException>();
        exception.Which.Message.Should().Be(
            "Erasure subject kind 'user' identifies subjects by Guid, but the scope's subject is a String."
        );
        (await CountRunsAsync(tenantId)).Should().Be(0);
    }

    [Fact]
    public async Task Erasure_Refuses_A_Kind_No_Retained_Entity_Declares_Before_Any_Run()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        var act = () => Host.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("customer", Guid.NewGuid(), allowSoftDeleteAsErasure: true),
            asOf
        );

        var exception = await act.Should().ThrowAsync<InvalidOperationException>();
        exception.Which.Message.Should().Be(
            "Erasure subject kind 'customer' isn't declared by any retained entity's [ErasureSubject], so the erasure would match nothing. Declared kinds: 'person', 'user'."
        );
        (await CountRunsAsync(tenantId)).Should().Be(0);
    }

    [Fact]
    public async Task Erasing_A_Person_Matches_Only_Person_Columns_And_Records_The_Kind()
    {
        // Note has a "user" column and a "person" column; SoftDeleteRecord and
        // AnonymisedContact have only "user" columns, so a person erasure skips them, and
        // their SoftDelete category is never refused.
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var personNoteId = Guid.NewGuid();
        var userNoteId = Guid.NewGuid();
        var softDeleteId = Guid.NewGuid();
        await SeedKindFixturesAsync(tenantId, subjectId, asOf, personNoteId, userNoteId, softDeleteId);

        using var erasureHost = new CohortTestHost(GetConnectionString(), CreateErasureCategoryRepository());
        var result = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("person", subjectId),
            asOf
        );

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle().Which.Should().Be(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
        );
        (await LoadRowDetailsAsync(result.SweepId)).Should().ContainSingle()
            .Which.RecordId.Should().Be(personNoteId.ToString());
        (await LoadErasureSubjectKindAsync(result.SweepId)).Should().Be("person");

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.Where(note => note.TenantId == tenantId).Select(note => note.Id).ToListAsync())
            .Should().Equal(userNoteId);
        (await verify.SoftDeleteRecords.SingleAsync(record => record.Id == softDeleteId))
            .IsDeleted.Should().BeFalse();
        (await verify.AnonymisedContacts.SingleAsync(contact => contact.TenantId == tenantId))
            .EmailAddress.Should().Be("kind@example.com");
    }

    [Fact]
    public async Task A_Dry_Run_Erasure_Counts_Only_Rows_Its_Kind_Matches()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        await SeedKindFixturesAsync(tenantId, subjectId, asOf, Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

        using var erasureHost = new CohortTestHost(GetConnectionString(), CreateErasureCategoryRepository());
        var person = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("person", subjectId, dryRun: true),
            asOf
        );
        var user = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true, dryRun: true),
            asOf
        );

        person.Counts.Should().ContainSingle().Which.Should().Be(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
        );
        (await LoadRunAsync(person.SweepId)).TotalAffected.Should().Be(1);
        (await LoadErasureSubjectKindAsync(person.SweepId)).Should().Be("person");
        user.Counts.Select(count => (count.EntityType, count.Affected)).Should().BeEquivalentTo(
            [
                (typeof(Note), 1L),
                (typeof(SoftDeleteRecord), 1L),
                (typeof(AnonymisedContact), 1L),
                // An exempt category: visited by a user erasure, counted as nothing.
                (typeof(TombstoneRecord), 0L),
            ]
        );
        (await LoadRunAsync(user.SweepId)).TotalAffected.Should().Be(3);
        (await LoadErasureSubjectKindAsync(user.SweepId)).Should().Be("user");

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.CountAsync(note => note.TenantId == tenantId)).Should().Be(2);
        (await verify.SoftDeleteRecords.SingleAsync(record => record.TenantId == tenantId))
            .IsDeleted.Should().BeFalse();
        (await verify.AnonymisedContacts.SingleAsync(contact => contact.TenantId == tenantId))
            .AnonymisedAt.Should().BeNull();
    }

    private async Task SeedKindFixturesAsync(
        Guid tenantId,
        Guid subjectId,
        DateTimeOffset asOf,
        Guid personNoteId,
        Guid userNoteId,
        Guid softDeleteId
    )
    {
        await using var db = Host.CreateDbContext();
        db.Notes.AddRange(
            new Note { Id = personNoteId, TenantId = tenantId, SubjectId = Guid.NewGuid(), PersonId = subjectId, CreatedAt = EligibleErasureCreatedAt(asOf), Body = "person-note" },
            new Note { Id = userNoteId, TenantId = tenantId, SubjectId = subjectId, PersonId = Guid.NewGuid(), CreatedAt = EligibleErasureCreatedAt(asOf), Body = "user-note" }
        );
        db.SoftDeleteRecords.Add(
            new SoftDeleteRecord { Id = softDeleteId, TenantId = tenantId, SubjectId = subjectId, CreatedAt = EligibleErasureCreatedAt(asOf), Body = "user-soft-delete" }
        );
        db.AnonymisedContacts.Add(
            new AnonymisedContact { Id = Guid.NewGuid(), TenantId = tenantId, SubjectId = subjectId, CreatedAt = EligibleErasureCreatedAt(asOf), EmailAddress = "kind@example.com", GivenName = "Kind", Surname = "Fixture" }
        );
        await db.SaveChangesAsync();
    }

    private async Task<string?> LoadErasureSubjectKindAsync(Guid sweepId)
    {
        await using var db = Host.CreateDbContext();
        await using var command = await CreateCommandAsync(db, sweepId);
        command.CommandText = """
            SELECT "ErasureSubjectKind" FROM "sweep_run" WHERE "SweepId" = @sweepId
            """;
        var value = await command.ExecuteScalarAsync();
        return value is DBNull ? null : (string?)value;
    }

    private async Task<long> CountRunsAsync(Guid tenantId)
    {
        await using var db = Host.CreateDbContext();
        return await db.Database
            .SqlQuery<long>($"""SELECT COUNT(*) AS "Value" FROM "sweep_run" WHERE "TenantId" = {tenantId}""")
            .SingleAsync();
    }

    [Fact]
    public async Task Erasure_Matches_Subjects_Using_The_Subject_Columns_Store_Type()
    {
        // citext columns compare case-insensitively everywhere else in the host; erasure must
        // not silently fall back to case-sensitive text equality and leave the subject's data.
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildPredicateResolutionServiceProvider<CitextSubjectDbContext>(
            database.ConnectionString,
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["citext-subject-purge"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    ),
                    ["citext-subject-anonymise"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                }
            )
        );
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<CitextSubjectDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Add(new CitextSubjectPurgeRecord { Id = Guid.NewGuid(), TenantId = tenantId, Email = "Bob@X.com", CreatedAt = asOf });
            db.Add(new CitextSubjectAnonymiseRecord { Id = Guid.NewGuid(), TenantId = tenantId, Email = "Bob@X.com", CreatedAt = asOf });
            await db.SaveChangesAsync();
        }

        await using (var scope = services.CreateAsyncScope())
        {
            await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope("email", "bob@x.com"),
                asOf
            );
        }

        await using var verifyScope = services.CreateAsyncScope();
        var verify = verifyScope.ServiceProvider.GetRequiredService<CitextSubjectDbContext>();
        (await verify.Set<CitextSubjectPurgeRecord>().CountAsync()).Should().Be(0);
        (await verify.Set<CitextSubjectAnonymiseRecord>().SingleAsync()).Email.Should().BeNull();
    }

    [Fact]
    public async Task Length_Limited_Subject_And_Record_Ids_Match_Exactly_Instead_Of_Being_Truncated()
    {
        // An explicit cast to varchar(8) silently truncates; "ABCDEFGH-2" must not reach "ABCDEFGH".
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildPredicateResolutionServiceProvider<BoundedSubjectDbContext>(
            database.ConnectionString,
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["bounded-subject-purge"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    ),
                }
            )
        );
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<BoundedSubjectDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Add(new BoundedSubjectRecord { Id = "ABCDEFGH", TenantId = tenantId, Email = "ABCDEFGH", CreatedAt = asOf });
            await db.SaveChangesAsync();
        }

        await using (var scope = services.CreateAsyncScope())
        {
            var create = () => scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    BoundedSubjectRecord.RetentionId,
                    "ABCDEFGH-2",
                    tenantId,
                    "litigation",
                    asOf
                ),
                CancellationToken.None
            );
            await create.Should().ThrowAsync<InvalidOperationException>();
        }

                await using (var scope = services.CreateAsyncScope())
        {
            var result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope("email", "ABCDEFGH-2"),
                asOf
            );
            result.EntityFailures.Should().BeEmpty();
        }

await using var verifyScope = services.CreateAsyncScope();
        var verify = verifyScope.ServiceProvider.GetRequiredService<BoundedSubjectDbContext>();
        (await verify.Set<BoundedSubjectRecord>().CountAsync()).Should().Be(1);
    }

    [Fact]
    public async Task An_Anonymise_Literal_Longer_Than_Its_Column_Fails_The_Entity_Instead_Of_Being_Truncated()
    {
        // The literal is bound without the column's length modifier, so PostgreSQL rejects it
        // rather than silently cutting it to fit varchar(8).
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildPredicateResolutionServiceProvider<BoundedAnonymiseDbContext>(
            database.ConnectionString,
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["bounded-anonymise"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                }
            )
        );
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<BoundedAnonymiseDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Add(new BoundedAnonymiseRecord { Id = Guid.NewGuid(), TenantId = tenantId, SubjectId = subjectId, Email = "a@b.c", CreatedAt = asOf });
            await db.SaveChangesAsync();
        }

        await using (var scope = services.CreateAsyncScope())
        {
            var result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope("user", subjectId),
                asOf
            );
            // 22001: string_data_right_truncation, "value too long for type character varying(8)".
            result.EntityFailures.Should().ContainSingle().Which.Should().Contain("code=sqlstate:22001");
        }

        await using var verifyScope = services.CreateAsyncScope();
        var verify = verifyScope.ServiceProvider.GetRequiredService<BoundedAnonymiseDbContext>();
        var record = await verify.Set<BoundedAnonymiseRecord>().SingleAsync();
        record.Email.Should().Be("a@b.c");
        record.AnonymisedAt.Should().BeNull();
    }

    [Fact]
    public async Task Erasure_Validates_Each_Ef_Model_A_Host_Switches_Between()
    {
        // Hosts using IModelCacheKeyFactory get different IModel instances per scope. Metadata
        // validated for one model must not be reused for another.
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().AddInMemoryCollection().Build());
        services.AddLogging();
        var variant = new ModelVariant();
        services.AddSingleton(variant);
        services.AddDbContext<ModelVariantDbContext>(options =>
            options
                .UseNpgsql(database.ConnectionString)
                .ReplaceService<Microsoft.EntityFrameworkCore.Infrastructure.IModelCacheKeyFactory, ModelVariantCacheKeyFactory>()
        );
        services.AddSingleton<IRetentionRuleProvider>(
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["model-variant"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    ),
                }
            )
        );
        services.AddCohort<ModelVariantDbContext>();
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());

        variant.IncludeExtended = true;
        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<ModelVariantDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Add(new ModelVariantBaseRecord { Id = Guid.NewGuid(), TenantId = tenantId, SubjectId = subjectId, CreatedAt = asOf });
            db.Add(new ModelVariantExtendedRecord { Id = Guid.NewGuid(), TenantId = tenantId, SubjectId = subjectId, CreatedAt = asOf });
            await db.SaveChangesAsync();
        }

        variant.IncludeExtended = false;
        await using (var scope = provider.CreateAsyncScope())
        {
            await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>()
                .EraseAsync(tenant, new ErasureScope("user", subjectId), asOf);
        }

        ErasureResult extended;
        variant.IncludeExtended = true;
        await using (var scope = provider.CreateAsyncScope())
        {
            extended = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>()
                .EraseAsync(tenant, new ErasureScope("user", subjectId), asOf);
        }

        extended.EntityFailures.Should().BeEmpty();
        extended.Counts.Select(count => count.EntityType).Should().Contain(typeof(ModelVariantExtendedRecord));
        await using var verifyScope = provider.CreateAsyncScope();
        var verify = verifyScope.ServiceProvider.GetRequiredService<ModelVariantDbContext>();
        (await verify.Set<ModelVariantBaseRecord>().CountAsync()).Should().Be(0);
        (await verify.Set<ModelVariantExtendedRecord>().CountAsync()).Should().Be(0);
    }

    [Fact]
    public async Task Erasure_Anchor_Eligibility_Depends_Only_On_Positive_LegalMin()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var database = await TemporaryDatabase.CreateAsync(GetConnectionString()))
        await using (var services = BuildPredicateResolutionServiceProvider<SinglePredicateResolutionDbContext>(
            database.ConnectionString,
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["single-subject-erasure"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(365), Strategy.Purge)
                    ),
                }
            )
        ))
        {
            await using (var seedScope = services.CreateAsyncScope())
            {
                var db = seedScope.ServiceProvider.GetRequiredService<SinglePredicateResolutionDbContext>();
                await db.Database.EnsureCreatedAsync();
                db.SingleSubjectRecords.AddRange(
                    new SingleSubjectPredicateRecord { Id = Guid.NewGuid(), TenantId = tenantId, CustomerReference = subjectId, CreatedAt = null },
                    new SingleSubjectPredicateRecord { Id = Guid.NewGuid(), TenantId = tenantId, CustomerReference = subjectId, CreatedAt = asOf.AddDays(1) }
                );
                await db.SaveChangesAsync();
            }

            ErasureResult result;
            await using (var executionScope = services.CreateAsyncScope())
            {
                result = await executionScope.ServiceProvider
                    .GetRequiredService<IRetentionErasureService>()
                    .EraseAsync(
                        new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                        new ErasureScope("user", subjectId),
                        asOf
                    );
            }

            result.Counts.Should().ContainSingle().Which.Should().Be(
                new EntitySweepCount(
                    typeof(SingleSubjectPredicateRecord),
                    "single-subject-erasure",
                    tenantId,
                    Strategy.Purge,
                    2
                )
            );
            await using var verifyScope = services.CreateAsyncScope();
            var verify = verifyScope.ServiceProvider.GetRequiredService<SinglePredicateResolutionDbContext>();
            (await verify.SingleSubjectRecords.CountAsync()).Should().Be(0);
            var summary = (await LoadSummariesAsync(verify, result.SweepId)).Should().ContainSingle().Which;
            summary.ResolvedPeriod.Should().Be(TimeSpan.Zero);
            summary.NullAnchorCount.Should().Be(0);
        }

        await using (var database = await TemporaryDatabase.CreateAsync(GetConnectionString()))
        await using (var services = BuildPredicateResolutionServiceProvider<SinglePredicateResolutionDbContext>(
            database.ConnectionString,
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["single-subject-erasure"] = new StaticTestRetentionRule(
                        new RetentionRule(
                            TimeSpan.FromDays(365),
                            Strategy.Purge,
                            TimeSpan.FromDays(90)
                        )
                    ),
                }
            )
        ))
        {
            var nullAnchorId = Guid.NewGuid();
            var exactBoundaryId = Guid.NewGuid();
            var eligibleId = Guid.NewGuid();
            var futureAnchorId = Guid.NewGuid();
            await using (var seedScope = services.CreateAsyncScope())
            {
                var db = seedScope.ServiceProvider.GetRequiredService<SinglePredicateResolutionDbContext>();
                await db.Database.EnsureCreatedAsync();
                db.SingleSubjectRecords.AddRange(
                    new SingleSubjectPredicateRecord { Id = nullAnchorId, TenantId = tenantId, CustomerReference = subjectId, CreatedAt = null },
                    new SingleSubjectPredicateRecord { Id = exactBoundaryId, TenantId = tenantId, CustomerReference = subjectId, CreatedAt = asOf.AddDays(-90) },
                    new SingleSubjectPredicateRecord { Id = eligibleId, TenantId = tenantId, CustomerReference = subjectId, CreatedAt = asOf.AddDays(-90).AddTicks(-1) },
                    new SingleSubjectPredicateRecord { Id = futureAnchorId, TenantId = tenantId, CustomerReference = subjectId, CreatedAt = asOf.AddDays(1) }
                );
                await db.SaveChangesAsync();
            }

            ErasureResult result;
            await using (var executionScope = services.CreateAsyncScope())
            {
                result = await executionScope.ServiceProvider
                    .GetRequiredService<IRetentionErasureService>()
                    .EraseAsync(
                        new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                        new ErasureScope("user", subjectId),
                        asOf
                    );
            }

            result.Counts.Should().ContainSingle().Which.Should().Be(
                new EntitySweepCount(
                    typeof(SingleSubjectPredicateRecord),
                    "single-subject-erasure",
                    tenantId,
                    Strategy.Purge,
                    1,
                    NullAnchorCount: 1
                )
            );
            await using var verifyScope = services.CreateAsyncScope();
            var verify = verifyScope.ServiceProvider.GetRequiredService<SinglePredicateResolutionDbContext>();
            (await verify.SingleSubjectRecords.Select(record => record.Id).ToListAsync())
                .Should()
                .BeEquivalentTo([nullAnchorId, exactBoundaryId, futureAnchorId]);
            var summary = (await LoadSummariesAsync(verify, result.SweepId)).Should().ContainSingle().Which;
            summary.ResolvedPeriod.Should().Be(TimeSpan.FromDays(90));
            summary.NullAnchorCount.Should().Be(1);
        }
    }

    [Fact]
    public async Task Erasure_Service_Executes_Primary_And_Alternate_Subject_Matches()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildPredicateResolutionServiceProvider<MultiPredicateResolutionDbContext>(
            database.ConnectionString,
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["multi-subject-erasure"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge, AuditRowDetail: AuditRowDetail.PerRow)
                    ),
                }
            )
        );
        var subjectId = Guid.Parse("22222222-2222-2222-2222-222222222222");
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var primaryMatchId = Guid.NewGuid();
        var alternateMatchId = Guid.NewGuid();
        var nonMatchId = Guid.NewGuid();
        var wrongTenantId = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<MultiPredicateResolutionDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Records.AddRange(
                new MultiSubjectPredicateRecord { Id = primaryMatchId, TenantId = tenantId, PrimarySubjectId = subjectId, DelegateSubjectId = Guid.NewGuid(), CreatedAt = EligibleErasureCreatedAt(asOf) },
                new MultiSubjectPredicateRecord { Id = alternateMatchId, TenantId = tenantId, PrimarySubjectId = Guid.NewGuid(), DelegateSubjectId = subjectId, CreatedAt = EligibleErasureCreatedAt(asOf) },
                new MultiSubjectPredicateRecord { Id = nonMatchId, TenantId = tenantId, PrimarySubjectId = Guid.NewGuid(), DelegateSubjectId = Guid.NewGuid(), CreatedAt = EligibleErasureCreatedAt(asOf) },
                new MultiSubjectPredicateRecord { Id = wrongTenantId, TenantId = Guid.NewGuid(), PrimarySubjectId = subjectId, DelegateSubjectId = subjectId, CreatedAt = EligibleErasureCreatedAt(asOf) }
            );
            await db.SaveChangesAsync();
        }

        ErasureResult result;
        await using (var scope = services.CreateAsyncScope())
        {
            result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope("user", subjectId),
                asOf
            );
        }

        result.Counts.Should().ContainSingle().Which.Affected.Should().Be(2);
        await using var verifyScope = services.CreateAsyncScope();
        var verify = verifyScope.ServiceProvider.GetRequiredService<MultiPredicateResolutionDbContext>();
        (await verify.Records.Select(record => record.Id).ToListAsync()).Should().BeEquivalentTo([nonMatchId, wrongTenantId]);
        (await LoadSummariesAsync(verify, result.SweepId)).Should().ContainSingle().Which.Affected.Should().Be(2);
        (await LoadRowDetailsAsync(verify, result.SweepId)).Select(detail => detail.RecordId)
            .Should().BeEquivalentTo(primaryMatchId.ToString(), alternateMatchId.ToString());
    }

    [Fact]
    public async Task Startup_Validation_Fails_When_One_Kind_Holds_Two_Clr_Types_Across_The_Model()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildPredicateResolutionServiceProvider<IncompatiblePredicateResolutionDbContext>(
            database.ConnectionString,
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["incompatible-multi-subject-erasure"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    ),
                    ["incompatible-kind-erasure"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    ),
                }
            )
        );

        await using var scope = services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IncompatiblePredicateResolutionDbContext>().Database.EnsureCreatedAsync();
        var validator = scope.ServiceProvider.GetRequiredService<RetentionStartupValidator>();
        var act = () => validator.ValidateAsync();

        // One entity may mix kinds of different types ("user" Guid, "email" string); one
        // kind may not hold two types, even on different entities.
        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception.Which.Errors.Should().ContainSingle().Which.Should().Be(
            $"[ErasureSubject(\"user\")] columns must all hold one CLR type (after nullable unwrapping), but they hold Guid, String: {typeof(IncompatibleKindRecord).FullName}.UserEmail:String, {typeof(IncompatibleMultiSubjectPredicateRecord).FullName}.PrimarySubjectId:Guid. Give each sort of subject its own kind."
        );
    }

    [Fact]
    public async Task Erasure_Path_DryRun_And_Live_MultiSubject_Matches_Ignore_Period_While_Holds_Block_Mutation()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var previewServices = BuildMultiSubjectServiceProvider(database.ConnectionString);
        await using var liveServices = BuildMultiSubjectServiceProvider(database.ConnectionString);

        var tenantId = Guid.NewGuid();
        var otherTenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var otherSubjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var eligibleId = Guid.NewGuid();
        var cutoffBlockedId = Guid.NewGuid();
        var heldId = Guid.NewGuid();

        await using (var scope = previewServices.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<MultiSubjectDbContext>();
            await db.Database.EnsureCreatedAsync();

            db.MultiSubjectFixtureRecords.AddRange(
                new MultiSubjectFixtureRecord
                {
                    Id = eligibleId,
                    TenantId = tenantId,
                    PrimarySubjectId = subjectId,
                    DelegateSubjectId = otherSubjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "eligible-primary-match",
                },
                new MultiSubjectFixtureRecord
                {
                    Id = cutoffBlockedId,
                    TenantId = tenantId,
                    PrimarySubjectId = otherSubjectId,
                    DelegateSubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-5),
                    Body = "cutoff-blocked-delegate-match",
                },
                new MultiSubjectFixtureRecord
                {
                    Id = heldId,
                    TenantId = tenantId,
                    PrimarySubjectId = otherSubjectId,
                    DelegateSubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "held-delegate-match",
                },
                new MultiSubjectFixtureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    PrimarySubjectId = otherSubjectId,
                    DelegateSubjectId = otherSubjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "other-subject",
                },
                new MultiSubjectFixtureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = otherTenantId,
                    PrimarySubjectId = subjectId,
                    DelegateSubjectId = otherSubjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "other-tenant",
                }
            );
            await db.SaveChangesAsync();
        }

        await using (var scope = previewServices.CreateAsyncScope())
        {
            var repository = scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>();
            await repository.CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    RetentionEntityIdentity.For<MultiSubjectFixtureRecord>(),
                    heldId.ToString(),
                    tenantId,
                    "multi-subject-erasure-hold",
                    asOf.AddDays(-1)
                ),
                CancellationToken.None
            );
        }

        ErasureResult previewResult;
        await using (var scope = previewServices.CreateAsyncScope())
        {
            var erasureService =
                scope.ServiceProvider.GetRequiredService<IRetentionErasureService>();
            previewResult = await erasureService.EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true, dryRun: true),
                asOf
            );
        }

        previewResult
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(MultiSubjectFixtureRecord),
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    2,
                    HeldCount: 1
                )
            );

        await using (var scope = previewServices.CreateAsyncScope())
        {
            var verify = scope.ServiceProvider.GetRequiredService<MultiSubjectDbContext>();
            (
                await verify
                    .MultiSubjectFixtureRecords.Select(record => record.Body)
                    .OrderBy(body => body)
                    .ToListAsync()
            )
                .Should()
                .Equal(
                    "cutoff-blocked-delegate-match",
                    "eligible-primary-match",
                    "held-delegate-match",
                    "other-subject",
                    "other-tenant"
                );
        }

        ErasureResult liveResult;
        await using (var scope = liveServices.CreateAsyncScope())
        {
            var erasureService =
                scope.ServiceProvider.GetRequiredService<IRetentionErasureService>();
            liveResult = await erasureService.EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true),
                asOf
            );
        }

        // Dry-run previews and live erasure both measure holds, so the counts agree.
        liveResult.Counts.Should().BeEquivalentTo(previewResult.Counts);

        await using (var scope = liveServices.CreateAsyncScope())
        {
            var verify = scope.ServiceProvider.GetRequiredService<MultiSubjectDbContext>();
            (
                await verify
                    .MultiSubjectFixtureRecords.Select(record => record.Body)
                    .OrderBy(body => body)
                    .ToListAsync()
            )
                .Should()
                .Equal(
                    "held-delegate-match",
                    "other-subject",
                    "other-tenant"
                );
            (await verify.MultiSubjectFixtureRecords.AnyAsync(record => record.Id == eligibleId))
                .Should()
                .BeFalse();
            (await verify.MultiSubjectFixtureRecords.AnyAsync(record => record.Id == cutoffBlockedId))
                .Should()
                .BeFalse();
            (await verify.MultiSubjectFixtureRecords.AnyAsync(record => record.Id == heldId))
                .Should()
                .BeTrue();
        }
    }

    [Fact]
    public async Task Erasure_Path_Converts_Erasure_Subject_Values_To_The_Provider_Type_Before_SQL_Comparison()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildConvertedErasureSubjectServiceProvider(
            database.ConnectionString
        );

        var tenantId = Guid.NewGuid();
        var otherTenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var otherSubjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var matchingId = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<ConvertedErasureSubjectDbContext>();
            await db.Database.EnsureCreatedAsync();

            db.ConvertedErasureSubjectFixtureRecords.AddRange(
                new ConvertedErasureSubjectFixtureRecord
                {
                    Id = matchingId,
                    TenantId = tenantId,
                    SubjectKey = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "converted-subject-match",
                },
                new ConvertedErasureSubjectFixtureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectKey = otherSubjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "other-subject",
                },
                new ConvertedErasureSubjectFixtureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = otherTenantId,
                    SubjectKey = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "other-tenant",
                }
            );
            await db.SaveChangesAsync();
        }

        ErasureResult result;
        await using (var scope = services.CreateAsyncScope())
        {
            var erasureService =
                scope.ServiceProvider.GetRequiredService<IRetentionErasureService>();
            result = await erasureService.EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true),
                asOf
            );
        }

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(ConvertedErasureSubjectFixtureRecord),
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    1
                )
            );

        await using (var scope = services.CreateAsyncScope())
        {
            var verify =
                scope.ServiceProvider.GetRequiredService<ConvertedErasureSubjectDbContext>();
            (
                await verify
                    .ConvertedErasureSubjectFixtureRecords.Select(record => record.Body)
                    .OrderBy(body => body)
                    .ToListAsync()
            )
                .Should()
                .Equal("other-subject", "other-tenant");
            (
                await verify.ConvertedErasureSubjectFixtureRecords.AnyAsync(record =>
                    record.Id == matchingId
                )
            )
                .Should()
                .BeFalse();
        }
    }

    [Fact]
    public async Task Erase_Path_Executes_SetBased_And_PerRow_FactoryBacked_Anonymise_Fields()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildFactoryBackedErasureServiceProvider(
            database.ConnectionString
        );
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
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    ExternalId = Guid.NewGuid(),
                    Notes = "set-based-first",
                },
                new SetBasedFactoryErasureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    ExternalId = Guid.NewGuid(),
                    Notes = "set-based-second",
                },
                new SetBasedFactoryErasureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = otherSubjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    ExternalId = Guid.NewGuid(),
                    Notes = "set-based-other-subject",
                },
                new SetBasedFactoryErasureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = otherTenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
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
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    ExternalId = "alpha",
                    DisplayName = "first",
                    Notes = "per-row-first",
                },
                new PerRowFactoryErasureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    ExternalId = "beta",
                    DisplayName = "second",
                    Notes = "per-row-second",
                },
                new PerRowFactoryErasureRecord
                {
                    Id = heldPerRowId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    ExternalId = "held",
                    DisplayName = "held",
                    Notes = "per-row-held",
                },
                new PerRowFactoryErasureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = otherSubjectId,
                    CreatedAt = EligibleLegalMinErasureCreatedAt(asOf),
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
                    RetentionEntityIdentity.For<PerRowFactoryErasureRecord>(),
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
            var erasureService =
                scope.ServiceProvider.GetRequiredService<IRetentionErasureService>();
            result = await erasureService.EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true),
                asOf
            );
        }

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SetBasedFactoryErasureRecord),
                    "factory-backed-set-based-erasure",
                    tenantId,
                    Strategy.Anonymise,
                    2
                )
            );
        result
            .Counts.Should()
            .Contain(
                // Held counts are measured directly, so the held per-row record is reported
                // even though it is excluded from candidate selection up front.
                new EntitySweepCount(
                    typeof(PerRowFactoryErasureRecord),
                    "factory-backed-per-row-erasure",
                    tenantId,
                    Strategy.Anonymise,
                    2,
                    HeldCount: 1
                )
            );

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<FactoryBackedErasureDbContext>();
            var setBasedRecords = await db
                .SetBasedFactoryErasureRecords.OrderBy(record => record.Notes)
                .ToListAsync();
            var perRowRecords = await db
                .PerRowFactoryErasureRecords.OrderBy(record => record.Notes)
                .ToListAsync();
            var setBasedFactory =
                scope.ServiceProvider.GetRequiredService<FactorySetBasedGuidFactory>();
            var originalFactory =
                scope.ServiceProvider.GetRequiredService<FactoryOriginalValueEchoFactory>();
            var perRowFactory =
                scope.ServiceProvider.GetRequiredService<FactoryPerRowSequenceFactory>();

            setBasedRecords
                .Single(record => record.Notes == "set-based-first")
                .ExternalId.Should()
                .Be(FactorySetBasedGuidFactory.ScrubbedValue);
            setBasedRecords
                .Single(record => record.Notes == "set-based-second")
                .ExternalId.Should()
                .Be(FactorySetBasedGuidFactory.ScrubbedValue);
            setBasedRecords
                .Single(record => record.Notes == "set-based-other-subject")
                .ExternalId.Should()
                .NotBe(FactorySetBasedGuidFactory.ScrubbedValue);
            setBasedRecords
                .Single(record => record.Notes == "set-based-other-tenant")
                .ExternalId.Should()
                .NotBe(FactorySetBasedGuidFactory.ScrubbedValue);

            perRowRecords
                .Where(record =>
                    record.ExternalId == "alpha-scrubbed" || record.ExternalId == "beta-scrubbed"
                )
                .Select(record => record.DisplayName)
                .Should()
                .BeEquivalentTo(["erasure-per-row-1", "erasure-per-row-2"]);
            perRowRecords
                .Single(record => record.Notes == "per-row-held")
                .ExternalId.Should()
                .Be("held");
            perRowRecords
                .Single(record => record.Notes == "per-row-held")
                .DisplayName.Should()
                .Be("held");
            perRowRecords
                .Single(record => record.Notes == "per-row-other-subject")
                .ExternalId.Should()
                .Be("other-subject");

            setBasedFactory.Contexts.Should().ContainSingle();
            setBasedFactory.Contexts[0].OriginalValue.Should().BeNull();
            setBasedFactory.Contexts[0].TenantId.Should().Be(tenantId);
            setBasedFactory
                .Contexts[0]
                .MemberName.Should()
                .Be(nameof(SetBasedFactoryErasureRecord.ExternalId));

            originalFactory.Contexts.Should().HaveCount(2);
            originalFactory
                .Contexts.Select(context => context.OriginalValue)
                .Should()
                .BeEquivalentTo(new object?[] { "alpha", "beta" });
            originalFactory.Contexts.Should().OnlyContain(context => context.TenantId == tenantId);
            originalFactory
                .Contexts.Should()
                .OnlyContain(context =>
                    context.MemberName == nameof(PerRowFactoryErasureRecord.ExternalId)
                );

            perRowFactory.Contexts.Should().HaveCount(2);
            perRowFactory.Contexts.Should().OnlyContain(context => context.OriginalValue == null);
            perRowFactory
                .Contexts.Should()
                .OnlyContain(context =>
                    context.MemberName == nameof(PerRowFactoryErasureRecord.DisplayName)
                );
        }
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
                    RetentionEntityIdentity.ForTable(tableName),
                    recordId.ToString(),
                    tenantId,
                    "erasure-hold",
                    asOf.AddDays(-1)
                ),
                CancellationToken.None
            );
        });
    }

    private static DateTimeOffset EligibleErasureCreatedAt(DateTimeOffset asOf)
    {
        return asOf.AddDays(-45);
    }

    private static DateTimeOffset EligibleLegalMinErasureCreatedAt(DateTimeOffset asOf)
    {
        return asOf.AddDays(-120);
    }

    private static async Task WaitForSummaryInsertLockAsync(
        string connectionString,
        int blockerBackendId
    )
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        while (DateTime.UtcNow < deadline)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks waiter
                    WHERE waiter.locktype = 'relation'
                      AND waiter.relation = to_regclass('"sweep_run_entity_summary"')
                      AND waiter.mode = 'RowExclusiveLock'
                      AND NOT waiter.granted
                      AND @blockerBackendId = ANY(pg_blocking_pids(waiter.pid))
                )
                """;
            command.Parameters.AddWithValue("blockerBackendId", blockerBackendId);

            if ((bool)(await command.ExecuteScalarAsync())!)
            {
                return;
            }

            await Task.Delay(50);
        }

        throw new TimeoutException(
            "Timed out waiting for the dry-run erasure session to block on sweep_run_entity_summary."
        );
    }

    [Fact]
    public async Task Erase_Continues_Past_A_Stale_First_Discovery()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var staleId = Guid.NewGuid();
        var remainingId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = staleId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-121),
                    Body = "stale-first-discovery",
                },
                new Note
                {
                    Id = remainingId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "remaining-discovery",
                }
            );
            await db.SaveChangesAsync();
        }

        await using var blocker = new NpgsqlConnection(GetConnectionString());
        await blocker.OpenAsync();
        await using var blockerTransaction = await blocker.BeginTransactionAsync();
        await using (var advisoryCommand = blocker.CreateCommand())
        {
            advisoryCommand.Transaction = blockerTransaction;
            advisoryCommand.CommandText = "SELECT pg_advisory_xact_lock(hashtextextended(@lockKey, @hashSeed))";
            advisoryCommand.Parameters.AddWithValue(
                "lockKey",
                $"{RetentionEntityIdentity.For<Note>():D}:{tenantId:D}:{staleId.ToString().Length}:{staleId}"
            );
            advisoryCommand.Parameters.AddWithValue("hashSeed", 4_341_726_887L);
            await advisoryCommand.ExecuteNonQueryAsync();
        }

        using var erasureHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:DryRun"] = "False",
                [$"{CohortOptions.SectionName}:SweepBatchSize"] = "1",
            }
        );
        var erasureTask = erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );
        await WaitForAdvisoryLockWaiterAsync(blocker.ProcessID);

        await using (var deleteCommand = blocker.CreateCommand())
        {
            deleteCommand.Transaction = blockerTransaction;
            deleteCommand.CommandText = "DELETE FROM \"notes\" WHERE \"Id\" = @id";
            deleteCommand.Parameters.AddWithValue("id", staleId);
            await deleteCommand.ExecuteNonQueryAsync();
        }
        await blockerTransaction.CommitAsync();

        var result = await erasureTask;
        result.Counts.Should().Contain(count => count.EntityType == typeof(Note) && count.Affected == 1);
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == remainingId)).Should().BeFalse();
    }

    private async Task WaitForAdvisoryLockWaiterAsync(int blockerBackendId)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync(timeout.Token);
        while (true)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks waiting
                    JOIN pg_locks held
                      ON held.locktype = waiting.locktype
                     AND held.database IS NOT DISTINCT FROM waiting.database
                     AND held.classid IS NOT DISTINCT FROM waiting.classid
                     AND held.objid IS NOT DISTINCT FROM waiting.objid
                     AND held.objsubid IS NOT DISTINCT FROM waiting.objsubid
                    WHERE waiting.locktype = 'advisory'
                      AND NOT waiting.granted
                      AND held.granted
                      AND held.pid = @blockerBackendId
                )
                """;
            command.Parameters.AddWithValue("blockerBackendId", blockerBackendId);
            if ((bool)(await command.ExecuteScalarAsync(timeout.Token))!)
            {
                return;
            }
            await Task.Delay(20, timeout.Token);
        }
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

    private async Task<SweepRunRow> LoadRunAsync(Guid sweepId)
    {
        await using var db = Host.CreateDbContext();
        await using var command = await CreateCommandAsync(db, sweepId);
        command.CommandText = """
            SELECT "SweepId", "StartedAt", "SettledAt", "Duration", "TriggerKind", "DryRun", "TenantId", "TotalAffected"
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
            reader.GetInt64(7)
        );
    }

    private async Task<IReadOnlyList<SweepRunEntitySummaryRow>> LoadSummariesAsync(Guid sweepId)
    {
        await using var db = Host.CreateDbContext();
        return await LoadSummariesAsync(db, sweepId);
    }

    private static async Task<IReadOnlyList<SweepRunEntitySummaryRow>> LoadSummariesAsync(
        DbContext db,
        Guid sweepId
    )
    {
        await using var command = await CreateCommandAsync(db, sweepId);
        command.CommandText = """
            SELECT "SweepId", "EntityType", "Category", "TenantId", "Strategy", "ResolvedPeriod", "Affected", "HeldCount", "SkippedCount", "RuleSource", "RuleReason", "NullAnchorCount"
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
                    reader.GetInt64(6),
                    reader.GetInt64(7),
                    reader.GetInt64(8),
                    reader.IsDBNull(9) ? null : reader.GetString(9),
                    reader.IsDBNull(10) ? null : reader.GetString(10),
                    reader.GetInt64(11)
                )
            );
        }

        return rows;
    }

    private async Task<IReadOnlyList<SweepRunRowDetailRow>> LoadRowDetailsAsync(Guid sweepId)
    {
        await using var db = Host.CreateDbContext();
        return await LoadRowDetailsAsync(db, sweepId);
    }

    private static async Task<IReadOnlyList<SweepRunRowDetailRow>> LoadRowDetailsAsync(
        DbContext db,
        Guid sweepId
    )
    {
        await using var command = await CreateCommandAsync(db, sweepId);
        command.CommandText = """
            SELECT "SweepId", "EntityType", "RecordId", "Category", "Strategy", "TenantId"
            FROM "sweep_run_row_detail"
            WHERE "SweepId" = @sweepId
            ORDER BY "EntityType", "RecordId"
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

    private static async Task<DbCommand> CreateCommandAsync(DbContext db, Guid sweepId)
    {
        await db.Database.OpenConnectionAsync();
        var command = db.Database.GetDbConnection().CreateCommand();
        var parameter = command.CreateParameter();
        parameter.ParameterName = "sweepId";
        parameter.Value = sweepId;
        command.Parameters.Add(parameter);
        return command;
    }

    private static ServiceProvider BuildPredicateResolutionServiceProvider<TContext>(
        string connectionString,
        ITestRetentionRuleProvider repository
    )
        where TContext : DbContext
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().AddInMemoryCollection().Build());
        services.AddLogging();
        services.AddDbContext<TContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(repository);
        services.AddCohort<TContext>();
        return services.BuildServiceProvider(validateScopes: true);
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

    private sealed record SweepRunRow(
        Guid SweepId,
        DateTimeOffset StartedAt,
        DateTimeOffset CompletedAt,
        TimeSpan? Duration,
        SweepTriggerKind Trigger,
        bool DryRun,
        Guid TenantId,
        long TotalAffected
    );

    private sealed record SweepRunEntitySummaryRow(
        Guid SweepId,
        string EntityType,
        string Category,
        Guid TenantId,
        Strategy Strategy,
        TimeSpan ResolvedPeriod,
        long Affected,
        long HeldCount,
        long SkippedCount = 0,
        string? RuleSource = null,
        string? RuleReason = null,
        long NullAnchorCount = 0
    );

    private sealed record SweepRunRowDetailRow(
        Guid SweepId,
        string EntityType,
        string RecordId,
        string Category,
        Strategy Strategy,
        Guid TenantId
    );

    private async Task WaitForBlockedRowMutationAsync(int blockerBackendId)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var observer = new NpgsqlConnection(GetConnectionString());
        await observer.OpenAsync(timeout.Token);
        while (true)
        {
            await using var command = observer.CreateCommand();
            command.CommandText = """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity waiter
                    WHERE @blockerBackendId = ANY(pg_blocking_pids(waiter.pid))
                )
                """;
            command.Parameters.AddWithValue("blockerBackendId", blockerBackendId);
            if ((bool)(await command.ExecuteScalarAsync(timeout.Token))!)
            {
                return;
            }

            await Task.Delay(10, timeout.Token);
        }
    }

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }

    private static ITestRetentionRuleProvider CreateErasureCategoryRepository(
        RetentionRule? shortLivedRule = null,
        RetentionRule? softDeleteRule = null,
        RetentionRule? anonymiseRule = null
    )
    {
        return new StaticCategoryRepository(
            new Dictionary<string, ITestRetentionRule>
            {
                ["short-lived"] = new StaticTestRetentionRule(
                    shortLivedRule
                        ?? new RetentionRule(
                            TimeSpan.FromDays(30),
                            Strategy.Purge,
                            AuditRowDetail: AuditRowDetail.PerRow
                        )
                ),
                ["soft-delete"] = new StaticTestRetentionRule(
                    softDeleteRule ?? new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
                ["anonymise"] = new StaticTestRetentionRule(
                    anonymiseRule ?? new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                ),
            }
        );
    }

    private static ServiceProvider BuildFactoryBackedErasureServiceProvider(string connectionString)
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection().Build();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging();
        services.AddDbContext<FactoryBackedErasureDbContext>(options =>
            options.UseNpgsql(connectionString)
        );
        services.AddSingleton<IRetentionRuleProvider>(
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["factory-backed-set-based-erasure"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                    ["factory-backed-per-row-erasure"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                    ["converted-set-based-erasure"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                    ["converted-original-value-erasure"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                }
            )
        );
        services.AddSingleton<FactorySetBasedGuidFactory>();
        services.AddSingleton<FactoryPerRowSequenceFactory>();
        services.AddSingleton<FactoryOriginalValueEchoFactory>();
        services.AddSingleton<ConvertedSetBasedErasureFactory>();
        services.AddSingleton<ConvertedOriginalValueErasureFactory>();
        services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<FactorySetBasedGuidFactory>()
        );
        services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<FactoryPerRowSequenceFactory>()
        );
        services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<FactoryOriginalValueEchoFactory>()
        );
        services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<ConvertedSetBasedErasureFactory>()
        );
        services.AddSingleton<IAnonymiseValueFactory>(sp =>
            sp.GetRequiredService<ConvertedOriginalValueErasureFactory>()
        );
        services.AddCohort<FactoryBackedErasureDbContext>();

        return services.BuildServiceProvider(validateScopes: true);
    }

    private static ServiceProvider BuildMultiSubjectServiceProvider(string connectionString)
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection()
            .Build();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging();
        services.AddDbContext<MultiSubjectDbContext>(options =>
            options.UseNpgsql(connectionString)
        );
        services.AddSingleton<IRetentionRuleProvider>(
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["short-lived"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    ),
                }
            )
        );
        services.AddCohort<MultiSubjectDbContext>();

        return services.BuildServiceProvider(validateScopes: true);
    }

    private static ServiceProvider BuildConvertedErasureSubjectServiceProvider(
        string connectionString
    )
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection().Build();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging();
        services.AddDbContext<ConvertedErasureSubjectDbContext>(options =>
            options.UseNpgsql(connectionString)
        );
        services.AddSingleton<IRetentionRuleProvider>(
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["short-lived"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    ),
                }
            )
        );
        services.AddCohort<ConvertedErasureSubjectDbContext>();

        return services.BuildServiceProvider(validateScopes: true);
    }
}

internal sealed class FactoryBackedErasureDbContext(
    DbContextOptions<FactoryBackedErasureDbContext> options
) : DbContext(options)
{
    public DbSet<SetBasedFactoryErasureRecord> SetBasedFactoryErasureRecords =>
        Set<SetBasedFactoryErasureRecord>();
    public DbSet<PerRowFactoryErasureRecord> PerRowFactoryErasureRecords =>
        Set<PerRowFactoryErasureRecord>();
    public DbSet<ConvertedSetBasedErasureRecord> ConvertedSetBasedErasureRecords =>
        Set<ConvertedSetBasedErasureRecord>();
    public DbSet<ConvertedOriginalValueErasureRecord> ConvertedOriginalValueErasureRecords =>
        Set<ConvertedOriginalValueErasureRecord>();

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

        modelBuilder.Entity<ConvertedSetBasedErasureRecord>(entity =>
        {
            entity.ToTable("converted_set_based_erasure_records");
            entity.HasKey(record => record.Id);
            entity.Property(record => record.TenantId).HasColumnName("tenant_id");
            entity.Property(record => record.SubjectId).HasColumnName("subject_id");
            entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
            entity
                .Property(record => record.ExternalId)
                .HasColumnName("external_id")
                .HasConversion(
                    value => value.ToUpperInvariant(),
                    value => value.ToLowerInvariant()
                );
            entity.Property(record => record.Notes).HasColumnName("notes");
        });

        modelBuilder.Entity<ConvertedOriginalValueErasureRecord>(entity =>
        {
            entity.ToTable("converted_original_value_erasure_records");
            entity.HasKey(record => record.Id);
            entity.Property(record => record.TenantId).HasColumnName("tenant_id");
            entity.Property(record => record.SubjectId).HasColumnName("subject_id");
            entity.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
            entity
                .Property(record => record.ExternalId)
                .HasColumnName("external_id")
                .HasConversion(
                    value => value.ToUpperInvariant(),
                    value => value.ToLowerInvariant()
                );
            entity.Property(record => record.Notes).HasColumnName("notes");
        });

        modelBuilder.ConfigureCohortTables();
    }
}

[Retain("factory-backed-set-based-erasure", nameof(SetBasedFactoryErasureRecord.CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-000000000011")]
internal sealed class SetBasedFactoryErasureRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("user")]
    public Guid? SubjectId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    [AnonymiseWith(typeof(FactorySetBasedGuidFactory))]
    public Guid ExternalId { get; set; }

    public string Notes { get; set; } = "";

    public DateTimeOffset? AnonymisedAt { get; set; }
}

[Retain("factory-backed-per-row-erasure", nameof(PerRowFactoryErasureRecord.CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-000000000012")]
internal sealed class PerRowFactoryErasureRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("user")]
    public Guid? SubjectId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    [AnonymiseWith(typeof(FactoryOriginalValueEchoFactory))]
    public string ExternalId { get; set; } = "";

    [AnonymiseWith(typeof(FactoryPerRowSequenceFactory))]
    public string DisplayName { get; set; } = "";

    public string Notes { get; set; } = "";

    public DateTimeOffset? AnonymisedAt { get; set; }
}

[Retain("converted-set-based-erasure", nameof(ConvertedSetBasedErasureRecord.CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-000000000013")]
internal sealed class ConvertedSetBasedErasureRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("user")]
    public Guid? SubjectId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    [AnonymiseWith(typeof(ConvertedSetBasedErasureFactory))]
    public string ExternalId { get; set; } = "";

    public string Notes { get; set; } = "";

    public DateTimeOffset? AnonymisedAt { get; set; }
}

[Retain("converted-original-value-erasure", nameof(ConvertedOriginalValueErasureRecord.CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-000000000014")]
internal sealed class ConvertedOriginalValueErasureRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("user")]
    public Guid? SubjectId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    [AnonymiseWith(typeof(ConvertedOriginalValueErasureFactory))]
    public string ExternalId { get; set; } = "";

    public string Notes { get; set; } = "";

    public DateTimeOffset? AnonymisedAt { get; set; }
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

internal sealed class FactoryPerRowSequenceFactory : IAnonymiseValueFactory
{
    public AnonymiseFactoryExecutionMode ExecutionMode =>
        AnonymiseFactoryExecutionMode.PerRow;
    public List<AnonymiseValueContext> Contexts { get; } = [];
    private int sequence = 0;

    public object? Create(AnonymiseValueContext context)
    {
        Contexts.Add(context);
        sequence++;
        return $"erasure-per-row-{sequence}";
    }
}

internal sealed class FactoryOriginalValueEchoFactory : IAnonymiseValueFactory
{
    public AnonymiseFactoryExecutionMode ExecutionMode =>
        AnonymiseFactoryExecutionMode.PerRowWithOriginalValue;
    public List<AnonymiseValueContext> Contexts { get; } = [];

    public object? Create(AnonymiseValueContext context)
    {
        Contexts.Add(context);
        return $"{context.OriginalValue}-scrubbed";
    }
}

internal sealed class ConvertedSetBasedErasureFactory : IAnonymiseValueFactory
{
    public List<AnonymiseValueContext> Contexts { get; } = [];

    public object? Create(AnonymiseValueContext context)
    {
        Contexts.Add(context);
        return "set-based-erasure-scrubbed";
    }
}

internal sealed class ConvertedOriginalValueErasureFactory : IAnonymiseValueFactory
{
    public AnonymiseFactoryExecutionMode ExecutionMode =>
        AnonymiseFactoryExecutionMode.PerRowWithOriginalValue;
    public List<AnonymiseValueContext> Contexts { get; } = [];

    public object? Create(AnonymiseValueContext context)
    {
        Contexts.Add(context);
        return $"{context.OriginalValue}-scrubbed";
    }
}

internal sealed class MultiSubjectDbContext(DbContextOptions<MultiSubjectDbContext> options)
    : DbContext(options)
{
    public DbSet<MultiSubjectFixtureRecord> MultiSubjectFixtureRecords =>
        Set<MultiSubjectFixtureRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MultiSubjectFixtureRecord>(builder =>
        {
            builder.ToTable("multi_subject_fixture_records");
            builder.HasKey(record => record.Id);
            builder.Property(record => record.TenantId).IsRequired();
            builder.Property(record => record.PrimarySubjectId).HasColumnName("primary_subject_id");
            builder
                .Property(record => record.DelegateSubjectId)
                .HasColumnName("delegate_subject_id");
            builder.Property(record => record.CreatedAt).IsRequired();
            builder.Property(record => record.Body).IsRequired();
        });

        modelBuilder.ConfigureCohortTables();
    }
}

[Retain("short-lived", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-000000000016")]
internal sealed class MultiSubjectFixtureRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("user")]
    public Guid? PrimarySubjectId { get; set; }

    [ErasureSubject("user")]
    public Guid? DelegateSubjectId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
    public string Body { get; set; } = "";
}

internal sealed class ConvertedErasureSubjectDbContext(
    DbContextOptions<ConvertedErasureSubjectDbContext> options
) : DbContext(options)
{
    public DbSet<ConvertedErasureSubjectFixtureRecord> ConvertedErasureSubjectFixtureRecords =>
        Set<ConvertedErasureSubjectFixtureRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ConvertedErasureSubjectFixtureRecord>(builder =>
        {
            builder.ToTable("converted_erasure_subject_fixture_records");
            builder.HasKey(record => record.Id);
            builder.Property(record => record.TenantId).IsRequired();
            builder
                .Property(record => record.SubjectKey)
                .HasColumnName("external_subject_key")
                .HasColumnType("text")
                .HasConversion(
                    value => value.ToString("N").ToUpperInvariant(),
                    value => Guid.ParseExact(value, "N")
                );
            builder.Property(record => record.CreatedAt).IsRequired();
            builder.Property(record => record.Body).IsRequired();
        });

        modelBuilder.ConfigureCohortTables();
    }
}

[Retain("short-lived", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-000000000017")]
internal sealed class ConvertedErasureSubjectFixtureRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("user")]
    public Guid SubjectKey { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
    public string Body { get; set; } = "";
}

internal sealed class ModelVariant
{
    public bool IncludeExtended { get; set; }
}

internal sealed class ModelVariantDbContext(
    DbContextOptions<ModelVariantDbContext> options,
    ModelVariant variant
) : DbContext(options)
{
    public bool IncludeExtended => variant.IncludeExtended;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ModelVariantBaseRecord>().ToTable("model_variant_base_records");
        if (variant.IncludeExtended)
        {
            modelBuilder.Entity<ModelVariantExtendedRecord>().ToTable("model_variant_extended_records");
        }

        modelBuilder.ConfigureCohortTables();
    }
}

internal sealed class ModelVariantCacheKeyFactory : Microsoft.EntityFrameworkCore.Infrastructure.IModelCacheKeyFactory
{
    public object Create(DbContext context, bool designTime) =>
        (context.GetType(), ((ModelVariantDbContext)context).IncludeExtended, designTime);
}

[Retain("model-variant", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-0000000000a6")]
internal sealed class ModelVariantBaseRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("user")]
    public Guid SubjectId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
}

[Retain("model-variant", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-0000000000a7")]
internal sealed class ModelVariantExtendedRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("user")]
    public Guid SubjectId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
}

internal sealed class CitextSubjectDbContext(DbContextOptions<CitextSubjectDbContext> options)
    : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasPostgresExtension("citext");
        modelBuilder.Entity<CitextSubjectPurgeRecord>(builder =>
        {
            builder.ToTable("citext_subject_purge_records");
            builder.Property(record => record.Email).HasColumnType("citext");
        });
        modelBuilder.Entity<CitextSubjectAnonymiseRecord>(builder =>
        {
            builder.ToTable("citext_subject_anonymise_records");
            builder.Property(record => record.Email).HasColumnType("citext");
        });
        modelBuilder.ConfigureCohortTables();
    }
}

[Retain("citext-subject-purge", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-0000000000a1")]
internal sealed class CitextSubjectPurgeRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("email")]
    public string? Email { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
}

[Retain("citext-subject-anonymise", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-0000000000a2")]
internal sealed class CitextSubjectAnonymiseRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("email")]
    [Anonymise(AnonymiseMethod.Null)]
    public string? Email { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? AnonymisedAt { get; set; }
}

internal sealed class BoundedSubjectDbContext(DbContextOptions<BoundedSubjectDbContext> options)
    : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<BoundedSubjectRecord>(builder =>
        {
            builder.ToTable("bounded_subject_records");
            builder.Property(record => record.Id).HasMaxLength(8);
            builder.Property(record => record.Email).HasMaxLength(8);
        });
        modelBuilder.ConfigureCohortTables();
    }
}

[Retain("bounded-subject-purge", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-0000000000b1")]
internal sealed class BoundedSubjectRecord
{
    internal static readonly Guid RetentionId = Guid.Parse("00000000-0000-0000-0001-0000000000b1");

    public string Id { get; set; } = "";
    public Guid TenantId { get; set; }

    [ErasureSubject("email")]
    public string? Email { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
}

internal sealed class BoundedAnonymiseDbContext(DbContextOptions<BoundedAnonymiseDbContext> options)
    : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<BoundedAnonymiseRecord>(builder =>
        {
            builder.ToTable("bounded_anonymise_records");
            builder.Property(record => record.Email).HasMaxLength(8);
        });
        modelBuilder.ConfigureCohortTables();
    }
}

[Retain("bounded-anonymise", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-0000000000b2")]
internal sealed class BoundedAnonymiseRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("user")]
    public Guid SubjectId { get; set; }

    [Anonymise(AnonymiseMethod.FixedLiteral, "anonymised@example.invalid")]
    public string Email { get; set; } = "";

    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? AnonymisedAt { get; set; }
}

internal sealed class SinglePredicateResolutionDbContext(
    DbContextOptions<SinglePredicateResolutionDbContext> options
) : DbContext(options)
{
    public DbSet<SingleSubjectPredicateRecord> SingleSubjectRecords => Set<SingleSubjectPredicateRecord>();
    public DbSet<SubjectlessPredicateRecord> SubjectlessRecords => Set<SubjectlessPredicateRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SingleSubjectPredicateRecord>(builder =>
        {
            builder.ToTable("single_subject_predicate_records");
            builder.HasKey(record => record.Id);
            builder.Property(record => record.TenantId).HasColumnName("tenant_id");
            builder
                .Property(record => record.CustomerReference)
                .HasColumnName("external_subject_key");
            builder.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
        });

        modelBuilder.Entity<SubjectlessPredicateRecord>(builder =>
        {
            builder.ToTable("subjectless_predicate_records");
            builder.HasKey(record => record.Id);
            builder.Property(record => record.TenantId).HasColumnName("tenant_id");
            builder.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
        });

        modelBuilder.ConfigureCohortTables();
    }
}

[Retain("single-subject-erasure", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-000000000018")]
internal sealed class SingleSubjectPredicateRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("user")]
    public Guid? CustomerReference { get; set; }

    public DateTimeOffset? CreatedAt { get; set; }
}

[Retain("subjectless-erasure", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-000000000019")]
internal sealed class SubjectlessPredicateRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}

internal sealed class MultiPredicateResolutionDbContext(
    DbContextOptions<MultiPredicateResolutionDbContext> options
) : DbContext(options)
{
    public DbSet<MultiSubjectPredicateRecord> Records => Set<MultiSubjectPredicateRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MultiSubjectPredicateRecord>(builder =>
        {
            builder.ToTable("multi_subject_predicate_records");
            builder.HasKey(record => record.Id);
            builder.Property(record => record.TenantId).HasColumnName("tenant_id");
            builder.Property(record => record.PrimarySubjectId).HasColumnName("primary_subject_id");
            builder
                .Property(record => record.DelegateSubjectId)
                .HasColumnName("delegate_subject_id");
            builder.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
        });

        modelBuilder.ConfigureCohortTables();
    }
}

[Retain("multi-subject-erasure", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-00000000001a")]
internal sealed class MultiSubjectPredicateRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("user")]
    public Guid? PrimarySubjectId { get; set; }

    [ErasureSubject("user")]
    public Guid? DelegateSubjectId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
}

internal sealed class IncompatiblePredicateResolutionDbContext(
    DbContextOptions<IncompatiblePredicateResolutionDbContext> options
) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<IncompatibleMultiSubjectPredicateRecord>(builder =>
        {
            builder.ToTable("incompatible_multi_subject_predicate_records");
            builder.HasKey(record => record.Id);
            builder.Property(record => record.TenantId).HasColumnName("tenant_id");
            builder.Property(record => record.PrimarySubjectId).HasColumnName("primary_subject_id");
            builder
                .Property(record => record.AlternateSubjectId)
                .HasColumnName("alternate_subject_id");
            builder.Property(record => record.CreatedAt).HasColumnName("created_at_utc");
        });
        modelBuilder.Entity<IncompatibleKindRecord>(builder =>
            builder.ToTable("incompatible_kind_records")
        );

        modelBuilder.ConfigureCohortTables();
    }
}

[Retain("incompatible-multi-subject-erasure", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-00000000001b")]
internal sealed class IncompatibleMultiSubjectPredicateRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("user")]
    public Guid? PrimarySubjectId { get; set; }

    [ErasureSubject("email")]
    public string AlternateSubjectId { get; set; } = "";

    public DateTimeOffset CreatedAt { get; set; }
}

[Retain("incompatible-kind-erasure", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-00000000001d")]
internal sealed class IncompatibleKindRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject("user")]
    public string UserEmail { get; set; } = "";

    public DateTimeOffset CreatedAt { get; set; }
}

internal sealed class TemporaryDatabase(string connectionString, string databaseName)
    : IAsyncDisposable
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
            terminate.CommandText = $"""
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
