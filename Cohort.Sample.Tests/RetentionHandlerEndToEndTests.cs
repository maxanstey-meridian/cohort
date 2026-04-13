using System.Text.Json;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class RetentionHandlerEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Scheduled_Sweep_With_Handlers_Runs_OnBefore_In_Priority_Order_And_Queues_PostCommit_Work()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var sink = new HandlerExecutionSink();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "handler-target",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddRowHandler<Note, LowPriorityNoteHandler>();
                services.AddRowHandler<Note, HighPriorityNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
        );
        sink.BeforeCalls.Should().Equal("high", "low");

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeFalse();
        }

        var rowDetails = await LoadCapturedRowsAsync(result.SweepId);
        rowDetails.Should().ContainSingle();
        rowDetails[0].EntityType.Should().Be(typeof(Note).FullName);
        rowDetails[0].EntityId.Should().Be(noteId.ToString());

        using (var payload = JsonDocument.Parse(rowDetails[0].CapturedPayload))
        {
            payload.RootElement.GetProperty("body").GetString().Should().Be("handler-target");
            payload.RootElement.GetProperty("priority").GetString().Should().Be("high-first");
        }

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().HaveCount(2);
        statuses.Select(status => status.HandlerType).Should().Contain(type => type.Contains(nameof(HighPriorityNoteHandler), StringComparison.Ordinal));
        statuses.Select(status => status.HandlerType).Should().Contain(type => type.Contains(nameof(LowPriorityNoteHandler), StringComparison.Ordinal));
        statuses.All(status => status.State == 0 && status.Attempt == 0).Should().BeTrue();
    }

    [Fact]
    public async Task Scheduled_Sweep_Does_Not_Duplicate_Row_Detail_Audit_For_HandlerManaged_PerRow_Entities()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var logId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.PerRowAuditedLogs.Add(
                new PerRowAuditedLog
                {
                    Id = logId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Payload = "per-row-handler",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services => services.AddRowHandler<PerRowAuditedLog, PerRowAuditHandler>()
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(PerRowAuditedLog),
                "per-row-audit-override",
                tenantId,
                Strategy.Purge,
                1
            )
        );

        var rowDetails = await LoadCapturedRowsAsync(result.SweepId);
        rowDetails
            .Where(row => row.EntityType == typeof(PerRowAuditedLog).FullName)
            .Should()
            .ContainSingle(row => row.EntityId == logId.ToString());

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().ContainSingle(status => status.HandlerType.Contains(nameof(PerRowAuditHandler), StringComparison.Ordinal));
    }

    [Fact]
    public async Task Scheduled_Sweep_With_Handlers_Still_Executes_SoftDelete_And_Anonymise_Strategies()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var softDeleteId = Guid.NewGuid();
        var contactId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.SoftDeleteRecords.Add(
                new SoftDeleteRecord
                {
                    Id = softDeleteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "soft-delete-handler",
                    IsDeleted = false,
                }
            );
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = contactId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "handler@example.com",
                    GivenName = "Handler",
                    Surname = "Contact",
                    Notes = "keep-me",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.AddRowHandler<SoftDeleteRecord, SoftDeleteRecordHandler>();
                services.AddRowHandler<AnonymisedContact, AnonymisedContactHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
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

        await using (var verify = Host.CreateDbContext())
        {
            var softDeleted = await verify.SoftDeleteRecords.SingleAsync(record => record.Id == softDeleteId);
            softDeleted.IsDeleted.Should().BeTrue();
            softDeleted.DeletedAt.Should().Be(asOf);

            var anonymised = await verify.AnonymisedContacts.SingleAsync(contact => contact.Id == contactId);
            anonymised.EmailAddress.Should().BeNull();
            anonymised.GivenName.Should().BeEmpty();
            anonymised.Surname.Should().Be("[redacted]");
            anonymised.Notes.Should().Be("keep-me");
        }

        var rowDetails = await LoadCapturedRowsAsync(result.SweepId);
        rowDetails.Should().Contain(row => row.EntityType == typeof(SoftDeleteRecord).FullName && row.EntityId == softDeleteId.ToString());
        rowDetails.Should().Contain(row => row.EntityType == typeof(AnonymisedContact).FullName && row.EntityId == contactId.ToString());

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().Contain(status => status.HandlerType.Contains(nameof(SoftDeleteRecordHandler), StringComparison.Ordinal));
        statuses.Should().Contain(status => status.HandlerType.Contains(nameof(AnonymisedContactHandler), StringComparison.Ordinal));
    }

    [Fact]
    public async Task Scheduled_Sweep_Without_Handlers_Remains_On_The_Bulk_Path()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "bulk-delete-target",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
        );

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeFalse();
        }

        (await LoadCapturedRowsAsync(result.SweepId)).Should().BeEmpty();
        (await LoadHandlerStatusesAsync(result.SweepId)).Should().BeEmpty();
    }

    [Fact]
    public async Task Scheduled_Sweep_When_OnBefore_Fails_Skips_Only_That_Row_And_Continues_With_Siblings()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var successfulNoteId = Guid.NewGuid();
        var failingNoteId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = successfulNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "surviving-sibling",
                },
                new Note
                {
                    Id = failingNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = SelectivelyFailingNoteHandler.FailingBody,
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services => services.AddRowHandler<Note, SelectivelyFailingNoteHandler>()
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
        );

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == successfulNoteId)).Should().BeFalse();
            (await verify.Notes.AnyAsync(note => note.Id == failingNoteId)).Should().BeTrue();
        }

        var rowDetails = await LoadCapturedRowsAsync(result.SweepId);
        rowDetails.Should().ContainSingle(row => row.EntityId == successfulNoteId.ToString());
        rowDetails.Should().NotContain(row => row.EntityId == failingNoteId.ToString());

        using (var payload = JsonDocument.Parse(rowDetails[0].CapturedPayload))
        {
            payload.RootElement.GetProperty("body").GetString().Should().Be("surviving-sibling");
        }

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().ContainSingle(
            status => status.HandlerType.Contains(nameof(SelectivelyFailingNoteHandler), StringComparison.Ordinal)
        );

        var summaries = await LoadEntitySummariesAsync(result.SweepId);
        summaries.Should().ContainSingle(
            summary =>
                summary.EntityType == typeof(Note).FullName
                && summary.Affected == 1
                && summary.HeldCount == 1
        );
    }

    [Fact]
    public async Task Live_Erasure_When_OnBefore_Fails_Counts_The_Skipped_Row_In_Entity_Summaries()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var successfulNoteId = Guid.NewGuid();
        var failingNoteId = Guid.NewGuid();
        var successfulSoftDeleteId = Guid.NewGuid();
        var failingSoftDeleteId = Guid.NewGuid();
        var successfulContactId = Guid.NewGuid();
        var failingContactId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = successfulNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "successful-erasure-note",
                },
                new Note
                {
                    Id = failingNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = SelectivelyFailingErasureNoteHandler.FailingBody,
                }
            );
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord
                {
                    Id = successfulSoftDeleteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "successful-erasure-soft-delete",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = failingSoftDeleteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = SelectivelyFailingErasureSoftDeleteHandler.FailingBody,
                    IsDeleted = false,
                }
            );
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = successfulContactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    EmailAddress = "successful-erasure@example.com",
                    GivenName = "Successful",
                    Surname = "Contact",
                    Notes = "keep-successful-notes",
                },
                new AnonymisedContact
                {
                    Id = failingContactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    EmailAddress = SelectivelyFailingErasureAnonymisedContactHandler.FailingEmailAddress,
                    GivenName = "Failing",
                    Surname = "Contact",
                    Notes = "keep-failing-notes",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            CreateHandlerErasureCategoryRepository(),
            configureServices: services =>
            {
                services.AddRowHandler<Note, SelectivelyFailingErasureNoteHandler>();
                services.AddRowHandler<SoftDeleteRecord, SelectivelyFailingErasureSoftDeleteHandler>();
                services.AddRowHandler<AnonymisedContact, SelectivelyFailingErasureAnonymisedContactHandler>();
            }
        );

        var result = await handlerHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(SoftDeleteRecord), "soft-delete", tenantId, Strategy.SoftDelete, 1)
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(AnonymisedContact), "anonymise", tenantId, Strategy.Anonymise, 1)
        );

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == successfulNoteId)).Should().BeFalse();
            (await verify.Notes.AnyAsync(note => note.Id == failingNoteId)).Should().BeTrue();

            var successfulSoftDelete = await verify.SoftDeleteRecords.SingleAsync(record =>
                record.Id == successfulSoftDeleteId
            );
            successfulSoftDelete.IsDeleted.Should().BeTrue();
            successfulSoftDelete.DeletedAt.Should().Be(asOf);

            (await verify.SoftDeleteRecords.SingleAsync(record => record.Id == failingSoftDeleteId))
                .IsDeleted.Should().BeFalse();

            var successfulContact = await verify.AnonymisedContacts.SingleAsync(candidate =>
                candidate.Id == successfulContactId
            );
            successfulContact.EmailAddress.Should().BeNull();
            successfulContact.GivenName.Should().BeEmpty();
            successfulContact.Surname.Should().Be("[redacted]");
            successfulContact.Notes.Should().Be("keep-successful-notes");

            var failingContact = await verify.AnonymisedContacts.SingleAsync(candidate =>
                candidate.Id == failingContactId
            );
            failingContact.EmailAddress.Should().Be(
                SelectivelyFailingErasureAnonymisedContactHandler.FailingEmailAddress
            );
            failingContact.GivenName.Should().Be("Failing");
            failingContact.Surname.Should().Be("Contact");
            failingContact.Notes.Should().Be("keep-failing-notes");
        }

        var rowDetails = await LoadCapturedRowsAsync(result.SweepId);
        rowDetails.Select(row => row.EntityId).Should().BeEquivalentTo(
            [
                successfulNoteId.ToString(),
                successfulSoftDeleteId.ToString(),
                successfulContactId.ToString(),
            ]
        );

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().HaveCount(3);
        statuses.Select(status => status.HandlerType).Should().Contain(
            type => type.Contains(nameof(SelectivelyFailingErasureNoteHandler), StringComparison.Ordinal)
        );
        statuses.Select(status => status.HandlerType).Should().Contain(
            type =>
                type.Contains(nameof(SelectivelyFailingErasureSoftDeleteHandler), StringComparison.Ordinal)
        );
        statuses.Select(status => status.HandlerType).Should().Contain(
            type =>
                type.Contains(
                    nameof(SelectivelyFailingErasureAnonymisedContactHandler),
                    StringComparison.Ordinal
                )
        );

        var summaries = await LoadEntitySummariesAsync(result.SweepId);
        summaries.Should().Contain(
            new EntitySummaryRow(typeof(Note).FullName!, Strategy.Purge, 1, 1)
        );
        summaries.Should().Contain(
            new EntitySummaryRow(typeof(SoftDeleteRecord).FullName!, Strategy.SoftDelete, 1, 1)
        );
        summaries.Should().Contain(
            new EntitySummaryRow(typeof(AnonymisedContact).FullName!, Strategy.Anonymise, 1, 1)
        );
    }

    [Fact]
    public async Task Live_Erasure_With_Handlers_Captures_Target_Rows_And_Skips_Held_And_Exempt_Work()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var heldNoteId = Guid.NewGuid();
        var softDeleteId = Guid.NewGuid();
        var heldSoftDeleteId = Guid.NewGuid();
        var contactId = Guid.NewGuid();
        var heldContactId = Guid.NewGuid();
        var exemptRecordId = Guid.NewGuid();
        var sink = new HandlerExecutionSink();

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
                }
            );
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = contactId,
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
                    Id = heldContactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    EmailAddress = "held@example.com",
                    GivenName = "Held",
                    Surname = "Contact",
                    Notes = "held-notes",
                }
            );
            db.ErasureSubjectRecords.Add(
                new ErasureSubjectRecord
                {
                    Id = exemptRecordId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "exempt-erasure-record",
                }
            );
            await db.SaveChangesAsync();
        }

        await CreateHoldAsync("notes", heldNoteId, tenantId, asOf);
        await CreateHoldAsync("soft_delete_records", heldSoftDeleteId, tenantId, asOf);
        await CreateHoldAsync("anonymised_contacts", heldContactId, tenantId, asOf);

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            CreateHandlerErasureCategoryRepository(),
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddRowHandler<Note, NoteErasureTrackingHandler>();
                services.AddRowHandler<SoftDeleteRecord, SoftDeleteErasureTrackingHandler>();
                services.AddRowHandler<AnonymisedContact, AnonymisedContactErasureTrackingHandler>();
                services.AddRowHandler<ErasureSubjectRecord, ExemptErasureTrackingHandler>();
            }
        );

        var result = await handlerHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(SoftDeleteRecord), "soft-delete", tenantId, Strategy.SoftDelete, 1)
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(AnonymisedContact), "anonymise", tenantId, Strategy.Anonymise, 1)
        );
        sink.BeforeCalls.Should().BeEquivalentTo(
            [
                "note:erase-note",
                "soft:erase-soft-delete",
                "contact:subject@example.com",
            ]
        );

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeFalse();
            (await verify.Notes.AnyAsync(note => note.Id == heldNoteId)).Should().BeTrue();

            var softDelete = await verify.SoftDeleteRecords.SingleAsync(record => record.Id == softDeleteId);
            softDelete.IsDeleted.Should().BeTrue();
            softDelete.DeletedAt.Should().Be(asOf);
            (await verify.SoftDeleteRecords.SingleAsync(record => record.Id == heldSoftDeleteId))
                .IsDeleted.Should().BeFalse();

            var contact = await verify.AnonymisedContacts.SingleAsync(candidate => candidate.Id == contactId);
            contact.EmailAddress.Should().BeNull();
            contact.GivenName.Should().BeEmpty();
            contact.Surname.Should().Be("[redacted]");
            (await verify.AnonymisedContacts.SingleAsync(candidate => candidate.Id == heldContactId))
                .EmailAddress.Should().Be("held@example.com");

            (await verify.ErasureSubjectRecords.SingleAsync(record => record.Id == exemptRecordId))
                .Body.Should().Be("exempt-erasure-record");
        }

        var rowDetails = await LoadCapturedRowsAsync(result.SweepId);
        rowDetails.Should().HaveCount(3);
        rowDetails.Select(row => row.EntityId).Should().BeEquivalentTo(
            [noteId.ToString(), softDeleteId.ToString(), contactId.ToString()]
        );

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().HaveCount(3);
        statuses.All(status => status.State == 0 && status.Attempt == 0).Should().BeTrue();
        statuses.Select(status => status.HandlerType).Should().Contain(
            type => type.Contains(nameof(NoteErasureTrackingHandler), StringComparison.Ordinal)
        );
        statuses.Select(status => status.HandlerType).Should().Contain(
            type => type.Contains(nameof(SoftDeleteErasureTrackingHandler), StringComparison.Ordinal)
        );
        statuses.Select(status => status.HandlerType).Should().Contain(
            type => type.Contains(nameof(AnonymisedContactErasureTrackingHandler), StringComparison.Ordinal)
        );
        statuses.Select(status => status.HandlerType).Should().NotContain(
            type => type.Contains(nameof(ExemptErasureTrackingHandler), StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task DryRun_Erasure_With_Handlers_Does_Not_Invoke_Handlers_Or_Persist_Handler_Work()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var softDeleteId = Guid.NewGuid();
        var contactId = Guid.NewGuid();
        var exemptRecordId = Guid.NewGuid();
        var sink = new HandlerExecutionSink();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "dry-run-note",
                }
            );
            db.SoftDeleteRecords.Add(
                new SoftDeleteRecord
                {
                    Id = softDeleteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "dry-run-soft-delete",
                    IsDeleted = false,
                }
            );
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = contactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    EmailAddress = "dry-run@example.com",
                    GivenName = "Dry",
                    Surname = "Run",
                    Notes = "keep-me",
                }
            );
            db.ErasureSubjectRecords.Add(
                new ErasureSubjectRecord
                {
                    Id = exemptRecordId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-1),
                    Body = "dry-run-exempt",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            CreateHandlerErasureCategoryRepository(),
            CreateCohortSettings(dryRun: true),
            services =>
            {
                services.AddSingleton(sink);
                services.AddRowHandler<Note, NoteErasureTrackingHandler>();
                services.AddRowHandler<SoftDeleteRecord, SoftDeleteErasureTrackingHandler>();
                services.AddRowHandler<AnonymisedContact, AnonymisedContactErasureTrackingHandler>();
                services.AddRowHandler<ErasureSubjectRecord, ExemptErasureTrackingHandler>();
            }
        );

        var result = await handlerHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(SoftDeleteRecord), "soft-delete", tenantId, Strategy.SoftDelete, 1)
        );
        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(AnonymisedContact), "anonymise", tenantId, Strategy.Anonymise, 1)
        );
        sink.BeforeCalls.Should().BeEmpty();
        (await LoadCapturedRowsAsync(result.SweepId)).Should().BeEmpty();
        (await LoadHandlerStatusesAsync(result.SweepId)).Should().BeEmpty();

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
        (await verify.SoftDeleteRecords.SingleAsync(record => record.Id == softDeleteId))
            .IsDeleted.Should().BeFalse();
        (await verify.AnonymisedContacts.SingleAsync(candidate => candidate.Id == contactId))
            .EmailAddress.Should().Be("dry-run@example.com");
        (await verify.ErasureSubjectRecords.SingleAsync(record => record.Id == exemptRecordId))
            .Body.Should().Be("dry-run-exempt");
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
                    "handler-erasure-hold",
                    asOf.AddDays(-1)
                ),
                CancellationToken.None
            );
        });
    }

    private async Task<IReadOnlyList<CapturedRow>> LoadCapturedRowsAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT "EntityType", "EntityId", COALESCE("CapturedPayload", '')
            FROM "sweep_run_row_detail"
            WHERE "SweepId" = @sweepId
            ORDER BY "Id"
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);

        var rows = new List<CapturedRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new CapturedRow(reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        }

        return rows;
    }

    private async Task<IReadOnlyList<HandlerStatusRow>> LoadHandlerStatusesAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT status."HandlerType", status."State", status."Attempt"
            FROM "sweep_row_handler_status" AS status
            INNER JOIN "sweep_run_row_detail" AS detail ON detail."Id" = status."SweepRunRowDetailId"
            WHERE detail."SweepId" = @sweepId
            ORDER BY status."Id"
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);

        var rows = new List<HandlerStatusRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new HandlerStatusRow(reader.GetString(0), reader.GetInt32(1), reader.GetInt32(2)));
        }

        return rows;
    }

    private async Task<IReadOnlyList<EntitySummaryRow>> LoadEntitySummariesAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT "EntityType", "Strategy", "Affected", "HeldCount"
            FROM "sweep_run_entity_summary"
            WHERE "SweepId" = @sweepId
            ORDER BY "EntityType"
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);

        var rows = new List<EntitySummaryRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(
                new EntitySummaryRow(
                    reader.GetString(0),
                    (Strategy)reader.GetInt32(1),
                    reader.GetInt32(2),
                    reader.GetInt32(3)
                )
            );
        }

        return rows;
    }

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }

    private static IRetentionCategoryRepository CreateHandlerErasureCategoryRepository()
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

    private sealed record CapturedRow(string EntityType, string EntityId, string CapturedPayload);

    private sealed record HandlerStatusRow(string HandlerType, int State, int Attempt);

    private sealed record EntitySummaryRow(
        string EntityType,
        Strategy Strategy,
        int Affected,
        int HeldCount
    );

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
}

file sealed class HandlerExecutionSink
{
    public List<string> BeforeCalls { get; } = [];
}

[RowHandlerPriority(20)]
file sealed class LowPriorityNoteHandler(HandlerExecutionSink sink) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        sink.BeforeCalls.Add("low");
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }
}

[RowHandlerPriority(10)]
file sealed class HighPriorityNoteHandler(HandlerExecutionSink sink) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        sink.BeforeCalls.Add("high");
        ctx.Snapshot["priority"] = "high-first";
        return Task.CompletedTask;
    }
}

file sealed class PerRowAuditHandler : IRetentionHandler<PerRowAuditedLog>
{
    public Task OnBeforeAsync(
        PerRowAuditedLog row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        ctx.Snapshot["payload"] = row.Payload;
        return Task.CompletedTask;
    }
}

file sealed class SoftDeleteRecordHandler : IRetentionHandler<SoftDeleteRecord>
{
    public Task OnBeforeAsync(
        SoftDeleteRecord row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }
}

file sealed class AnonymisedContactHandler : IRetentionHandler<AnonymisedContact>
{
    public Task OnBeforeAsync(
        AnonymisedContact row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        ctx.Snapshot["email"] = row.EmailAddress;
        return Task.CompletedTask;
    }
}

file sealed class SelectivelyFailingNoteHandler : IRetentionHandler<Note>
{
    public const string FailingBody = "fail-before-delete";

    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        if (string.Equals(row.Body, FailingBody, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Simulated handler failure.");
        }

        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }
}

file sealed class NoteErasureTrackingHandler(HandlerExecutionSink sink) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        sink.BeforeCalls.Add($"note:{row.Body}");
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }
}

file sealed class SelectivelyFailingErasureNoteHandler : IRetentionHandler<Note>
{
    public const string FailingBody = "fail-erasure-note";

    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        if (string.Equals(row.Body, FailingBody, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Simulated note erasure failure.");
        }

        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }
}

file sealed class SoftDeleteErasureTrackingHandler(HandlerExecutionSink sink)
    : IRetentionHandler<SoftDeleteRecord>
{
    public Task OnBeforeAsync(
        SoftDeleteRecord row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        sink.BeforeCalls.Add($"soft:{row.Body}");
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }
}

file sealed class SelectivelyFailingErasureSoftDeleteHandler : IRetentionHandler<SoftDeleteRecord>
{
    public const string FailingBody = "fail-erasure-soft-delete";

    public Task OnBeforeAsync(
        SoftDeleteRecord row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        if (string.Equals(row.Body, FailingBody, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Simulated soft-delete erasure failure.");
        }

        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }
}

file sealed class AnonymisedContactErasureTrackingHandler(HandlerExecutionSink sink)
    : IRetentionHandler<AnonymisedContact>
{
    public Task OnBeforeAsync(
        AnonymisedContact row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        sink.BeforeCalls.Add($"contact:{row.EmailAddress}");
        ctx.Snapshot["email"] = row.EmailAddress;
        return Task.CompletedTask;
    }
}

file sealed class SelectivelyFailingErasureAnonymisedContactHandler
    : IRetentionHandler<AnonymisedContact>
{
    public const string FailingEmailAddress = "fail-erasure@example.com";

    public Task OnBeforeAsync(
        AnonymisedContact row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        if (string.Equals(row.EmailAddress, FailingEmailAddress, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Simulated anonymise erasure failure.");
        }

        ctx.Snapshot["email"] = row.EmailAddress;
        return Task.CompletedTask;
    }
}

file sealed class ExemptErasureTrackingHandler(HandlerExecutionSink sink)
    : IRetentionHandler<ErasureSubjectRecord>
{
    public Task OnBeforeAsync(
        ErasureSubjectRecord row,
        RetentionBeforeContext ctx,
        CancellationToken ct
    )
    {
        sink.BeforeCalls.Add($"exempt:{row.Body}");
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }
}
