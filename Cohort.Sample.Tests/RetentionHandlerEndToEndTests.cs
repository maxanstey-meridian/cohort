using System.Text.Json;

using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class RetentionHandlerEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    private const int PendingState = 0;
    private const int SucceededState = 2;
    private const int DeadLetteredState = 3;

    [Fact]
    public async Task Scheduled_Sweep_With_A_BlobBacked_Fixture_Captures_StoragePath_And_Uses_TestHost_Service_Registrations()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var fileId = Guid.NewGuid();
        var cleanupStore = new BlobCleanupStoreSpy();

        await using (var db = Host.CreateDbContext())
        {
            db.BlobBackedFiles.Add(
                new BlobBackedFile
                {
                    Id = fileId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    StoragePath = "blob://tenant-a/archive/invoice.pdf",
                    OriginalFileName = "invoice.pdf",
                    ContentType = "application/pdf",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.AddSingleton(cleanupStore);
                services.AddRowHandler<BlobBackedFile, BlobBackedFileCleanupHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(typeof(BlobBackedFile), "blob-cleanup", tenantId, Strategy.Purge, 1)
        );

        var rowDetails = await LoadCapturedRowsAsync(result.SweepId);
        rowDetails.Should().ContainSingle(row => row.EntityType == typeof(BlobBackedFile).FullName);
        using (var payload = JsonDocument.Parse(rowDetails[0].CapturedPayload))
        {
            payload.RootElement.GetProperty("storagePath").GetString().Should().Be("blob://tenant-a/archive/invoice.pdf");
            payload.RootElement.GetProperty("originalFileName").GetString().Should().Be("invoice.pdf");
        }

        var queuedStatuses = await LoadHandlerStatusesAsync(result.SweepId);
        queuedStatuses.Should().ContainSingle(status =>
            status.HandlerType.Contains(nameof(BlobBackedFileCleanupHandler), StringComparison.Ordinal)
            && status.State == PendingState
            && status.Attempt == 0
        );
        cleanupStore.DeletedPaths.Should().BeEmpty();

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        cleanupStore.DeletedPaths.Should().Equal("blob://tenant-a/archive/invoice.pdf");

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.BlobBackedFiles.AnyAsync(file => file.Id == fileId)).Should().BeFalse();
        }

        var completedStatuses = await LoadHandlerStatusesAsync(result.SweepId);
        completedStatuses.Should().ContainSingle();
        completedStatuses[0].State.Should().Be(SucceededState);
        completedStatuses[0].Attempt.Should().Be(1);
        completedStatuses[0].CompletedAt.Should().NotBeNull();
        completedStatuses[0].LastError.Should().BeNull();
    }

    [Fact]
    public async Task Scheduled_Sweep_Flows_OnBefore_Snapshot_Into_OnAfter_With_The_Persisted_Metadata()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var recorder = new AfterContextRecorder();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "scheduled-after-context-target",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxParallelism"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(recorder);
                services.AddScoped<ScopedDispatchProbe>();
                services.AddRowHandler<Note, ContextCapturingNoteHandler>();
            }
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
            (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeFalse();
        }

        var persistedRows = await LoadCapturedRowDetailsAsync(result.SweepId);
        persistedRows.Should().ContainSingle(row => row.EntityId == noteId.ToString());

        var queuedStatuses = await LoadHandlerStatusesAsync(result.SweepId);
        queuedStatuses.Should().ContainSingle(status =>
            status.HandlerType.Contains(nameof(ContextCapturingNoteHandler), StringComparison.Ordinal)
            && status.State == PendingState
            && status.Attempt == 0
        );

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        var call = recorder.Load().Should().ContainSingle().Subject;
        var persisted = persistedRows.Single();

        call.SweepId.Should().Be(result.SweepId);
        call.EntityId.Should().Be(noteId.ToString());
        call.Category.Should().Be(persisted.Category);
        call.Strategy.Should().Be(persisted.Strategy);
        call.TenantId.Should().Be(persisted.TenantId);
        call.At.Should().Be(persisted.At);
        call.Attempt.Should().Be(1);
        call.Body.Should().Be("scheduled-after-context-target");
        call.Body.Should().Be(persisted.Body);

        var completedStatuses = await LoadHandlerStatusesAsync(result.SweepId);
        completedStatuses.Should().ContainSingle(status =>
            status.HandlerType.Contains(nameof(ContextCapturingNoteHandler), StringComparison.Ordinal)
            && status.State == SucceededState
            && status.Attempt == 1
            && status.CompletedAt != null
            && status.LastError == null
        );
    }

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
    public async Task Scheduled_Sweep_Without_Handlers_Keeps_SoftDelete_And_Anonymise_On_The_Bulk_Path()
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
                    Body = "bulk-soft-delete-target",
                    IsDeleted = false,
                }
            );
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = contactId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "bulk-path@example.com",
                    GivenName = "Bulk",
                    Surname = "Path",
                    Notes = "preserve-me",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunSweepAsync(
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
            anonymised.Notes.Should().Be("preserve-me");
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

    [Fact]
    public async Task FlushAsync_Claims_Each_Queued_Status_At_Most_Once_When_Called_Concurrently()
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
                    Body = "concurrent-dispatch",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddRowHandler<Note, DispatchRecordingNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await Task.WhenAll(dispatcher.FlushAsync(), dispatcher.FlushAsync());
        });

        sink.AfterCalls.Should().Equal("after:concurrent-dispatch:1");

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().ContainSingle();
        statuses[0].State.Should().Be(SucceededState);
        statuses[0].Attempt.Should().Be(1);
        statuses[0].ClaimedAt.Should().NotBeNull();
        statuses[0].CompletedAt.Should().NotBeNull();
        statuses[0].LastError.Should().BeNull();
    }

    [Fact]
    public async Task FlushAsync_Cancellation_Requeues_All_Claimed_Rows_So_A_Later_Flush_Can_Drain_Them()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var firstNoteId = Guid.NewGuid();
        var secondNoteId = Guid.NewGuid();
        var sink = new HandlerExecutionSink();
        var gate = new DispatchBlockGate();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = firstNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "cancelled-batch-first",
                },
                new Note
                {
                    Id = secondNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "cancelled-batch-second",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:BatchSize"] = "10",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxParallelism"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddSingleton(gate);
                services.AddRowHandler<Note, BlockingDispatchNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        using var cancellation = new CancellationTokenSource();
        await handlerHost.RunWithServicesAsync(
            async serviceProvider =>
            {
                var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
                var flushTask = dispatcher.FlushAsync(cancellation.Token);

                await gate.WaitUntilBlockedAsync();
                cancellation.Cancel();

                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => flushTask);
            }
        );

        gate.Release();

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        sink.AfterCalls.Should().BeEquivalentTo(
            [
                "after:cancelled-batch-first:1",
                "after:cancelled-batch-second:1",
            ]
        );

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().HaveCount(2);
        statuses.All(status => status.State == SucceededState).Should().BeTrue();
        statuses.All(status => status.Attempt == 1).Should().BeTrue();
        statuses.All(status => status.CompletedAt is not null).Should().BeTrue();
    }

    [Fact]
    public async Task FlushAsync_Cancellation_After_Handler_Completes_Does_Not_Requeue_The_Same_Row()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var sink = new HandlerExecutionSink();
        var blocker = new StatusUpdateBlocker(GetConnectionString());

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "post-handler-cancellation",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:BatchSize"] = "10",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxParallelism"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddSingleton(blocker);
                services.AddRowHandler<Note, LocksStatusThenReturnsNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        using var cancellation = new CancellationTokenSource();
        await handlerHost.RunWithServicesAsync(
            async serviceProvider =>
            {
                var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
                var flushTask = dispatcher.FlushAsync(cancellation.Token);

                await blocker.WaitUntilLockedAsync();
                cancellation.Cancel();
                await blocker.ReleaseAsync();

                try
                {
                    await flushTask;
                }
                catch (OperationCanceledException)
                {
                }
            }
        );

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        sink.AfterCalls.Should().Equal("after:post-handler-cancellation:1");

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().ContainSingle();
        statuses[0].State.Should().Be(SucceededState);
        statuses[0].Attempt.Should().Be(1);
        statuses[0].CompletedAt.Should().NotBeNull();
        statuses[0].LastError.Should().BeNull();
    }

    [Fact]
    public async Task Hosted_Dispatcher_Requeues_Retryable_Failures_With_Backoff_And_Last_Error()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var tracker = new DispatchAttemptTracker();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "retry-pending",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:PollInterval"] = "00:10:00",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:BaseBackoff"] = "00:05:00",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "3",
            },
            configureServices: services =>
            {
                services.AddSingleton(tracker);
                services.AddRowHandler<Note, AlwaysFailingDispatchNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var hostedDispatcher = serviceProvider.GetServices<IHostedService>().Single(service =>
                service is IRetentionRowDispatcher
            );
            await hostedDispatcher.StartAsync(CancellationToken.None);

            try
            {
                var status = await WaitForHandlerStatusAsync(
                    result.SweepId,
                    row => row.State == PendingState && row.Attempt == 1 && row.LastError is not null
                );
                var failureAt = tracker.LoadLastFailure(noteId.ToString());

                status.ClaimedAt.Should().BeNull();
                status.CompletedAt.Should().BeNull();
                failureAt.Should().NotBeNull();
                status.NextAttemptAt.Should().BeCloseTo(
                    failureAt!.Value.AddMinutes(5),
                    TimeSpan.FromSeconds(15)
                );
                status.LastError.Should().Contain("Simulated permanent after-dispatch failure.");
            }
            finally
            {
                await hostedDispatcher.StopAsync(CancellationToken.None);
            }
        });

        tracker.LoadAttempt(noteId.ToString()).Should().Be(1);
    }

    [Fact]
    public async Task FlushAsync_Rebuilds_Full_AfterContext_From_Persisted_Row_Detail_Using_A_Fresh_Scope_Per_Dispatch()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var firstNoteId = Guid.NewGuid();
        var secondNoteId = Guid.NewGuid();
        var recorder = new AfterContextRecorder();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = firstNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "after-context-first",
                },
                new Note
                {
                    Id = secondNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "after-context-second",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxParallelism"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(recorder);
                services.AddScoped<ScopedDispatchProbe>();
                services.AddRowHandler<Note, ContextCapturingNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        var persistedRows = await LoadCapturedRowDetailsAsync(result.SweepId);
        var observed = recorder.Load().OrderBy(call => call.EntityId, StringComparer.Ordinal).ToArray();

        observed.Should().HaveCount(2);
        observed.Select(call => call.ScopeInstanceId).Should().OnlyHaveUniqueItems();

        foreach (var call in observed)
        {
            var persisted = persistedRows.Single(row => row.EntityId == call.EntityId);
            call.SweepId.Should().Be(result.SweepId);
            call.Category.Should().Be(persisted.Category);
            call.Strategy.Should().Be(persisted.Strategy);
            call.TenantId.Should().Be(persisted.TenantId);
            call.At.Should().Be(persisted.At);
            call.Attempt.Should().Be(1);
            call.Body.Should().Be(persisted.Body);
        }
    }

    [Fact]
    public async Task FlushAsync_Drains_All_Due_Work_Across_Multiple_Claim_Batches_In_One_Call()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var firstNoteId = Guid.NewGuid();
        var secondNoteId = Guid.NewGuid();
        var thirdNoteId = Guid.NewGuid();
        var sink = new HandlerExecutionSink();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = firstNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "batch-one",
                },
                new Note
                {
                    Id = secondNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "batch-two",
                },
                new Note
                {
                    Id = thirdNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "batch-three",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:BatchSize"] = "1",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxParallelism"] = "1",
            },
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddRowHandler<Note, DispatchRecordingNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        sink.AfterCalls.Should().BeEquivalentTo(
            [
                "after:batch-one:1",
                "after:batch-two:1",
                "after:batch-three:1",
            ]
        );

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().HaveCount(3);
        statuses.All(status => status.State == SucceededState).Should().BeTrue();
        statuses.All(status => status.Attempt == 1).Should().BeTrue();
        statuses.All(status => status.CompletedAt is not null).Should().BeTrue();
    }

    [Fact]
    public async Task FlushAsync_Retries_Transient_Failures_And_Drains_Them_To_Succeeded()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var sink = new HandlerExecutionSink();
        var tracker = new DispatchAttemptTracker();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "retry-then-succeed",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:BaseBackoff"] = "00:05:00",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "3",
            },
            configureServices: services =>
            {
                services.AddSingleton(sink);
                services.AddSingleton(tracker);
                services.AddRowHandler<Note, RetryOnceDispatchNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        tracker.LoadAttempt(noteId.ToString()).Should().Be(2);
        sink.AfterCalls.Should().Equal("attempt:1", "attempt:2");

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().ContainSingle();
        statuses[0].State.Should().Be(SucceededState);
        statuses[0].Attempt.Should().Be(2);
        statuses[0].CompletedAt.Should().NotBeNull();
        statuses[0].LastError.Should().BeNull();
    }

    [Fact]
    public async Task FlushAsync_DeadLetters_Exhausted_Handler_Failures()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var tracker = new DispatchAttemptTracker();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "dead-letter-target",
                }
            );
            await db.SaveChangesAsync();
        }

        using var handlerHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:BaseBackoff"] = "00:05:00",
                [$"{CohortOptions.SectionName}:RowHandlerDispatch:MaxAttempts"] = "2",
            },
            configureServices: services =>
            {
                services.AddSingleton(tracker);
                services.AddRowHandler<Note, AlwaysFailingDispatchNoteHandler>();
            }
        );

        var result = await handlerHost.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await handlerHost.RunWithServicesAsync(async serviceProvider =>
        {
            var dispatcher = serviceProvider.GetRequiredService<IRetentionRowDispatcher>();
            await dispatcher.FlushAsync();
        });

        tracker.LoadAttempt(noteId.ToString()).Should().Be(2);

        var statuses = await LoadHandlerStatusesAsync(result.SweepId);
        statuses.Should().ContainSingle();
        statuses[0].State.Should().Be(DeadLetteredState);
        statuses[0].Attempt.Should().Be(2);
        statuses[0].CompletedAt.Should().NotBeNull();
        statuses[0].LastError.Should().Contain("Simulated permanent after-dispatch failure.");
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

    private async Task<IReadOnlyList<CapturedRowDetail>> LoadCapturedRowDetailsAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT
                "EntityId",
                "Category",
                "Strategy",
                "TenantId",
                "At",
                COALESCE("CapturedPayload", '')
            FROM "sweep_run_row_detail"
            WHERE "SweepId" = @sweepId
            ORDER BY "Id"
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);

        var rows = new List<CapturedRowDetail>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var payload = reader.GetString(5);
            using var snapshot = JsonDocument.Parse(payload);
            rows.Add(
                new CapturedRowDetail(
                    reader.GetString(0),
                    reader.GetString(1),
                    (Strategy)reader.GetInt32(2),
                    reader.GetGuid(3),
                    reader.GetFieldValue<DateTimeOffset>(4),
                    snapshot.RootElement.GetProperty("body").GetString()!
                )
            );
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
            SELECT
                status."HandlerType",
                status."State",
                status."Attempt",
                status."NextAttemptAt",
                status."ClaimedAt",
                status."CompletedAt",
                status."LastError"
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
            rows.Add(
                new HandlerStatusRow(
                    reader.GetString(0),
                    reader.GetInt32(1),
                    reader.GetInt32(2),
                    reader.GetFieldValue<DateTimeOffset>(3),
                    reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTimeOffset>(4),
                    reader.IsDBNull(5) ? null : reader.GetFieldValue<DateTimeOffset>(5),
                    reader.IsDBNull(6) ? null : reader.GetString(6)
                )
            );
        }

        return rows;
    }

    private async Task<HandlerStatusRow> WaitForHandlerStatusAsync(
        Guid sweepId,
        Func<HandlerStatusRow, bool> predicate,
        TimeSpan? timeout = null
    )
    {
        var deadline = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));

        while (DateTimeOffset.UtcNow <= deadline)
        {
            var match = (await LoadHandlerStatusesAsync(sweepId)).SingleOrDefault(predicate);
            if (match is not null)
            {
                return match;
            }

            await Task.Delay(50);
        }

        throw new TimeoutException(
            $"Timed out waiting for a handler status row that matched the requested predicate for sweep {sweepId}."
        );
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

    private sealed record CapturedRowDetail(
        string EntityId,
        string Category,
        Strategy Strategy,
        Guid TenantId,
        DateTimeOffset At,
        string Body
    );

    private sealed record HandlerStatusRow(
        string HandlerType,
        int State,
        int Attempt,
        DateTimeOffset NextAttemptAt,
        DateTimeOffset? ClaimedAt,
        DateTimeOffset? CompletedAt,
        string? LastError
    );

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

    public List<string> AfterCalls { get; } = [];
}

file sealed class BlobCleanupStoreSpy
{
    public List<string> DeletedPaths { get; } = [];

    public Task DeleteAsync(string storagePath)
    {
        DeletedPaths.Add(storagePath);
        return Task.CompletedTask;
    }
}

file sealed class DispatchAttemptTracker
{
    private readonly object gate = new();
    private readonly Dictionary<string, int> attempts = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DateTimeOffset> lastFailures = new(StringComparer.Ordinal);

    public int Increment(string entityId)
    {
        lock (gate)
        {
            var next = attempts.TryGetValue(entityId, out var current) ? current + 1 : 1;
            attempts[entityId] = next;
            return next;
        }
    }

    public int LoadAttempt(string entityId)
    {
        lock (gate)
        {
            return attempts.TryGetValue(entityId, out var attempt) ? attempt : 0;
        }
    }

    public void RecordFailure(string entityId, DateTimeOffset at)
    {
        lock (gate)
        {
            lastFailures[entityId] = at;
        }
    }

    public DateTimeOffset? LoadLastFailure(string entityId)
    {
        lock (gate)
        {
            return lastFailures.TryGetValue(entityId, out var failureAt) ? failureAt : null;
        }
    }
}

file sealed class DispatchBlockGate
{
    private readonly TaskCompletionSource blocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private volatile bool released;

    public Task WaitUntilBlockedAsync()
    {
        return blocked.Task;
    }

    public async Task WaitForReleaseAsync(CancellationToken ct)
    {
        blocked.TrySetResult();

        while (!released)
        {
            await Task.Delay(10, ct);
        }
    }

    public void Release()
    {
        released = true;
    }
}

file sealed class StatusUpdateBlocker(string connectionString) : IAsyncDisposable
{
    private readonly TaskCompletionSource locked = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private NpgsqlConnection? connection;
    private NpgsqlTransaction? transaction;

    public Task WaitUntilLockedAsync()
    {
        return locked.Task;
    }

    public async Task AcquireAsync(CancellationToken ct)
    {
        connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        transaction = await connection.BeginTransactionAsync(ct);

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """LOCK TABLE "sweep_row_handler_status" IN ACCESS EXCLUSIVE MODE""";
        await command.ExecuteNonQueryAsync(ct);
        locked.TrySetResult();
    }

    public async Task ReleaseAsync()
    {
        if (transaction is not null)
        {
            await transaction.CommitAsync();
            await transaction.DisposeAsync();
            transaction = null;
        }

        if (connection is not null)
        {
            await connection.DisposeAsync();
            connection = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await ReleaseAsync();
    }
}

file sealed class AfterContextRecorder
{
    private readonly object gate = new();
    private readonly List<AfterContextCall> calls = [];

    public void Record(AfterContextCall call)
    {
        lock (gate)
        {
            calls.Add(call);
        }
    }

    public IReadOnlyList<AfterContextCall> Load()
    {
        lock (gate)
        {
            return calls.ToArray();
        }
    }
}

file sealed record AfterContextCall(
    Guid SweepId,
    string EntityId,
    string Category,
    Strategy Strategy,
    Guid TenantId,
    DateTimeOffset At,
    int Attempt,
    string Body,
    Guid ScopeInstanceId
);

file sealed class ScopedDispatchProbe
{
    public Guid InstanceId { get; } = Guid.NewGuid();
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

file sealed class BlobBackedFileCleanupHandler(BlobCleanupStoreSpy cleanupStore)
    : IRetentionHandler<BlobBackedFile>
{
    public Task OnBeforeAsync(BlobBackedFile row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["storagePath"] = row.StoragePath;
        ctx.Snapshot["originalFileName"] = row.OriginalFileName;
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<BlobBackedFile> ctx, CancellationToken ct)
    {
        return cleanupStore.DeleteAsync((string)ctx.Snapshot["storagePath"]!);
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

file sealed class DispatchRecordingNoteHandler(HandlerExecutionSink sink) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public async Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        await Task.Delay(100, ct);
        sink.AfterCalls.Add($"after:{ctx.Snapshot["body"]}:{ctx.Attempt}");
    }
}

file sealed class ContextCapturingNoteHandler(
    AfterContextRecorder recorder,
    ScopedDispatchProbe probe
) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        recorder.Record(
            new AfterContextCall(
                ctx.SweepId,
                ctx.EntityId,
                ctx.Category,
                ctx.Strategy,
                ctx.TenantId,
                ctx.At,
                ctx.Attempt,
                (string)ctx.Snapshot["body"]!,
                probe.InstanceId
            )
        );

        return Task.CompletedTask;
    }
}

file sealed class RetryOnceDispatchNoteHandler(
    HandlerExecutionSink sink,
    DispatchAttemptTracker tracker
) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        var attempt = tracker.Increment(ctx.EntityId);
        sink.AfterCalls.Add($"attempt:{attempt}");

        if (attempt == 1)
        {
            throw new InvalidOperationException("Simulated transient after-dispatch failure.");
        }

        return Task.CompletedTask;
    }
}

file sealed class AlwaysFailingDispatchNoteHandler(
    DispatchAttemptTracker tracker
) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        tracker.Increment(ctx.EntityId);
        tracker.RecordFailure(ctx.EntityId, DateTimeOffset.UtcNow);
        throw new InvalidOperationException("Simulated permanent after-dispatch failure.");
    }
}

file sealed class BlockingDispatchNoteHandler(
    HandlerExecutionSink sink,
    DispatchBlockGate gate
) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public async Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        await gate.WaitForReleaseAsync(ct);
        sink.AfterCalls.Add($"after:{ctx.Snapshot["body"]}:{ctx.Attempt}");
    }
}

file sealed class LocksStatusThenReturnsNoteHandler(
    HandlerExecutionSink sink,
    StatusUpdateBlocker blocker
) : IRetentionHandler<Note>
{
    public Task OnBeforeAsync(Note row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["body"] = row.Body;
        return Task.CompletedTask;
    }

    public async Task OnAfterAsync(RetentionAfterContext<Note> ctx, CancellationToken ct)
    {
        sink.AfterCalls.Add($"after:{ctx.Snapshot["body"]}:{ctx.Attempt}");
        await blocker.AcquireAsync(ct);
    }
}
