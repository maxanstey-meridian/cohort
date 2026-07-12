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
using Microsoft.Extensions.Options;
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
            new ErasureScope(subject, allowSoftDeleteAsErasure: true),
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
        var inputScope = new ErasureScope(subjectId, allowSoftDeleteAsErasure: true);

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
            CreateErasureCategoryRepository(
                shortLivedRule: new RetentionRule(
                    TimeSpan.FromDays(30),
                    Strategy.Purge,
                    AuditRowDetail: AuditRowDetail.PerRow,
                    Provenance: new RetentionRuleProvenance(
                        "retention-policy",
                        "subject erasure override"
                    )
                )
            ),
            CreateCohortSettings(dryRun: false)
        );

        var result = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            inputScope,
            asOf
        );

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
        // Held counts are measured directly (subject-matching, past cutoff, actively
        // held), so the anonymise erase path reports the held row too.
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
        var summaries = await LoadSummariesAsync(result.SweepId);
        var rowDetails = await LoadRowDetailsAsync(result.SweepId);

        run.Trigger.Should().Be(SweepTriggerKind.Erasure);
        run.DryRun.Should().BeFalse();
        result.DryRun.Should().BeFalse();
        run.TotalAffected.Should().Be(3);
        run.TenantId.Should().Be(tenantId);
        result.Scope.Should().Be(inputScope);
        result.StartedAt.Should().BeCloseTo(run.StartedAt, TimeSpan.FromMicroseconds(1));
        result.CompletedAt.Should().BeCloseTo(run.CompletedAt, TimeSpan.FromMicroseconds(1));
        result.CompletedAt.Should().BeOnOrAfter(result.StartedAt);
        summaries
            .Should()
            .Contain(
                new SweepRunEntitySummaryRow(
                    result.SweepId,
                    typeof(Note).FullName!,
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    TimeSpan.Zero,
                    1,
                    1,
                    0,
                    "retention-policy",
                    "subject erasure override"
                )
            );
        summaries
            .Should()
            .Contain(
                new SweepRunEntitySummaryRow(
                    result.SweepId,
                    typeof(SoftDeleteRecord).FullName!,
                    "soft-delete",
                    tenantId,
                    Strategy.SoftDelete,
                    TimeSpan.Zero,
                    1,
                    1,
                    0,
                    null,
                    null
                )
            );
        summaries
            .Should()
            .Contain(
                new SweepRunEntitySummaryRow(
                    result.SweepId,
                    typeof(AnonymisedContact).FullName!,
                    "anonymise",
                    tenantId,
                    Strategy.Anonymise,
                    TimeSpan.Zero,
                    1,
                    1,
                    0,
                    null,
                    null
                )
            );
        rowDetails.Should().ContainSingle();
        rowDetails[0]
            .Should()
            .Be(
                new SweepRunRowDetailRow(
                    result.SweepId,
                    typeof(Note).FullName!,
                    noteId.ToString(),
                    "short-lived",
                    Strategy.Purge,
                    tenantId
                )
            );
        result
            .Counts.Should()
            .BeEquivalentTo(
                summaries.Select(summary => new EntitySweepCount(
                    ResolveEntityType(summary.EntityType),
                    summary.Category,
                    summary.TenantId,
                    summary.Strategy,
                    summary.Affected,
                    summary.HeldCount,
                    summary.SkippedCount
                ))
            );

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.OrderBy(note => note.Body).Select(note => note.Body).ToListAsync())
            .Should()
            .Equal("held-note", "other-subject-note", "other-tenant-note");

        var softDeleteRecords = await verify
            .SoftDeleteRecords.OrderBy(record => record.Body)
            .ToListAsync();
        softDeleteRecords.Single(record => record.Id == softDeleteId).IsDeleted.Should().BeTrue();
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
            .BeNull();
        contacts.Single(contact => contact.Id == anonymisedContactId).GivenName.Should().BeEmpty();
        contacts
            .Single(contact => contact.Id == anonymisedContactId)
            .Surname.Should()
            .Be("[redacted]");
        contacts
            .Single(contact => contact.Id == anonymisedContactId)
            .Notes.Should()
            .Be("keep-notes");
        contacts
            .Single(contact => contact.Id == heldAnonymisedContactId)
            .EmailAddress.Should()
            .Be("held@example.com");
        contacts
            .Single(contact => contact.EmailAddress == "other@example.com")
            .GivenName.Should()
            .Be("Other");
        contacts
            .Single(contact => contact.EmailAddress == "tenant@example.com")
            .GivenName.Should()
            .Be("Other");
        verify
            .ErasureSubjectRecords.Single(record => record.Id == exemptErasureSubjectRecordId)
            .Body.Should()
            .Be("exempt-erasure-subject-record");
        verify.ErasureSubjectRecords.Should().HaveCount(2);
    }

    [Fact]
    public async Task Erase_Refuses_SoftDelete_Categories_Without_Explicit_OptIn()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        Guid recordId;

        await using (var db = Host.CreateDbContext())
        {
            var record = new SoftDeleteRecord
            {
                Id = Guid.NewGuid(),
                TenantId = tenantId,
                CreatedAt = asOf.AddDays(-120),
                Body = "refusal-record",
                SubjectId = subjectId,
            };
            recordId = record.Id;
            db.SoftDeleteRecords.Add(record);
            await db.SaveChangesAsync();
        }

        var act = async () =>
            await Host.RunErasureAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope(subjectId),
                asOf
            );

        await act.Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("*SoftDelete strategy*allowSoftDeleteAsErasure*");

        await using var verify = Host.CreateDbContext();
        var record2 = await verify.SoftDeleteRecords.SingleAsync(r => r.Id == recordId);
        record2.IsDeleted.Should().BeFalse();
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
            CreateErasureCategoryRepository(),
            CreateCohortSettings(dryRun: true)
        );

        var result = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
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
                    CreatedAt = EligibleErasureCreatedAt(asOf),
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
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
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
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "lock-check-note",
                }
            );
            await db.SaveChangesAsync();
        }

        using var erasureHost = new CohortTestHost(
            GetConnectionString(),
            CreateErasureCategoryRepository(),
            CreateCohortSettings(dryRun: true)
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
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
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
    public async Task Erase_Ignores_Ordinary_Period()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var withinPeriodNoteId = Guid.NewGuid();
        var eligibleNoteId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = withinPeriodNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-10),
                    Body = "within-period-note",
                },
                new Note
                {
                    Id = eligibleNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "eligible-period-note",
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
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 2)
            );

        var run = await LoadRunAsync(result.SweepId);
        var summaries = await LoadSummariesAsync(result.SweepId);

        run.TotalAffected.Should().Be(2);
        summaries
            .Should()
            .Contain(
                new SweepRunEntitySummaryRow(
                    result.SweepId,
                    typeof(Note).FullName!,
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    TimeSpan.Zero,
                    2,
                    0,
                    0
                )
            );

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == eligibleNoteId)).Should().BeFalse();
        (await verify.Notes.AnyAsync(note => note.Id == withinPeriodNoteId)).Should().BeFalse();
    }

    [Fact]
    public async Task Erase_Does_Not_Touch_Row_Within_LegalMin()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var withinLegalMinNoteId = Guid.NewGuid();
        var eligibleNoteId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = withinLegalMinNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "within-legal-min-note",
                },
                new Note
                {
                    Id = eligibleNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "eligible-legal-min-note",
                }
            );
            await db.SaveChangesAsync();
        }

        using var erasureHost = new CohortTestHost(
            GetConnectionString(),
            CreateErasureCategoryRepository(
                shortLivedRule: new RetentionRule(
                    TimeSpan.FromDays(30),
                    Strategy.Purge,
                    TimeSpan.FromDays(90),
                    AuditRowDetail: AuditRowDetail.PerRow
                )
            )
        );

        var result = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
            );

        var run = await LoadRunAsync(result.SweepId);
        var summaries = await LoadSummariesAsync(result.SweepId);

        run.TotalAffected.Should().Be(1);
        summaries
            .Should()
            .Contain(
                new SweepRunEntitySummaryRow(
                    result.SweepId,
                    typeof(Note).FullName!,
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    TimeSpan.FromDays(90),
                    1,
                    0,
                    0
                )
            );

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == eligibleNoteId)).Should().BeFalse();

        var withinLegalMinNote = await verify.Notes.SingleAsync(note =>
            note.Id == withinLegalMinNoteId
        );
        withinLegalMinNote.Body.Should().Be("within-legal-min-note");
        withinLegalMinNote.CreatedAt.Should().Be(asOf.AddDays(-45));
    }

    [Fact]
    public async Task PreviewErase_Matches_Mutation_Eligibility()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var noteEligibleId = Guid.NewGuid();
        var noteWithinPeriodId = Guid.NewGuid();
        var softDeleteEligibleId = Guid.NewGuid();
        var softDeleteWithinPeriodId = Guid.NewGuid();
        var contactEligibleId = Guid.NewGuid();
        var contactWithinPeriodId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = noteEligibleId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "preview-live-note-eligible",
                },
                new Note
                {
                    Id = noteWithinPeriodId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-10),
                    Body = "preview-live-note-within-period",
                }
            );
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord
                {
                    Id = softDeleteEligibleId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "preview-live-soft-delete-eligible",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = softDeleteWithinPeriodId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-10),
                    Body = "preview-live-soft-delete-within-period",
                    IsDeleted = false,
                }
            );
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = contactEligibleId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-45),
                    EmailAddress = "eligible@example.com",
                    GivenName = "Eligible",
                    Surname = "Contact",
                    Notes = "preview-live-contact-eligible",
                },
                new AnonymisedContact
                {
                    Id = contactWithinPeriodId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-10),
                    EmailAddress = "within-period@example.com",
                    GivenName = "Within",
                    Surname = "Period",
                    Notes = "preview-live-contact-within-period",
                }
            );
            await db.SaveChangesAsync();
        }

        var previewRepository = CreateErasureCategoryRepository();
        using var previewHost = new CohortTestHost(
            GetConnectionString(),
            previewRepository,
            CreateCohortSettings(dryRun: true)
        );

        var previewResult = await previewHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );

        var previewRun = await LoadRunAsync(previewResult.SweepId);
        var previewSummaries = await LoadSummariesAsync(previewResult.SweepId);

        previewRun.DryRun.Should().BeTrue();
        previewRun.TotalAffected.Should().Be(6);
        previewResult
            .Counts.Should()
            .Contain(
                new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 2)
            );
        previewResult
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantId,
                    Strategy.SoftDelete,
                    2
                )
            );
        previewResult
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(AnonymisedContact),
                    "anonymise",
                    tenantId,
                    Strategy.Anonymise,
                    2
                )
            );

        await using (var afterPreview = Host.CreateDbContext())
        {
            (await afterPreview.Notes.AnyAsync(note => note.Id == noteEligibleId))
                .Should()
                .BeTrue();
            (await afterPreview.Notes.AnyAsync(note => note.Id == noteWithinPeriodId))
                .Should()
                .BeTrue();

            (
                await afterPreview.SoftDeleteRecords.SingleAsync(record =>
                    record.Id == softDeleteEligibleId
                )
            )
                .IsDeleted.Should()
                .BeFalse();
            (
                await afterPreview.SoftDeleteRecords.SingleAsync(record =>
                    record.Id == softDeleteWithinPeriodId
                )
            )
                .IsDeleted.Should()
                .BeFalse();

            var previewEligibleContact = await afterPreview.AnonymisedContacts.SingleAsync(
                contact => contact.Id == contactEligibleId
            );
            previewEligibleContact.EmailAddress.Should().Be("eligible@example.com");
            previewEligibleContact.GivenName.Should().Be("Eligible");
            previewEligibleContact.Surname.Should().Be("Contact");
        }

        using var liveHost = new CohortTestHost(
            GetConnectionString(),
            CreateErasureCategoryRepository(),
            CreateCohortSettings(dryRun: false)
        );

        var liveResult = await liveHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );

        var liveRun = await LoadRunAsync(liveResult.SweepId);
        var liveSummaries = await LoadSummariesAsync(liveResult.SweepId);

        liveRun.DryRun.Should().BeFalse();
        liveRun.TotalAffected.Should().Be(6);
        previewResult.Counts.Should().BeEquivalentTo(liveResult.Counts);
        previewSummaries
            .Select(ProjectSummary)
            .Should()
            .BeEquivalentTo(liveSummaries.Select(ProjectSummary));

        await using var afterLive = Host.CreateDbContext();
        (await afterLive.Notes.AnyAsync(note => note.Id == noteEligibleId)).Should().BeFalse();
        (await afterLive.Notes.AnyAsync(note => note.Id == noteWithinPeriodId)).Should().BeFalse();

        (await afterLive.SoftDeleteRecords.SingleAsync(record => record.Id == softDeleteEligibleId))
            .IsDeleted.Should()
            .BeTrue();
        (
            await afterLive.SoftDeleteRecords.SingleAsync(record =>
                record.Id == softDeleteWithinPeriodId
            )
        )
            .IsDeleted.Should()
            .BeTrue();

        var liveEligibleContact = await afterLive.AnonymisedContacts.SingleAsync(contact =>
            contact.Id == contactEligibleId
        );
        liveEligibleContact.EmailAddress.Should().BeNull();
        liveEligibleContact.GivenName.Should().BeEmpty();
        liveEligibleContact.Surname.Should().Be("[redacted]");

        var liveWithinPeriodContact = await afterLive.AnonymisedContacts.SingleAsync(contact =>
            contact.Id == contactWithinPeriodId
        );
        liveWithinPeriodContact.EmailAddress.Should().BeNull();
        liveWithinPeriodContact.GivenName.Should().BeEmpty();
        liveWithinPeriodContact.Surname.Should().Be("[redacted]");
    }

    [Fact]
    public async Task PreviewErase_Matches_Mutation_Eligibility_When_LegalMin_Exceeds_Period()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var noteWithinLegalMinId = Guid.NewGuid();
        var noteEligibleId = Guid.NewGuid();
        var softDeleteWithinLegalMinId = Guid.NewGuid();
        var softDeleteEligibleId = Guid.NewGuid();
        var contactWithinLegalMinId = Guid.NewGuid();
        var contactEligibleId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var legalMinRule = new RetentionRule(
            TimeSpan.FromDays(30),
            Strategy.Purge,
            TimeSpan.FromDays(90),
            AuditRowDetail: AuditRowDetail.PerRow
        );

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = noteWithinLegalMinId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "preview-legal-min-note-within",
                },
                new Note
                {
                    Id = noteEligibleId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "preview-legal-min-note-eligible",
                }
            );
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord
                {
                    Id = softDeleteWithinLegalMinId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "preview-legal-min-soft-delete-within",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = softDeleteEligibleId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "preview-legal-min-soft-delete-eligible",
                    IsDeleted = false,
                }
            );
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = contactWithinLegalMinId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-45),
                    EmailAddress = "within-legal-min@example.com",
                    GivenName = "WithinLegalMin",
                    Surname = "Contact",
                    Notes = "preview-legal-min-contact-within",
                },
                new AnonymisedContact
                {
                    Id = contactEligibleId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "eligible-legal-min@example.com",
                    GivenName = "EligibleLegalMin",
                    Surname = "Contact",
                    Notes = "preview-legal-min-contact-eligible",
                }
            );
            await db.SaveChangesAsync();
        }

        var previewRepository = CreateErasureCategoryRepository(
            shortLivedRule: legalMinRule,
            softDeleteRule: new RetentionRule(
                TimeSpan.FromDays(30),
                Strategy.SoftDelete,
                TimeSpan.FromDays(90)
            ),
            anonymiseRule: new RetentionRule(
                TimeSpan.FromDays(30),
                Strategy.Anonymise,
                TimeSpan.FromDays(90)
            )
        );
        using var previewHost = new CohortTestHost(
            GetConnectionString(),
            previewRepository,
            CreateCohortSettings(dryRun: true)
        );

        var previewResult = await previewHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );

        var previewRun = await LoadRunAsync(previewResult.SweepId);
        var previewSummaries = await LoadSummariesAsync(previewResult.SweepId);

        previewRun.DryRun.Should().BeTrue();
        previewRun.TotalAffected.Should().Be(3);
        previewResult
            .Counts.Should()
            .Contain(
                new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
            );
        previewResult
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantId,
                    Strategy.SoftDelete,
                    1
                )
            );
        previewResult
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(AnonymisedContact),
                    "anonymise",
                    tenantId,
                    Strategy.Anonymise,
                    1
                )
            );
        previewSummaries
            .Should()
            .Contain(
                new SweepRunEntitySummaryRow(
                    previewResult.SweepId,
                    typeof(Note).FullName!,
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    TimeSpan.FromDays(90),
                    1,
                    0,
                    0
                )
            );
        previewSummaries
            .Should()
            .Contain(
                new SweepRunEntitySummaryRow(
                    previewResult.SweepId,
                    typeof(SoftDeleteRecord).FullName!,
                    "soft-delete",
                    tenantId,
                    Strategy.SoftDelete,
                    TimeSpan.FromDays(90),
                    1,
                    0,
                    0
                )
            );
        previewSummaries
            .Should()
            .Contain(
                new SweepRunEntitySummaryRow(
                    previewResult.SweepId,
                    typeof(AnonymisedContact).FullName!,
                    "anonymise",
                    tenantId,
                    Strategy.Anonymise,
                    TimeSpan.FromDays(90),
                    1,
                    0,
                    0
                )
            );

        await using (var afterPreview = Host.CreateDbContext())
        {
            (await afterPreview.Notes.AnyAsync(note => note.Id == noteWithinLegalMinId))
                .Should()
                .BeTrue();
            (await afterPreview.Notes.AnyAsync(note => note.Id == noteEligibleId))
                .Should()
                .BeTrue();

            (
                await afterPreview.SoftDeleteRecords.SingleAsync(record =>
                    record.Id == softDeleteWithinLegalMinId
                )
            )
                .IsDeleted.Should()
                .BeFalse();
            (
                await afterPreview.SoftDeleteRecords.SingleAsync(record =>
                    record.Id == softDeleteEligibleId
                )
            )
                .IsDeleted.Should()
                .BeFalse();

            var previewWithinLegalMinContact = await afterPreview.AnonymisedContacts.SingleAsync(
                contact => contact.Id == contactWithinLegalMinId
            );
            previewWithinLegalMinContact.EmailAddress.Should().Be("within-legal-min@example.com");

            var previewEligibleContact = await afterPreview.AnonymisedContacts.SingleAsync(
                contact => contact.Id == contactEligibleId
            );
            previewEligibleContact.EmailAddress.Should().Be("eligible-legal-min@example.com");
        }

        using var liveHost = new CohortTestHost(
            GetConnectionString(),
            CreateErasureCategoryRepository(
                shortLivedRule: legalMinRule,
                softDeleteRule: new RetentionRule(
                    TimeSpan.Zero,
                    Strategy.SoftDelete,
                    TimeSpan.FromDays(90)
                ),
                anonymiseRule: new RetentionRule(
                    TimeSpan.FromDays(30),
                    Strategy.Anonymise,
                    TimeSpan.FromDays(90)
                )
            ),
            CreateCohortSettings(dryRun: false)
        );

        var liveResult = await liveHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );

        var liveRun = await LoadRunAsync(liveResult.SweepId);
        var liveSummaries = await LoadSummariesAsync(liveResult.SweepId);

        liveRun.DryRun.Should().BeFalse();
        liveRun.TotalAffected.Should().Be(3);
        previewResult.Counts.Should().BeEquivalentTo(liveResult.Counts);
        previewSummaries
            .Select(ProjectSummary)
            .Should()
            .BeEquivalentTo(liveSummaries.Select(ProjectSummary));

        await using var afterLive = Host.CreateDbContext();
        (await afterLive.Notes.AnyAsync(note => note.Id == noteEligibleId)).Should().BeFalse();
        (await afterLive.Notes.AnyAsync(note => note.Id == noteWithinLegalMinId)).Should().BeTrue();

        (await afterLive.SoftDeleteRecords.SingleAsync(record => record.Id == softDeleteEligibleId))
            .IsDeleted.Should()
            .BeTrue();
        (
            await afterLive.SoftDeleteRecords.SingleAsync(record =>
                record.Id == softDeleteWithinLegalMinId
            )
        )
            .IsDeleted.Should()
            .BeFalse();

        var liveEligibleContact = await afterLive.AnonymisedContacts.SingleAsync(contact =>
            contact.Id == contactEligibleId
        );
        liveEligibleContact.EmailAddress.Should().BeNull();
        liveEligibleContact.GivenName.Should().BeEmpty();
        liveEligibleContact.Surname.Should().Be("[redacted]");

        var liveWithinLegalMinContact = await afterLive.AnonymisedContacts.SingleAsync(contact =>
            contact.Id == contactWithinLegalMinId
        );
        liveWithinLegalMinContact.EmailAddress.Should().Be("within-legal-min@example.com");
        liveWithinLegalMinContact.GivenName.Should().Be("WithinLegalMin");
        liveWithinLegalMinContact.Surname.Should().Be("Contact");
    }

    [Fact]
    public async Task Erase_Still_Respects_Active_Holds_When_Period_Is_Ignored()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var eligibleHeldNoteId = Guid.NewGuid();
        var eligibleUnheldNoteId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = eligibleHeldNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "eligible-held-note",
                },
                new Note
                {
                    Id = eligibleUnheldNoteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "eligible-unheld-note",
                }
            );
            await db.SaveChangesAsync();
        }

        await CreateHoldAsync("notes", eligibleHeldNoteId, tenantId, asOf);

        using var erasureHost = new CohortTestHost(
            GetConnectionString(),
            CreateErasureCategoryRepository()
        );

        var result = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );

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

        var summaries = await LoadSummariesAsync(result.SweepId);
        summaries
            .Should()
            .Contain(
                new SweepRunEntitySummaryRow(
                    result.SweepId,
                    typeof(Note).FullName!,
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    TimeSpan.Zero,
                    1,
                    1,
                    0
                )
            );

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == eligibleUnheldNoteId)).Should().BeFalse();

        var heldNote = await verify.Notes.SingleAsync(note => note.Id == eligibleHeldNoteId);
        heldNote.Body.Should().Be("eligible-held-note");
        heldNote.CreatedAt.Should().Be(asOf.AddDays(-45));
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
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
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
    public async Task Predicate_Construction_Failures_Are_Sanitized_Entity_Failures()
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

        var result = await Host.RunErasureAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope("not-a-guid", allowSoftDeleteAsErasure: true),
                asOf
            );

        result.EntityFailures.Should().NotBeEmpty();
        result.EntityFailures.Should().AllSatisfy(failure =>
        {
            failure.Should().MatchRegex(
                "^type=System\\.InvalidOperationException;code=hresult:0x80131509;diagnosticId=[0-9a-f]{32}$"
            );
            failure.Should().NotContain("SubjectId");
        });
    }

    [Fact]
    public async Task Erasure_Service_Executes_A_Single_Subject_Match_And_Skips_Non_Matching_Entities()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildPredicateResolutionServiceProvider<SinglePredicateResolutionDbContext>(
            database.ConnectionString,
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["single-subject-erasure"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge, AuditRowDetail: AuditRowDetail.PerRow)
                    ),
                    ["subjectless-erasure"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    ),
                }
            )
        );
        var tenantId = Guid.NewGuid();
        var wrongTenantId = Guid.NewGuid();
        var subjectId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var matchingId = Guid.NewGuid();
        var nonMatchingId = Guid.NewGuid();
        var wrongTenantIdRecord = Guid.NewGuid();
        var subjectlessId = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<SinglePredicateResolutionDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.SingleSubjectRecords.AddRange(
                new SingleSubjectPredicateRecord { Id = matchingId, TenantId = tenantId, CustomerReference = subjectId, CreatedAt = EligibleErasureCreatedAt(asOf) },
                new SingleSubjectPredicateRecord { Id = nonMatchingId, TenantId = tenantId, CustomerReference = Guid.NewGuid(), CreatedAt = EligibleErasureCreatedAt(asOf) },
                new SingleSubjectPredicateRecord { Id = wrongTenantIdRecord, TenantId = wrongTenantId, CustomerReference = subjectId, CreatedAt = EligibleErasureCreatedAt(asOf) }
            );
            db.SubjectlessRecords.Add(new SubjectlessPredicateRecord { Id = subjectlessId, TenantId = tenantId, CreatedAt = EligibleErasureCreatedAt(asOf) });
            await db.SaveChangesAsync();
        }

        ErasureResult result;
        await using (var scope = services.CreateAsyncScope())
        {
            result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>().EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope(subjectId),
                asOf
            );
        }

        result.Counts.Should().ContainSingle().Which.Should().Be(new EntitySweepCount(typeof(SingleSubjectPredicateRecord), "single-subject-erasure", tenantId, Strategy.Purge, 1));
        result.Counts.Should().NotContain(count => count.Category == "subjectless-erasure");

        await using var verifyScope = services.CreateAsyncScope();
        var verify = verifyScope.ServiceProvider.GetRequiredService<SinglePredicateResolutionDbContext>();
        (await verify.SingleSubjectRecords.Select(record => record.Id).ToListAsync())
            .Should().BeEquivalentTo([nonMatchingId, wrongTenantIdRecord]);
        (await verify.SubjectlessRecords.Select(record => record.Id).ToListAsync()).Should().ContainSingle().Which.Should().Be(subjectlessId);
        (await LoadSummariesAsync(verify, result.SweepId)).Should().ContainSingle().Which.Affected.Should().Be(1);
        (await LoadRowDetailsAsync(verify, result.SweepId)).Should().ContainSingle().Which.RecordId.Should().Be(matchingId.ToString());
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
                        new ErasureScope(subjectId),
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
                        new ErasureScope(subjectId),
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
                new ErasureScope(subjectId),
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
    public async Task Startup_Validation_Fails_When_Multi_Subject_Metadata_Uses_Incompatible_Effective_Types()
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
                }
            )
        );

        await using var scope = services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IncompatiblePredicateResolutionDbContext>().Database.EnsureCreatedAsync();
        var validator = scope.ServiceProvider.GetRequiredService<RetentionStartupValidator>();
        var act = () => validator.ValidateAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        var error = exception.Which.Errors.Should().ContainSingle().Which;
        error.Should().Contain("incompatible [ErasureSubject] properties");
        error.Should().Contain("AlternateSubjectId:String");
        error.Should().Contain("PrimarySubjectId:Guid");
    }

    [Fact]
    public async Task Erase_Path_Converts_SetBased_Factory_Output_To_Provider_Values_Before_Writing()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildFactoryBackedErasureServiceProvider(
            database.ConnectionString
        );
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<FactoryBackedErasureDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.ConvertedSetBasedErasureRecords.Add(
                new ConvertedSetBasedErasureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    ExternalId = "seed-value",
                    Notes = "converted-set-based-erasure",
                }
            );
            await db.SaveChangesAsync();
        }

        await using (var scope = services.CreateAsyncScope())
        {
            var erasureService =
                scope.ServiceProvider.GetRequiredService<IRetentionErasureService>();
            var result = await erasureService.EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
                asOf
            );

            result
                .Counts.Should()
                .Contain(
                    new EntitySweepCount(
                        typeof(ConvertedSetBasedErasureRecord),
                        "converted-set-based-erasure",
                        tenantId,
                        Strategy.Anonymise,
                        1
                    )
                );
        }

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<FactoryBackedErasureDbContext>();
            var factory =
                scope.ServiceProvider.GetRequiredService<ConvertedSetBasedErasureFactory>();
            var record = await db.ConvertedSetBasedErasureRecords.SingleAsync();
            var providerValue = await ReadProviderStringAsync(
                db,
                """
                SELECT external_id
                FROM converted_set_based_erasure_records
                """
            );

            record.ExternalId.Should().Be("set-based-erasure-scrubbed");
            providerValue.Should().Be("SET-BASED-ERASURE-SCRUBBED");
            factory.Contexts.Should().ContainSingle();
            factory.Contexts[0].OriginalValue.Should().BeNull();
            factory.Contexts[0].TenantId.Should().Be(tenantId);
            factory
                .Contexts[0]
                .MemberName.Should()
                .Be(nameof(ConvertedSetBasedErasureRecord.ExternalId));
        }
    }

    [Fact]
    public async Task Erase_Path_Converts_Provider_Values_Back_To_Clr_Values_Before_Building_OriginalValue_Context()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildFactoryBackedErasureServiceProvider(
            database.ConnectionString
        );
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<FactoryBackedErasureDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.ConvertedOriginalValueErasureRecords.Add(
                new ConvertedOriginalValueErasureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    ExternalId = "alpha",
                    Notes = "converted-original-value-erasure",
                }
            );
            await db.SaveChangesAsync();
        }

        await using (var scope = services.CreateAsyncScope())
        {
            var erasureService =
                scope.ServiceProvider.GetRequiredService<IRetentionErasureService>();
            var result = await erasureService.EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
                asOf
            );

            result
                .Counts.Should()
                .Contain(
                    new EntitySweepCount(
                        typeof(ConvertedOriginalValueErasureRecord),
                        "converted-original-value-erasure",
                        tenantId,
                        Strategy.Anonymise,
                        1
                    )
                );
        }

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<FactoryBackedErasureDbContext>();
            var factory =
                scope.ServiceProvider.GetRequiredService<ConvertedOriginalValueErasureFactory>();
            var record = await db.ConvertedOriginalValueErasureRecords.SingleAsync();
            var providerValue = await ReadProviderStringAsync(
                db,
                """
                SELECT external_id
                FROM converted_original_value_erasure_records
                """
            );

            record.ExternalId.Should().Be("alpha-scrubbed");
            providerValue.Should().Be("ALPHA-SCRUBBED");
            factory.Contexts.Should().ContainSingle();
            factory.Contexts[0].OriginalValue.Should().Be("alpha");
            factory.Contexts[0].TenantId.Should().Be(tenantId);
            factory
                .Contexts[0]
                .MemberName.Should()
                .Be(nameof(ConvertedOriginalValueErasureRecord.ExternalId));
        }
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
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "alias-match",
                },
                new AliasSubjectFixtureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CustomerReference = otherSubjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "alias-other-subject",
                },
                new AliasSubjectFixtureRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = otherTenantId,
                    CustomerReference = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "alias-other-tenant",
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
                new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
                asOf
            );
        }

        result
            .Counts.Should()
            .Contain(
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
            (
                await verify
                    .AliasSubjectFixtureRecords.Select(record => record.Body)
                    .OrderBy(body => body)
                    .ToListAsync()
            )
                .Should()
                .Equal("alias-other-subject", "alias-other-tenant");
            (await verify.AliasSubjectFixtureRecords.AnyAsync(record => record.Id == matchingId))
                .Should()
                .BeFalse();
        }
    }

    [Fact]
    public async Task Erasure_Path_Matches_When_The_First_Marked_Subject_Property_Equals_The_Requested_Subject()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildMultiSubjectServiceProvider(database.ConnectionString);

        var tenantId = Guid.NewGuid();
        var otherTenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var otherSubjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var matchingId = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<MultiSubjectDbContext>();
            await db.Database.EnsureCreatedAsync();

            db.MultiSubjectFixtureRecords.AddRange(
                new MultiSubjectFixtureRecord
                {
                    Id = matchingId,
                    TenantId = tenantId,
                    PrimarySubjectId = subjectId,
                    DelegateSubjectId = otherSubjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "primary-match",
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

        ErasureResult result;
        await using (var scope = services.CreateAsyncScope())
        {
            var erasureService =
                scope.ServiceProvider.GetRequiredService<IRetentionErasureService>();
            result = await erasureService.EraseAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
                asOf
            );
        }

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(MultiSubjectFixtureRecord),
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    1
                )
            );

        await using (var scope = services.CreateAsyncScope())
        {
            var verify = scope.ServiceProvider.GetRequiredService<MultiSubjectDbContext>();
            (
                await verify
                    .MultiSubjectFixtureRecords.Select(record => record.Body)
                    .OrderBy(body => body)
                    .ToListAsync()
            )
                .Should()
                .Equal("other-subject", "other-tenant");
            (await verify.MultiSubjectFixtureRecords.AnyAsync(record => record.Id == matchingId))
                .Should()
                .BeFalse();
        }
    }

    [Fact]
    public async Task Erasure_Path_Matches_When_Only_The_Second_Marked_Subject_Property_Equals_The_Requested_Subject()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var services = BuildMultiSubjectServiceProvider(database.ConnectionString);

        var tenantId = Guid.NewGuid();
        var otherTenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var otherSubjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var matchingId = Guid.NewGuid();

        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<MultiSubjectDbContext>();
            await db.Database.EnsureCreatedAsync();

            db.MultiSubjectFixtureRecords.AddRange(
                new MultiSubjectFixtureRecord
                {
                    Id = matchingId,
                    TenantId = tenantId,
                    PrimarySubjectId = otherSubjectId,
                    DelegateSubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "delegate-match",
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
                    PrimarySubjectId = otherSubjectId,
                    DelegateSubjectId = subjectId,
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
                new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
                asOf
            );
        }

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(MultiSubjectFixtureRecord),
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    1
                )
            );

        await using (var scope = services.CreateAsyncScope())
        {
            var verify = scope.ServiceProvider.GetRequiredService<MultiSubjectDbContext>();
            (
                await verify
                    .MultiSubjectFixtureRecords.Select(record => record.Body)
                    .OrderBy(body => body)
                    .ToListAsync()
            )
                .Should()
                .Equal("other-subject", "other-tenant");
            (await verify.MultiSubjectFixtureRecords.AnyAsync(record => record.Id == matchingId))
                .Should()
                .BeFalse();
        }
    }

    [Fact]
    public async Task Erasure_Path_DryRun_And_Live_MultiSubject_Matches_Ignore_Period_While_Holds_Block_Mutation()
    {
        await using var database = await TemporaryDatabase.CreateAsync(GetConnectionString());
        await using var previewServices = BuildMultiSubjectServiceProvider(
            database.ConnectionString,
            dryRun: true
        );
        await using var liveServices = BuildMultiSubjectServiceProvider(
            database.ConnectionString,
            dryRun: false
        );

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
                new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
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
                new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
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
                new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
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
                new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
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
                    CreatedAt = EligibleLegalMinErasureCreatedAt(asOf),
                    Body = "erasure-effective-period-note",
                }
            );
            await db.SaveChangesAsync();
        }

        using var erasureHost = new CohortTestHost(
            GetConnectionString(),
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["short-lived"] = new StaticTestRetentionRule(
                        new RetentionRule(
                            TimeSpan.FromDays(30),
                            Strategy.Purge,
                            TimeSpan.FromDays(90)
                        )
                    ),
                    ["soft-delete"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                    ),
                    ["anonymise"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Anonymise)
                    ),
                }
            )
        );

        var result = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );
        var summaries = await LoadSummariesAsync(result.SweepId);

        summaries
            .Should()
            .Contain(
                new SweepRunEntitySummaryRow(
                    result.SweepId,
                    typeof(Note).FullName!,
                    "short-lived",
                    tenantId,
                    Strategy.Purge,
                    TimeSpan.FromDays(90),
                    1,
                    0,
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

    private static async Task<string> ReadProviderStringAsync(DbContext db, string sql)
    {
        var connection = db.Database.GetDbConnection();
        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync();
        }

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return (string)(await command.ExecuteScalarAsync())!;
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
    public async Task Erasure_Startup_Rejects_A_Declared_Strategy_Whose_Metadata_Is_Invalid()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        var noteId = Guid.NewGuid();
        var contactId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    Body = "erased-despite-other-entity-failure",
                }
            );
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = contactId,
                    TenantId = tenantId,
                    SubjectId = subjectId,
                    CreatedAt = EligibleErasureCreatedAt(asOf),
                    EmailAddress = "kept@example.org",
                    GivenName = "Kept",
                    Surname = "Untouched",
                    Notes = "entity whose category misresolves at runtime",
                }
            );
            await db.SaveChangesAsync();
        }

        using var erasureHost = new CohortTestHost(
            GetConnectionString(),
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["short-lived"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                    ),
                    // Opaque deferred resolver: passes startup validation, then resolves
                    // SoftDelete for an entity with no IsDeleted member — an erase-time failure.
                    ["anonymise"] = new OpaqueSoftDeleteRuleResolver(),
                }
            ),
            CreateCohortSettings(dryRun: false)
        );

        var act = () => erasureHost.ValidateAndScanAsync();

        var exception = await act.Should().ThrowAsync<RetentionConfigurationException>();
        exception
            .Which.Errors.Should()
            .ContainSingle(error =>
                error.Contains(nameof(AnonymisedContact), StringComparison.Ordinal)
                && error.Contains("Soft-delete convention", StringComparison.Ordinal)
            );

        await using (var verify = Host.CreateDbContext())
        {
            (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
            (await verify.AnonymisedContacts.SingleAsync(contact => contact.Id == contactId))
                .EmailAddress.Should()
                .Be("kept@example.org");
        }

    }

    [Fact]
    public async Task Erase_Retires_The_Whole_Backlog_When_BatchSize_Is_Smaller_Than_The_Backlog()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            for (var index = 0; index < 3; index++)
            {
                db.Notes.Add(
                    new Note
                    {
                        Id = Guid.NewGuid(),
                        TenantId = tenantId,
                        SubjectId = subjectId,
                        CreatedAt = EligibleErasureCreatedAt(asOf),
                        Body = $"batched-erase-{index}",
                    }
                );
            }

            await db.SaveChangesAsync();
        }

        using var erasureHost = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:DryRun"] = "False",
                [$"{CohortOptions.SectionName}:SweepBatchSize"] = "1",
            }
        );

        var result = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            asOf
        );

        result
            .Counts.Should()
            .Contain(count => count.EntityType == typeof(Note) && count.Affected == 3);

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.TenantId == tenantId)).Should().BeFalse();
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
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
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

    private static Type ResolveEntityType(string entityType)
    {
        var resolved = AppDomain
            .CurrentDomain.GetAssemblies()
            .Select(assembly =>
                assembly.GetType(entityType, throwOnError: false, ignoreCase: false)
            )
            .FirstOrDefault(type => type is not null);

        return resolved
            ?? throw new InvalidOperationException(
                $"Could not resolve entity type '{entityType}'."
            );
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

    private static SummaryProjection ProjectSummary(SweepRunEntitySummaryRow summary)
    {
        return new SummaryProjection(
            summary.EntityType,
            summary.Category,
            summary.TenantId,
            summary.Strategy,
            summary.ResolvedPeriod,
            summary.Affected,
            summary.HeldCount,
            summary.SkippedCount,
            summary.NullAnchorCount
        );
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

    private sealed record SummaryProjection(
        string EntityType,
        string Category,
        Guid TenantId,
        Strategy Strategy,
        TimeSpan ResolvedPeriod,
        long Affected,
        long HeldCount,
        long SkippedCount,
        long NullAnchorCount
    );

    private sealed class StaticOptionsMonitor<T>(T currentValue) : IOptionsMonitor<T>
    {
        public T CurrentValue => currentValue;

        public T Get(string? name)
        {
            return currentValue;
        }

        public IDisposable? OnChange(Action<T, string?> listener)
        {
            return null;
        }
    }

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

    private static ServiceProvider BuildAliasSubjectServiceProvider(string connectionString)
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection().Build();

        services.AddSingleton<IConfiguration>(configuration);
        services.AddLogging();
        services.AddDbContext<AliasSubjectDbContext>(options =>
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
        services.AddCohort<AliasSubjectDbContext>();

        return services.BuildServiceProvider(validateScopes: true);
    }

    private static ServiceProvider BuildMultiSubjectServiceProvider(
        string connectionString,
        bool dryRun = false
    )
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(CreateCohortSettings(dryRun))
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

    [ErasureSubject]
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

    [ErasureSubject]
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

    [ErasureSubject]
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

    [ErasureSubject]
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

internal sealed class AliasSubjectDbContext(DbContextOptions<AliasSubjectDbContext> options)
    : DbContext(options)
{
    public DbSet<AliasSubjectFixtureRecord> AliasSubjectFixtureRecords =>
        Set<AliasSubjectFixtureRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AliasSubjectFixtureRecord>(builder =>
        {
            builder.ToTable("alias_subject_fixture_records");
            builder.HasKey(record => record.Id);
            builder.Property(record => record.TenantId).IsRequired();
            builder
                .Property(record => record.CustomerReference)
                .HasColumnName("external_subject_key");
            builder.Property(record => record.CreatedAt).IsRequired();
            builder.Property(record => record.Body).IsRequired();
        });

        modelBuilder.ConfigureCohortTables();
    }
}

[Retain("short-lived", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-000000000015")]
internal sealed class AliasSubjectFixtureRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject]
    public Guid? CustomerReference { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
    public string Body { get; set; } = "";
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

    [ErasureSubject]
    public Guid? PrimarySubjectId { get; set; }

    [ErasureSubject]
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

    [ErasureSubject]
    public Guid SubjectKey { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
    public string Body { get; set; } = "";
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

    [ErasureSubject]
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

    [ErasureSubject]
    public Guid? PrimarySubjectId { get; set; }

    [ErasureSubject]
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

        modelBuilder.ConfigureCohortTables();
    }
}

[Retain("incompatible-multi-subject-erasure", nameof(CreatedAt))]
[RetentionEntityId("00000000-0000-0000-0001-00000000001b")]
internal sealed class IncompatibleMultiSubjectPredicateRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }

    [ErasureSubject]
    public Guid? PrimarySubjectId { get; set; }

    [ErasureSubject]
    public string AlternateSubjectId { get; set; } = "";

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
