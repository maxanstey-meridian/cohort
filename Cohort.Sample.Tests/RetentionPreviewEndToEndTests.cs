using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class RetentionPreviewEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Shared_Host_Preview_Path_Returns_Candidate_Counts_Without_Deleting_Rows()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "preview-delete-me",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-5),
                    Body = "preview-keep-newer",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "preview-keep-other-tenant",
                }
            );
            db.ExemptDocuments.Add(
                new ExemptDocument
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = asOf.AddDays(-400),
                    Title = "preview-exempt-document",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunPreviewAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
        );

        result
            .Counts.Should()
            .Contain(new EntitySweepCount(typeof(Note), "short-lived", tenantA, Strategy.Purge, 1));
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantA,
                    Strategy.SoftDelete,
                    0
                )
            );
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(AnonymisedContact),
                    "anonymise",
                    tenantA,
                    Strategy.Anonymise,
                    0
                )
            );

        await using var verify = Host.CreateDbContext();
        var noteBodies = await verify
            .Notes.OrderBy(note => note.Body)
            .Select(note => note.Body)
            .ToListAsync();
        noteBodies
            .Should()
            .Equal("preview-delete-me", "preview-keep-newer", "preview-keep-other-tenant");
        var exemptTitles = await verify
            .ExemptDocuments.OrderBy(document => document.Title)
            .Select(document => document.Title)
            .ToListAsync();
        exemptTitles.Should().Equal("preview-exempt-document");
    }

    [Fact]
    public async Task Preview_Path_Returns_Zero_Counts_For_Exempt_Runtime_Rules_Without_Deleting_Rows()
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
                    CreatedAt = asOf.AddDays(-180),
                    Body = "preview-exempt-note",
                }
            );
            await db.SaveChangesAsync();
        }

        using var previewHost = new CohortTestHost(
            GetConnectionString(),
            new StaticCategoryRepository(
                new Dictionary<string, ITestRetentionRule>
                {
                    ["short-lived"] = new StaticTestRetentionRule(
                        new RetentionRule(TimeSpan.FromDays(30), Strategy.Exempt)
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

        var result = await previewHost.RunPreviewAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Exempt, 0)
            );
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantId,
                    Strategy.SoftDelete,
                    0
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
                    0
                )
            );

        await using var verify = Host.CreateDbContext();
        var noteBodies = await verify.Notes.Select(note => note.Body).ToListAsync();
        noteBodies.Should().Equal("preview-exempt-note");
    }

    [Fact]
    public async Task Preview_Path_Uses_The_Greater_Of_Period_And_Legal_Min_Without_Deleting_Rows()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-45),
                    Body = "preview-keep-legal-min",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "preview-count-legal-min",
                }
            );
            await db.SaveChangesAsync();
        }

        using var previewHost = new CohortTestHost(
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

        var result = await previewHost.RunPreviewAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(typeof(Note), "short-lived", tenantId, Strategy.Purge, 1)
            );
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantId,
                    Strategy.SoftDelete,
                    0
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
                    0
                )
            );

        await using var verify = Host.CreateDbContext();
        var noteBodies = await verify
            .Notes.OrderBy(note => note.Body)
            .Select(note => note.Body)
            .ToListAsync();
        noteBodies.Should().Equal("preview-count-legal-min", "preview-keep-legal-min");
    }

    [Fact]
    public async Task Preview_Path_Ignores_Legacy_Notes_With_Null_TenantId_For_Targeted_Runs()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "preview-count-target-tenant",
                },
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "preview-ignore-other-tenant",
                }
            );
            await db.SaveChangesAsync();

            await db.Database.ExecuteSqlRawAsync(
                """
                ALTER TABLE "notes" ALTER COLUMN "TenantId" DROP NOT NULL;
                INSERT INTO "notes" ("Id", "TenantId", "SubjectId", "CreatedAt", "Body")
                VALUES ({0}, NULL, NULL, {1}, {2});
                """,
                Guid.NewGuid(),
                asOf.AddDays(-120),
                "preview-ignore-null-tenant"
            );
        }

        try
        {
            var result = await Host.RunPreviewAsync(
                new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
                asOf
            );

            result
                .Counts.Should()
                .Contain(new EntitySweepCount(typeof(Note), "short-lived", tenantA, Strategy.Purge, 1));
            result
                .Counts.Should()
                .Contain(
                    new EntitySweepCount(
                        typeof(SoftDeleteRecord),
                        "soft-delete",
                        tenantA,
                        Strategy.SoftDelete,
                        0
                    )
                );
            result
                .Counts.Should()
                .Contain(
                    new EntitySweepCount(
                        typeof(AnonymisedContact),
                        "anonymise",
                        tenantA,
                        Strategy.Anonymise,
                        0
                    )
                );

            await using var verify = Host.CreateDbContext();
            var noteBodies = await verify
                .Notes.OrderBy(note => note.Body)
                .Select(note => note.Body)
                .ToListAsync();
            noteBodies
                .Should()
                .Equal(
                    "preview-count-target-tenant",
                    "preview-ignore-null-tenant",
                    "preview-ignore-other-tenant"
                );
        }
        finally
        {
            await using var cleanup = Host.CreateDbContext();
            await cleanup.Database.ExecuteSqlRawAsync(
                """
                DELETE FROM "notes" WHERE "TenantId" IS NULL;
                ALTER TABLE "notes" ALTER COLUMN "TenantId" SET NOT NULL;
                """
            );
        }
    }

    [Fact]
    public async Task Preview_Path_Counts_Anonymise_Candidates_Without_Modifying_Rows()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-45),
                    EmailAddress = "preview-expired@example.com",
                    GivenName = "Expired",
                    Surname = "Candidate",
                    Notes = "preview-count-target-tenant",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantA,
                    CreatedAt = asOf.AddDays(-5),
                    EmailAddress = "preview-current@example.com",
                    GivenName = "Current",
                    Surname = "Candidate",
                    Notes = "preview-ignore-current-row",
                },
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantB,
                    CreatedAt = asOf.AddDays(-45),
                    EmailAddress = "preview-other-tenant@example.com",
                    GivenName = "Other",
                    Surname = "Tenant",
                    Notes = "preview-ignore-other-tenant",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunPreviewAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
        );

        result
            .Counts.Should()
            .Contain(new EntitySweepCount(typeof(Note), "short-lived", tenantA, Strategy.Purge, 0));
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantA,
                    Strategy.SoftDelete,
                    0
                )
            );
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(AnonymisedContact),
                    "anonymise",
                    tenantA,
                    Strategy.Anonymise,
                    1
                )
            );

        await using var verify = Host.CreateDbContext();
        var contacts = await verify
            .AnonymisedContacts.OrderBy(contact => contact.Notes)
            .Select(contact => new
            {
                contact.EmailAddress,
                contact.GivenName,
                contact.Surname,
                contact.Notes,
            })
            .ToListAsync();

        contacts
            .Should()
            .Equal(
                new
                {
                    EmailAddress = (string?)"preview-expired@example.com",
                    GivenName = "Expired",
                    Surname = "Candidate",
                    Notes = "preview-count-target-tenant",
                },
                new
                {
                    EmailAddress = (string?)"preview-current@example.com",
                    GivenName = "Current",
                    Surname = "Candidate",
                    Notes = "preview-ignore-current-row",
                },
                new
                {
                    EmailAddress = (string?)"preview-other-tenant@example.com",
                    GivenName = "Other",
                    Surname = "Tenant",
                    Notes = "preview-ignore-other-tenant",
                }
            );
    }

    [Fact]
    public async Task Preview_Path_Excludes_Active_Holds_From_Candidate_Counts()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var heldNoteId = Guid.NewGuid();
        var heldSoftDeleteId = Guid.NewGuid();
        var heldAnonymisedContactId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "preview-unheld-note",
                },
                new Note
                {
                    Id = heldNoteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "preview-held-note",
                }
            );
            db.SoftDeleteRecords.AddRange(
                new SoftDeleteRecord
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "preview-unheld-soft-delete",
                    IsDeleted = false,
                },
                new SoftDeleteRecord
                {
                    Id = heldSoftDeleteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "preview-held-soft-delete",
                    IsDeleted = false,
                }
            );
            db.AnonymisedContacts.AddRange(
                new AnonymisedContact
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "preview-unheld@example.com",
                    GivenName = "Preview",
                    Surname = "Unheld",
                    Notes = "keep",
                },
                new AnonymisedContact
                {
                    Id = heldAnonymisedContactId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "preview-held@example.com",
                    GivenName = "Preview",
                    Surname = "Held",
                    Notes = "keep",
                }
            );
            await db.SaveChangesAsync();
        }

        await CreateHoldAsync("notes", heldNoteId, tenantId, asOf);
        await CreateHoldAsync("soft_delete_records", heldSoftDeleteId, tenantId, asOf);
        await CreateHoldAsync("anonymised_contacts", heldAnonymisedContactId, tenantId, asOf);

        var result = await Host.RunPreviewAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
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

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.OrderBy(note => note.Body).Select(note => note.Body).ToListAsync())
            .Should()
            .Equal("preview-held-note", "preview-unheld-note");
        (
            await verify
                .SoftDeleteRecords.OrderBy(record => record.Body)
                .Select(record => record.Body)
                .ToListAsync()
        )
            .Should()
            .Equal("preview-held-soft-delete", "preview-unheld-soft-delete");
        (
            await verify
                .AnonymisedContacts.OrderBy(contact => contact.EmailAddress)
                .Select(contact => contact.EmailAddress)
                .ToListAsync()
        )
            .Should()
            .Equal("preview-held@example.com", "preview-unheld@example.com");
    }

    [Fact]
    public async Task Preview_Reports_The_Same_Measured_Held_And_Null_Anchor_Counts_As_Dry_Run()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var heldId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.NullableAnchorEvents.AddRange(
                new NullableAnchorEvent
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    OccurredAt = asOf.AddDays(-120),
                    Payload = "preview-measured-eligible",
                },
                new NullableAnchorEvent
                {
                    Id = heldId,
                    TenantId = tenantId,
                    OccurredAt = asOf.AddDays(-120),
                    Payload = "preview-measured-held",
                },
                new NullableAnchorEvent
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    OccurredAt = null,
                    Payload = "preview-measured-null-anchor",
                }
            );
            await db.SaveChangesAsync();
        }

        await CreateHoldAsync("nullable_anchor_events", heldId, tenantId, asOf);
        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());

        var preview = await Host.RunPreviewAsync(tenant, asOf);
        var previewCount = preview.Counts.Single(count =>
            count.EntityType == typeof(NullableAnchorEvent)
        );

        previewCount.Affected.Should().Be(1);
        previewCount.HeldCount.Should().Be(1);
        previewCount.NullAnchorCount.Should().Be(1);
        (await SweepRunExistsAsync(preview.SweepId)).Should().BeFalse();

        RetentionSweepResult? dryRun = null;
        await Host.RunWithServicesAsync(async services =>
        {
            dryRun = await services
                .GetRequiredService<RetentionSweepEngine>()
                .DryRunAsync(
                    tenant,
                    asOf,
                    SweepTriggerKind.Manual,
                    SweepEntityScope.TenantedOnly
                );
        });

        dryRun.Should().NotBeNull();
        var dryRunCount = dryRun!.Counts.Single(count =>
            count.EntityType == typeof(NullableAnchorEvent)
        );
        previewCount.Should().Be(dryRunCount);

        await using var verify = Host.CreateDbContext();
        (
            await verify
                .NullableAnchorEvents.Where(record => record.TenantId == tenantId)
                .OrderBy(record => record.Payload)
                .Select(record => record.Payload)
                .ToListAsync()
        )
            .Should()
            .Equal(
                "preview-measured-eligible",
                "preview-measured-held",
                "preview-measured-null-anchor"
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
                    "preview-hold",
                    asOf.AddDays(-1)
                ),
                CancellationToken.None
            );
        });
    }

    private async Task<bool> SweepRunExistsAsync(Guid sweepId)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT EXISTS (SELECT 1 FROM \"sweep_run\" WHERE \"SweepId\" = @sweepId)";
        command.Parameters.AddWithValue("sweepId", sweepId);
        return (bool)(await command.ExecuteScalarAsync())!;
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

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }
}
