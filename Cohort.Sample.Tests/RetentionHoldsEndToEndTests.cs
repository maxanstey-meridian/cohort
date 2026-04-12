using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests;

public sealed class RetentionHoldsEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Repository_Path_Can_Create_List_Check_And_Remove_Holds_Through_The_Default_Ef_Repository()
    {
        var holdId = Guid.NewGuid();
        var recordId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(2026, 4, 10, 12, 0, 0, TimeSpan.Zero);
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await CreateHoldAsync(
            new RetentionHoldRequest(
                holdId,
                "notes",
                recordId,
                tenantId,
                "investigation",
                createdAt
            )
        );

        var activeBeforeRemoval = await HasActiveHoldAsync("notes", recordId, tenantId, asOf);
        var activeBeforeCreation = await HasActiveHoldAsync(
            "notes",
            recordId,
            tenantId,
            createdAt.AddMinutes(-1)
        );
        var listedBeforeRemoval = await ListActiveAsync(asOf);

        await RemoveHoldAsync(holdId, asOf.AddMinutes(-30));

        var activeAfterRemoval = await HasActiveHoldAsync("notes", recordId, tenantId, asOf);
        var listedAfterRemoval = await ListActiveAsync(asOf);

        activeBeforeRemoval.Should().BeTrue();
        activeBeforeCreation.Should().BeFalse();
        listedBeforeRemoval.Should().ContainSingle();
        listedBeforeRemoval[0].Should().Be(
            new RetentionHold(
                holdId,
                "notes",
                recordId,
                tenantId,
                "investigation",
                createdAt,
                null,
                null
            )
        );
        activeAfterRemoval.Should().BeFalse();
        listedAfterRemoval.Should().BeEmpty();

        await using var verify = Host.CreateDbContext();
        var stored = await verify.HeldRecords.SingleAsync();
        stored.HoldId.Should().Be(holdId);
        stored.TableName.Should().Be("notes");
        stored.RecordId.Should().Be(recordId);
        stored.TenantId.Should().Be(tenantId);
        stored.Reason.Should().Be("investigation");
        stored.CreatedAt.Should().Be(createdAt);
        stored.RemovedAt.Should().Be(asOf.AddMinutes(-30));
    }

    [Fact]
    public async Task Purge_Path_Excludes_Rows_With_Active_Holds_And_Allows_Expired_Or_Removed_Holds()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        Guid heldId;
        Guid expiredHoldId;
        Guid removedHoldId;

        await using (var db = Host.CreateDbContext())
        {
            var held = new Note
            {
                Id = Guid.NewGuid(),
                TenantId = tenantA,
                CreatedAt = asOf.AddDays(-120),
                Body = "purge-active-hold",
            };
            var expiredHold = new Note
            {
                Id = Guid.NewGuid(),
                TenantId = tenantA,
                CreatedAt = asOf.AddDays(-120),
                Body = "purge-expired-hold",
            };
            var removedHold = new Note
            {
                Id = Guid.NewGuid(),
                TenantId = tenantA,
                CreatedAt = asOf.AddDays(-120),
                Body = "purge-removed-hold",
            };
            var otherTenant = new Note
            {
                Id = Guid.NewGuid(),
                TenantId = tenantB,
                CreatedAt = asOf.AddDays(-120),
                Body = "purge-other-tenant",
            };

            heldId = held.Id;
            expiredHoldId = expiredHold.Id;
            removedHoldId = removedHold.Id;

            db.Notes.AddRange(held, expiredHold, removedHold, otherTenant);
            await db.SaveChangesAsync();
        }

        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                "notes",
                heldId,
                tenantA,
                "legal hold",
                asOf.AddDays(-10)
            )
        );
        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                "notes",
                expiredHoldId,
                tenantA,
                "expired hold",
                asOf.AddDays(-10),
                asOf.AddDays(-1)
            )
        );
        var removedHoldMarker = Guid.NewGuid();
        await CreateHoldAsync(
            new RetentionHoldRequest(
                removedHoldMarker,
                "notes",
                removedHoldId,
                tenantA,
                "removed hold",
                asOf.AddDays(-10)
            )
        );
        await RemoveHoldAsync(removedHoldMarker, asOf.AddMinutes(-5));

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(Note),
                "short-lived",
                tenantA,
                Strategy.Purge,
                2
            )
        );

        await using var verify = Host.CreateDbContext();
        var remaining = await verify.Notes.OrderBy(note => note.Body).Select(note => note.Body).ToListAsync();
        remaining.Should().Equal("purge-active-hold", "purge-other-tenant");
    }

    [Fact]
    public async Task SoftDelete_Path_Excludes_Rows_With_Active_Holds_And_Allows_Expired_Or_Removed_Holds()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        Guid heldId;
        Guid expiredHoldId;
        Guid removedHoldId;

        await using (var db = Host.CreateDbContext())
        {
            var held = new SoftDeleteRecord
            {
                Id = Guid.NewGuid(),
                TenantId = tenantA,
                CreatedAt = asOf.AddDays(-120),
                Body = "soft-delete-active-hold",
                IsDeleted = false,
            };
            var expiredHold = new SoftDeleteRecord
            {
                Id = Guid.NewGuid(),
                TenantId = tenantA,
                CreatedAt = asOf.AddDays(-120),
                Body = "soft-delete-expired-hold",
                IsDeleted = false,
            };
            var removedHold = new SoftDeleteRecord
            {
                Id = Guid.NewGuid(),
                TenantId = tenantA,
                CreatedAt = asOf.AddDays(-120),
                Body = "soft-delete-removed-hold",
                IsDeleted = false,
            };
            var otherTenant = new SoftDeleteRecord
            {
                Id = Guid.NewGuid(),
                TenantId = tenantB,
                CreatedAt = asOf.AddDays(-120),
                Body = "soft-delete-other-tenant",
                IsDeleted = false,
            };

            heldId = held.Id;
            expiredHoldId = expiredHold.Id;
            removedHoldId = removedHold.Id;

            db.SoftDeleteRecords.AddRange(held, expiredHold, removedHold, otherTenant);
            await db.SaveChangesAsync();
        }

        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                "soft_delete_records",
                heldId,
                tenantA,
                "legal hold",
                asOf.AddDays(-10)
            )
        );
        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                "soft_delete_records",
                expiredHoldId,
                tenantA,
                "expired hold",
                asOf.AddDays(-10),
                asOf.AddDays(-1)
            )
        );
        var removedHoldMarker = Guid.NewGuid();
        await CreateHoldAsync(
            new RetentionHoldRequest(
                removedHoldMarker,
                "soft_delete_records",
                removedHoldId,
                tenantA,
                "removed hold",
                asOf.AddDays(-10)
            )
        );
        await RemoveHoldAsync(removedHoldMarker, asOf.AddMinutes(-5));

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(SoftDeleteRecord),
                "soft-delete",
                tenantA,
                Strategy.SoftDelete,
                2
            )
        );

        await using var verify = Host.CreateDbContext();
        var records = await verify
            .SoftDeleteRecords.OrderBy(record => record.Body)
            .Select(record => new
            {
                record.Body,
                record.IsDeleted,
                record.DeletedAt,
            })
            .ToListAsync();

        records.Should().Equal(
            new
            {
                Body = "soft-delete-active-hold",
                IsDeleted = false,
                DeletedAt = (DateTimeOffset?)null,
            },
            new
            {
                Body = "soft-delete-expired-hold",
                IsDeleted = true,
                DeletedAt = (DateTimeOffset?)asOf,
            },
            new
            {
                Body = "soft-delete-other-tenant",
                IsDeleted = false,
                DeletedAt = (DateTimeOffset?)null,
            },
            new
            {
                Body = "soft-delete-removed-hold",
                IsDeleted = true,
                DeletedAt = (DateTimeOffset?)asOf,
            }
        );
    }

    [Fact]
    public async Task Anonymise_Path_Excludes_Rows_With_Active_Holds_And_Allows_Expired_Or_Removed_Holds()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        Guid heldId;
        Guid expiredHoldId;
        Guid removedHoldId;

        await using (var db = Host.CreateDbContext())
        {
            var held = new AnonymisedContact
            {
                Id = Guid.NewGuid(),
                TenantId = tenantA,
                CreatedAt = asOf.AddDays(-120),
                EmailAddress = "hold@example.com",
                GivenName = "Held",
                Surname = "Contact",
                Notes = "anonymise-active-hold",
            };
            var expiredHold = new AnonymisedContact
            {
                Id = Guid.NewGuid(),
                TenantId = tenantA,
                CreatedAt = asOf.AddDays(-120),
                EmailAddress = "expired@example.com",
                GivenName = "Expired",
                Surname = "Contact",
                Notes = "anonymise-expired-hold",
            };
            var removedHold = new AnonymisedContact
            {
                Id = Guid.NewGuid(),
                TenantId = tenantA,
                CreatedAt = asOf.AddDays(-120),
                EmailAddress = "removed@example.com",
                GivenName = "Removed",
                Surname = "Contact",
                Notes = "anonymise-removed-hold",
            };
            var otherTenant = new AnonymisedContact
            {
                Id = Guid.NewGuid(),
                TenantId = tenantB,
                CreatedAt = asOf.AddDays(-120),
                EmailAddress = "other@example.com",
                GivenName = "Other",
                Surname = "Tenant",
                Notes = "anonymise-other-tenant",
            };

            heldId = held.Id;
            expiredHoldId = expiredHold.Id;
            removedHoldId = removedHold.Id;

            db.AnonymisedContacts.AddRange(held, expiredHold, removedHold, otherTenant);
            await db.SaveChangesAsync();
        }

        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                "anonymised_contacts",
                heldId,
                tenantA,
                "legal hold",
                asOf.AddDays(-10)
            )
        );
        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                "anonymised_contacts",
                expiredHoldId,
                tenantA,
                "expired hold",
                asOf.AddDays(-10),
                asOf.AddDays(-1)
            )
        );
        var removedHoldMarker = Guid.NewGuid();
        await CreateHoldAsync(
            new RetentionHoldRequest(
                removedHoldMarker,
                "anonymised_contacts",
                removedHoldId,
                tenantA,
                "removed hold",
                asOf.AddDays(-10)
            )
        );
        await RemoveHoldAsync(removedHoldMarker, asOf.AddMinutes(-5));

        var result = await Host.RunSweepAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.Counts.Should().Contain(
            new EntitySweepCount(
                typeof(AnonymisedContact),
                "anonymise",
                tenantA,
                Strategy.Anonymise,
                2
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

        contacts.Should().Equal(
            new
            {
                EmailAddress = (string?)"hold@example.com",
                GivenName = "Held",
                Surname = "Contact",
                Notes = "anonymise-active-hold",
            },
            new
            {
                EmailAddress = (string?)null,
                GivenName = string.Empty,
                Surname = "[redacted]",
                Notes = "anonymise-expired-hold",
            },
            new
            {
                EmailAddress = (string?)"other@example.com",
                GivenName = "Other",
                Surname = "Tenant",
                Notes = "anonymise-other-tenant",
            },
            new
            {
                EmailAddress = (string?)null,
                GivenName = string.Empty,
                Surname = "[redacted]",
                Notes = "anonymise-removed-hold",
            }
        );
    }

    private Task CreateHoldAsync(RetentionHoldRequest request)
    {
        return Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            await repository.CreateAsync(request, CancellationToken.None);
        });
    }

    private Task RemoveHoldAsync(Guid holdId, DateTimeOffset removedAt)
    {
        return Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            await repository.RemoveAsync(holdId, removedAt, CancellationToken.None);
        });
    }

    private Task<IReadOnlyList<RetentionHold>> ListActiveAsync(DateTimeOffset asOf)
    {
        return Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            return await repository.ListActiveAsync(asOf, CancellationToken.None);
        });
    }

    private Task<bool> HasActiveHoldAsync(
        string tableName,
        Guid recordId,
        Guid tenantId,
        DateTimeOffset asOf
    )
    {
        return Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            return await repository.HasActiveHoldAsync(
                tableName,
                recordId,
                tenantId,
                asOf,
                CancellationToken.None
            );
        });
    }
}
