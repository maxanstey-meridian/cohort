using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class RetentionHoldsEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Hold_Creation_Racing_Purge_Preserves_The_Row()
    {
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var holdId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = noteId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = "hold-racing-purge",
                }
            );
            await db.SaveChangesAsync();
        }

        await RaceHoldAgainstSweepAsync(
            RetentionEntityIdentity.For<Note>(), noteId, tenantId, holdId, asOf
        );

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
        (await verify.HeldRecords.AnyAsync(hold => hold.HoldId == holdId)).Should().BeTrue();
    }

    [Fact]
    public async Task Hold_Creation_Racing_Anonymise_Preserves_The_Row_Unchanged()
    {
        var tenantId = Guid.NewGuid();
        var contactId = Guid.NewGuid();
        var holdId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await using (var db = Host.CreateDbContext())
        {
            db.AnonymisedContacts.Add(
                new AnonymisedContact
                {
                    Id = contactId,
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    EmailAddress = "race@example.com",
                    GivenName = "Race",
                    Surname = "Target",
                    Notes = "hold-racing-anonymise",
                }
            );
            await db.SaveChangesAsync();
        }

        await RaceHoldAgainstSweepAsync(
            RetentionEntityIdentity.For<AnonymisedContact>(), contactId, tenantId, holdId, asOf
        );

        await using var verify = Host.CreateDbContext();
        var contact = await verify.AnonymisedContacts.SingleAsync(row => row.Id == contactId);
        contact.EmailAddress.Should().Be("race@example.com");
        contact.GivenName.Should().Be("Race");
        contact.Surname.Should().Be("Target");
        contact.AnonymisedAt.Should().BeNull();
    }

    [Fact]
    public async Task Repository_Path_Can_Create_List_Check_And_Remove_Holds_Through_The_Default_Ef_Repository()
    {
        var activeHoldId = Guid.NewGuid();
        var expiredHoldId = Guid.NewGuid();
        var removedHoldId = Guid.NewGuid();
        var activeRecordId = Guid.NewGuid();
        var expiredRecordId = Guid.NewGuid();
        var removedRecordId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(2026, 4, 10, 12, 0, 0, TimeSpan.Zero);
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await CreateHoldAsync(
            new RetentionHoldRequest(
                activeHoldId,
                RetentionEntityIdentity.For<Note>(),
                activeRecordId.ToString(),
                tenantId,
                "investigation",
                createdAt
            )
        );
        await CreateHoldAsync(
            new RetentionHoldRequest(
                expiredHoldId,
                RetentionEntityIdentity.For<Note>(),
                expiredRecordId.ToString(),
                tenantId,
                "expired investigation",
                createdAt,
                asOf.AddHours(-1)
            )
        );
        await CreateHoldAsync(
            new RetentionHoldRequest(
                removedHoldId,
                RetentionEntityIdentity.For<Note>(),
                removedRecordId.ToString(),
                tenantId,
                "removed investigation",
                createdAt
            )
        );

        var activeBeforeRemoval = await HasActiveHoldAsync(
            RetentionEntityIdentity.For<Note>(), activeRecordId, tenantId, asOf
        );
        var activeBeforeCreation = await HasActiveHoldAsync(
            RetentionEntityIdentity.For<Note>(),
            activeRecordId,
            tenantId,
            createdAt.AddMinutes(-1)
        );
        var expiredAtQueryTime = await HasActiveHoldAsync(
            RetentionEntityIdentity.For<Note>(), expiredRecordId, tenantId, asOf
        );
        var removedAtQueryTime = await HasActiveHoldAsync(
            RetentionEntityIdentity.For<Note>(), removedRecordId, tenantId, asOf
        );
        var listedBeforeRemoval = await ListActiveAsync(asOf);

        await RemoveHoldAsync(removedHoldId, asOf.AddMinutes(-30));

        var activeAfterRemoval = await HasActiveHoldAsync(
            RetentionEntityIdentity.For<Note>(), removedRecordId, tenantId, asOf
        );
        var listedAfterRemoval = await ListActiveAsync(asOf);

        activeBeforeRemoval.Should().BeTrue();
        activeBeforeCreation.Should().BeFalse();
        expiredAtQueryTime.Should().BeFalse();
        removedAtQueryTime.Should().BeTrue();
        listedBeforeRemoval.Should().HaveCount(2);
        listedBeforeRemoval
            .Should()
            .Contain(
                new RetentionHold(
                    activeHoldId,
                    RetentionEntityIdentity.For<Note>(),
                    activeRecordId.ToString(),
                    tenantId,
                    "investigation",
                    createdAt,
                    null,
                    null
                )
            );
        listedBeforeRemoval
            .Should()
            .Contain(
                new RetentionHold(
                    removedHoldId,
                    RetentionEntityIdentity.For<Note>(),
                    removedRecordId.ToString(),
                    tenantId,
                    "removed investigation",
                    createdAt,
                    null,
                    null
                )
            );
        activeAfterRemoval.Should().BeFalse();
        listedAfterRemoval.Should().ContainSingle();
        listedAfterRemoval[0]
            .Should()
            .Be(
                new RetentionHold(
                    activeHoldId,
                    RetentionEntityIdentity.For<Note>(),
                    activeRecordId.ToString(),
                    tenantId,
                    "investigation",
                    createdAt,
                    null,
                    null
                )
            );

        await using var verify = Host.CreateDbContext();
        var stored = await verify.HeldRecords.OrderBy(record => record.Reason).ToListAsync();
        stored.Should().HaveCount(3);
        stored
            .Select(record => record.Reason)
            .Should()
            .Equal("expired investigation", "investigation", "removed investigation");
        stored.Single(record => record.HoldId == activeHoldId).RemovedAt.Should().BeNull();
        stored
            .Single(record => record.HoldId == expiredHoldId)
            .ExpiresAt.Should()
            .Be(asOf.AddHours(-1));
        stored
            .Single(record => record.HoldId == removedHoldId)
            .RemovedAt.Should()
            .Be(asOf.AddMinutes(-30));
    }

    [Fact]
    public async Task Hold_Is_Active_When_CreatedAt_Equals_AsOf()
    {
        var recordId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                RetentionEntityIdentity.For<Note>(),
                recordId.ToString(),
                tenantId,
                "created boundary",
                asOf
            )
        );

        (await HasActiveHoldAsync(RetentionEntityIdentity.For<Note>(), recordId, tenantId, asOf))
            .Should()
            .BeTrue();
    }

    [Fact]
    public async Task Hold_Is_Inactive_When_ExpiresAt_Equals_AsOf()
    {
        var recordId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                RetentionEntityIdentity.For<Note>(),
                recordId.ToString(),
                tenantId,
                "expiry boundary",
                asOf.AddDays(-1),
                asOf
            )
        );

        (await HasActiveHoldAsync(RetentionEntityIdentity.For<Note>(), recordId, tenantId, asOf))
            .Should()
            .BeFalse();
    }

    [Fact]
    public async Task Hold_Is_Inactive_When_RemovedAt_Equals_AsOf()
    {
        var holdId = Guid.NewGuid();
        var recordId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await CreateHoldAsync(
            new RetentionHoldRequest(
                holdId,
                RetentionEntityIdentity.For<Note>(),
                recordId.ToString(),
                tenantId,
                "removal boundary",
                asOf.AddDays(-1)
            )
        );
        await RemoveHoldAsync(holdId, asOf);

        (await HasActiveHoldAsync(RetentionEntityIdentity.For<Note>(), recordId, tenantId, asOf))
            .Should()
            .BeFalse();
    }

    [Fact]
    public async Task CreateAsync_Composes_With_Caller_Owned_Ef_Transactions()
    {
        var rolledBackHoldId = Guid.NewGuid();
        var committedHoldId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(2026, 4, 10, 12, 0, 0, TimeSpan.Zero);

        await Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();

            await using (var transaction = await db.Database.BeginTransactionAsync())
            {
                await repository.CreateAsync(
                    new RetentionHoldRequest(
                        rolledBackHoldId,
                        RetentionEntityIdentity.For<Note>(),
                        Guid.NewGuid().ToString(),
                        tenantId,
                        "rolled back hold",
                        createdAt
                    ),
                    CancellationToken.None
                );
                await transaction.RollbackAsync();
            }

            await using (var transaction = await db.Database.BeginTransactionAsync())
            {
                await repository.CreateAsync(
                    new RetentionHoldRequest(
                        committedHoldId,
                        RetentionEntityIdentity.For<Note>(),
                        Guid.NewGuid().ToString(),
                        tenantId,
                        "committed hold",
                        createdAt
                    ),
                    CancellationToken.None
                );
                await transaction.CommitAsync();
            }
        });

        await using var verify = Host.CreateDbContext();
        (await verify.HeldRecords.AnyAsync(hold => hold.HoldId == rolledBackHoldId))
            .Should()
            .BeFalse();
        (await verify.HeldRecords.AnyAsync(hold => hold.HoldId == committedHoldId))
            .Should()
            .BeTrue();
    }

    [Fact]
    public async Task HasActiveHoldAsync_Sees_A_Hold_Created_In_The_Current_Ef_Transaction()
    {
        var recordId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            await using var transaction = await db.Database.BeginTransactionAsync();

            await repository.CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    RetentionEntityIdentity.For<Note>(),
                    recordId.ToString(),
                    tenantId,
                    "transactional visibility",
                    asOf.AddDays(-1)
                ),
                CancellationToken.None
            );

            var active = await repository.HasActiveHoldAsync(
                RetentionEntityIdentity.For<Note>(),
                recordId.ToString(),
                tenantId,
                asOf,
                CancellationToken.None
            );

            active.Should().BeTrue();
            await transaction.RollbackAsync();
        });
    }

    [Fact]
    public async Task ListActiveAsync_Composes_With_Ambient_Ef_Transaction_Commit_And_Rollback()
    {
        var rolledBackHoldId = Guid.NewGuid();
        var committedHoldId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();

            await using (var transaction = await db.Database.BeginTransactionAsync())
            {
                await repository.CreateAsync(
                    new RetentionHoldRequest(
                        rolledBackHoldId,
                        RetentionEntityIdentity.For<Note>(),
                        Guid.NewGuid().ToString(),
                        tenantId,
                        "rolled back list visibility",
                        asOf.AddDays(-1)
                    ),
                    CancellationToken.None
                );

                (await repository.ListActiveAsync(asOf, CancellationToken.None))
                    .Should()
                    .ContainSingle(hold => hold.HoldId == rolledBackHoldId);
                await transaction.RollbackAsync();
            }

            await using (var transaction = await db.Database.BeginTransactionAsync())
            {
                await repository.CreateAsync(
                    new RetentionHoldRequest(
                        committedHoldId,
                        RetentionEntityIdentity.For<Note>(),
                        Guid.NewGuid().ToString(),
                        tenantId,
                        "committed list visibility",
                        asOf.AddDays(-1)
                    ),
                    CancellationToken.None
                );

                (await repository.ListActiveAsync(asOf, CancellationToken.None))
                    .Should()
                    .ContainSingle(hold => hold.HoldId == committedHoldId);
                await transaction.CommitAsync();
            }
        });

        var persisted = await ListActiveAsync(asOf);
        persisted.Should().NotContain(hold => hold.HoldId == rolledBackHoldId);
        persisted.Should().ContainSingle(hold => hold.HoldId == committedHoldId);
    }

    [Fact]
    public async Task HasActiveHoldAsync_Sees_A_Hold_Removed_In_The_Current_Ef_Transaction()
    {
        var holdId = Guid.NewGuid();
        var recordId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        await CreateHoldAsync(
            new RetentionHoldRequest(
                holdId,
                RetentionEntityIdentity.For<Note>(),
                recordId.ToString(),
                tenantId,
                "transactional removal visibility",
                asOf.AddDays(-1)
            )
        );

        await Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            await using var transaction = await db.Database.BeginTransactionAsync();

            await repository.RemoveAsync(holdId, asOf, CancellationToken.None);
            var active = await repository.HasActiveHoldAsync(
                RetentionEntityIdentity.For<Note>(),
                recordId.ToString(),
                tenantId,
                asOf,
                CancellationToken.None
            );

            active.Should().BeFalse();
            await transaction.RollbackAsync();
        });
    }

    [Fact]
    public async Task RemoveAsync_Composes_With_Caller_Owned_Ef_Transactions()
    {
        var holdId = Guid.NewGuid();
        var removedAt = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        await CreateHoldAsync(
            new RetentionHoldRequest(
                holdId,
                RetentionEntityIdentity.For<Note>(),
                Guid.NewGuid().ToString(),
                Guid.NewGuid(),
                "transactional removal",
                removedAt.AddDays(-1)
            )
        );

        await Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();

            await using (var transaction = await db.Database.BeginTransactionAsync())
            {
                await repository.RemoveAsync(holdId, removedAt, CancellationToken.None);
                await transaction.RollbackAsync();
            }

            await using (var transaction = await db.Database.BeginTransactionAsync())
            {
                await repository.RemoveAsync(holdId, removedAt, CancellationToken.None);
                await transaction.CommitAsync();
            }
        });

        await using var verify = Host.CreateDbContext();
        (await verify.HeldRecords.SingleAsync(hold => hold.HoldId == holdId))
            .RemovedAt.Should()
            .Be(removedAt);
    }

    [Fact]
    public async Task RemoveAsync_Fails_Deterministically_For_Missing_And_Already_Removed_Holds()
    {
        var missingHoldId = Guid.NewGuid();
        var removedHoldId = Guid.NewGuid();
        var removedAt = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        await CreateHoldAsync(
            new RetentionHoldRequest(
                removedHoldId,
                RetentionEntityIdentity.For<Note>(),
                Guid.NewGuid().ToString(),
                Guid.NewGuid(),
                "already removed hold",
                removedAt.AddDays(-1)
            )
        );
        await RemoveHoldAsync(removedHoldId, removedAt);

        await Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();

            var removeMissing = async () =>
                await repository.RemoveAsync(missingHoldId, removedAt, CancellationToken.None);
            var removeAgain = async () =>
                await repository.RemoveAsync(removedHoldId, removedAt, CancellationToken.None);

            await removeMissing
                .Should()
                .ThrowAsync<InvalidOperationException>()
                .WithMessage(
                    $"Retention hold '{missingHoldId}' could not be removed because it does not exist or is already removed."
                );
            await removeAgain
                .Should()
                .ThrowAsync<InvalidOperationException>()
                .WithMessage(
                    $"Retention hold '{removedHoldId}' could not be removed because it does not exist or is already removed."
                );
        });
    }

    [Fact]
    public async Task Hold_Created_After_A_Backdated_Sweeps_Logical_Now_Still_Protects_The_Row()
    {
        // Hold activity is evaluated against the database wall clock, not the sweep's
        // logical 'now' — a litigation hold protects from the moment it exists, even
        // when an operator runs a backdated sweep.
        var tenantId = Guid.NewGuid();
        var backdatedAsOf = DateTimeOffset.UtcNow.AddDays(-7);
        Guid heldId;

        await using (var db = Host.CreateDbContext())
        {
            var held = new Note
            {
                Id = Guid.NewGuid(),
                TenantId = tenantId,
                CreatedAt = backdatedAsOf.AddDays(-120),
                Body = "backdated-sweep-hold",
            };
            heldId = held.Id;
            db.Notes.Add(held);
            await db.SaveChangesAsync();
        }

        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                RetentionEntityIdentity.For<Note>(),
                heldId.ToString(),
                tenantId,
                "hold created after the backdated as-of",
                DateTimeOffset.UtcNow
            )
        );

        await Host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            backdatedAsOf
        );

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == heldId)).Should().BeTrue();
    }

    [Fact]
    public async Task CreateAsync_Rejects_Retention_Entity_Ids_That_Do_Not_Match_The_Registry()
    {
        var act = async () =>
            await CreateHoldAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    Guid.NewGuid(),
                    Guid.NewGuid().ToString(),
                    Guid.NewGuid(),
                    "typo'd table name",
                    new DateTimeOffset(2026, 4, 10, 12, 0, 0, TimeSpan.Zero)
                )
            );

        await act.Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("*does not match a retained entity in the EF model*");
    }

    [Fact]
    public async Task CreateAsync_Rejects_NonGuid_Record_Ids_For_Guid_Keyed_Tables()
    {
        var act = async () =>
            await CreateHoldAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    RetentionEntityIdentity.For<Note>(),
                    "not-a-guid",
                    Guid.NewGuid(),
                    "malformed record id",
                    new DateTimeOffset(2026, 4, 10, 12, 0, 0, TimeSpan.Zero)
                )
            );

        await act.Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("*not a valid Guid*");
    }

    [Fact]
    public async Task CreateAsync_Rejects_Holds_Whose_Tenant_Does_Not_Match_The_Existing_Rows_Tenant()
    {
        // The sweep-side exclusion on tenanted tables only honours holds whose TenantId
        // matches the row's tenant — a hold created under the wrong tenant would look
        // persisted while protecting nothing.
        var rowTenantId = Guid.NewGuid();
        var wrongTenantId = Guid.NewGuid();
        Guid noteId;

        await using (var db = Host.CreateDbContext())
        {
            var note = new Note
            {
                Id = Guid.NewGuid(),
                TenantId = rowTenantId,
                CreatedAt = new DateTimeOffset(2026, 4, 1, 12, 0, 0, TimeSpan.Zero),
                Body = "wrong-tenant-hold-target",
            };
            noteId = note.Id;
            db.Notes.Add(note);
            await db.SaveChangesAsync();
        }

        var act = async () =>
            await CreateHoldAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    RetentionEntityIdentity.For<Note>(),
                    noteId.ToString(),
                    wrongTenantId,
                    "mis-scoped hold",
                    new DateTimeOffset(2026, 4, 10, 12, 0, 0, TimeSpan.Zero)
                )
            );

        await act.Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage($"*belongs to tenant '{rowTenantId}'*");

        // The same hold under the row's actual tenant is accepted.
        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                RetentionEntityIdentity.For<Note>(),
                noteId.ToString(),
                rowTenantId,
                "correctly scoped hold",
                new DateTimeOffset(2026, 4, 10, 12, 0, 0, TimeSpan.Zero)
            )
        );

        (
            await HasActiveHoldAsync(
                RetentionEntityIdentity.For<Note>(),
                noteId,
                rowTenantId,
                new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero)
            )
        )
            .Should()
            .BeTrue();
    }

    [Fact]
    public async Task CreateAsync_Normalises_Guid_Record_Id_Formats_So_The_Hold_Still_Protects_The_Row()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        Guid heldId;

        await using (var db = Host.CreateDbContext())
        {
            var held = new Note
            {
                Id = Guid.NewGuid(),
                TenantId = tenantId,
                CreatedAt = asOf.AddDays(-120),
                Body = "purge-uppercase-hold",
            };
            heldId = held.Id;
            db.Notes.Add(held);
            await db.SaveChangesAsync();
        }

        // "N"-format uppercase would never match CAST(uuid AS text) without normalisation.
        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                RetentionEntityIdentity.For<Note>(),
                heldId.ToString("N").ToUpperInvariant(),
                tenantId,
                "legal hold with non-canonical record id",
                asOf.AddDays(-10)
            )
        );

        await Host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == heldId)).Should().BeTrue();
    }

    [Fact]
    public async Task HasActiveHoldAsync_Normalises_Guid_Record_Id_Formats_Consistently_With_CreateAsync()
    {
        var recordId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                RetentionEntityIdentity.For<Note>(),
                recordId.ToString("B").ToUpperInvariant(),
                tenantId,
                "guid format consistency",
                asOf.AddDays(-1)
            )
        );

        var active = await Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            return await repository.HasActiveHoldAsync(
                RetentionEntityIdentity.For<Note>(),
                recordId.ToString("N").ToUpperInvariant(),
                tenantId,
                asOf,
                CancellationToken.None
            );
        });

        active.Should().BeTrue();
    }

    [Fact]
    public async Task Repository_Uses_Selected_Integer_Record_Id_And_Tenantless_Hold_Matching()
    {
        var holdId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);

        await CreateHoldAsync(
            new RetentionHoldRequest(
                holdId,
                RetentionEntityIdentity.For<ExternalNumberedLog>(),
                "00042",
                null,
                "selected integer id",
                asOf.AddDays(-1)
            )
        );

        var result = await Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            var active = await repository.HasActiveHoldAsync(
                RetentionEntityIdentity.For<ExternalNumberedLog>(),
                "000042",
                null,
                asOf,
                CancellationToken.None
            );
            var holds = await repository.ListActiveAsync(asOf, CancellationToken.None);
            return (active, holds.Single(hold => hold.HoldId == holdId));
        });

        result.active.Should().BeTrue();
        result.Item2.RecordId.Should().Be("42");
    }

    [Fact]
    public async Task CreateAsync_Translates_Provider_Record_Id_Conversion_Errors()
    {
        var act = async () =>
            await CreateHoldAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    RetentionEntityIdentity.For<ExternalNumberedLog>(),
                    "not-an-integer",
                    null,
                    "invalid provider id",
                    DateTimeOffset.UtcNow
                )
            );

        await act.Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("*not valid for provider type*integer*");
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
                RetentionEntityIdentity.For<Note>(),
                heldId.ToString(),
                tenantA,
                "legal hold",
                asOf.AddDays(-10)
            )
        );
        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                RetentionEntityIdentity.For<Note>(),
                expiredHoldId.ToString(),
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
                RetentionEntityIdentity.For<Note>(),
                removedHoldId.ToString(),
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

        result.EntityFailures.Should().BeEmpty();
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(Note),
                    "short-lived",
                    tenantA,
                    Strategy.Purge,
                    2,
                    HeldCount: 1
                )
            );

        await using var verify = Host.CreateDbContext();
        var remaining = await verify
            .Notes.OrderBy(note => note.Body)
            .Select(note => note.Body)
            .ToListAsync();
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
                RetentionEntityIdentity.For<SoftDeleteRecord>(),
                heldId.ToString(),
                tenantA,
                "legal hold",
                asOf.AddDays(-10)
            )
        );
        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                RetentionEntityIdentity.For<SoftDeleteRecord>(),
                expiredHoldId.ToString(),
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
                RetentionEntityIdentity.For<SoftDeleteRecord>(),
                removedHoldId.ToString(),
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

        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(SoftDeleteRecord),
                    "soft-delete",
                    tenantA,
                    Strategy.SoftDelete,
                    2,
                    HeldCount: 1
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

        records
            .Should()
            .Equal(
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
                RetentionEntityIdentity.For<AnonymisedContact>(),
                heldId.ToString(),
                tenantA,
                "legal hold",
                asOf.AddDays(-10)
            )
        );
        await CreateHoldAsync(
            new RetentionHoldRequest(
                Guid.NewGuid(),
                RetentionEntityIdentity.For<AnonymisedContact>(),
                expiredHoldId.ToString(),
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
                RetentionEntityIdentity.For<AnonymisedContact>(),
                removedHoldId.ToString(),
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

        result.EntityFailures.Should().BeEmpty();
        result
            .Counts.Should()
            .Contain(
                new EntitySweepCount(
                    typeof(AnonymisedContact),
                    "anonymise",
                    tenantA,
                    Strategy.Anonymise,
                    2,
                    HeldCount: 1
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
        Guid retentionEntityId,
        Guid recordId,
        Guid tenantId,
        DateTimeOffset asOf
    )
    {
        return Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            return await repository.HasActiveHoldAsync(
                retentionEntityId,
                recordId.ToString(),
                tenantId,
                asOf,
                CancellationToken.None
            );
        });
    }

    private async Task WaitForAdvisoryLockWaiterAsync(long key, int blockerBackendId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (true)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks waiter
                    WHERE waiter.locktype = 'advisory'
                      AND waiter.classid = ((@key >> 32) & 4294967295)::oid
                      AND waiter.objid = (@key & 4294967295)::oid
                      AND waiter.objsubid = 1
                      AND NOT waiter.granted
                      AND @blockerBackendId = ANY(pg_blocking_pids(waiter.pid))
                )
                """;
            command.Parameters.AddWithValue("key", key);
            command.Parameters.AddWithValue("blockerBackendId", blockerBackendId);
            if ((bool)(await command.ExecuteScalarAsync(timeout.Token))!)
            {
                return;
            }

            await Task.Yield();
        }
    }

    private async Task WaitForSweepLockWaiterAsync()
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (true)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT COUNT(*) >= 2
                FROM pg_locks
                WHERE locktype = 'advisory'
                  AND NOT granted
                """;
            if ((bool)(await command.ExecuteScalarAsync(timeout.Token))!)
            {
                return;
            }

            await Task.Yield();
        }
    }

    private async Task RaceHoldAgainstSweepAsync(
        Guid retentionEntityId,
        Guid recordId,
        Guid tenantId,
        Guid holdId,
        DateTimeOffset asOf
    )
    {
        const long testBarrierKey = 7_310_042_119;
        NpgsqlConnection? blockerConnection = null;
        NpgsqlTransaction? blockerTransaction = null;
        var barrierReleased = false;

        try
        {
            await using (var setup = new NpgsqlConnection(ConnectionString))
            {
                await setup.OpenAsync();
                await ExecuteNonQueryAsync(
                    setup,
                    transaction: null,
                    $"""
                    CREATE OR REPLACE FUNCTION cohort_test_pause_hold_insert() RETURNS trigger AS $$
                    BEGIN
                        IF NEW."HoldId" = '{holdId}'::uuid THEN
                            PERFORM pg_advisory_xact_lock({testBarrierKey});
                            NEW."CreatedAt" := clock_timestamp();
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                    DROP TRIGGER IF EXISTS cohort_test_pause_hold_insert ON "retention_holds";
                    CREATE TRIGGER cohort_test_pause_hold_insert
                    BEFORE INSERT ON "retention_holds"
                    FOR EACH ROW EXECUTE FUNCTION cohort_test_pause_hold_insert();
                    """
                );
            }

            blockerConnection = new NpgsqlConnection(ConnectionString);
            await blockerConnection.OpenAsync();
            blockerTransaction = await blockerConnection.BeginTransactionAsync();
            await ExecuteNonQueryAsync(
                blockerConnection,
                blockerTransaction,
                $"SELECT pg_advisory_xact_lock({testBarrierKey})"
            );

            var createHold = CreateHoldAsync(
                new RetentionHoldRequest(
                    holdId,
                    retentionEntityId,
                    recordId.ToString(),
                    tenantId,
                    "concurrent legal hold",
                    DateTimeOffset.UtcNow
                )
            );
            await WaitForAdvisoryLockWaiterAsync(
                testBarrierKey,
                blockerConnection.ProcessID
            );
            var sweep = Host.RunSweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                asOf
            );
            await WaitForSweepLockWaiterAsync();

            await blockerTransaction.CommitAsync();
            barrierReleased = true;
            await Task.WhenAll(createHold, sweep);
        }
        finally
        {
            try
            {
                if (!barrierReleased && blockerTransaction is not null)
                {
                    await blockerTransaction.RollbackAsync();
                }
            }
            finally
            {
                if (blockerTransaction is not null)
                {
                    await blockerTransaction.DisposeAsync();
                }

                if (blockerConnection is not null)
                {
                    await blockerConnection.DisposeAsync();
                }

                await using var cleanup = new NpgsqlConnection(ConnectionString);
                await cleanup.OpenAsync();
                await ExecuteNonQueryAsync(
                    cleanup,
                    transaction: null,
                    """
                    DROP TRIGGER IF EXISTS cohort_test_pause_hold_insert ON "retention_holds";
                    DROP FUNCTION IF EXISTS cohort_test_pause_hold_insert();
                    """
                );
            }
        }
    }

    private static async Task ExecuteNonQueryAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string sql
    )
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }
}
