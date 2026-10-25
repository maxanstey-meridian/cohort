using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests;

// ─── EXEMPLAR #3 — end-to-end test ──────────────────────────────────────────
//
// Pattern: end-to-end test. THIS IS THE PATTERN.
//
// Feed real data in the front. Run the real code path. Assert what comes out
// the back. Use this whenever the code under test touches a port (DbContext,
// IOptions with real config binding, IHostedService, file/HTTP I/O).
//
// Do NOT abstract.
// Do NOT share a base class beyond IntegrationTestBase.
// Do NOT add mocks — NSubstitute is intentionally absent from this project.
// ────────────────────────────────────────────────────────────────────────────

public sealed class RetentionDeletionEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Any_Active_Hold_Protects_All_Targets_Without_Running_The_Callback()
    {
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        const int externalId = 42;
        var callbackRan = false;

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(new Note
            {
                Id = noteId,
                TenantId = tenantId,
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-60),
                Body = "unheld target",
            });
            db.ExternalNumberedLogs.Add(new ExternalNumberedLog
            {
                Id = Guid.NewGuid(),
                ExternalId = externalId,
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-60),
                Payload = "held target",
            });
            await db.SaveChangesAsync();
        }

        await Host.RunWithServicesAsync(async services =>
        {
            await services.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                new RetentionHoldRequest(
                    Guid.NewGuid(),
                    RetentionEntityIdentity.For<ExternalNumberedLog>(),
                    "00042",
                    null,
                    "host deletion protection",
                    DateTimeOffset.UtcNow.AddMinutes(-1)
                ),
                CancellationToken.None
            );
        });

        var outcome = await Host.RunWithServicesAsync(async services =>
        {
            var deletion = services.GetRequiredService<IRetentionDeletion>();
            return await deletion.ExecuteAsync(
                [
                    new RetentionTarget(
                        RetentionEntityIdentity.For<Note>(),
                        noteId.ToString(),
                        tenantId
                    ),
                    new RetentionTarget(
                        RetentionEntityIdentity.For<ExternalNumberedLog>(),
                        "00042",
                        null
                    ),
                ],
                _ =>
                {
                    callbackRan = true;
                    return Task.CompletedTask;
                }
            );
        });

        outcome.Should().Be(RetentionDeletionOutcome.Protected);
        callbackRan.Should().BeFalse();
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
        (await verify.ExternalNumberedLogs.AnyAsync(log => log.ExternalId == externalId))
            .Should()
            .BeTrue();
    }

    [Fact]
    public async Task Callback_Uses_The_Scoped_DbContext_Transaction_And_Commits_All_Targets()
    {
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        const int externalId = 43;

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(new Note
            {
                Id = noteId,
                TenantId = tenantId,
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-60),
                Body = "delete in callback",
            });
            db.ExternalNumberedLogs.Add(new ExternalNumberedLog
            {
                Id = Guid.NewGuid(),
                ExternalId = externalId,
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-60),
                Payload = "delete in callback",
            });
            await db.SaveChangesAsync();
        }

        var outcome = await Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            var deletion = services.GetRequiredService<IRetentionDeletion>();
            return await deletion.ExecuteAsync(
                [
                    new RetentionTarget(
                        RetentionEntityIdentity.For<ExternalNumberedLog>(),
                        externalId.ToString(),
                        null
                    ),
                    new RetentionTarget(
                        RetentionEntityIdentity.For<Note>(),
                        noteId.ToString(),
                        tenantId
                    ),
                ],
                async ct =>
                {
                    db.Database.CurrentTransaction.Should().NotBeNull();
                    db.Notes.Remove(await db.Notes.SingleAsync(note => note.Id == noteId, ct));
                    db.ExternalNumberedLogs.Remove(
                        await db.ExternalNumberedLogs.SingleAsync(
                            log => log.ExternalId == externalId,
                            ct
                        )
                    );
                    await db.SaveChangesAsync(ct);
                }
            );
        });

        outcome.Should().Be(RetentionDeletionOutcome.Executed);
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeFalse();
        (await verify.ExternalNumberedLogs.AnyAsync(log => log.ExternalId == externalId))
            .Should()
            .BeFalse();
    }

    [Fact]
    public async Task Callback_Failure_Rolls_Back_Host_Deletion()
    {
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(new Note
            {
                Id = noteId,
                TenantId = tenantId,
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-60),
                Body = "rollback callback",
            });
            await db.SaveChangesAsync();
        }

        var act = async () => await Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            await services.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
                [
                    new RetentionTarget(
                        RetentionEntityIdentity.For<Note>(),
                        noteId.ToString(),
                        tenantId
                    ),
                ],
                async ct =>
                {
                    db.Notes.Remove(await db.Notes.SingleAsync(note => note.Id == noteId, ct));
                    await db.SaveChangesAsync(ct);
                    throw new InvalidOperationException("host deletion failed");
                }
            );
        });

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("host deletion failed");
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
    }

    [Fact]
    public async Task Callback_Deleting_An_Unlisted_Held_Row_Is_Refused_And_Rolled_Back()
    {
        var tenantId = Guid.NewGuid();
        var listedId = Guid.NewGuid();
        var heldId = Guid.NewGuid();
        await SeedNotesAsync(tenantId, listedId, heldId);
        var holdId = await CreateHoldAsync(heldId, tenantId);

        var act = () => Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            await services.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
                [new RetentionTarget(RetentionEntityIdentity.For<Note>(), listedId.ToString(), tenantId)],
                async ct =>
                {
                    db.Notes.RemoveRange(
                        await db.Notes.Where(note => note.Id == listedId || note.Id == heldId).ToListAsync(ct)
                    );
                    await db.SaveChangesAsync(ct);
                }
            );
        });

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage($"*{holdId}*");
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.CountAsync(note => note.Id == listedId || note.Id == heldId)).Should().Be(2);
    }

    [Fact]
    public async Task Callback_Deleting_A_Held_Row_Through_Raw_Sql_Cascade_Is_Refused()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        using var host = new CohortTestHost(database.ConnectionString);
        var tenantId = Guid.NewGuid();
        var listedId = Guid.NewGuid();
        var heldId = Guid.NewGuid();
        var parentId = Guid.NewGuid();
        await using (var db = host.CreateDbContext())
        {
            await db.Database.MigrateAsync();
            // An FK Cohort's model never sees: the host's database cascades into a retained table.
            await db.Database.ExecuteSqlRawAsync(
                """
                CREATE TABLE public.cascade_parent (id uuid PRIMARY KEY);
                ALTER TABLE public.notes
                    ADD COLUMN "ParentId" uuid NULL REFERENCES public.cascade_parent (id) ON DELETE CASCADE;
                """
            );
            db.Notes.AddRange(
                new Note { Id = listedId, TenantId = tenantId, CreatedAt = DateTimeOffset.UtcNow.AddDays(-60), Body = "listed" },
                new Note { Id = heldId, TenantId = tenantId, CreatedAt = DateTimeOffset.UtcNow.AddDays(-60), Body = "held child" }
            );
            await db.SaveChangesAsync();
            await db.Database.ExecuteSqlAsync(
                $"""
                INSERT INTO public.cascade_parent (id) VALUES ({parentId});
                UPDATE public.notes SET "ParentId" = {parentId} WHERE "Id" = {heldId};
                """
            );
        }

        var holdId = Guid.NewGuid();
        await host.RunWithServicesAsync(services =>
            services.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                new RetentionHoldRequest(holdId, RetentionEntityIdentity.For<Note>(), heldId.ToString(), tenantId, "cascade target", DateTimeOffset.UtcNow),
                CancellationToken.None
            )
        );

        var act = () => host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            await services.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
                [new RetentionTarget(RetentionEntityIdentity.For<Note>(), listedId.ToString(), tenantId)],
                async ct =>
                {
                    db.Notes.Remove(await db.Notes.SingleAsync(note => note.Id == listedId, ct));
                    await db.SaveChangesAsync(ct);
                    await db.Database.ExecuteSqlAsync($"DELETE FROM public.cascade_parent WHERE id = {parentId}", ct);
                }
            );
        });

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage($"*{holdId}*");
        await using var verify = host.CreateDbContext();
        (await verify.Notes.CountAsync(note => note.Id == listedId || note.Id == heldId)).Should().Be(2);
        (await verify.Database.SqlQuery<Guid>($"SELECT id AS \"Value\" FROM public.cascade_parent").ToListAsync())
            .Should()
            .Equal(parentId);
    }

    [Fact]
    public async Task Callback_Deleting_A_Held_Integer_Keyed_Row_Is_Refused()
    {
        // The held row's id is an integer the hold stores canonically ("00042" as "42"), so the
        // after-callback check must match it through the integer branch, not a uuid one.
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        const int externalId = 42;
        await SeedNotesAsync(tenantId, noteId);
        await using (var db = Host.CreateDbContext())
        {
            db.ExternalNumberedLogs.Add(new ExternalNumberedLog
            {
                Id = Guid.NewGuid(),
                ExternalId = externalId,
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-60),
                Payload = "held log",
            });
            await db.SaveChangesAsync();
        }

        var holdId = Guid.NewGuid();
        await Host.RunWithServicesAsync(services =>
            services.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                new RetentionHoldRequest(holdId, RetentionEntityIdentity.For<ExternalNumberedLog>(), "00042", null, "held log", DateTimeOffset.UtcNow),
                CancellationToken.None
            )
        );

        var act = () => Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            await services.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
                [new RetentionTarget(RetentionEntityIdentity.For<Note>(), noteId.ToString(), tenantId)],
                async ct =>
                {
                    await db.Notes.Where(note => note.Id == noteId).ExecuteDeleteAsync(ct);
                    await db.ExternalNumberedLogs.Where(log => log.ExternalId == externalId).ExecuteDeleteAsync(ct);
                }
            );
        });

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage($"*{holdId}*");
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
        (await verify.ExternalNumberedLogs.AnyAsync(log => log.ExternalId == externalId)).Should().BeTrue();
    }

    [Fact]
    public async Task Callback_Returning_With_Unsaved_Changes_Is_Refused_And_Cannot_Be_Saved_Later()
    {
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        await SeedNotesAsync(tenantId, noteId);

        var savedLater = await Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            var act = () => services.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
                [new RetentionTarget(RetentionEntityIdentity.For<Note>(), noteId.ToString(), tenantId)],
                async ct => db.Notes.Remove(await db.Notes.SingleAsync(note => note.Id == noteId, ct))
            );

            await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*unsaved changes*");
            return await db.SaveChangesAsync();
        });

        savedLater.Should().Be(0);
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
    }

    [Fact]
    public async Task Callback_Throwing_With_Unsaved_Changes_Leaves_Nothing_To_Save_Later()
    {
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        await SeedNotesAsync(tenantId, noteId);

        var savedLater = await Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            var act = () => services.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
                [new RetentionTarget(RetentionEntityIdentity.For<Note>(), noteId.ToString(), tenantId)],
                async ct =>
                {
                    db.Notes.Remove(await db.Notes.SingleAsync(note => note.Id == noteId, ct));
                    throw new InvalidOperationException("host deletion failed before saving");
                }
            );

            await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("host deletion failed before saving");
            return await db.SaveChangesAsync();
        });

        savedLater.Should().Be(0);
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
    }

    [Fact]
    public async Task ExecuteAsync_Refuses_A_Context_With_Pending_Changes()
    {
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var pendingId = Guid.NewGuid();
        var callbackRan = false;
        await SeedNotesAsync(tenantId, noteId);

        var act = () => Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            db.Notes.Add(new Note { Id = pendingId, TenantId = tenantId, CreatedAt = DateTimeOffset.UtcNow, Body = "pending" });
            await services.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
                [new RetentionTarget(RetentionEntityIdentity.For<Note>(), noteId.ToString(), tenantId)],
                _ =>
                {
                    callbackRan = true;
                    return Task.CompletedTask;
                }
            );
        });

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*no unsaved changes*");
        callbackRan.Should().BeFalse();
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == pendingId)).Should().BeFalse();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
    }

    [Fact]
    public async Task ExecuteAsync_Refuses_An_Undetected_Edit_When_Automatic_Change_Detection_Is_Off()
    {
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        var editedId = Guid.NewGuid();
        var callbackRan = false;
        await SeedNotesAsync(tenantId, noteId, editedId);

        var act = () => Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            var edited = await db.Notes.SingleAsync(note => note.Id == editedId);
            db.ChangeTracker.AutoDetectChangesEnabled = false;
            edited.Body = "edited, not saved";
            await services.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
                [new RetentionTarget(RetentionEntityIdentity.For<Note>(), noteId.ToString(), tenantId)],
                _ =>
                {
                    callbackRan = true;
                    return Task.CompletedTask;
                }
            );
        });

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*no unsaved changes*");
        callbackRan.Should().BeFalse();
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
    }

    [Fact]
    public async Task A_Preexisting_Orphaned_Hold_Does_Not_Block_An_Unrelated_Deletion()
    {
        var tenantId = Guid.NewGuid();
        var noteId = Guid.NewGuid();
        await SeedNotesAsync(tenantId, noteId);
        // An active hold whose row an earlier, out-of-band delete already removed.
        await using (var db = Host.CreateDbContext())
        {
            await db.Database.ExecuteSqlAsync(
                $"""
                INSERT INTO "retention_holds"
                    ("HoldId", "RetentionEntityId", "RecordId", "TenantId", "Reason", "CreatedAt", "ExpiresAt", "RemovedAt")
                VALUES ({Guid.NewGuid()}, {RetentionEntityIdentity.For<Note>()}, {Guid.NewGuid().ToString()}, {tenantId}, 'orphan', {DateTimeOffset.UtcNow.AddDays(-1)}, NULL, NULL)
                """
            );
        }

        var outcome = await Host.RunWithServicesAsync(async services =>
        {
            var db = services.GetRequiredService<SampleDbContext>();
            return await services.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
                [new RetentionTarget(RetentionEntityIdentity.For<Note>(), noteId.ToString(), tenantId)],
                async ct =>
                {
                    db.Notes.Remove(await db.Notes.SingleAsync(note => note.Id == noteId, ct));
                    await db.SaveChangesAsync(ct);
                }
            );
        });

        outcome.Should().Be(RetentionDeletionOutcome.Executed);
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeFalse();
    }

    private async Task SeedNotesAsync(Guid tenantId, params Guid[] noteIds)
    {
        await using var db = Host.CreateDbContext();
        db.Notes.AddRange(noteIds.Select(id => new Note
        {
            Id = id,
            TenantId = tenantId,
            CreatedAt = DateTimeOffset.UtcNow.AddDays(-60),
            Body = "deletion target",
        }));
        await db.SaveChangesAsync();
    }

    private async Task<Guid> CreateHoldAsync(Guid noteId, Guid tenantId)
    {
        var holdId = Guid.NewGuid();
        await Host.RunWithServicesAsync(services =>
            services.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                new RetentionHoldRequest(holdId, RetentionEntityIdentity.For<Note>(), noteId.ToString(), tenantId, "deletion guard", DateTimeOffset.UtcNow),
                CancellationToken.None
            )
        );
        return holdId;
    }
}
