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
}
