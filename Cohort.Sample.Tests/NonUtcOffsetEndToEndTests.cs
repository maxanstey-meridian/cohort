using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Cohort.Sample.Tests;

// Npgsql refuses to write a DateTimeOffset with a non-zero offset to timestamptz, so every
// caller-supplied time must reach SQL as UTC. These run the public ports with +01:00 values.
public sealed class NonUtcOffsetEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    private static readonly TimeSpan PlusOneHour = TimeSpan.FromHours(1);

    [Fact]
    public async Task Holds_Created_Queried_And_Removed_With_Non_Utc_Offsets_Behave_As_Utc()
    {
        var tenantId = Guid.NewGuid();
        var removedRecordId = Guid.NewGuid();
        var keptRecordId = Guid.NewGuid();
        var removedHoldId = Guid.NewGuid();
        var keptHoldId = Guid.NewGuid();
        // Whole seconds: Postgres keeps microseconds, so tick-precise times wouldn't round-trip.
        var now = DateTimeOffset.FromUnixTimeSeconds(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        var createdAt = now.AddDays(-2);
        var expiresAt = now.AddDays(8);
        var removedAt = now.AddMinutes(-30);
        var removedLaterAt = now.AddDays(1);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = removedRecordId, TenantId = tenantId, CreatedAt = DateTimeOffset.UtcNow, Body = "non-utc hold target" },
                new Note { Id = keptRecordId, TenantId = tenantId, CreatedAt = DateTimeOffset.UtcNow, Body = "non-utc hold target" }
            );
            await db.SaveChangesAsync();
        }

        var observed = await Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
            foreach (var (holdId, recordId) in new[] { (removedHoldId, removedRecordId), (keptHoldId, keptRecordId) })
            {
                await repository.CreateAsync(
                    new RetentionHoldRequest(
                        holdId,
                        RetentionEntityIdentity.For<Note>(),
                        recordId.ToString(),
                        tenantId,
                        "non-utc hold",
                        createdAt.ToOffset(PlusOneHour),
                        expiresAt.ToOffset(PlusOneHour)
                    ),
                    CancellationToken.None
                );
            }

            var activeBeforeRemoval = await repository.HasActiveHoldAsync(
                RetentionEntityIdentity.For<Note>(),
                removedRecordId.ToString(),
                tenantId,
                CancellationToken.None
            );
            await repository.RemoveAsync(removedHoldId, removedAt.ToOffset(PlusOneHour), CancellationToken.None);
            await repository.RemoveAsync(keptHoldId, removedLaterAt.ToOffset(PlusOneHour), CancellationToken.None);
            var removedAfterRemoval = await repository.HasActiveHoldAsync(
                RetentionEntityIdentity.For<Note>(),
                removedRecordId.ToString(),
                tenantId,
                CancellationToken.None
            );
            var keptAfterRemoval = await repository.HasActiveHoldAsync(
                RetentionEntityIdentity.For<Note>(),
                keptRecordId.ToString(),
                tenantId,
                CancellationToken.None
            );
            var listed = await repository.ListActiveAsync(CancellationToken.None);
            return (activeBeforeRemoval, removedAfterRemoval, keptAfterRemoval, listed);
        });

        observed.activeBeforeRemoval.Should().BeTrue();
        observed.removedAfterRemoval.Should().BeFalse();
        observed.keptAfterRemoval.Should().BeTrue();
        observed.listed.Should().ContainSingle().Which.Should().Be(
            new RetentionHold(
                keptHoldId,
                RetentionEntityIdentity.For<Note>(),
                keptRecordId.ToString(),
                tenantId,
                "non-utc hold",
                createdAt,
                expiresAt,
                removedLaterAt
            )
        );

        await using var verify = Host.CreateDbContext();
        var stored = await verify.HeldRecords.SingleAsync(hold => hold.HoldId == removedHoldId);
        stored.CreatedAt.Should().Be(createdAt);
        stored.ExpiresAt.Should().Be(expiresAt);
        stored.RemovedAt.Should().Be(removedAt);
    }

    [Fact]
    public async Task Preview_And_Sweep_With_A_Non_Utc_Now_Behave_As_Utc()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var expiredNoteId = Guid.NewGuid();
        var currentNoteId = Guid.NewGuid();
        var softDeleteId = Guid.NewGuid();
        var contactId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = expiredNoteId, TenantId = tenantId, CreatedAt = asOf.AddDays(-45), Body = "expired" },
                new Note { Id = currentNoteId, TenantId = tenantId, CreatedAt = asOf.AddDays(-5), Body = "current" }
            );
            db.SoftDeleteRecords.Add(new SoftDeleteRecord
            {
                Id = softDeleteId,
                TenantId = tenantId,
                CreatedAt = asOf.AddDays(-45),
                Body = "expired soft delete",
            });
            db.AnonymisedContacts.Add(new AnonymisedContact
            {
                Id = contactId,
                TenantId = tenantId,
                CreatedAt = asOf.AddDays(-45),
                EmailAddress = "offset@example.com",
                GivenName = "Offset",
                Surname = "Target",
                Notes = "expired contact",
            });
            await db.SaveChangesAsync();
        }

        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());
        var preview = await Host.RunPreviewAsync(tenant, asOf.ToOffset(PlusOneHour));
        var sweep = await Host.RunSweepAsync(tenant, asOf.ToOffset(PlusOneHour));

        foreach (var result in new[] { preview, sweep })
        {
            result.EntityFailures.Should().BeEmpty();
            result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note)).Which.Affected.Should().Be(1);
            result.Counts.Should().ContainSingle(count => count.EntityType == typeof(SoftDeleteRecord)).Which.Affected.Should().Be(1);
            result.Counts.Should().ContainSingle(count => count.EntityType == typeof(AnonymisedContact)).Which.Affected.Should().Be(1);
        }

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.Select(note => note.Id).ToListAsync()).Should().Equal(currentNoteId);
        var softDeleted = await verify.SoftDeleteRecords.SingleAsync(record => record.Id == softDeleteId);
        softDeleted.IsDeleted.Should().BeTrue();
        softDeleted.DeletedAt.Should().Be(asOf);
        var contact = await verify.AnonymisedContacts.SingleAsync(row => row.Id == contactId);
        contact.AnonymisedAt.Should().Be(asOf);
    }

    [Fact]
    public async Task Erasure_With_A_Non_Utc_Now_Applies_The_Legal_Minimum_As_Utc()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var pastMinimumNoteId = Guid.NewGuid();
        var withinMinimumNoteId = Guid.NewGuid();
        var softDeleteId = Guid.NewGuid();
        using var erasureHost = new CohortTestHost(ConnectionString, new LegalMinimumRuleProvider());

        await using (var db = erasureHost.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = pastMinimumNoteId, TenantId = tenantId, SubjectId = subjectId, CreatedAt = asOf.AddDays(-45), Body = "past minimum" },
                new Note { Id = withinMinimumNoteId, TenantId = tenantId, SubjectId = subjectId, CreatedAt = asOf.AddDays(-5), Body = "within minimum" }
            );
            db.SoftDeleteRecords.Add(new SoftDeleteRecord
            {
                Id = softDeleteId,
                TenantId = tenantId,
                SubjectId = subjectId,
                CreatedAt = asOf.AddDays(-45),
                Body = "past minimum soft delete",
            });
            await db.SaveChangesAsync();
        }

        var result = await erasureHost.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true),
            asOf.ToOffset(PlusOneHour)
        );

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note)).Which.Affected.Should().Be(1);
        result.Counts.Should().ContainSingle(count => count.EntityType == typeof(SoftDeleteRecord)).Which.Affected.Should().Be(1);

        await using var verify = erasureHost.CreateDbContext();
        (await verify.Notes.Select(note => note.Id).ToListAsync()).Should().Equal(withinMinimumNoteId);
        var softDeleted = await verify.SoftDeleteRecords.SingleAsync(record => record.Id == softDeleteId);
        softDeleted.IsDeleted.Should().BeTrue();
        softDeleted.DeletedAt.Should().Be(asOf);
    }

    [Fact]
    public async Task Today_Comes_From_The_Configured_Zone_Not_From_The_Offset_Of_Now()
    {
        var tenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        // 23:30 UTC on 12 Apr is already 13 Apr at +01:00. In the default UTC zone today is
        // 12 Apr, so 30 days back is 13 Mar and a row anchored on 13 Mar is kept.
        var asOf = new DateTimeOffset(2026, 4, 12, 23, 30, 0, TimeSpan.Zero).ToOffset(PlusOneHour);
        var sweptNoteId = Guid.NewGuid();
        var erasedNoteId = Guid.NewGuid();
        using var erasureHost = new CohortTestHost(ConnectionString, new LegalMinimumRuleProvider());

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = sweptNoteId, TenantId = tenantId, CreatedAt = new DateTimeOffset(2026, 3, 13, 12, 0, 0, TimeSpan.Zero), Body = "swept tenant row" },
                new Note { Id = erasedNoteId, TenantId = tenantId, SubjectId = subjectId, CreatedAt = new DateTimeOffset(2026, 3, 13, 12, 0, 0, TimeSpan.Zero), Body = "erasure subject row" }
            );
            await db.SaveChangesAsync();
        }

        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());
        var preview = await Host.RunPreviewAsync(tenant, asOf);
        await erasureHost.RunErasureAsync(tenant, new ErasureScope("user", subjectId, allowSoftDeleteAsErasure: true), asOf);
        await Host.RunSweepAsync(tenant, asOf);

        preview.Counts.Should().ContainSingle(count => count.EntityType == typeof(Note)).Which.Affected.Should().Be(0);
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.Where(note => note.TenantId == tenantId).Select(note => note.Id).ToListAsync())
            .Should()
            .BeEquivalentTo([sweptNoteId, erasedNoteId]);
    }

    private sealed class LegalMinimumRuleProvider : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider sample = new();

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            sample.GetCapabilities(category);

        public async Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            var rule = await sample.ResolveAsync(context, ct);
            return rule is null
                ? null
                : new RetentionRule(rule.Period, rule.Strategy, new RetentionPeriod(Days: 30), rule.AuditRowDetail);
        }
    }
}
