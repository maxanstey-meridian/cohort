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
        var recordId = Guid.NewGuid();
        var holdId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(2026, 4, 10, 12, 0, 0, TimeSpan.Zero);
        var expiresAt = new DateTimeOffset(2026, 4, 20, 12, 0, 0, TimeSpan.Zero);
        var asOf = new DateTimeOffset(2026, 4, 12, 12, 0, 0, TimeSpan.Zero);
        var removedAt = asOf.AddMinutes(-30);

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(new Note
            {
                Id = recordId,
                TenantId = tenantId,
                CreatedAt = DateTimeOffset.UtcNow,
                Body = "non-utc hold target",
            });
            await db.SaveChangesAsync();
        }

        var observed = await Host.RunWithServicesAsync(async services =>
        {
            var repository = services.GetRequiredService<IRetentionHoldsRepository>();
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
            var activeBeforeRemoval = await repository.HasActiveHoldAsync(
                RetentionEntityIdentity.For<Note>(),
                recordId.ToString(),
                tenantId,
                asOf.ToOffset(PlusOneHour),
                CancellationToken.None
            );
            var listed = await repository.ListActiveAsync(asOf.ToOffset(PlusOneHour), CancellationToken.None);
            await repository.RemoveAsync(holdId, removedAt.ToOffset(PlusOneHour), CancellationToken.None);
            var activeAfterRemoval = await repository.HasActiveHoldAsync(
                RetentionEntityIdentity.For<Note>(),
                recordId.ToString(),
                tenantId,
                asOf.ToOffset(PlusOneHour),
                CancellationToken.None
            );
            return (activeBeforeRemoval, listed, activeAfterRemoval);
        });

        observed.activeBeforeRemoval.Should().BeTrue();
        observed.listed.Should().ContainSingle().Which.Should().Be(
            new RetentionHold(
                holdId,
                RetentionEntityIdentity.For<Note>(),
                recordId.ToString(),
                tenantId,
                "non-utc hold",
                createdAt,
                expiresAt,
                null
            )
        );
        observed.activeAfterRemoval.Should().BeFalse();

        await using var verify = Host.CreateDbContext();
        var stored = await verify.HeldRecords.SingleAsync(hold => hold.HoldId == holdId);
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
                : new RetentionRule(rule.Period, rule.Strategy, TimeSpan.FromDays(30), rule.AuditRowDetail);
        }
    }
}
