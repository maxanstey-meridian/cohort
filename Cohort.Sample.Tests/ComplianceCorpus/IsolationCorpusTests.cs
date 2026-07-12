using Cohort.Domain;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests.ComplianceCorpus;

public sealed class IsolationCorpusTests(PostgresFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Tenant_and_subject_boundaries_prevent_cross_scope_mutation()
    {
        var tenantId = Guid.NewGuid();
        var otherTenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var otherSubjectId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var targetId = Guid.NewGuid();
        var otherTenantRowId = Guid.NewGuid();
        var otherSubjectRowId = Guid.NewGuid();
        var tenantlessTargetId = Guid.NewGuid();
        var tenantlessOtherSubjectId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = targetId, TenantId = tenantId, SubjectId = subjectId, CreatedAt = now.AddDays(-60), Body = "target" },
                new Note { Id = otherTenantRowId, TenantId = otherTenantId, SubjectId = subjectId, CreatedAt = now.AddDays(-60), Body = "other tenant" },
                new Note { Id = otherSubjectRowId, TenantId = tenantId, SubjectId = otherSubjectId, CreatedAt = now.AddDays(-60), Body = "other subject" }
            );
            db.TenantlessLogs.AddRange(
                new TenantlessLog { Id = tenantlessTargetId, SubjectId = subjectId, CreatedAt = now.AddDays(-60), Payload = "tenantless target" },
                new TenantlessLog { Id = tenantlessOtherSubjectId, SubjectId = otherSubjectId, CreatedAt = now.AddDays(-60), Payload = "tenantless other subject" }
            );
            await db.SaveChangesAsync();
        }

        var result = await Host.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            now
        );

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Sum(count => count.Affected).Should().Be(1);
        result.Counts.Single(count => count.EntityType == typeof(Note)).Affected.Should().Be(1);
        result.Counts.Should().NotContain(count => count.EntityType == typeof(TenantlessLog));
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == targetId)).Should().BeFalse();
        (await verify.Notes.AnyAsync(note => note.Id == otherTenantRowId)).Should().BeTrue();
        (await verify.Notes.AnyAsync(note => note.Id == otherSubjectRowId)).Should().BeTrue();
        var tenantlessRows = await verify.TenantlessLogs
            .Where(row => row.Id == tenantlessTargetId || row.Id == tenantlessOtherSubjectId)
            .OrderBy(row => row.Payload)
            .ToListAsync();
        tenantlessRows.Should().HaveCount(2);
        tenantlessRows.Should().Contain(row =>
            row.Id == tenantlessTargetId
            && row.SubjectId == subjectId
            && row.Payload == "tenantless target"
        );
        tenantlessRows.Should().Contain(row =>
            row.Id == tenantlessOtherSubjectId
            && row.SubjectId == otherSubjectId
            && row.Payload == "tenantless other subject"
        );
    }
}
