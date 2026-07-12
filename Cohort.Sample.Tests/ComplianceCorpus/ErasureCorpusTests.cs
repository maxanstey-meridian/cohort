using System.Collections.Concurrent;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests.ComplianceCorpus;

public sealed class ErasureCorpusTests(PostgresFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Erasure_is_subject_and_tenant_scoped_and_requires_soft_delete_opt_in()
    {
        var tenantId = Guid.NewGuid();
        var otherTenantId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var otherSubjectId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var targetId = Guid.NewGuid();
        var otherSubjectIdRow = Guid.NewGuid();
        var otherTenantIdRow = Guid.NewGuid();
        var softDeleteId = Guid.NewGuid();
        var anonymiseId = Guid.NewGuid();
        var observer = new RecordingObserver();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                new Note { Id = targetId, TenantId = tenantId, SubjectId = subjectId, CreatedAt = now.AddDays(-1), Body = "target" },
                new Note { Id = otherSubjectIdRow, TenantId = tenantId, SubjectId = otherSubjectId, CreatedAt = now.AddDays(-60), Body = "other subject" },
                new Note { Id = otherTenantIdRow, TenantId = otherTenantId, SubjectId = subjectId, CreatedAt = now.AddDays(-60), Body = "other tenant" }
            );
            db.SoftDeleteRecords.Add(new SoftDeleteRecord
            {
                Id = softDeleteId,
                TenantId = tenantId,
                SubjectId = subjectId,
                CreatedAt = now.AddDays(-1),
                Body = "requires opt in",
            });
            db.AnonymisedContacts.Add(new AnonymisedContact
            {
                Id = anonymiseId,
                TenantId = tenantId,
                SubjectId = subjectId,
                CreatedAt = now.AddDays(-1),
                EmailAddress = "erase@example.org",
                GivenName = "Erase",
                Surname = "Subject",
                Notes = "preserve",
            });
            await db.SaveChangesAsync();
        }

        using var host = new CohortTestHost(
            ConnectionString,
            configureServices: services => services.AddSingleton<IRetentionAuditObserver>(observer)
        );
        var sourceBefore = await ReadSourceStateAsync(
            targetId, otherSubjectIdRow, otherTenantIdRow, softDeleteId, anonymiseId
        );
        var auditBefore = await ReadAuditStateAsync();
        var refused = () => host.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId),
            now
        );
        await refused.Should().ThrowAsync<InvalidOperationException>();

        (await ReadSourceStateAsync(
            targetId, otherSubjectIdRow, otherTenantIdRow, softDeleteId, anonymiseId
        )).Should().Be(sourceBefore);
        (await ReadAuditStateAsync()).Should().Be(auditBefore);
        observer.Events.Should().BeEmpty();

        var result = await host.RunErasureAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            new ErasureScope(subjectId, allowSoftDeleteAsErasure: true),
            now
        );

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Where(count => count.Affected > 0)
            .Select(count => (count.EntityType, count.Affected)).Should().BeEquivalentTo([
                (typeof(Note), 1L),
                (typeof(SoftDeleteRecord), 1L),
                (typeof(AnonymisedContact), 1L),
            ]);
        result.Counts.Sum(count => count.Affected).Should().Be(3);
        observer.Events.OfType<SweepEvent.Started>().Should().ContainSingle().Which.SweepId.Should().Be(result.SweepId);
        observer.Events.OfType<SweepEvent.Completed>().Should().ContainSingle().Which.TotalAffected.Should().Be(3);
        observer.Events.OfType<SweepEvent.EntitySummary>()
            .Where(summary => summary.Affected > 0)
            .Select(summary => (summary.RetentionEntityId, summary.EntityType, summary.Category,
                summary.TenantId, summary.Strategy, summary.Affected))
            .Should().BeEquivalentTo([
                (Note.RetentionIdentity, typeof(Note), "short-lived", tenantId, Strategy.Purge, 1L),
                (Guid.Parse("6107ff39-bf33-413c-889e-6347c909ba15"), typeof(SoftDeleteRecord), "soft-delete", tenantId, Strategy.SoftDelete, 1L),
                (Guid.Parse("fd4a533e-e6a9-44ea-948e-cbf881f35e57"), typeof(AnonymisedContact), "anonymise", tenantId, Strategy.Anonymise, 1L),
            ]);
        observer.Events.OfType<SweepEvent.RowDetail>().Should().BeEmpty();
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == targetId)).Should().BeFalse();
        (await verify.Notes.AnyAsync(note => note.Id == otherSubjectIdRow)).Should().BeTrue();
        (await verify.Notes.AnyAsync(note => note.Id == otherTenantIdRow)).Should().BeTrue();
        (await verify.SoftDeleteRecords.SingleAsync(row => row.Id == softDeleteId)).IsDeleted.Should().BeTrue();
        var anonymised = await verify.AnonymisedContacts.SingleAsync(row => row.Id == anonymiseId);
        anonymised.EmailAddress.Should().BeNull();
        anonymised.GivenName.Should().BeEmpty();
        anonymised.Surname.Should().Be("[redacted]");
        anonymised.Notes.Should().Be("preserve");
        anonymised.AnonymisedAt.Should().Be(now);

        await AssertAcceptedAuditAsync(result.SweepId, tenantId);
    }

    private async Task<string> ReadSourceStateAsync(params Guid[] ids)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT jsonb_build_object(
                'notes', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row."Id"), '[]'::jsonb) FROM "notes" row WHERE row."Id" = ANY(@ids)),
                'soft', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row."Id"), '[]'::jsonb) FROM "soft_delete_records" row WHERE row."Id" = ANY(@ids)),
                'contacts', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row."Id"), '[]'::jsonb) FROM "anonymised_contacts" row WHERE row."Id" = ANY(@ids))
            )::text
            """;
        command.Parameters.AddWithValue("ids", ids);
        return (string)(await command.ExecuteScalarAsync())!;
    }

    private async Task<string> ReadAuditStateAsync()
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT jsonb_build_object(
                'runs', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row."SweepId"), '[]'::jsonb) FROM "sweep_run" row),
                'summaries', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row."SweepId", row."RetentionEntityId", row."Category", row."TenantId", row."Strategy"), '[]'::jsonb) FROM "sweep_run_entity_summary" row),
                'details', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row."Id"), '[]'::jsonb) FROM "sweep_run_row_detail" row),
                'statuses', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row."Id"), '[]'::jsonb) FROM "sweep_row_handler_status" row)
            )::text
            """;
        return (string)(await command.ExecuteScalarAsync())!;
    }

    private async Task AssertAcceptedAuditAsync(Guid sweepId, Guid tenantId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT run."Status", run."TriggerKind", run."DryRun", run."TenantId", run."TotalAffected", run."Error",
                   COUNT(summary.*), COALESCE(SUM(summary."Affected"), 0),
                   COUNT(detail.*), COUNT(status.*)
            FROM "sweep_run" run
            LEFT JOIN "sweep_run_entity_summary" summary ON summary."SweepId" = run."SweepId"
            LEFT JOIN "sweep_run_row_detail" detail ON detail."SweepId" = run."SweepId"
            LEFT JOIN "sweep_row_handler_status" status ON status."SweepRunRowDetailId" = detail."Id"
            WHERE run."SweepId" = @sweepId
            GROUP BY run."SweepId"
            """;
        command.Parameters.AddWithValue("sweepId", sweepId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt32(0).Should().Be((int)SweepRunStatus.Succeeded);
        reader.GetInt32(1).Should().Be((int)SweepTriggerKind.Erasure);
        reader.GetBoolean(2).Should().BeFalse();
        reader.GetGuid(3).Should().Be(tenantId);
        reader.GetInt64(4).Should().Be(3);
        reader.IsDBNull(5).Should().BeTrue();
        reader.GetInt64(6).Should().Be(4);
        reader.GetInt64(7).Should().Be(3);
        reader.GetInt64(8).Should().Be(0);
        reader.GetInt64(9).Should().Be(0);

        await reader.DisposeAsync();
        await using var identities = connection.CreateCommand();
        identities.CommandText = """
            SELECT "RetentionEntityId", "EntityType", "Category", "TenantId", "Strategy", "Affected"
            FROM "sweep_run_entity_summary"
            WHERE "SweepId" = @sweepId
            ORDER BY "RetentionEntityId"
            """;
        identities.Parameters.AddWithValue("sweepId", sweepId);
        await using var identityReader = await identities.ExecuteReaderAsync();
        var actual = new List<(Guid, string, string, Guid, int, long)>();
        while (await identityReader.ReadAsync())
        {
            actual.Add((identityReader.GetGuid(0), identityReader.GetString(1), identityReader.GetString(2),
                identityReader.GetGuid(3), identityReader.GetInt32(4), identityReader.GetInt64(5)));
        }
        actual.Should().BeEquivalentTo([
            (Note.RetentionIdentity, typeof(Note).FullName!, "short-lived", tenantId, (int)Strategy.Purge, 1L),
            (Guid.Parse("6107ff39-bf33-413c-889e-6347c909ba15"), typeof(SoftDeleteRecord).FullName!, "soft-delete", tenantId, (int)Strategy.SoftDelete, 1L),
            (Guid.Parse("fd4a533e-e6a9-44ea-948e-cbf881f35e57"), typeof(AnonymisedContact).FullName!, "anonymise", tenantId, (int)Strategy.Anonymise, 1L),
            (Guid.Parse("6ebbc096-d3b8-4077-8f21-bf9b4d53c869"), typeof(TombstoneRecord).FullName!, "tombstone-anonymise", tenantId, (int)Strategy.Anonymise, 0L),
        ]);
    }

    private sealed class RecordingObserver : IRetentionAuditObserver
    {
        private readonly ConcurrentQueue<SweepEvent> events = new();
        public IReadOnlyList<SweepEvent> Events => events.ToArray();

        public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct)
        {
            events.Enqueue(evt);
            return Task.CompletedTask;
        }
    }
}
