using System.Collections.Concurrent;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Sample.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests.ComplianceCorpus;

public sealed class AccountabilityCorpusTests(PostgresFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Consumer_observer_registration_cannot_suppress_authoritative_audit()
    {
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var recordId = Guid.NewGuid();
        var observer = new RecordingObserver();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(
                new Note
                {
                    Id = recordId,
                    TenantId = tenantId,
                    CreatedAt = now.AddDays(-60),
                    Body = "authoritative-audit",
                }
            );
            await db.SaveChangesAsync();
        }

        using var host = new CohortTestHost(
            ConnectionString,
            configureServices: services => services.AddSingleton<IRetentionAuditObserver>(observer)
        );

        var result = await host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now
        );

        observer.Events.First().Should().BeOfType<SweepEvent.Started>();
        observer.Events.Last().Should().BeOfType<SweepEvent.Completed>();
        observer.Events.OfType<SweepEvent.Started>().Should().ContainSingle();
        observer.Events.OfType<SweepEvent.Completed>().Should().ContainSingle();
        observer.Events.OfType<SweepEvent.Completed>().Should().ContainSingle()
            .Which.TotalAffected.Should().Be(1);

        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT "Status", "TotalAffected"
            FROM "sweep_run"
            WHERE "SweepId" = @sweepId
            """;
        command.Parameters.AddWithValue("sweepId", result.SweepId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt32(0).Should().Be((int)SweepRunStatus.Succeeded);
        reader.GetInt64(1).Should().Be(1);
    }

    [Fact]
    public async Task Mutating_run_persists_coherent_summary_and_row_evidence()
    {
        var tenantId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        var recordId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.PerRowAuditedLogs.Add(new PerRowAuditedLog
            {
                Id = recordId,
                TenantId = tenantId,
                CreatedAt = now.AddDays(-60),
                Payload = "accountability",
            });
            await db.SaveChangesAsync();
        }

        var observer = new RecordingObserver();
        using var host = new CohortTestHost(
            ConnectionString,
            new ProvenanceRuleProvider(),
            configureServices: services => services.AddSingleton<IRetentionAuditObserver>(observer)
        );
        var result = await host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            now
        );
        result.EntityFailures.Should().BeEmpty();
        result.Counts.Sum(count => count.Affected).Should().Be(1);
        result.Counts.Count(count => count.Affected > 0).Should().Be(1);
        observer.Events.First().Should().BeOfType<SweepEvent.Started>();
        observer.Events.Last().Should().BeOfType<SweepEvent.Completed>();
        observer.Events.OfType<SweepEvent.Started>().Should().ContainSingle();
        observer.Events.OfType<SweepEvent.Completed>().Should().ContainSingle();
        observer.Events.OfType<SweepEvent.Completed>().Should().ContainSingle()
            .Which.TotalAffected.Should().Be(1);

        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                run."Status",
                run."TotalAffected",
                summary."RetentionEntityId",
                summary."EntityType",
                summary."Category",
                summary."TenantId",
                summary."Strategy",
                summary."ResolvedPeriod",
                summary."Affected",
                summary."RuleSource",
                summary."RuleReason",
                detail."RetentionEntityId",
                detail."EntityType",
                detail."RecordId",
                detail."Category",
                detail."TenantId",
                detail."Strategy"
                ,(SELECT COUNT(*) FROM "sweep_run_entity_summary" all_summary WHERE all_summary."SweepId" = run."SweepId")
                ,(SELECT COUNT(*) FROM "sweep_run_row_detail" all_detail WHERE all_detail."SweepId" = run."SweepId")
                ,(SELECT COUNT(*) FROM "sweep_run_entity_summary" matching_summary WHERE matching_summary."SweepId" = run."SweepId" AND matching_summary."Category" = 'per-row-audit-override')
                ,(SELECT COUNT(DISTINCT (matching_summary."RetentionEntityId", matching_summary."EntityType", matching_summary."Category", matching_summary."TenantId", matching_summary."Strategy")) FROM "sweep_run_entity_summary" matching_summary WHERE matching_summary."SweepId" = run."SweepId" AND matching_summary."Category" = 'per-row-audit-override')
                ,(SELECT COUNT(DISTINCT (matching_detail."RetentionEntityId", matching_detail."EntityType", matching_detail."RecordId", matching_detail."Category", matching_detail."TenantId", matching_detail."Strategy")) FROM "sweep_run_row_detail" matching_detail WHERE matching_detail."SweepId" = run."SweepId" AND matching_detail."RecordId" = @recordId)
            FROM "sweep_run" run
            INNER JOIN "sweep_run_entity_summary" summary ON summary."SweepId" = run."SweepId" AND summary."Category" = 'per-row-audit-override'
            INNER JOIN "sweep_run_row_detail" detail ON detail."SweepId" = run."SweepId" AND detail."RecordId" = @recordId
            WHERE run."SweepId" = @sweepId
            """;
        command.Parameters.AddWithValue("sweepId", result.SweepId);
        command.Parameters.AddWithValue("recordId", recordId.ToString());
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);
        reader.GetInt64(1).Should().Be(1);
        var retentionEntityId = Guid.Parse("42670ee7-c26a-4a2a-a2ab-d9571db7d4f6");
        reader.GetGuid(2).Should().Be(retentionEntityId);
        reader.GetString(3).Should().Be(typeof(PerRowAuditedLog).FullName);
        reader.GetString(4).Should().Be("per-row-audit-override");
        reader.GetGuid(5).Should().Be(tenantId);
        reader.GetInt32(6).Should().Be((int)Strategy.Purge);
        reader.GetFieldValue<TimeSpan>(7).Should().Be(TimeSpan.FromDays(30));
        reader.GetInt64(8).Should().Be(1);
        reader.GetString(9).Should().Be("compliance-corpus");
        reader.GetString(10).Should().Be("accountability evidence");
        reader.GetGuid(11).Should().Be(retentionEntityId);
        reader.GetString(12).Should().Be(typeof(PerRowAuditedLog).FullName);
        reader.GetString(13).Should().Be(recordId.ToString());
        reader.GetString(14).Should().Be("per-row-audit-override");
        reader.GetGuid(15).Should().Be(tenantId);
        reader.GetInt32(16).Should().Be((int)Strategy.Purge);
        reader.GetInt64(17).Should().Be(result.Counts.Count);
        reader.GetInt64(18).Should().Be(1);
        reader.GetInt64(19).Should().Be(1);
        reader.GetInt64(20).Should().Be(1);
        reader.GetInt64(21).Should().Be(1);

        var observedSummary = observer.Events.OfType<SweepEvent.EntitySummary>()
            .Should().ContainSingle(summary => summary.Category == "per-row-audit-override").Which;
        (observedSummary.RetentionEntityId, observedSummary.EntityType.FullName, observedSummary.Category,
            observedSummary.TenantId, observedSummary.Strategy, observedSummary.Affected).Should().Be(
            (retentionEntityId, typeof(PerRowAuditedLog).FullName, "per-row-audit-override",
                tenantId, Strategy.Purge, 1L)
        );
        var observedDetail = observer.Events.OfType<SweepEvent.RowDetail>().Should().ContainSingle().Which;
        (observedDetail.RetentionEntityId, observedDetail.EntityType.FullName, observedDetail.RecordId,
            observedDetail.Category, observedDetail.TenantId, observedDetail.Strategy).Should().Be(
            (retentionEntityId, typeof(PerRowAuditedLog).FullName, recordId.ToString(),
                "per-row-audit-override", tenantId, Strategy.Purge)
        );

        await using var verify = Host.CreateDbContext();
        (await verify.PerRowAuditedLogs.AnyAsync(row => row.Id == recordId)).Should().BeFalse();
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

    private sealed class ProvenanceRuleProvider : IRetentionRuleProvider
    {
        private readonly SampleRetentionRuleProvider inner = new();

        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            inner.GetCapabilities(category);

        public async Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        )
        {
            var rule = await inner.ResolveAsync(context, ct);
            return context.Category == "per-row-audit-override" && rule is not null
                ? new RetentionRule(
                    rule.Period,
                    rule.Strategy,
                    rule.LegalMin,
                    rule.AuditRowDetail,
                    new RetentionRuleProvenance("compliance-corpus", "accountability evidence")
                )
                : rule;
        }
    }
}
