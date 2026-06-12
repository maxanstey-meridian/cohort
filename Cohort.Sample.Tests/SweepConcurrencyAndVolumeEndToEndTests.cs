using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;

using Npgsql;

namespace Cohort.Sample.Tests;

public sealed class SweepConcurrencyAndVolumeEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Two_Concurrent_Engines_Sweep_Every_Row_Exactly_Once()
    {
        // Direct IRetentionSweep calls race by design: the advisory lock guards only the
        // hosted worker. FOR UPDATE SKIP LOCKED batching must make that race safe —
        // every expired row retired exactly once across both runs, no deadlocks.
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        const int expiredRows = 400;

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.AddRange(
                Enumerable.Range(0, expiredRows).Select(index => new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-120),
                    Body = $"concurrent-expired-{index}",
                })
            );
            db.Notes.Add(
                new Note
                {
                    Id = Guid.NewGuid(),
                    TenantId = tenantId,
                    CreatedAt = asOf.AddDays(-5),
                    Body = "concurrent-fresh",
                }
            );
            await db.SaveChangesAsync();
        }

        // Small batches force the two engines to interleave instead of one grabbing
        // everything in a single pass.
        using var host = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:SweepBatchSize"] = "50",
            }
        );
        var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());

        var results = await Task.WhenAll(
            host.RunSweepAsync(tenant, asOf),
            host.RunSweepAsync(tenant, asOf)
        );

        var totalNotesAffected = results
            .SelectMany(result => result.Counts)
            .Where(count => count.EntityType == typeof(Note))
            .Sum(count => count.Affected);
        totalNotesAffected.Should().Be(expiredRows);

        results.Should().AllSatisfy(result => result.EntityFailures.Should().BeEmpty());

        await using var verify = Host.CreateDbContext();
        var remaining = await verify.Notes.Where(note => note.TenantId == tenantId).ToListAsync();
        remaining.Should().ContainSingle(note => note.Body == "concurrent-fresh");
    }

    [Fact]
    public async Task Sweep_Retires_A_Large_Backlog_Across_Many_Batches()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 13, 12, 0, 0, TimeSpan.Zero);
        const int backlogRows = 25_000;

        await SeedNotesInBulkAsync(tenantId, asOf.AddDays(-120), backlogRows);

        using var host = new CohortTestHost(
            GetConnectionString(),
            configurationOverrides: new Dictionary<string, string?>
            {
                [$"{CohortOptions.SectionName}:SweepBatchSize"] = "1000",
            }
        );

        var result = await host.RunSweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf
        );

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().Contain(count =>
            count.EntityType == typeof(Note) && count.Affected == backlogRows
        );

        await using var verify = Host.CreateDbContext();
        (await verify.Notes.CountAsync(note => note.TenantId == tenantId)).Should().Be(0);
    }

    private string GetConnectionString()
    {
        using var db = Host.CreateDbContext();
        return db.Database.GetConnectionString()!;
    }

    private async Task SeedNotesInBulkAsync(Guid tenantId, DateTimeOffset createdAt, int count)
    {
        await using var connection = new NpgsqlConnection(GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            INSERT INTO "notes" ("Id", "TenantId", "SubjectId", "CreatedAt", "Body")
            SELECT gen_random_uuid(), @tenantId, NULL, @createdAt, 'volume-' || g
            FROM generate_series(1, @count) AS g
            """;
        command.Parameters.AddWithValue("tenantId", tenantId);
        command.Parameters.AddWithValue("createdAt", createdAt);
        command.Parameters.AddWithValue("count", count);
        await command.ExecuteNonQueryAsync();
    }
}
