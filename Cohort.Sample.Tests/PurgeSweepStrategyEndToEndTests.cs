using System.Data;

using Cohort.Domain;
using Cohort.Infrastructure.Sweep;

using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class PurgeSweepStrategyEndToEndTests(PostgresFixture fixture)
{
    [Fact]
    public async Task SweepAsync_Deletes_Only_Rows_Past_Cutoff_For_The_Target_Tenant()
    {
        const string tableName = "purge_candidate_records";
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();

        await using (var setup = connection.CreateCommand())
        {
            setup.CommandText =
                """
                DROP TABLE IF EXISTS "purge_candidate_records";
                CREATE TABLE "purge_candidate_records" (
                    "Id" uuid PRIMARY KEY,
                    "TenantId" uuid NOT NULL,
                    "CreatedAt" timestamp with time zone NOT NULL,
                    "Body" text NOT NULL
                );
                """;
            await setup.ExecuteNonQueryAsync();
        }

        await InsertRecordAsync(connection, Guid.NewGuid(), tenantA, now.AddDays(-45), "delete-me");
        await InsertRecordAsync(connection, Guid.NewGuid(), tenantA, now.AddDays(-5), "keep-newer");
        await InsertRecordAsync(connection, Guid.NewGuid(), tenantB, now.AddDays(-45), "keep-other-tenant");

        var strategy = new PurgeSweepStrategy();
        var entry = new RetentionEntry(
            typeof(PurgeCandidateRecord),
            tableName,
            "short-lived",
            nameof(PurgeCandidateRecord.CreatedAt),
            "CreatedAt",
            [],
            new TenantConvention(nameof(PurgeCandidateRecord.TenantId), "TenantId"),
            null
        );
        var rule = new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge);
        var context = new RetentionResolutionContext(
            "short-lived",
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            now,
            []
        );

        var affected = await strategy.SweepAsync(entry, rule, context, connection, CancellationToken.None);

        affected.Should().Be(1);
        var remainingRows = await GetRemainingRowsAsync(connection);
        remainingRows.Should().BeEquivalentTo(
            [
                new RemainingRow(tenantA, "keep-newer"),
                new RemainingRow(tenantB, "keep-other-tenant"),
            ]
        );
    }

    private static async Task InsertRecordAsync(
        NpgsqlConnection connection,
        Guid id,
        Guid tenantId,
        DateTimeOffset createdAt,
        string body
    )
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            INSERT INTO "purge_candidate_records" ("Id", "TenantId", "CreatedAt", "Body")
            VALUES (@id, @tenantId, @createdAt, @body)
            """;
        command.Parameters.Add(CreateParameter(command, "id", id));
        command.Parameters.Add(CreateParameter(command, "tenantId", tenantId));
        command.Parameters.Add(CreateParameter(command, "createdAt", createdAt));
        command.Parameters.Add(CreateParameter(command, "body", body));
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<IReadOnlyList<RemainingRow>> GetRemainingRowsAsync(NpgsqlConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT "TenantId", "Body"
            FROM "purge_candidate_records"
            ORDER BY "Body"
            """;

        var rows = new List<RemainingRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new RemainingRow(reader.GetGuid(0), reader.GetString(1)));
        }

        return rows;
    }

    private static IDbDataParameter CreateParameter(
        NpgsqlCommand command,
        string name,
        object value
    )
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        return parameter;
    }

    private sealed class PurgeCandidateRecord
    {
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed record RemainingRow(Guid TenantId, string Body);
}
