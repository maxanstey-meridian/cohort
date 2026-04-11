using Cohort.Application;
using Cohort.Domain;
using Cohort.Infrastructure.Sweep;

using Microsoft.EntityFrameworkCore;

using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class RetentionSweepEngineEndToEndTests(PostgresFixture fixture)
{
    [Fact]
    public async Task SweepAsync_Deletes_Purge_Entries_And_Reports_Exempt_Entries_For_One_Tenant()
    {
        var tenantA = Guid.NewGuid();
        var tenantB = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

        await using var setupConnection = new NpgsqlConnection(fixture.ConnectionString);
        await setupConnection.OpenAsync();

        await using (var setup = setupConnection.CreateCommand())
        {
            setup.CommandText =
                """
                DROP TABLE IF EXISTS "engine_purge_records";
                DROP TABLE IF EXISTS "engine_exempt_records";
                CREATE TABLE "engine_purge_records" (
                    "Id" uuid PRIMARY KEY,
                    "TenantId" uuid NOT NULL,
                    "CreatedAt" timestamp with time zone NOT NULL,
                    "Body" text NOT NULL
                );
                CREATE TABLE "engine_exempt_records" (
                    "Id" uuid PRIMARY KEY,
                    "TenantId" uuid NOT NULL,
                    "CreatedAt" timestamp with time zone NOT NULL,
                    "Title" text NOT NULL
                );
                """;
            await setup.ExecuteNonQueryAsync();
        }

        await InsertPurgeRecordAsync(
            setupConnection,
            Guid.NewGuid(),
            tenantA,
            asOf.AddDays(-120),
            "delete-me"
        );
        await InsertPurgeRecordAsync(
            setupConnection,
            Guid.NewGuid(),
            tenantA,
            asOf.AddDays(-45),
            "keep-legal-min"
        );
        await InsertPurgeRecordAsync(
            setupConnection,
            Guid.NewGuid(),
            tenantB,
            asOf.AddDays(-120),
            "keep-other-tenant"
        );
        await InsertExemptRecordAsync(
            setupConnection,
            Guid.NewGuid(),
            tenantA,
            asOf.AddDays(-365),
            "keep-exempt"
        );

        var options = new DbContextOptionsBuilder<SweepEngineDbContext>()
            .UseNpgsql(fixture.ConnectionString)
            .Options;
        await using var db = new SweepEngineDbContext(options);
        var categoryRepository = new StaticCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["purge-category"] = new StaticRetentionRuleResolver(
                    new RetentionRule(
                        TimeSpan.FromDays(30),
                        Strategy.Purge,
                        TimeSpan.FromDays(90)
                    )
                ),
                ["exempt-category"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Exempt)
                ),
            }
        );
        var engine = new RetentionSweepEngine(
            db,
            new RetentionRegistry(db),
            categoryRepository,
            new PurgeSweepStrategy()
        );

        var result = await engine.SweepAsync(
            new TenantContext(tenantA, "uk", new Dictionary<string, string>()),
            asOf,
            CancellationToken.None
        );

        result.SweepId.Should().NotBe(Guid.Empty);
        result.CompletedAt.Should().BeOnOrAfter(result.StartedAt);
        result.Counts.Should().HaveCount(2);
        result.Counts.Should().ContainEquivalentOf(
            new EntitySweepCount(
                typeof(PurgeEngineRecord),
                "purge-category",
                tenantA,
                Strategy.Purge,
                1
            )
        );
        result.Counts.Should().ContainEquivalentOf(
            new EntitySweepCount(
                typeof(ExemptEngineRecord),
                "exempt-category",
                tenantA,
                Strategy.Exempt,
                0
            )
        );

        var purgeBodies = await GetPurgeBodiesAsync(setupConnection);
        purgeBodies.Should().Equal("keep-legal-min", "keep-other-tenant");

        var exemptBodies = await GetExemptTitlesAsync(setupConnection);
        exemptBodies.Should().Equal("keep-exempt");
    }

    [Fact]
    public async Task SweepAsync_Rolls_Back_When_A_Runtime_Strategy_Is_Not_Supported()
    {
        var tenantId = Guid.NewGuid();
        var asOf = new DateTimeOffset(2026, 4, 11, 12, 0, 0, TimeSpan.Zero);

        await using var setupConnection = new NpgsqlConnection(fixture.ConnectionString);
        await setupConnection.OpenAsync();

        await using (var setup = setupConnection.CreateCommand())
        {
            setup.CommandText =
                """
                DROP TABLE IF EXISTS "a_purge_then_fail_records";
                DROP TABLE IF EXISTS "z_unsupported_records";
                CREATE TABLE "a_purge_then_fail_records" (
                    "Id" uuid PRIMARY KEY,
                    "TenantId" uuid NOT NULL,
                    "CreatedAt" timestamp with time zone NOT NULL,
                    "Body" text NOT NULL
                );
                CREATE TABLE "z_unsupported_records" (
                    "Id" uuid PRIMARY KEY,
                    "TenantId" uuid NOT NULL,
                    "CreatedAt" timestamp with time zone NOT NULL,
                    "Body" text NOT NULL
                );
                """;
            await setup.ExecuteNonQueryAsync();
        }

        await InsertTableRowAsync(
            setupConnection,
            "a_purge_then_fail_records",
            Guid.NewGuid(),
            tenantId,
            asOf.AddDays(-120),
            "must-roll-back"
        );
        await InsertTableRowAsync(
            setupConnection,
            "z_unsupported_records",
            Guid.NewGuid(),
            tenantId,
            asOf.AddDays(-120),
            "unsupported"
        );

        var options = new DbContextOptionsBuilder<UnsupportedStrategyDbContext>()
            .UseNpgsql(fixture.ConnectionString)
            .Options;
        await using var db = new UnsupportedStrategyDbContext(options);
        var categoryRepository = new StaticCategoryRepository(
            new Dictionary<string, IRetentionRuleResolver>
            {
                ["purge-category"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
                ),
                ["unsupported-category"] = new StaticRetentionRuleResolver(
                    new RetentionRule(TimeSpan.FromDays(30), Strategy.SoftDelete)
                ),
            }
        );
        var engine = new RetentionSweepEngine(
            db,
            new RetentionRegistry(db),
            categoryRepository,
            new PurgeSweepStrategy()
        );

        var act = () => engine.SweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
            asOf,
            CancellationToken.None
        );

        await act
            .Should()
            .ThrowAsync<InvalidOperationException>()
            .WithMessage("*Milestone A sweep engine*");

        var purgeBodies = await GetBodiesAsync(setupConnection, "a_purge_then_fail_records");
        purgeBodies.Should().Equal("must-roll-back");
    }

    private static async Task InsertPurgeRecordAsync(
        NpgsqlConnection connection,
        Guid id,
        Guid tenantId,
        DateTimeOffset createdAt,
        string body
    )
    {
        await InsertTableRowAsync(connection, "engine_purge_records", id, tenantId, createdAt, body);
    }

    private static async Task InsertExemptRecordAsync(
        NpgsqlConnection connection,
        Guid id,
        Guid tenantId,
        DateTimeOffset createdAt,
        string title
    )
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            INSERT INTO "engine_exempt_records" ("Id", "TenantId", "CreatedAt", "Title")
            VALUES (@id, @tenantId, @createdAt, @title)
            """;
        command.Parameters.Add(CreateParameter(command, "id", id));
        command.Parameters.Add(CreateParameter(command, "tenantId", tenantId));
        command.Parameters.Add(CreateParameter(command, "createdAt", createdAt));
        command.Parameters.Add(CreateParameter(command, "title", title));
        await command.ExecuteNonQueryAsync();
    }

    private static async Task InsertTableRowAsync(
        NpgsqlConnection connection,
        string tableName,
        Guid id,
        Guid tenantId,
        DateTimeOffset createdAt,
        string body
    )
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO "{tableName}" ("Id", "TenantId", "CreatedAt", "Body")
            VALUES (@id, @tenantId, @createdAt, @body)
            """;
        command.Parameters.Add(CreateParameter(command, "id", id));
        command.Parameters.Add(CreateParameter(command, "tenantId", tenantId));
        command.Parameters.Add(CreateParameter(command, "createdAt", createdAt));
        command.Parameters.Add(CreateParameter(command, "body", body));
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<IReadOnlyList<string>> GetPurgeBodiesAsync(NpgsqlConnection connection)
    {
        return await GetBodiesAsync(connection, "engine_purge_records");
    }

    private static async Task<IReadOnlyList<string>> GetExemptTitlesAsync(NpgsqlConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT "Title"
            FROM "engine_exempt_records"
            ORDER BY "Title"
            """;

        var values = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetString(0));
        }

        return values;
    }

    private static async Task<IReadOnlyList<string>> GetBodiesAsync(
        NpgsqlConnection connection,
        string tableName
    )
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT "Body"
            FROM "{tableName}"
            ORDER BY "Body"
            """;

        var values = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetString(0));
        }

        return values;
    }

    private static NpgsqlParameter CreateParameter(
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

    [Retain("purge-category", nameof(PurgeEngineRecord.CreatedAt))]
    private sealed class PurgeEngineRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public string Body { get; init; } = "";
    }

    [Retain("exempt-category", nameof(ExemptEngineRecord.CreatedAt))]
    private sealed class ExemptEngineRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public string Title { get; init; } = "";
    }

    [Retain("purge-category", nameof(APurgeThenFailRecord.CreatedAt))]
    private sealed class APurgeThenFailRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public string Body { get; init; } = "";
    }

    [Retain("unsupported-category", nameof(ZUnsupportedRecord.CreatedAt))]
    private sealed class ZUnsupportedRecord
    {
        public Guid Id { get; init; }
        public Guid TenantId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public string Body { get; init; } = "";
    }

    private sealed class SweepEngineDbContext(DbContextOptions<SweepEngineDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PurgeEngineRecord>(entity =>
            {
                entity.ToTable("engine_purge_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.TenantId).HasColumnName("TenantId");
                entity.Property(record => record.CreatedAt).HasColumnName("CreatedAt");
                entity.Property(record => record.Body).HasColumnName("Body");
            });

            modelBuilder.Entity<ExemptEngineRecord>(entity =>
            {
                entity.ToTable("engine_exempt_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.TenantId).HasColumnName("TenantId");
                entity.Property(record => record.CreatedAt).HasColumnName("CreatedAt");
                entity.Property(record => record.Title).HasColumnName("Title");
            });
        }
    }

    private sealed class UnsupportedStrategyDbContext(
        DbContextOptions<UnsupportedStrategyDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<APurgeThenFailRecord>(entity =>
            {
                entity.ToTable("a_purge_then_fail_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.TenantId).HasColumnName("TenantId");
                entity.Property(record => record.CreatedAt).HasColumnName("CreatedAt");
                entity.Property(record => record.Body).HasColumnName("Body");
            });

            modelBuilder.Entity<ZUnsupportedRecord>(entity =>
            {
                entity.ToTable("z_unsupported_records");
                entity.HasKey(record => record.Id);
                entity.Property(record => record.TenantId).HasColumnName("TenantId");
                entity.Property(record => record.CreatedAt).HasColumnName("CreatedAt");
                entity.Property(record => record.Body).HasColumnName("Body");
            });
        }
    }

    private sealed class StaticCategoryRepository(
        IReadOnlyDictionary<string, IRetentionRuleResolver> resolvers
    ) : IRetentionCategoryRepository
    {
        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
        {
            resolvers.TryGetValue(category, out var resolver);
            return Task.FromResult(resolver);
        }
    }
}
