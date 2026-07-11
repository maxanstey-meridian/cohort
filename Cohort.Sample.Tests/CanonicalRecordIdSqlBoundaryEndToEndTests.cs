using System.Globalization;
using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Cohort.Sample.Tests;

[Collection("Integration")]
public sealed class CanonicalRecordIdSqlBoundaryEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Public_Sweep_Uses_Postgres_Canonical_Record_Ids_Under_A_NonInvariant_Culture()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var connectionString = database.ConnectionString;
        var purgedId = new ConvertedRecordId(1234.50m);
        var heldId = new ConvertedRecordId(9876.50m);
        const string purgedCanonicalId = "1234.50";
        const string heldCanonicalId = "9876.50";
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        var originalCulture = CultureInfo.CurrentCulture;
        var originalUiCulture = CultureInfo.CurrentUICulture;

        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fr-FR");
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo("fr-FR");
            purgedId.Value.ToString().Should().Be("1234,50");

            await using var services = BuildServiceProvider(connectionString);
            await using (var scope = services.CreateAsyncScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<CanonicalRecordIdDbContext>();
                await db.Database.EnsureCreatedAsync();
                db.Records.AddRange(
                    new CanonicalRecordIdRecord
                    {
                        Id = purgedId,
                        CreatedAt = asOf.AddDays(-60),
                        Payload = "purge",
                    },
                    new CanonicalRecordIdRecord
                    {
                        Id = heldId,
                        CreatedAt = asOf.AddDays(-60),
                        Payload = "hold",
                    }
                );
                await db.SaveChangesAsync();

                var holds = scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>();
                await holds.CreateAsync(
                    new RetentionHoldRequest(
                        Guid.NewGuid(),
                        RetentionEntityIdentity.For<CanonicalRecordIdRecord>(),
                        heldCanonicalId,
                        null,
                        "canonical identity regression",
                        asOf.AddDays(-1)
                    ),
                    CancellationToken.None
                );
            }

            var sweep = services.GetRequiredService<IRetentionSweep>();
            var result = await sweep.ExecuteAsync(
                RetentionSweepRequest.Tenantless(asOf)
            );

            result.EntityFailures.Should().BeEmpty();
            result.Counts.Should().ContainSingle().Which.Should().Be(
                new EntitySweepCount(
                    typeof(CanonicalRecordIdRecord),
                    "canonical-record-id",
                    Guid.Empty,
                    Strategy.Anonymise,
                    1,
                    HeldCount: 1
                )
            );

            await using var verify = new NpgsqlConnection(connectionString);
            await verify.OpenAsync();
            await using var verifyCommand = verify.CreateCommand();
            verifyCommand.CommandText = """
                SELECT
                    (SELECT "EntityId" FROM "sweep_run_row_detail" WHERE "SweepId" = @sweepId),
                    (SELECT "RecordId" FROM "retention_holds" WHERE "RetentionEntityId" = '00000000-0000-0000-0004-000000000001'),
                    (SELECT "Id"::text FROM "canonical_record_id_regression" WHERE "Id" = 9876.50),
                    (SELECT "Payload" FROM "canonical_record_id_regression" WHERE "Id" = 1234.50)
                """;
            verifyCommand.Parameters.AddWithValue("sweepId", result.SweepId);
            await using var reader = await verifyCommand.ExecuteReaderAsync();
            (await reader.ReadAsync()).Should().BeTrue();
            reader.GetString(0).Should().Be(purgedCanonicalId);
            reader.GetString(1).Should().Be(heldCanonicalId);
            reader.GetString(2).Should().Be(heldCanonicalId);
            reader.GetString(3).Should().BeEmpty();
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
            CultureInfo.CurrentUICulture = originalUiCulture;

        }
    }

    [Fact]
    public async Task Handler_Aware_Purge_Converts_Record_Id_Before_Postgres_Canonicalization()
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var recordId = new ConvertedRecordId(1234.50m);
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);

        await using var services = BuildServiceProvider(database.ConnectionString, Strategy.Purge);
        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<CanonicalRecordIdDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Records.Add(
                new CanonicalRecordIdRecord
                {
                    Id = recordId,
                    CreatedAt = asOf.AddDays(-60),
                    Payload = "purge",
                }
            );
            await db.SaveChangesAsync();
        }

        var result = await services.GetRequiredService<IRetentionSweep>().ExecuteAsync(
            RetentionSweepRequest.Tenantless(asOf)
        );

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().ContainSingle().Which.Affected.Should().Be(1);
        await using var verify = new NpgsqlConnection(database.ConnectionString);
        await verify.OpenAsync();
        await using var command = verify.CreateCommand();
        command.CommandText = """
            SELECT
                (SELECT "EntityId" FROM "sweep_run_row_detail" WHERE "SweepId" = @sweepId),
                EXISTS (SELECT 1 FROM "canonical_record_id_regression" WHERE "Id" = 1234.50)
            """;
        command.Parameters.AddWithValue("sweepId", result.SweepId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetString(0).Should().Be("1234.50");
        reader.GetBoolean(1).Should().BeFalse();
    }

    [Theory]
    [InlineData(Strategy.Purge)]
    [InlineData(Strategy.Anonymise)]
    public async Task Ordinary_Sql_Path_Uses_Postgres_Canonical_Record_Ids_Under_A_NonInvariant_Culture(
        Strategy strategy
    )
    {
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var mutatedId = new ConvertedRecordId(1234.50m);
        var heldId = new ConvertedRecordId(9876.50m);
        const string mutatedCanonicalId = "1234.50";
        const string heldCanonicalId = "9876.50";
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        var originalCulture = CultureInfo.CurrentCulture;
        var originalUiCulture = CultureInfo.CurrentUICulture;

        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fr-FR");
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo("fr-FR");
            mutatedId.Value.ToString().Should().Be("1234,50");

            await using var services = BuildServiceProvider(
                database.ConnectionString,
                strategy,
                registerHandler: false
            );
            await using (var scope = services.CreateAsyncScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<CanonicalRecordIdDbContext>();
                await db.Database.EnsureCreatedAsync();
                db.Records.AddRange(
                    new CanonicalRecordIdRecord
                    {
                        Id = mutatedId,
                        CreatedAt = asOf.AddDays(-60),
                        Payload = "mutate",
                    },
                    new CanonicalRecordIdRecord
                    {
                        Id = heldId,
                        CreatedAt = asOf.AddDays(-60),
                        Payload = "hold",
                    }
                );
                await db.SaveChangesAsync();

                await scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>().CreateAsync(
                    new RetentionHoldRequest(
                        Guid.NewGuid(),
                        RetentionEntityIdentity.For<CanonicalRecordIdRecord>(),
                        heldCanonicalId,
                        null,
                        "ordinary SQL canonical identity regression",
                        asOf.AddDays(-1)
                    ),
                    CancellationToken.None
                );
            }

            var result = await services.GetRequiredService<IRetentionSweep>().ExecuteAsync(
                RetentionSweepRequest.Tenantless(asOf)
            );

            result.EntityFailures.Should().BeEmpty();
            result.Counts.Should().ContainSingle().Which.Should().Be(
                new EntitySweepCount(
                    typeof(CanonicalRecordIdRecord),
                    "canonical-record-id",
                    Guid.Empty,
                    strategy,
                    1,
                    HeldCount: 1
                )
            );

            await using var verify = new NpgsqlConnection(database.ConnectionString);
            await verify.OpenAsync();
            await using var command = verify.CreateCommand();
            command.CommandText = """
                SELECT
                    (SELECT "EntityId" FROM "sweep_run_row_detail" WHERE "SweepId" = @sweepId),
                    EXISTS (SELECT 1 FROM "canonical_record_id_regression" WHERE "Id" = 9876.50),
                    (SELECT "Payload" FROM "canonical_record_id_regression" WHERE "Id" = 9876.50),
                    EXISTS (SELECT 1 FROM "canonical_record_id_regression" WHERE "Id" = 1234.50),
                    (SELECT "Payload" FROM "canonical_record_id_regression" WHERE "Id" = 1234.50)
                """;
            command.Parameters.AddWithValue("sweepId", result.SweepId);
            await using var reader = await command.ExecuteReaderAsync();
            (await reader.ReadAsync()).Should().BeTrue();
            reader.GetString(0).Should().Be(mutatedCanonicalId);
            reader.GetBoolean(1).Should().BeTrue();
            reader.GetString(2).Should().Be("hold");

            if (strategy == Strategy.Purge)
            {
                reader.GetBoolean(3).Should().BeFalse();
                reader.IsDBNull(4).Should().BeTrue();
            }
            else
            {
                reader.GetBoolean(3).Should().BeTrue();
                reader.GetString(4).Should().BeEmpty();
            }
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
            CultureInfo.CurrentUICulture = originalUiCulture;
        }
    }

    private static ServiceProvider BuildServiceProvider(
        string connectionString,
        Strategy strategy = Strategy.Anonymise,
        bool registerHandler = true
    )
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(
            new ConfigurationBuilder().AddInMemoryCollection().Build()
        );
        services.AddLogging();
        services.AddDbContext<CanonicalRecordIdDbContext>(options =>
            options.UseNpgsql(connectionString)
        );
        services.AddSingleton<IRetentionCategoryRepository>(new CategoryRepository(strategy));
        services.AddCohort<CanonicalRecordIdDbContext>();
        if (registerHandler)
        {
            services.AddRowHandler<CanonicalRecordIdRecord, CanonicalRecordIdHandler>();
        }

        return services.BuildServiceProvider(validateScopes: true);
    }

    private sealed class CategoryRepository(Strategy strategy) : IRetentionCategoryRepository
    {
        private readonly IRetentionRuleResolver resolver = new StaticRetentionRuleResolver(
            new RetentionRule(TimeSpan.FromDays(30), strategy)
        );

        public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct) =>
            Task.FromResult<IRetentionRuleResolver?>(
                category == "canonical-record-id" ? resolver : null
            );
    }

    private sealed class CanonicalRecordIdDbContext(
        DbContextOptions<CanonicalRecordIdDbContext> options
    ) : DbContext(options)
    {
        public DbSet<CanonicalRecordIdRecord> Records => Set<CanonicalRecordIdRecord>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CanonicalRecordIdRecord>(entity =>
            {
                entity.ToTable("canonical_record_id_regression");
                entity.HasKey(record => record.Id);
                entity
                    .Property(record => record.Id)
                    .HasConversion(id => id.Value, value => new ConvertedRecordId(value))
                    .HasColumnType("numeric(18,2)");
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    private sealed class CanonicalRecordIdHandler : IRetentionHandler<CanonicalRecordIdRecord>;

    [Retain("canonical-record-id", nameof(CreatedAt), AuditRowDetail = AuditRowDetail.PerRow)]
    [RetentionEntityId("00000000-0000-0000-0004-000000000001")]
    [RetentionTenantless]
    private sealed class CanonicalRecordIdRecord
    {
        [RetentionRecordId]
        public ConvertedRecordId Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        [Anonymise(AnonymiseMethod.EmptyString)]
        public string Payload { get; init; } = "";
        public DateTimeOffset? AnonymisedAt { get; init; }
    }

    private readonly record struct ConvertedRecordId(decimal Value);
}
