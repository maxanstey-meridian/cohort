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
                    HeldCount: 1,
                    SkippedCount: 0,
                    NullAnchorCount: 0,
                    RetainedCount: 0,
                    DeferredCount: 0,
                    BlockedCount: 0
                )
            );

            await using var verify = new NpgsqlConnection(connectionString);
            await verify.OpenAsync();
            await using var verifyCommand = verify.CreateCommand();
            verifyCommand.CommandText = """
                SELECT
                    (SELECT "RecordId" FROM "sweep_run_row_detail" WHERE "SweepId" = @sweepId),
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
                (SELECT "RecordId" FROM "sweep_run_row_detail" WHERE "SweepId" = @sweepId),
                EXISTS (SELECT 1 FROM "canonical_record_id_regression" WHERE "Id" = 1234.50)
            """;
        command.Parameters.AddWithValue("sweepId", result.SweepId);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetString(0).Should().Be("1234.50");
        reader.GetBoolean(1).Should().BeFalse();
    }

    [Fact]
    public async Task Hold_Stores_The_Rows_Canonical_Id_When_The_Request_Spells_It_Differently()
    {
        // numeric without a typmod keeps the stored scale ('1.00'), and citext compares
        // case-insensitively ('ABC' = 'abc'). A hold must protect the row the request resolved to.
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        await using var provider = BuildSpelledServiceProvider(database.ConnectionString);
        await SeedSpelledRecordsAsync(provider, asOf);

        var result = await provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(
            RetentionSweepRequest.Tenantless(asOf)
        );

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().HaveCount(2).And.OnlyContain(count => count.Affected == 0 && count.HeldCount == 1);
        await using var verify = new NpgsqlConnection(database.ConnectionString);
        await verify.OpenAsync();
        await using var command = verify.CreateCommand();
        command.CommandText = """
            SELECT string_agg("RecordId", ',' ORDER BY "RecordId") FROM "retention_holds"
            """;
        (await command.ExecuteScalarAsync()).Should().Be("1.00,ABC");
    }

    [Fact]
    public async Task Deletion_Checks_Numeric_And_Citext_Holds_Through_Their_Own_Store_Types()
    {
        // The deletion's "active holds whose row is missing" check casts each hold's stored text
        // to its entity's key type. A deletion that removes no held row must pass every cast; one
        // that removes the numeric row held as "1.00" (requested as "1"), or the citext row held
        // as "abc" (stored as "ABC"), must be refused.
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        await using var provider = BuildSpelledServiceProvider(database.ConnectionString);
        var (numericHoldId, citextHoldId) = await SeedSpelledRecordsAsync(provider, asOf);
        var unheldTarget = new RetentionTarget(RetentionEntityIdentity.For<NumericKeyedRecord>(), "2", null);

        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<SpelledRecordIdDbContext>();
            var refused = () => scope.ServiceProvider.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
                [unheldTarget],
                ct => db.Set<NumericKeyedRecord>().Where(record => record.Id <= 2m).ExecuteDeleteAsync(ct)
            );

            await refused.Should().ThrowAsync<InvalidOperationException>().WithMessage($"*{numericHoldId}*");
        }

        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<SpelledRecordIdDbContext>();
            var refused = () => scope.ServiceProvider.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
                [unheldTarget],
                ct => db.Set<CitextKeyedRecord>().Where(record => record.Id == "ABC").ExecuteDeleteAsync(ct)
            );

            await refused.Should().ThrowAsync<InvalidOperationException>().WithMessage($"*{citextHoldId}*");
        }

        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<SpelledRecordIdDbContext>();
            var outcome = await scope.ServiceProvider.GetRequiredService<IRetentionDeletion>().ExecuteAsync(
                [unheldTarget],
                ct => db.Set<NumericKeyedRecord>().Where(record => record.Id == 2m).ExecuteDeleteAsync(ct)
            );

            outcome.Should().Be(RetentionDeletionOutcome.Executed);
        }

        await using var verify = new NpgsqlConnection(database.ConnectionString);
        await verify.OpenAsync();
        await using var command = verify.CreateCommand();
        command.CommandText = """
            SELECT string_agg("Id"::text, ',' ORDER BY "Id") FROM "numeric_keyed_record"
            """;
        (await command.ExecuteScalarAsync()).Should().Be("1.00");
        command.CommandText = """
            SELECT count(*) FROM "citext_keyed_record"
            """;
        (await command.ExecuteScalarAsync()).Should().Be(1L);
    }

    [Fact]
    public async Task A_Sweep_In_Small_Batches_Reaches_Every_Row_Whatever_Its_Key_And_Anchor_Types()
    {
        // Each batch continues after the last (anchor, record-id text) the run reached. Numeric
        // ids sort differently as text ("10" before "2"), citext compares case-insensitively,
        // and anchors (a DateTime and a DateTimeOffset, both timestamptz) must be compared as
        // themselves, not through text in the session's time zone (here far from UTC). A cursor
        // that disagreed with discovery's order would skip rows and leave them deferred.
        await using var database = await TemporaryDatabase.CreateAsync(ConnectionString);
        var connectionString = new NpgsqlConnectionStringBuilder(database.ConnectionString) { Timezone = "Pacific/Kiritimati" }
            .ConnectionString;
        var asOf = new DateTimeOffset(2026, 7, 11, 12, 0, 0, TimeSpan.Zero);
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(
            new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?> { [$"{CohortOptions.SectionName}:SweepBatchSize"] = "2" })
                .Build()
        );
        services.AddLogging();
        services.AddDbContext<KeysetDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(new CategoryRepository(Strategy.Purge));
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<KeysetDbContext>();
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        // Three anchors, a few hours apart, each shared by several rows.
        var anchors = Enumerable.Range(0, 3)
            .Select(index => asOf.UtcDateTime.AddDays(-60).AddHours(index * 5).AddTicks(index * 10))
            .ToArray();
        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<KeysetDbContext>();
            await db.Database.EnsureCreatedAsync();
            for (var index = 0; index < 12; index++)
            {
                db.Add(new KeysetNumericRecord { Id = index + 1, CreatedAt = anchors[index % 3] });
                db.Add(new KeysetCitextRecord { Id = $"{(char)((index % 2 == 0 ? 'a' : 'A') + index)}{index}", CreatedOn = new DateTimeOffset(anchors[index % 3]) });
            }
            await db.SaveChangesAsync();
        }

        var result = await provider.GetRequiredService<IRetentionSweep>().ExecuteAsync(
            RetentionSweepRequest.Tenantless(asOf)
        );

        result.EntityFailures.Should().BeEmpty();
        result.Counts.Should().HaveCount(2)
            .And.OnlyContain(count => count.Affected == 12 && count.DeferredCount == 0 && count.BlockedCount == 0);
        await using var verify = new NpgsqlConnection(database.ConnectionString);
        await verify.OpenAsync();
        await using var command = verify.CreateCommand();
        command.CommandText = """
            SELECT (SELECT count(*) FROM "keyset_numeric_record") + (SELECT count(*) FROM "keyset_citext_record")
            """;
        (await command.ExecuteScalarAsync()).Should().Be(0L);
    }

    private static ServiceProvider BuildSpelledServiceProvider(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().AddInMemoryCollection().Build());
        services.AddLogging();
        services.AddDbContext<SpelledRecordIdDbContext>(options => options.UseNpgsql(connectionString));
        services.AddSingleton<IRetentionRuleProvider>(new CategoryRepository(Strategy.Purge));
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<SpelledRecordIdDbContext>();
        return services.BuildServiceProvider(validateScopes: true);
    }

    /// <summary>
    /// Seeds numeric 1.00 held as "1", citext "ABC" held as "abc", and an unheld numeric 2.
    /// </summary>
    private static async Task<(Guid NumericHoldId, Guid CitextHoldId)> SeedSpelledRecordsAsync(
        ServiceProvider provider,
        DateTimeOffset asOf
    )
    {
        var numericHoldId = Guid.NewGuid();
        var citextHoldId = Guid.NewGuid();
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<SpelledRecordIdDbContext>();
        await db.Database.EnsureCreatedAsync();
        db.Add(new NumericKeyedRecord { Id = 1.00m, CreatedAt = asOf.AddDays(-60) });
        db.Add(new NumericKeyedRecord { Id = 2m, CreatedAt = asOf });
        db.Add(new CitextKeyedRecord { Id = "ABC", CreatedAt = asOf.AddDays(-60) });
        await db.SaveChangesAsync();

        var holds = scope.ServiceProvider.GetRequiredService<IRetentionHoldsRepository>();
        await holds.CreateAsync(
            new RetentionHoldRequest(numericHoldId, RetentionEntityIdentity.For<NumericKeyedRecord>(), "1", null, "numeric", asOf),
            CancellationToken.None
        );
        await holds.CreateAsync(
            new RetentionHoldRequest(citextHoldId, RetentionEntityIdentity.For<CitextKeyedRecord>(), "abc", null, "citext", asOf),
            CancellationToken.None
        );
        return (numericHoldId, citextHoldId);
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
        services.AddSingleton<IRetentionRuleProvider>(new CategoryRepository(strategy));
        services.AddSingleton(RetentionExecutionScope.Unisolated);
        services.AddCohort<CanonicalRecordIdDbContext>();
        if (registerHandler)
        {
            services.AddRowHandler<CanonicalRecordIdRecord, CanonicalRecordIdHandler>();
        }

        return services.BuildServiceProvider(validateScopes: true);
    }

    private sealed class CategoryRepository(Strategy strategy) : ITestRetentionRuleProvider
    {
        private readonly ITestRetentionRule resolver = new StaticTestRetentionRule(
            new RetentionRule(new RetentionPeriod(Days: 30), strategy)
        );

        public Task<ITestRetentionRule?> GetAsync(string category, CancellationToken ct) =>
            Task.FromResult<ITestRetentionRule?>(
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

    [RowHandlerId("8c862b29-2d3a-4f4c-91ac-9d0d87373b92")]
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

    private sealed class SpelledRecordIdDbContext(DbContextOptions<SpelledRecordIdDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasPostgresExtension("citext");
            modelBuilder.Entity<NumericKeyedRecord>(entity =>
            {
                entity.ToTable("numeric_keyed_record");
                entity.Property(record => record.Id).HasColumnType("numeric");
            });
            modelBuilder.Entity<CitextKeyedRecord>(entity =>
            {
                entity.ToTable("citext_keyed_record");
                entity.Property(record => record.Id).HasColumnType("citext");
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("canonical-record-id", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000002")]
    [RetentionTenantless]
    private sealed class NumericKeyedRecord
    {
        public decimal Id { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
    }

    [Retain("canonical-record-id", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000003")]
    [RetentionTenantless]
    private sealed class CitextKeyedRecord
    {
        public string Id { get; init; } = "";
        public DateTimeOffset CreatedAt { get; init; }
    }

    private sealed class KeysetDbContext(DbContextOptions<KeysetDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasPostgresExtension("citext");
            modelBuilder.Entity<KeysetNumericRecord>(entity =>
            {
                entity.ToTable("keyset_numeric_record");
                entity.Property(record => record.Id).HasColumnType("numeric").ValueGeneratedNever();
            });
            modelBuilder.Entity<KeysetCitextRecord>(entity =>
            {
                entity.ToTable("keyset_citext_record");
                entity.Property(record => record.Id).HasColumnType("citext");
                // timestamptz(3): Npgsql names the type with its precision.
                entity.Property(record => record.CreatedOn).HasPrecision(3);
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("canonical-record-id", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000004")]
    [RetentionTenantless]
    private sealed class KeysetNumericRecord
    {
        public decimal Id { get; init; }
        public DateTime CreatedAt { get; init; }
    }

    [Retain("canonical-record-id", nameof(CreatedOn))]
    [RetentionEntityId("00000000-0000-0000-0004-000000000005")]
    [RetentionTenantless]
    private sealed class KeysetCitextRecord
    {
        public string Id { get; init; } = "";
        public DateTimeOffset CreatedOn { get; init; }
    }
}
