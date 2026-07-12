using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;
using Cohort.Sample.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Npgsql;
using System.Collections.Concurrent;

namespace Cohort.Sample.Tests.ComplianceCorpus;

public sealed class RetentionScopeCorpusTests(PostgresFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Annotations_define_the_retention_scope()
    {
        var noteId = Guid.NewGuid();
        var exemptId = Guid.NewGuid();

        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(new Note
            {
                Id = noteId,
                TenantId = Guid.NewGuid(),
                CreatedAt = DateTimeOffset.UtcNow.AddYears(-1),
                Body = "retained",
            });
            db.ExemptDocuments.Add(new ExemptDocument
            {
                Id = exemptId,
                CreatedAt = DateTimeOffset.UtcNow.AddYears(-1),
                Title = "explicitly exempt",
            });
            await db.SaveChangesAsync();
        }

        var entries = await Host.ValidateAndScanAsync();

        entries.Should().ContainKey(typeof(Note));
        entries.Should().NotContainKey(typeof(ExemptDocument));
        entries.Should().NotContainKey(typeof(HeldRecord));
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.AnyAsync(note => note.Id == noteId)).Should().BeTrue();
        (await verify.ExemptDocuments.AnyAsync(document => document.Id == exemptId)).Should().BeTrue();

        await AssertUnannotatedRowsSurvivePublicSweepAsync();
    }

    private async Task AssertUnannotatedRowsSurvivePublicSweepAsync()
    {
        var tenantId = Guid.NewGuid();
        var retainedId = Guid.NewGuid();
        var unannotatedId = Guid.NewGuid();
        var now = new DateTimeOffset(2026, 7, 12, 12, 0, 0, TimeSpan.Zero);
        await ExecuteSqlAsync("""
            CREATE TABLE IF NOT EXISTS "scope_public_rows" (
                "Id" uuid PRIMARY KEY,
                "TenantId" uuid NOT NULL,
                "CreatedAt" timestamp with time zone NOT NULL,
                "Payload" text NOT NULL
            );
            CREATE TABLE IF NOT EXISTS "scope_public_unannotated_rows" (
                "Id" uuid PRIMARY KEY,
                "TenantId" uuid NOT NULL,
                "CreatedAt" timestamp with time zone NOT NULL,
                "Payload" text NOT NULL
            )
            """);

        await using (var db = new PublicScopeDbContext(
            new DbContextOptionsBuilder<PublicScopeDbContext>().UseNpgsql(ConnectionString).Options
        ))
        {
            db.RetainedRows.Add(new PublicRetainedRow
            {
                Id = retainedId, TenantId = tenantId, CreatedAt = now.AddDays(-60), Payload = "mutate",
            });
            db.UnannotatedRows.Add(new PublicUnannotatedRow
            {
                Id = unannotatedId, TenantId = tenantId, CreatedAt = now.AddDays(-60), Payload = "survive",
            });
            await db.SaveChangesAsync();
        }

        using var host = BuildHost<PublicScopeDbContext>(new RecordingObserver());
        var sweep = host.Services.GetRequiredService<IRetentionSweep>();
        var result = await sweep.SweepAsync(
            new TenantContext(tenantId, "uk", new Dictionary<string, string>()), now
        );

        result.Counts.Should().ContainSingle(count =>
            count.EntityType == typeof(PublicRetainedRow) && count.Affected == 1
        );
        result.Counts.Should().NotContain(count => count.EntityType == typeof(PublicUnannotatedRow));
        await using var verify = new PublicScopeDbContext(
            new DbContextOptionsBuilder<PublicScopeDbContext>().UseNpgsql(ConnectionString).Options
        );
        (await verify.RetainedRows.AnyAsync(row => row.Id == retainedId)).Should().BeFalse();
        var survivor = await verify.UnannotatedRows.SingleAsync(row => row.Id == unannotatedId);
        (survivor.Id, survivor.TenantId, survivor.CreatedAt, survivor.Payload).Should().Be(
            (unannotatedId, tenantId, now.AddDays(-60), "survive")
        );
    }

    [Fact]
    public async Task Conflicting_annotations_fail_validation_before_mutation()
    {
        await AssertHostedAndPublicOperationFailBeforeMutationAsync<ConflictingAnnotationDbContext>(
            "scope_conflicting_rows",
            "*must declare exactly one of [Retain] or [ExemptFromRetention], not both*",
            """
            CREATE TABLE IF NOT EXISTS "scope_conflicting_rows" (
                "Id" uuid PRIMARY KEY,
                "TenantId" uuid NOT NULL,
                "CreatedAt" timestamp with time zone NOT NULL
            )
            """
        );
    }

    [Fact]
    public async Task Unsupported_relational_mappings_fail_validation_before_mutation()
    {
        await AssertHostedAndPublicOperationFailBeforeMutationAsync<OwnedMappingDbContext>(
            "scope_owned_rows",
            "*owned entity type*",
            """
            CREATE TABLE IF NOT EXISTS "scope_owned_containers" (
                "Id" uuid PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS "scope_owned_rows" (
                "OwnedContainerId" uuid PRIMARY KEY,
                "TenantId" uuid NOT NULL,
                "CreatedAt" timestamp with time zone NOT NULL
            )
            """,
            "OwnedContainerId"
        );
        await AssertHostedAndPublicOperationFailBeforeMutationAsync<SharedTableMappingDbContext>(
            "scope_shared_rows",
            "*shares table*",
            """
            CREATE TABLE IF NOT EXISTS "scope_shared_rows" (
                "Id" uuid PRIMARY KEY,
                "TenantId" uuid NOT NULL,
                "CreatedAt" timestamp with time zone NOT NULL,
                "Payload" text NULL
            )
            """
        );
        await AssertHostedAndPublicOperationFailBeforeMutationAsync<SplitTableMappingDbContext>(
            "scope_split_rows",
            "*mapped to multiple tables*",
            """
            CREATE TABLE IF NOT EXISTS "scope_split_rows" (
                "Id" uuid PRIMARY KEY,
                "TenantId" uuid NOT NULL,
                "CreatedAt" timestamp with time zone NOT NULL
            );
            CREATE TABLE IF NOT EXISTS "scope_split_payloads" (
                "Id" uuid PRIMARY KEY,
                "Payload" text NOT NULL
            )
            """
        );
        await AssertHostedAndPublicOperationFailBeforeMutationAsync<InheritanceMappingDbContext>(
            "scope_inheritance_rows",
            "*participates in an EF inheritance hierarchy*",
            """
            CREATE TABLE IF NOT EXISTS "scope_inheritance_rows" (
                "Id" uuid PRIMARY KEY,
                "TenantId" uuid NOT NULL,
                "CreatedAt" timestamp with time zone NOT NULL,
                "Discriminator" text NOT NULL DEFAULT 'InheritanceRetainedRow',
                "Extra" text NULL
            )
            """
        );
    }

    private async Task AssertHostedAndPublicOperationFailBeforeMutationAsync<TContext>(
        string table,
        string message,
        string createSql,
        string idColumn = "Id"
    )
        where TContext : DbContext
    {
        var recordId = Guid.NewGuid();
        var tenantId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(2025, 7, 12, 12, 0, 0, TimeSpan.Zero);
        await ExecuteSqlAsync(createSql);
        await ExecuteSqlAsync(
            $"INSERT INTO \"{table}\" (\"{idColumn}\", \"TenantId\", \"CreatedAt\") VALUES (@id, @tenantId, @createdAt)",
            recordId,
            tenantId,
            createdAt
        );
        var sourceBefore = await ReadRowJsonAsync(table, idColumn, recordId);
        var auditBefore = await ReadAuditStateAsync();
        var observer = new RecordingObserver();

        using (var host = BuildHost<TContext>(observer))
        {
            var start = () => host.StartAsync();
            await start.Should().ThrowAsync<RetentionConfigurationException>().WithMessage(message);
        }
        (await ReadRowJsonAsync(table, idColumn, recordId)).Should().Be(sourceBefore);
        (await ReadAuditStateAsync()).Should().Be(auditBefore);
        observer.Events.Should().BeEmpty();

        using (var host = BuildHost<TContext>(observer))
        {
            var sweep = host.Services.GetRequiredService<IRetentionSweep>();
            var execute = () => sweep.SweepAsync(
                new TenantContext(tenantId, "uk", new Dictionary<string, string>()),
                DateTimeOffset.UtcNow
            );
            await execute.Should().ThrowAsync<RetentionConfigurationException>().WithMessage(message);
        }
        (await ReadRowJsonAsync(table, idColumn, recordId)).Should().Be(sourceBefore);
        (await ReadAuditStateAsync()).Should().Be(auditBefore);
        observer.Events.Should().BeEmpty();
    }

    private IHost BuildHost<TContext>(IRetentionAuditObserver observer) where TContext : DbContext
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TContext>(options => options.UseNpgsql(ConnectionString));
        builder.Services.AddSingleton<IRetentionRuleProvider, ScopeRuleProvider>();
        builder.Services.AddSingleton<IRetentionAuditObserver>(observer);
        builder.Services.AddCohort<TContext>();
        return builder.Build();
    }

    private async Task ExecuteSqlAsync(
        string sql,
        Guid? recordId = null,
        Guid? tenantId = null,
        DateTimeOffset? createdAt = null
    )
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (recordId is not null)
        {
            command.Parameters.AddWithValue("id", recordId.Value);
            command.Parameters.AddWithValue("tenantId", tenantId!.Value);
            command.Parameters.AddWithValue("createdAt", createdAt!.Value);
        }
        await command.ExecuteNonQueryAsync();
    }

    private async Task<string> ReadRowJsonAsync(
        string table,
        string idColumn,
        Guid recordId
    )
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT to_jsonb(row)::text FROM \"{table}\" row WHERE \"{idColumn}\" = @id";
        command.Parameters.AddWithValue("id", recordId);
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

    private sealed class PublicScopeDbContext(DbContextOptions<PublicScopeDbContext> options)
        : DbContext(options)
    {
        public DbSet<PublicRetainedRow> RetainedRows => Set<PublicRetainedRow>();
        public DbSet<PublicUnannotatedRow> UnannotatedRows => Set<PublicUnannotatedRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PublicRetainedRow>().ToTable("scope_public_rows");
            modelBuilder.Entity<PublicUnannotatedRow>().ToTable("scope_public_unannotated_rows");
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("scope-public", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0005-000000000006")]
    private sealed class PublicRetainedRow
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public string Payload { get; set; } = string.Empty;
    }

    private sealed class PublicUnannotatedRow
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public string Payload { get; set; } = string.Empty;
    }

    private sealed class ConflictingAnnotationDbContext(
        DbContextOptions<ConflictingAnnotationDbContext> options
    ) : DbContext(options)
    {
        public DbSet<ConflictingAnnotationRow> ConflictingRows => Set<ConflictingAnnotationRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ConflictingAnnotationRow>().ToTable("scope_conflicting_rows");
            modelBuilder.ConfigureCohortTables();
        }
    }

    private sealed class OwnedMappingDbContext(DbContextOptions<OwnedMappingDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<OwnedContainer>(builder =>
            {
                builder.ToTable("scope_owned_containers");
                builder.HasKey(row => row.Id);
                builder.OwnsOne(row => row.Retained, owned =>
                {
                    owned.ToTable("scope_owned_rows");
                    owned.Property(row => row.TenantId);
                    owned.Property(row => row.CreatedAt);
                });
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    private sealed class SharedTableMappingDbContext(
        DbContextOptions<SharedTableMappingDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SharedRetainedRow>(builder =>
            {
                builder.ToTable("scope_shared_rows");
                builder.HasKey(row => row.Id);
                builder.HasOne<SharedCompanion>()
                    .WithOne()
                    .HasForeignKey<SharedCompanion>(row => row.Id);
            });
            modelBuilder.Entity<SharedCompanion>(builder =>
            {
                builder.ToTable("scope_shared_rows");
                builder.HasKey(row => row.Id);
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    private sealed class SplitTableMappingDbContext(
        DbContextOptions<SplitTableMappingDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SplitRetainedRow>(builder =>
            {
                builder.ToTable("scope_split_rows");
                builder.HasKey(row => row.Id);
                builder.SplitToTable(
                    "scope_split_payloads",
                    split => split.Property(row => row.Payload)
                );
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    private sealed class InheritanceMappingDbContext(
        DbContextOptions<InheritanceMappingDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<InheritanceRetainedRow>(builder =>
            {
                builder.ToTable("scope_inheritance_rows");
                builder.HasKey(row => row.Id);
            });
            modelBuilder.Entity<InheritanceDerivedRow>();
            modelBuilder.ConfigureCohortTables();
        }
    }

    [Retain("conflict", nameof(CreatedAt))]
    [ExemptFromRetention("conflicting fixture")]
    [RetentionEntityId("00000000-0000-0000-0005-000000000001")]
    private sealed class ConflictingAnnotationRow
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }

    private sealed class OwnedContainer
    {
        public Guid Id { get; set; }
        public OwnedRetainedRow Retained { get; set; } = new();
    }

    [Retain("unsupported-shape", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0005-000000000002")]
    private sealed class OwnedRetainedRow
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }

    [Retain("unsupported-shape", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0005-000000000003")]
    private sealed class SharedRetainedRow
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }

    private sealed class SharedCompanion
    {
        public Guid Id { get; set; }
        public string Payload { get; set; } = string.Empty;
    }

    [Retain("unsupported-shape", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0005-000000000004")]
    private sealed class SplitRetainedRow
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public string Payload { get; set; } = string.Empty;
    }

    [Retain("unsupported-shape", nameof(CreatedAt))]
    [RetentionEntityId("00000000-0000-0000-0005-000000000005")]
    private class InheritanceRetainedRow
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }

    private sealed class InheritanceDerivedRow : InheritanceRetainedRow
    {
        public string Extra { get; set; } = string.Empty;
    }

    private sealed class ScopeRuleProvider : IRetentionRuleProvider
    {
        public RetentionCategoryCapabilities? GetCapabilities(string category) =>
            new([Strategy.Purge]);

        public Task<RetentionRule?> ResolveAsync(
            RetentionResolutionContext context,
            CancellationToken ct
        ) => Task.FromResult<RetentionRule?>(
            new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)
        );
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
