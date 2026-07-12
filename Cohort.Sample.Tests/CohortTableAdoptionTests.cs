using Cohort.Domain;
using Cohort.Infrastructure;
using Cohort.Infrastructure.Migrations;
using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

// Narrow integration test: table adoption uses Npgsql model metadata but executes no SQL.
public sealed class CohortTableAdoptionTests
{
    private static readonly IReadOnlyList<string> CohortTableNames =
        CohortSchemaContract.TableNames;

    [Fact]
    public void ConfigureCohortTables_Maps_All_Tables_Explicitly_To_Public()
    {
        var options = new DbContextOptionsBuilder<PublicSchemaDbContext>()
            .UseNpgsqlMetadataModel($"public-cohort-schema-{Guid.NewGuid()}")
            .Options;
        using var db = new PublicSchemaDbContext(options);

        db.Model
            .GetEntityTypes()
            .Where(entityType => CohortTableNames.Contains(entityType.GetTableName()))
            .Should()
            .HaveCount(5)
            .And.OnlyContain(entityType => entityType.GetSchema() == "public");
    }

    [Fact]
    public void ConfigureCohortTables_Uses_Contract_Roles_And_Keys_For_Every_Table()
    {
        var options = new DbContextOptionsBuilder<PublicSchemaDbContext>()
            .UseNpgsqlMetadataModel($"contract-cohort-schema-{Guid.NewGuid()}")
            .Options;
        using var db = new PublicSchemaDbContext(options);

        var mapped = db.Model.GetEntityTypes()
            .Where(entityType => entityType[CohortStoreTables.TableRoleAnnotation] is string)
            .ToDictionary(
                entityType => (string)entityType[CohortStoreTables.TableRoleAnnotation]!,
                StringComparer.Ordinal
            );

        mapped.Keys.Should().BeEquivalentTo(CohortSchemaContract.TableNames);
        foreach (var table in CohortSchemaContract.Tables)
        {
            mapped[table.Role].FindPrimaryKey()!.Properties.Select(property => property.Name)
                .Should().Equal(table.PrimaryKey);
        }
    }

    [Fact]
    public void CohortStoreTables_Rejects_A_Model_Without_Cohort_Table_Mappings()
    {
        var options = new DbContextOptionsBuilder<UnconfiguredDbContext>()
            .UseNpgsqlMetadataModel($"unconfigured-cohort-schema-{Guid.NewGuid()}")
            .Options;
        using var db = new UnconfiguredDbContext(options);

        var act = () => CohortStoreTables.FromModel(db.Model);

        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("*ConfigureCohortTables*");
    }

    [Fact]
    public void ConfigureCohortTables_Maps_All_Tables_To_The_Supplied_Schema()
    {
        const string schema = "Cohort schema \"quoted\"";
        var options = new DbContextOptionsBuilder<CustomSchemaDbContext>()
            .UseNpgsqlMetadataModel($"custom-cohort-schema-{Guid.NewGuid()}")
            .Options;
        using var db = new CustomSchemaDbContext(options);

        db.Model
            .GetEntityTypes()
            .Where(entityType => CohortTableNames.Contains(entityType.GetTableName()))
            .Should()
            .HaveCount(5)
            .And.OnlyContain(entityType => entityType.GetSchema() == schema);
    }

    [Fact]
    public void ConfigureCohortTables_Does_Not_Adopt_A_Same_Named_Table_In_Another_Schema()
    {
        var options = new DbContextOptionsBuilder<OtherSchemaCollisionDbContext>()
            .UseNpgsqlMetadataModel($"other-schema-collision-{Guid.NewGuid()}")
            .Options;
        using var db = new OtherSchemaCollisionDbContext(options);

        var mappedSweepRuns = db.Model
            .GetEntityTypes()
            .Where(entityType => entityType.GetTableName() == "sweep_run")
            .ToArray();

        mappedSweepRuns.Should().HaveCount(2);
        mappedSweepRuns.Should().ContainSingle(entityType => entityType.GetSchema() == "host");
        mappedSweepRuns.Should().ContainSingle(entityType => entityType.GetSchema() == "cohort");
    }

    [Fact]
    public void Retained_Table_Identity_Falls_Back_To_Public()
    {
        var options = new DbContextOptionsBuilder<PublicRetainedSchemaDbContext>()
            .UseNpgsqlMetadataModel($"public-retained-schema-{Guid.NewGuid()}")
            .Options;
        using var db = new PublicRetainedSchemaDbContext(options);
        var entityType = db.Model.FindEntityType(typeof(SchemaRecord))!;

        var entry = new RetentionEntryBuilder(new RetentionModelConventions()).TryBuild(entityType);

        entry!.Table.Schema.Should().Be("public");
        entry.Table.Name.Should().Be("schema_records");
    }

    [Fact]
    public void Retained_Table_Identity_Uses_The_Model_Default_Schema()
    {
        var options = new DbContextOptionsBuilder<DefaultRetainedSchemaDbContext>()
            .UseNpgsqlMetadataModel($"default-retained-schema-{Guid.NewGuid()}")
            .Options;
        using var db = new DefaultRetainedSchemaDbContext(options);
        var entityType = db.Model.FindEntityType(typeof(SchemaRecord))!;

        var entry = new RetentionEntryBuilder(new RetentionModelConventions()).TryBuild(entityType);

        entry!.Table.Schema.Should().Be("Host Default \"Schema\"");
        entry.Table.Name.Should().Be("schema_records");
    }

    [Fact]
    public void ConfigureCohortTables_Rejects_Host_Entities_Coincidentally_Mapped_To_Cohort_Table_Names()
    {
        var options = new DbContextOptionsBuilder<RogueSweepRunDbContext>()
            .UseNpgsqlMetadataModel($"rogue-sweep-run-{Guid.NewGuid()}")
            .Options;
        using var db = new RogueSweepRunDbContext(options);

        var act = () => db.Model;

        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("*sweep_run*table-name collision*");
    }

    [Fact]
    public void ConfigureCohortTables_Adopts_Host_Entities_Declaring_The_Expected_Key()
    {
        var options = new DbContextOptionsBuilder<AdoptedSweepRunDbContext>()
            .UseNpgsqlMetadataModel($"adopted-sweep-run-{Guid.NewGuid()}")
            .Options;
        using var db = new AdoptedSweepRunDbContext(options);

        var adopted = db.Model.FindEntityType(typeof(HostSweepRun));

        adopted.Should().NotBeNull();
        adopted!.GetTableName().Should().Be("sweep_run");
        adopted!
            .FindPrimaryKey()!
            .Properties.Select(property => property.Name)
            .Should()
            .Equal("SweepId");
    }

    [Fact]
    public void ConfigureCohortTables_Adopts_Entity_Summary_With_Stable_Identity_Key()
    {
        var options = new DbContextOptionsBuilder<AdoptedSummaryDbContext>()
            .UseNpgsqlMetadataModel($"adopted-summary-{Guid.NewGuid()}")
            .Options;
        using var db = new AdoptedSummaryDbContext(options);

        db.Model
            .FindEntityType(typeof(HostSweepRunEntitySummary))!
            .FindPrimaryKey()!
            .Properties.Select(property => property.Name)
            .Should()
            .Equal("SweepId", "RetentionEntityId", "Category", "TenantId", "Strategy");
    }

    [Fact]
    public void ConfigureCohortTables_Rejects_Entity_Summary_With_Obsolete_Entity_Type_Key()
    {
        var options = new DbContextOptionsBuilder<ObsoleteSummaryDbContext>()
            .UseNpgsqlMetadataModel($"obsolete-summary-{Guid.NewGuid()}")
            .Options;
        using var db = new ObsoleteSummaryDbContext(options);

        var act = () => db.Model;

        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("*sweep_run_entity_summary*table-name collision*");
    }

    [Fact]
    public void ConfigureCohortTables_Uses_Adopted_Row_Detail_As_Handler_Status_Principal()
    {
        var options = new DbContextOptionsBuilder<AdoptedRowDetailDbContext>()
            .UseNpgsqlMetadataModel($"adopted-row-detail-{Guid.NewGuid()}")
            .Options;
        using var db = new AdoptedRowDetailDbContext(options);

        var handlerStatus = db.Model.GetEntityTypes().Single(entityType =>
            entityType.GetTableName() == "sweep_row_handler_status"
        );

        handlerStatus.GetForeignKeys().Single().PrincipalEntityType.ClrType
            .Should().Be(typeof(HostSweepRunRowDetail));
    }

    [Fact]
    public void ConfigureCohortTables_Rejects_A_Host_Handler_Status_Table_Collision()
    {
        var options = new DbContextOptionsBuilder<HandlerStatusCollisionDbContext>()
            .UseNpgsqlMetadataModel($"handler-status-collision-{Guid.NewGuid()}")
            .Options;
        using var db = new HandlerStatusCollisionDbContext(options);

        var act = () => db.Model;

        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("*sweep_row_handler_status*table-name collision*");
    }

    private sealed class RogueSweepRunDbContext(DbContextOptions<RogueSweepRunDbContext> options)
        : DbContext(options)
    {
        public DbSet<RogueSweepRun> SweepRuns => Set<RogueSweepRun>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<RogueSweepRun>(b =>
            {
                b.ToTable("sweep_run");
                b.HasKey(run => run.Id);
            });

            modelBuilder.ConfigureCohortTables();
        }
    }

    public sealed class RogueSweepRun
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class AdoptedSweepRunDbContext(
        DbContextOptions<AdoptedSweepRunDbContext> options
    ) : DbContext(options)
    {
        public DbSet<HostSweepRun> SweepRuns => Set<HostSweepRun>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<HostSweepRun>(b =>
            {
                b.ToTable("sweep_run");
                b.HasKey(run => run.SweepId);
            });

            modelBuilder.ConfigureCohortTables();
        }
    }

    public sealed class HostSweepRun
    {
        public Guid SweepId { get; set; }
        public DateTimeOffset StartedAt { get; set; }
        public int Status { get; set; }
        public DateTimeOffset? SettledAt { get; set; }
        public TimeSpan? Duration { get; set; }
        public int TriggerKind { get; set; }
        public bool DryRun { get; set; }
        public Guid TenantId { get; set; }
        public long? TotalAffected { get; set; }
        public string? Error { get; set; }
    }

    private sealed class AdoptedSummaryDbContext(
        DbContextOptions<AdoptedSummaryDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<HostSweepRunEntitySummary>(builder =>
            {
                builder.ToTable("sweep_run_entity_summary");
                builder.HasKey(
                    summary => new
                    {
                        summary.SweepId,
                        summary.RetentionEntityId,
                        summary.Category,
                        summary.TenantId,
                        summary.Strategy,
                    }
                );
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    private sealed class ObsoleteSummaryDbContext(
        DbContextOptions<ObsoleteSummaryDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<HostSweepRunEntitySummary>(builder =>
            {
                builder.ToTable("sweep_run_entity_summary");
                builder.HasKey(
                    summary => new
                    {
                        summary.SweepId,
                        summary.EntityType,
                        summary.Category,
                        summary.TenantId,
                        summary.Strategy,
                    }
                );
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    public sealed class HostSweepRunEntitySummary
    {
        public Guid SweepId { get; set; }
        public string EntityType { get; set; } = "";
        public Guid RetentionEntityId { get; set; }
        public string Category { get; set; } = "";
        public Guid TenantId { get; set; }
        public int Strategy { get; set; }
        public DateTimeOffset At { get; set; }
        public TimeSpan ResolvedPeriod { get; set; }
        public long Affected { get; set; }
        public long HeldCount { get; set; }
        public long SkippedCount { get; set; }
        public long NullAnchorCount { get; set; }
        public string? RuleSource { get; set; }
        public string? RuleReason { get; set; }
    }

    private sealed class AdoptedRowDetailDbContext(
        DbContextOptions<AdoptedRowDetailDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<HostSweepRunRowDetail>(builder =>
            {
                builder.ToTable("sweep_run_row_detail");
                builder.HasKey(detail => detail.Id);
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    public sealed class HostSweepRunRowDetail
    {
        public long Id { get; set; }
        public Guid SweepId { get; set; }
        public DateTimeOffset At { get; set; }
        public string EntityType { get; set; } = "";
        public Guid RetentionEntityId { get; set; }
        public string RecordId { get; set; } = "";
        public string Category { get; set; } = "";
        public int Strategy { get; set; }
        public Guid TenantId { get; set; }
        public string? CapturedPayload { get; set; }
    }

    private sealed class HandlerStatusCollisionDbContext(
        DbContextOptions<HandlerStatusCollisionDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<HostSweepRowHandlerStatus>(builder =>
            {
                builder.ToTable("sweep_row_handler_status");
                builder.HasKey(status => status.Id);
            });
            modelBuilder.ConfigureCohortTables();
        }
    }

    private sealed class HostSweepRowHandlerStatus
    {
        public long Id { get; set; }
        public long SweepRunRowDetailId { get; set; }
        public string HandlerType { get; set; } = "";
    }

    private sealed class PublicSchemaDbContext(DbContextOptions<PublicSchemaDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
        }
    }

    private sealed class UnconfiguredDbContext(DbContextOptions<UnconfiguredDbContext> options)
        : DbContext(options);

    private sealed class CustomSchemaDbContext(DbContextOptions<CustomSchemaDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables("Cohort schema \"quoted\"");
        }
    }

    private sealed class OtherSchemaCollisionDbContext(
        DbContextOptions<OtherSchemaCollisionDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<RogueSweepRun>(builder =>
            {
                builder.ToTable("sweep_run", "host");
                builder.HasKey(run => run.Id);
            });
            modelBuilder.ConfigureCohortTables("cohort");
        }
    }

    private sealed class PublicRetainedSchemaDbContext(
        DbContextOptions<PublicRetainedSchemaDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SchemaRecord>(builder =>
            {
                builder.ToTable("schema_records");
                builder.HasKey(record => record.Id);
            });
            modelBuilder.ConfigureCohortTables("cohort");
        }
    }

    private sealed class DefaultRetainedSchemaDbContext(
        DbContextOptions<DefaultRetainedSchemaDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasDefaultSchema("Host Default \"Schema\"");
            modelBuilder.Entity<SchemaRecord>(builder =>
            {
                builder.ToTable("schema_records");
                builder.HasKey(record => record.Id);
            });
            modelBuilder.ConfigureCohortTables("cohort");
        }
    }

    [Retain("schema", nameof(CreatedAt))]
    [RetentionEntityId("3a80b886-d4c4-405d-a5ef-f6d604edfe75")]
    private sealed class SchemaRecord
    {
        public Guid Id { get; set; }
        public Guid TenantId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }
}
