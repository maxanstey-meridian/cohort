using Cohort.Infrastructure;
using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;

namespace Cohort.Sample.Tests;

// Narrow integration test: CohortStoreTables reads the model's table-role annotations and the
// Npgsql model metadata serves them, with no connection opened.
public sealed class CohortStoreTablesTests
{
    [Fact]
    public void FromModel_Resolves_Each_Role_To_Its_Mapped_Table()
    {
        using var db = new CohortTablesDbContext(Options<CohortTablesDbContext>("resolves"));

        var tables = CohortStoreTables.FromModel(db.Model);

        tables
            .Should()
            .Be(
                new CohortStoreTables(
                    new RelationalObjectName("public", "retention_holds"),
                    new RelationalObjectName("public", "sweep_run"),
                    new RelationalObjectName("public", "sweep_run_entity_summary"),
                    new RelationalObjectName("public", "sweep_run_row_detail")
                )
            );
    }

    [Fact]
    public void FromModel_Names_The_Role_That_Two_Entity_Types_Carry()
    {
        using var db = new DuplicatedRoleDbContext(Options<DuplicatedRoleDbContext>("duplicated"));

        var act = () => CohortStoreTables.FromModel(db.Model);

        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("*partial Cohort table mapping*'sweep_run' is carried by 2 entity types*");
    }

    [Fact]
    public void FromModel_Resolves_A_Table_That_Takes_The_Models_Default_Schema()
    {
        using var db = new DefaultSchemaDbContext(Options<DefaultSchemaDbContext>("default-schema"));

        var tables = CohortStoreTables.FromModel(db.Model);

        tables
            .Should()
            .Be(
                new CohortStoreTables(
                    new RelationalObjectName("hosts", "retention_holds"),
                    new RelationalObjectName("hosts", "sweep_run"),
                    new RelationalObjectName("hosts", "sweep_run_entity_summary"),
                    new RelationalObjectName("hosts", "sweep_run_row_detail")
                )
            );
    }

    [Fact]
    public void ConfigureCohortTables_Finds_A_Host_Table_That_Takes_The_Models_Default_Schema()
    {
        using var db = new DefaultSchemaHostTableDbContext(
            Options<DefaultSchemaHostTableDbContext>("default-schema-host")
        );

        var act = () => db.Model;

        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("*sweep_run*table-name collision*");
    }

    [Fact]
    public void FromModel_Reports_A_Model_With_No_Cohort_Tables()
    {
        using var db = new NoCohortTablesDbContext(Options<NoCohortTablesDbContext>("none"));

        var act = () => CohortStoreTables.FromModel(db.Model);

        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("*no Cohort table mappings*");
    }

    private static DbContextOptions<TContext> Options<TContext>(string name)
        where TContext : DbContext =>
        new DbContextOptionsBuilder<TContext>()
            .UseNpgsqlMetadataModel($"cohort-store-tables-{name}-{Guid.NewGuid()}")
            .Options;

    private sealed class CohortTablesDbContext(DbContextOptions<CohortTablesDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.ConfigureCohortTables();
    }

    // Four entity types carry a role, so the count of four holds; sweep_run_row_detail is left
    // with none and sweep_run with two.
    private sealed class DuplicatedRoleDbContext(DbContextOptions<DuplicatedRoleDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureCohortTables();
            modelBuilder
                .Model.GetEntityTypes()
                .Single(entityType => entityType.GetTableName() == CohortTableNames.SweepRunRowDetail)
                .SetAnnotation(CohortStoreTables.TableRoleAnnotation, CohortTableNames.SweepRun);
        }
    }

    // The four tables are mapped by hand with no schema of their own, so each one's schema is the
    // model's default.
    private sealed class DefaultSchemaDbContext(DbContextOptions<DefaultSchemaDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasDefaultSchema("hosts");
            foreach (var role in CohortSchemaContract.TableNames)
            {
                var entity = modelBuilder.SharedTypeEntity<Dictionary<string, object>>(role);
                entity.IndexerProperty<Guid>("Id");
                entity.HasKey("Id");
                entity.ToTable(role);
                entity.Metadata.SetAnnotation(CohortStoreTables.TableRoleAnnotation, role);
            }
        }
    }

    // A host entity mapped to a Cohort table name with no schema of its own sits in the default
    // schema, so ConfigureCohortTables("hosts") finds it and refuses its key as a name collision.
    private sealed class DefaultSchemaHostTableDbContext(
        DbContextOptions<DefaultSchemaHostTableDbContext> options
    ) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasDefaultSchema("hosts");
            modelBuilder.Entity<HostRun>().ToTable(CohortTableNames.SweepRun).HasKey(row => row.Id);
            modelBuilder.ConfigureCohortTables("hosts");
        }
    }

    public sealed class HostRun
    {
        public Guid Id { get; set; }
    }

    private sealed class NoCohortTablesDbContext(DbContextOptions<NoCohortTablesDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.Entity<Unrelated>().ToTable("unrelated").HasKey(row => row.Id);
    }

    public sealed class Unrelated
    {
        public Guid Id { get; set; }
    }
}
