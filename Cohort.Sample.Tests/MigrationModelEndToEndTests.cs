using Cohort.Infrastructure;
using Cohort.Infrastructure.Migrations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Text.RegularExpressions;

namespace Cohort.Sample.Tests;

public sealed class MigrationModelEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public void Schema_Shape_Inventory_Is_Owned_Only_By_CohortSchemaContract()
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (root.GetFiles("Cohort.slnx").Length == 0)
        {
            root = root.Parent
                ?? throw new InvalidOperationException("Could not locate the Cohort solution root.");
        }

        string[] shapeConsumers =
        [
            "Cohort/Infrastructure/Migrations/CohortModelBuilder.cs",
            "Cohort/Infrastructure/CohortSchemaValidator.cs",
        ];
        foreach (var consumer in shapeConsumers)
        {
            var source = File.ReadAllText(Path.Combine(root.FullName, consumer));
            source.Should().Contain("CohortSchemaContract.Tables");
        }

        var contractPath = Path.GetFullPath(Path.Combine(
            root.FullName,
            "Cohort/Infrastructure/Migrations/CohortSchemaContract.cs"
        ));
        var descriptorConstruction = new Regex(
            @"\bnew\s*(?:CohortSchemaContract\.)?(?:Table|Column|Index|CheckConstraint|ForeignKey)Requirement\s*\(",
            RegexOptions.CultureInvariant
        );
        var inventoryConstruction = new Regex(
            """(?:\[|new\s*(?:string|List<string>|HashSet<string>)\s*\{)[^\]}]*(?:CohortTableNames\.(?:RetentionHolds|SweepRun|SweepRunEntitySummary|SweepRunRowDetail|SweepRowHandlerStatus)|"(?:retention_holds|sweep_run|sweep_run_entity_summary|sweep_run_row_detail|sweep_row_handler_status)")[^\]}]*(?:CohortTableNames\.(?:RetentionHolds|SweepRun|SweepRunEntitySummary|SweepRunRowDetail|SweepRowHandlerStatus)|"(?:retention_holds|sweep_run|sweep_run_entity_summary|sweep_run_row_detail|sweep_row_handler_status)")""",
            RegexOptions.CultureInvariant | RegexOptions.Singleline
        );

        Directory.EnumerateFiles(Path.Combine(root.FullName, "Cohort"), "*.cs", SearchOption.AllDirectories)
            .Where(path => !string.Equals(Path.GetFullPath(path), contractPath, StringComparison.Ordinal))
            .Select(path => (Path: Path.GetRelativePath(root.FullName, path), Source: File.ReadAllText(path)))
            .Should().OnlyContain(file =>
                !descriptorConstruction.IsMatch(file.Source)
                && !inventoryConstruction.IsMatch(file.Source),
                "schema requirement descriptors and table-role inventories must only be constructed by CohortSchemaContract"
            );
    }

    [Fact]
    public void Finalized_Cohort_Model_Matches_The_Schema_Contract()
    {
        using var db = Host.CreateDbContext();
        var model = db.GetService<IDesignTimeModel>().Model;
        var mapped = model.GetEntityTypes()
            .Where(entityType => entityType[CohortStoreTables.TableRoleAnnotation] is string)
            .ToDictionary(
                entityType => (string)entityType[CohortStoreTables.TableRoleAnnotation]!,
                StringComparer.Ordinal
            );

        mapped.Keys.Should().BeEquivalentTo(CohortSchemaContract.TableNames);
        foreach (var table in CohortSchemaContract.Tables)
        {
            var entityType = mapped[table.Role];
            entityType.GetTableName().Should().Be(table.Name);
            entityType.GetSchema().Should().Be("public");
            entityType.GetProperties().Should().HaveCount(table.Columns.Count);

            foreach (var column in table.Columns)
            {
                var property = entityType.FindProperty(column.Name);
                property.Should().NotBeNull($"{table.Name}.{column.Name} is required");
                property!.ClrType.Should().Be(column.ClrType);
                property.IsNullable.Should().Be(column.Nullable);
                property.GetColumnType().Should().Be(column.StoreType);
                property.ValueGenerated.Should().Be(
                    column.Generated ? ValueGenerated.OnAdd : ValueGenerated.Never
                );
            }

            entityType.FindPrimaryKey()!.Properties.Select(property => property.Name)
                .Should().Equal(table.PrimaryKey);

            foreach (var requiredIndex in table.RequiredIndexes)
            {
                var index = entityType.GetIndexes().SingleOrDefault(candidate =>
                    candidate.Properties.Select(property => property.Name)
                        .SequenceEqual(requiredIndex.Columns)
                );
                index.Should().NotBeNull(
                    $"{table.Name} requires index ({string.Join(", ", requiredIndex.Columns)})"
                );
                index!.IsUnique.Should().Be(requiredIndex.Unique);
                index.GetFilter().Should().Be(requiredIndex.Predicate);
                if (requiredIndex.Name is not null)
                {
                    index.GetDatabaseName().Should().Be(requiredIndex.Name);
                }
            }

            entityType.GetCheckConstraints()
                .Select(check => (check.Name, check.Sql))
                .Should().BeEquivalentTo(
                    table.RequiredChecks.Select(check => (check.Name, check.Sql))
                );

            foreach (var requiredForeignKey in table.RequiredForeignKeys)
            {
                var foreignKey = entityType.GetForeignKeys().Single(candidate =>
                    candidate.Properties.Select(property => property.Name)
                        .SequenceEqual(requiredForeignKey.Columns)
                );
                foreignKey.PrincipalEntityType.GetTableName()
                    .Should().Be(requiredForeignKey.PrincipalTable);
                foreignKey.PrincipalKey.Properties.Select(property => property.Name)
                    .Should().Equal(requiredForeignKey.PrincipalColumns);
                foreignKey.DeleteBehavior.Should().Be(requiredForeignKey.DeleteAction switch
                {
                    CohortSchemaContract.ForeignKeyDeleteAction.Restrict => DeleteBehavior.Restrict,
                    CohortSchemaContract.ForeignKeyDeleteAction.Cascade => DeleteBehavior.Cascade,
                    _ => throw new ArgumentOutOfRangeException(),
                });
            }
        }
    }

    [Fact]
    public void Sample_Model_Contains_Cohort_Audit_And_Hold_Tables()
    {
        using var db = Host.CreateDbContext();
        var tables = db
            .Model.GetEntityTypes()
            .Select(entityType => entityType.GetTableName())
            .Where(tableName => tableName is not null)
            .Cast<string>()
            .ToHashSet(StringComparer.Ordinal);

        tables.Should().Contain("retention_holds");
        tables.Should().Contain("sweep_run");
        tables.Should().Contain("sweep_run_entity_summary");
        tables.Should().Contain("sweep_run_row_detail");
        tables.Should().Contain("sweep_row_handler_status");
        tables.Should().Contain("erasure_subject_records");

        var sweepRunEntity = db
            .Model.GetEntityTypes()
            .Single(entityType =>
                string.Equals(entityType.GetTableName(), "sweep_run", StringComparison.Ordinal)
            );
        sweepRunEntity.FindProperty("TriggerKind").Should().NotBeNull();

        var entitySummaryEntity = db
            .Model.GetEntityTypes()
            .Single(entityType =>
                string.Equals(
                    entityType.GetTableName(),
                    "sweep_run_entity_summary",
                    StringComparison.Ordinal
                )
            );
        entitySummaryEntity.FindProperty("SkippedCount")!.IsNullable.Should().BeFalse();
        entitySummaryEntity.FindProperty("RuleSource")!.IsNullable.Should().BeTrue();
        entitySummaryEntity.FindProperty("RuleReason")!.IsNullable.Should().BeTrue();
        entitySummaryEntity
            .FindPrimaryKey()!
            .Properties.Select(property => property.Name)
            .Should()
            .Equal("SweepId", "RetentionEntityId", "Category", "TenantId", "Strategy");

        var rowDetailEntity = db
            .Model.GetEntityTypes()
            .Single(entityType =>
                string.Equals(
                    entityType.GetTableName(),
                    "sweep_run_row_detail",
                    StringComparison.Ordinal
                )
            );
        rowDetailEntity
            .FindPrimaryKey()!
            .Properties.Select(property => property.Name)
            .Should()
            .Equal("Id");
        rowDetailEntity.FindProperty("Id")!.ValueGenerated.Should().Be(ValueGenerated.OnAdd);
        rowDetailEntity.FindProperty("SweepId")!.IsNullable.Should().BeFalse();
        rowDetailEntity.FindProperty("TenantId")!.IsNullable.Should().BeFalse();
        rowDetailEntity.FindProperty("CapturedPayload")!.IsNullable.Should().BeTrue();
        rowDetailEntity.FindProperty("RetentionEntityId")!.IsNullable.Should().BeFalse();
        rowDetailEntity.FindProperty("RecordId")!.IsNullable.Should().BeFalse();
        rowDetailEntity.FindProperty("RuleSource").Should().BeNull();
        rowDetailEntity.FindProperty("RuleReason").Should().BeNull();
        rowDetailEntity
            .GetIndexes()
            .Any(index =>
                !index.IsUnique
                && index.Properties.Select(property => property.Name).SequenceEqual(["SweepId"])
            )
            .Should()
            .BeTrue();
        rowDetailEntity
            .GetIndexes()
            .Any(index =>
                index.IsUnique
                && index
                    .Properties.Select(property => property.Name)
                    .SequenceEqual([
                        "SweepId",
                        "RetentionEntityId",
                        "RecordId",
                        "Category",
                        "Strategy",
                        "TenantId",
                    ])
            )
            .Should()
            .BeTrue();

        var handlerStatusEntity = db
            .Model.GetEntityTypes()
            .Single(entityType =>
                string.Equals(
                    entityType.GetTableName(),
                    "sweep_row_handler_status",
                    StringComparison.Ordinal
                )
            );
        handlerStatusEntity
            .FindPrimaryKey()!
            .Properties.Select(property => property.Name)
            .Should()
            .Equal("Id");
        handlerStatusEntity.FindProperty("Id")!.ValueGenerated.Should().Be(ValueGenerated.OnAdd);
        handlerStatusEntity.FindProperty("SweepRunRowDetailId")!.IsNullable.Should().BeFalse();
        handlerStatusEntity.FindProperty("HandlerType")!.IsNullable.Should().BeFalse();
        handlerStatusEntity.FindProperty("DispatchPhase")!.IsNullable.Should().BeFalse();
        handlerStatusEntity.FindProperty("State")!.IsNullable.Should().BeFalse();
        handlerStatusEntity.FindProperty("Attempt")!.IsNullable.Should().BeFalse();
        handlerStatusEntity.FindProperty("QueuedAt")!.IsNullable.Should().BeFalse();
        handlerStatusEntity.FindProperty("NextAttemptAt")!.IsNullable.Should().BeFalse();
        handlerStatusEntity.FindProperty("ClaimedAt")!.IsNullable.Should().BeTrue();
        handlerStatusEntity.FindProperty("CompletedAt")!.IsNullable.Should().BeTrue();
        handlerStatusEntity.FindProperty("LastError")!.IsNullable.Should().BeTrue();

        var rowDetailForeignKey = handlerStatusEntity.GetForeignKeys().Single();
        rowDetailForeignKey.PrincipalEntityType.GetTableName().Should().Be("sweep_run_row_detail");
        rowDetailForeignKey
            .PrincipalKey.Properties.Select(property => property.Name)
            .Should()
            .Equal("Id");
        rowDetailForeignKey
            .Properties.Select(property => property.Name)
            .Should()
            .Equal("SweepRunRowDetailId");
        rowDetailForeignKey.IsRequired.Should().BeTrue();
        rowDetailForeignKey.DeleteBehavior.Should().Be(DeleteBehavior.Cascade);

        handlerStatusEntity
            .GetIndexes()
            .Any(index =>
                index.IsUnique
                && index
                    .Properties.Select(property => property.Name)
                    .SequenceEqual(["SweepRunRowDetailId", "HandlerType"])
            )
            .Should()
            .BeTrue();
        handlerStatusEntity
            .GetIndexes()
            .Any(index =>
                !index.IsUnique
                && index
                    .Properties.Select(property => property.Name)
                    .SequenceEqual(["State", "NextAttemptAt", "Id"])
            )
            .Should()
            .BeTrue();
    }
}
