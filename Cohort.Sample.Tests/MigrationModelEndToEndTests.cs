using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Cohort.Sample.Tests;

public sealed class MigrationModelEndToEndTests(PostgresFixture fixture)
    : IntegrationTestBase(fixture)
{
    [Fact]
    public void Sample_Model_Contains_Cohort_Audit_And_Hold_Tables()
    {
        using var db = Host.CreateDbContext();
        var tables = db.Model.GetEntityTypes()
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

        var sweepRunEntity = db.Model.GetEntityTypes().Single(entityType =>
            string.Equals(entityType.GetTableName(), "sweep_run", StringComparison.Ordinal)
        );
        sweepRunEntity.FindProperty("TriggerKind").Should().NotBeNull();

        var rowDetailEntity = db.Model.GetEntityTypes().Single(entityType =>
            string.Equals(entityType.GetTableName(), "sweep_run_row_detail", StringComparison.Ordinal)
        );
        rowDetailEntity.FindPrimaryKey()!.Properties.Select(property => property.Name).Should().Equal("Id");
        rowDetailEntity.FindProperty("Id")!.ValueGenerated.Should().Be(ValueGenerated.OnAdd);
        rowDetailEntity.FindProperty("CapturedPayload")!.IsNullable.Should().BeTrue();
        rowDetailEntity.GetIndexes().Any(index =>
            index.IsUnique
            && index.Properties.Select(property => property.Name).SequenceEqual(
                ["SweepId", "EntityType", "EntityId", "Category", "Strategy", "TenantId"]
            )
        ).Should().BeTrue();

        var handlerStatusEntity = db.Model.GetEntityTypes().Single(entityType =>
            string.Equals(entityType.GetTableName(), "sweep_row_handler_status", StringComparison.Ordinal)
        );
        handlerStatusEntity.FindPrimaryKey()!.Properties.Select(property => property.Name).Should().Equal("Id");
        handlerStatusEntity.FindProperty("SweepRunRowDetailId")!.IsNullable.Should().BeFalse();
        handlerStatusEntity.FindProperty("HandlerType")!.IsNullable.Should().BeFalse();
        handlerStatusEntity.FindProperty("State")!.IsNullable.Should().BeFalse();
        handlerStatusEntity.FindProperty("Attempt")!.IsNullable.Should().BeFalse();
        handlerStatusEntity.FindProperty("QueuedAt")!.IsNullable.Should().BeFalse();
        handlerStatusEntity.FindProperty("NextAttemptAt")!.IsNullable.Should().BeFalse();
        handlerStatusEntity.FindProperty("ClaimedAt")!.IsNullable.Should().BeTrue();
        handlerStatusEntity.FindProperty("CompletedAt")!.IsNullable.Should().BeTrue();
        handlerStatusEntity.FindProperty("LastError")!.IsNullable.Should().BeTrue();

        var rowDetailForeignKey = handlerStatusEntity.GetForeignKeys().Single();
        rowDetailForeignKey.PrincipalEntityType.GetTableName().Should().Be("sweep_run_row_detail");
        rowDetailForeignKey.PrincipalKey.Properties.Select(property => property.Name).Should().Equal("Id");
        rowDetailForeignKey.Properties.Select(property => property.Name).Should().Equal("SweepRunRowDetailId");

        handlerStatusEntity.GetIndexes().Any(index =>
            index.IsUnique
            && index.Properties.Select(property => property.Name).SequenceEqual(
                ["SweepRunRowDetailId", "HandlerType"]
            )
        ).Should().BeTrue();
        handlerStatusEntity.GetIndexes().Any(index =>
            !index.IsUnique
            && index.Properties.Select(property => property.Name).SequenceEqual(
                ["State", "NextAttemptAt", "Id"]
            )
        ).Should().BeTrue();
    }
}
