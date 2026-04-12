using Microsoft.EntityFrameworkCore;

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
        tables.Should().Contain("erasure_subject_records");

        var sweepRunEntity = db.Model.GetEntityTypes().Single(entityType =>
            string.Equals(entityType.GetTableName(), "sweep_run", StringComparison.Ordinal)
        );
        sweepRunEntity.FindProperty("TriggerKind").Should().NotBeNull();
    }
}
