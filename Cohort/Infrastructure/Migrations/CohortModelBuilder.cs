#nullable enable

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Cohort.Infrastructure.Migrations;

public static class CohortModelBuilder
{
    public static ModelBuilder ConfigureCohortTables(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        ConfigureHoldTable(modelBuilder);
        ConfigureSweepRunTable(modelBuilder);
        ConfigureSweepRunEntitySummaryTable(modelBuilder);
        ConfigureSweepRunRowDetailTable(modelBuilder);

        return modelBuilder;
    }

    private static void ConfigureHoldTable(ModelBuilder modelBuilder)
    {
        if (TryFindEntityMappedToTable(modelBuilder, CohortTableNames.RetentionHolds) is { } existing)
        {
            ConfigureRetentionHoldColumns(modelBuilder.Entity(existing.ClrType));
            return;
        }

        modelBuilder.SharedTypeEntity<Dictionary<string, object>>(
            CohortSharedTypeNames.RetentionHold,
            builder =>
            {
                builder.ToTable(CohortTableNames.RetentionHolds);
                ConfigureRetentionHoldColumns(builder);
            }
        );
    }

    private static void ConfigureSweepRunTable(ModelBuilder modelBuilder)
    {
        if (TryFindEntityMappedToTable(modelBuilder, CohortTableNames.SweepRun) is { } existing)
        {
            ConfigureSweepRunColumns(modelBuilder.Entity(existing.ClrType));
            return;
        }

        modelBuilder.SharedTypeEntity<Dictionary<string, object>>(
            CohortSharedTypeNames.SweepRun,
            builder =>
            {
                builder.ToTable(CohortTableNames.SweepRun);
                ConfigureSweepRunColumns(builder);
            }
        );
    }

    private static void ConfigureSweepRunEntitySummaryTable(ModelBuilder modelBuilder)
    {
        if (
            TryFindEntityMappedToTable(modelBuilder, CohortTableNames.SweepRunEntitySummary) is { } existing
        )
        {
            ConfigureSweepRunEntitySummaryColumns(modelBuilder.Entity(existing.ClrType));
            return;
        }

        modelBuilder.SharedTypeEntity<Dictionary<string, object>>(
            CohortSharedTypeNames.SweepRunEntitySummary,
            builder =>
            {
                builder.ToTable(CohortTableNames.SweepRunEntitySummary);
                ConfigureSweepRunEntitySummaryColumns(builder);
            }
        );
    }

    private static void ConfigureSweepRunRowDetailTable(ModelBuilder modelBuilder)
    {
        if (
            TryFindEntityMappedToTable(modelBuilder, CohortTableNames.SweepRunRowDetail) is { } existing
        )
        {
            ConfigureSweepRunRowDetailColumns(modelBuilder.Entity(existing.ClrType));
            return;
        }

        modelBuilder.SharedTypeEntity<Dictionary<string, object>>(
            CohortSharedTypeNames.SweepRunRowDetail,
            builder =>
            {
                builder.ToTable(CohortTableNames.SweepRunRowDetail);
                ConfigureSweepRunRowDetailColumns(builder);
            }
        );
    }

    private static void ConfigureRetentionHoldColumns(EntityTypeBuilder builder)
    {
        builder.Property<Guid>("HoldId").ValueGeneratedNever();
        builder.Property<string>("TableName").IsRequired();
        builder.Property<Guid>("RecordId").IsRequired();
        builder.Property<Guid>("TenantId").IsRequired();
        builder.Property<string>("Reason").IsRequired();
        builder.Property<DateTimeOffset>("CreatedAt").IsRequired();
        builder.Property<DateTimeOffset?>("ExpiresAt");
        builder.Property<DateTimeOffset?>("RemovedAt");
        builder.HasKey("HoldId");
        builder.HasIndex("TableName", "TenantId", "RecordId");
    }

    private static void ConfigureSweepRunColumns(EntityTypeBuilder builder)
    {
        builder.Property<Guid>("SweepId").ValueGeneratedNever();
        builder.Property<DateTimeOffset>("StartedAt").IsRequired();
        builder.Property<DateTimeOffset?>("CompletedAt");
        builder.Property<TimeSpan?>("Duration");
        builder.Property<bool>("DryRun").IsRequired();
        builder.Property<Guid>("TenantId").IsRequired();
        builder.Property<int?>("TotalAffected");
        builder.HasKey("SweepId");
    }

    private static void ConfigureSweepRunEntitySummaryColumns(EntityTypeBuilder builder)
    {
        builder.Property<Guid>("SweepId").ValueGeneratedNever();
        builder.Property<DateTimeOffset>("At").IsRequired();
        builder.Property<string>("EntityType").IsRequired();
        builder.Property<string>("Category").IsRequired();
        builder.Property<Guid>("TenantId").IsRequired();
        builder.Property<int>("Strategy").IsRequired();
        builder.Property<TimeSpan>("ResolvedPeriod").IsRequired();
        builder.Property<int>("Affected").IsRequired();
        builder.Property<int>("HeldCount").IsRequired();
        builder.HasKey("SweepId", "EntityType", "Category", "TenantId", "Strategy");
        builder.HasIndex("SweepId");
    }

    private static void ConfigureSweepRunRowDetailColumns(EntityTypeBuilder builder)
    {
        builder.Property<Guid>("SweepId").ValueGeneratedNever();
        builder.Property<DateTimeOffset>("At").IsRequired();
        builder.Property<string>("EntityType").IsRequired();
        builder.Property<Guid>("EntityId").IsRequired();
        builder.Property<string>("Category").IsRequired();
        builder.Property<int>("Strategy").IsRequired();
        builder.Property<Guid>("TenantId").IsRequired();
        builder.HasKey("SweepId", "EntityType", "EntityId", "Category", "Strategy", "TenantId");
        builder.HasIndex("SweepId");
    }

    private static Microsoft.EntityFrameworkCore.Metadata.IMutableEntityType? TryFindEntityMappedToTable(
        ModelBuilder modelBuilder,
        string tableName
    )
    {
        return modelBuilder
            .Model.GetEntityTypes()
            .FirstOrDefault(entityType => string.Equals(entityType.GetTableName(), tableName, StringComparison.Ordinal));
    }
}

internal static class CohortTableNames
{
    internal const string RetentionHolds = "retention_holds";
    internal const string SweepRun = "sweep_run";
    internal const string SweepRunEntitySummary = "sweep_run_entity_summary";
    internal const string SweepRunRowDetail = "sweep_run_row_detail";
}

internal static class CohortSharedTypeNames
{
    internal const string RetentionHold = "Cohort.RetentionHold";
    internal const string SweepRun = "Cohort.SweepRun";
    internal const string SweepRunEntitySummary = "Cohort.SweepRunEntitySummary";
    internal const string SweepRunRowDetail = "Cohort.SweepRunRowDetail";
}
