#nullable enable

using Cohort.Infrastructure.Handlers;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Cohort.Infrastructure.Migrations;

public static class CohortModelBuilder
{
    public static ModelBuilder ConfigureCohortTables(this ModelBuilder modelBuilder)
    {
        return ConfigureCohortTables(modelBuilder, "public");
    }

    public static ModelBuilder ConfigureCohortTables(this ModelBuilder modelBuilder, string schema)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);

        foreach (var table in CohortSchemaContract.Tables)
        {
            ConfigureTable(modelBuilder, schema, table);
        }

        foreach (var table in CohortSchemaContract.Tables)
        {
            ConfigureForeignKeys(modelBuilder, schema, table);
        }

        return modelBuilder;
    }

    private static void ConfigureTable(
        ModelBuilder modelBuilder,
        string schema,
        CohortSchemaContract.TableRequirement table
    )
    {
        EntityTypeBuilder builder;
        var existing = TryFindEntityMappedToTable(modelBuilder, schema, table.Name);

        if (table.Role == CohortTableNames.SweepRowHandlerStatus)
        {
            if (existing is not null && existing.ClrType != typeof(SweepRowHandlerStatusEntity))
            {
                throw new InvalidOperationException(
                    $"Entity {existing.ClrType.FullName} is mapped to table '{table.Name}', which Cohort manages with its runtime handler-status entity. This is a table-name collision; rename the host entity's table."
                );
            }

            builder = modelBuilder.Entity<SweepRowHandlerStatusEntity>();
        }
        else if (existing is not null)
        {
            EnsureAdoptableKey(existing, table);
            builder = modelBuilder.Entity(existing.ClrType);
        }
        else
        {
            builder = modelBuilder.SharedTypeEntity<Dictionary<string, object>>(
                table.SharedTypeName!
            );
        }

        builder.ToTable(
            table.Name,
            schema,
            tableBuilder =>
            {
                foreach (var check in table.RequiredChecks)
                {
                    tableBuilder.HasCheckConstraint(check.Name, check.Sql);
                }
            }
        );

        foreach (var column in table.Columns)
        {
            var property = builder
                .Property(column.ClrType, column.Name)
                .HasColumnType(column.StoreType)
                .IsRequired(!column.Nullable);
            if (column.Generated)
            {
                property.ValueGeneratedOnAdd();
            }
            else
            {
                property.ValueGeneratedNever();
            }
        }

        builder.HasKey(table.PrimaryKey.ToArray());
        foreach (var index in table.RequiredIndexes)
        {
            var indexBuilder = builder.HasIndex(index.Columns.ToArray()).IsUnique(index.Unique);
            if (index.Predicate is not null)
            {
                indexBuilder.HasFilter(index.Predicate);
            }
            if (index.Name is not null)
            {
                indexBuilder.HasDatabaseName(index.Name);
            }
        }

        builder.Metadata.SetAnnotation(CohortStoreTables.TableRoleAnnotation, table.Role);
    }

    private static void ConfigureForeignKeys(
        ModelBuilder modelBuilder,
        string schema,
        CohortSchemaContract.TableRequirement table
    )
    {
        if (table.RequiredForeignKeys.Count == 0)
        {
            return;
        }

        var entityType = TryFindEntityMappedToTable(modelBuilder, schema, table.Name)
            ?? throw new InvalidOperationException(
                $"Cohort could not resolve its mapped table '{schema}.{table.Name}'."
            );
        var builder = entityType.HasSharedClrType
            ? modelBuilder.SharedTypeEntity<Dictionary<string, object>>(entityType.Name)
            : modelBuilder.Entity(entityType.ClrType);

        foreach (var foreignKey in table.RequiredForeignKeys)
        {
            builder
                .HasOne(
                    FindEntityMappedToTable(modelBuilder, schema, foreignKey.PrincipalTable),
                    navigationName: null
                )
                .WithMany()
                .HasForeignKey(foreignKey.Columns.ToArray())
                .HasPrincipalKey(foreignKey.PrincipalColumns.ToArray())
                .OnDelete(foreignKey.DeleteAction switch
                {
                    CohortSchemaContract.ForeignKeyDeleteAction.Restrict => DeleteBehavior.Restrict,
                    CohortSchemaContract.ForeignKeyDeleteAction.Cascade => DeleteBehavior.Cascade,
                    _ => throw new ArgumentOutOfRangeException(nameof(foreignKey.DeleteAction)),
                });
        }
    }

    private static void EnsureAdoptableKey(
        Microsoft.EntityFrameworkCore.Metadata.IMutableEntityType existing,
        CohortSchemaContract.TableRequirement table
    )
    {
        var declaredKey = existing.FindPrimaryKey();
        if (declaredKey is null)
        {
            return;
        }

        var keyNames = declaredKey.Properties.Select(property => property.Name).ToArray();
        if (keyNames.SequenceEqual(table.PrimaryKey, StringComparer.Ordinal))
        {
            return;
        }

        throw new InvalidOperationException(
            $"Entity {existing.ClrType.FullName} is mapped to table '{table.Name}', which Cohort manages, but its primary key ({string.Join(", ", keyNames)}) does not match the key Cohort expects ({string.Join(", ", table.PrimaryKey)}). This looks like a table-name collision rather than an intentional adoption; rename the entity's table, or map it with Cohort's key shape."
        );
    }

    private static Microsoft.EntityFrameworkCore.Metadata.IMutableEntityType? TryFindEntityMappedToTable(
        ModelBuilder modelBuilder,
        string schema,
        string tableName
    )
    {
        return modelBuilder
            .Model.GetEntityTypes()
            .FirstOrDefault(entityType =>
                string.Equals(entityType.GetTableName(), tableName, StringComparison.Ordinal)
                && string.Equals(
                    entityType.GetSchema() ?? modelBuilder.Model.GetDefaultSchema() ?? "public",
                    schema,
                    StringComparison.Ordinal
                )
            );
    }

    private static string FindEntityMappedToTable(
        ModelBuilder modelBuilder,
        string schema,
        string tableName
    )
    {
        return TryFindEntityMappedToTable(modelBuilder, schema, tableName)?.Name
            ?? throw new InvalidOperationException(
                $"Cohort could not resolve its mapped table '{schema}.{tableName}'."
            );
    }
}
