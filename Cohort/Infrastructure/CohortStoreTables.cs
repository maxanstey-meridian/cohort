using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Cohort.Infrastructure;

internal sealed record CohortStoreTables(
    RelationalObjectName RetentionHolds,
    RelationalObjectName SweepRun,
    RelationalObjectName SweepRunEntitySummary,
    RelationalObjectName SweepRunRowDetail
)
{
    internal const string TableRoleAnnotation = "Cohort:TableRole";

    internal static CohortStoreTables FromModel(IModel model)
    {
        var mappedRoles = model
            .GetEntityTypes()
            .Select(entity => entity[TableRoleAnnotation])
            .OfType<string>()
            .ToLookup(role => role, StringComparer.Ordinal);
        if (mappedRoles.Count == 0)
        {
            throw new InvalidOperationException(
                "The EF model contains no Cohort table mappings. Call ConfigureCohortTables() from DbContext.OnModelCreating."
            );
        }

        foreach (var role in CohortSchemaContract.TableNames)
        {
            var carriedBy = mappedRoles[role].Count();
            if (carriedBy != 1)
            {
                throw new InvalidOperationException(
                    $"The EF model contains a partial Cohort table mapping. Configure all four Cohort tables together. Table role '{role}' is carried by {carriedBy} entity types, not one."
                );
            }
        }

        return new CohortStoreTables(
            Resolve(model, CohortTableNames.RetentionHolds),
            Resolve(model, CohortTableNames.SweepRun),
            Resolve(model, CohortTableNames.SweepRunEntitySummary),
            Resolve(model, CohortTableNames.SweepRunRowDetail)
        );
    }

    private static RelationalObjectName Resolve(IModel model, string role)
    {
        var entityType = model.GetEntityTypes().Single(entity =>
            string.Equals(entity[TableRoleAnnotation] as string, role, StringComparison.Ordinal)
        );
        var name = entityType.GetTableName()
            ?? throw new InvalidOperationException($"Cohort table role '{role}' has no table mapping.");
        return new RelationalObjectName(entityType.GetSchema() ?? "public", name);
    }
}
