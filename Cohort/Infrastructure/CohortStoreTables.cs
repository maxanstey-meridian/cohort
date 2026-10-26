using Cohort.Infrastructure.Migrations;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Cohort.Infrastructure;

internal sealed record CohortStoreTables(
    RelationalObjectName RetentionHolds,
    RelationalObjectName SweepRun,
    RelationalObjectName SweepRunEntitySummary,
    RelationalObjectName SweepRunRowDetail,
    RelationalObjectName SweepRowHandlerStatus
)
{
    internal const string TableRoleAnnotation = "Cohort:TableRole";

    internal static CohortStoreTables FromModel(IModel model)
    {
        ArgumentNullException.ThrowIfNull(model);
        var mappedRoles = model.GetEntityTypes().Count(entity =>
            entity[TableRoleAnnotation] is string
        );
        if (mappedRoles == 0)
        {
            throw new InvalidOperationException(
                "The EF model contains no Cohort table mappings. Call ConfigureCohortTables() from DbContext.OnModelCreating."
            );
        }
        if (mappedRoles != CohortSchemaContract.TableNames.Count)
        {
            throw new InvalidOperationException(
                "The EF model contains a partial Cohort table mapping. Configure all five Cohort tables together."
            );
        }

        return new CohortStoreTables(
            Resolve(model, CohortTableNames.RetentionHolds),
            Resolve(model, CohortTableNames.SweepRun),
            Resolve(model, CohortTableNames.SweepRunEntitySummary),
            Resolve(model, CohortTableNames.SweepRunRowDetail),
            Resolve(model, CohortTableNames.SweepRowHandlerStatus)
        );
    }

    /// <summary>The five tables as quoted, schema-qualified SQL identifiers.</summary>
    internal QuotedCohortTables Quoted() =>
        new(
            PostgreSqlIdentifier.Format(RetentionHolds),
            PostgreSqlIdentifier.Format(SweepRun),
            PostgreSqlIdentifier.Format(SweepRunEntitySummary),
            PostgreSqlIdentifier.Format(SweepRunRowDetail),
            PostgreSqlIdentifier.Format(SweepRowHandlerStatus)
        );

    private static RelationalObjectName Resolve(IModel model, string role)
    {
        var entityType = model.GetEntityTypes().Single(entity =>
            string.Equals(entity[TableRoleAnnotation] as string, role, StringComparison.Ordinal)
        );
        var name = entityType.GetTableName()
            ?? throw new InvalidOperationException($"Cohort table role '{role}' has no table mapping.");
        return new RelationalObjectName(
            entityType.GetSchema() ?? model.GetDefaultSchema() ?? "public",
            name
        );
    }
}

/// <summary>
/// Cohort's tables as quoted, schema-qualified identifiers, ready to interpolate into raw SQL.
/// </summary>
internal sealed record QuotedCohortTables(
    string RetentionHolds,
    string SweepRun,
    string SweepRunEntitySummary,
    string SweepRunRowDetail,
    string SweepRowHandlerStatus
);
