using System.Collections;

namespace Cohort.Infrastructure;

/// <summary>
/// The readiness checks whose success is recorded by value, per database, under what each reads.
/// The anonymise-target check isn't one: its outcome depends on session state no key can capture,
/// so it runs again for each model instance.
/// </summary>
internal enum RetentionReadinessCheck
{
    ServerVersion,
    Schema,
    RetainedTables,
    HoldRelationLinkColumns,
    ReferencedMarkerColumns,
    StoredEntityIdentities,
}

/// <summary>
/// One passed readiness check: the database it passed against, which check it was, and everything
/// the check read from the model, the capabilities, and the services, reduced to values. Each
/// element of <paramref name="Inputs"/> is a string, a <see cref="Guid"/>, or a record whose
/// members are such values, never a list, since a record compares a list by reference. Two keys
/// are equal when their inputs are equal element by element, whichever model instance built them.
/// </summary>
internal sealed record RetentionReadinessCheckKey(
    string DataSource,
    string Database,
    RetentionReadinessCheck Check,
    object[] Inputs
)
{
    public bool Equals(RetentionReadinessCheckKey? other) =>
        other is not null
        && string.Equals(DataSource, other.DataSource, StringComparison.Ordinal)
        && string.Equals(Database, other.Database, StringComparison.Ordinal)
        && Check == other.Check
        && StructuralComparisons.StructuralEqualityComparer.Equals(Inputs, other.Inputs);

    public override int GetHashCode() =>
        HashCode.Combine(
            DataSource,
            Database,
            Check,
            StructuralComparisons.StructuralEqualityComparer.GetHashCode(Inputs)
        );
}
