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
    MarkerColumns,
    StoredEntityIdentities,
}

/// <summary>
/// Which database a connection reaches, as the server sees it. Connection strings spell one
/// database many ways (<c>localhost</c> or <c>127.0.0.1</c>, a pooler or the direct port), and
/// one spelling can reach different databases (a pooler that routes by user name), so the
/// database name and OID, the server's start time, and the holds table's OID decide sameness
/// instead. A recreated database, from a template or not, gets a new OID.
/// </summary>
internal sealed record ServerDatabaseIdentity(
    string Database,
    long DatabaseOid,
    DateTime ServerStartedAt,
    long? HoldsTableOid
);

/// <summary>
/// One passed readiness check: the database it passed against, which check it was, and everything
/// the check read from the model, the capabilities, and the services, reduced to values. Each
/// element of <paramref name="Inputs"/> is a string, a <see cref="Guid"/>, or a record whose
/// members are such values, never a list, since a record compares a list by reference. Two keys
/// are equal when their database identities are equal and their inputs are equal element by
/// element, whichever model instance built them.
/// </summary>
internal sealed record RetentionReadinessCheckKey(
    ServerDatabaseIdentity Database,
    RetentionReadinessCheck Check,
    object[] Inputs
)
{
    public bool Equals(RetentionReadinessCheckKey? other) =>
        other is not null
        && Database == other.Database
        && Check == other.Check
        && StructuralComparisons.StructuralEqualityComparer.Equals(Inputs, other.Inputs);

    public override int GetHashCode() =>
        HashCode.Combine(
            Database,
            Check,
            StructuralComparisons.StructuralEqualityComparer.GetHashCode(Inputs)
        );
}
