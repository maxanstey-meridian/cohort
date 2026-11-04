using Cohort.Infrastructure.Sweep;

namespace Cohort.Infrastructure;

/// <summary>
/// The shape every SQL builder returns for a statement against Cohort's own five tables or a
/// PostgreSQL function it calls, and the read of a transaction's isolation level.
/// </summary>
internal static class CohortStatements
{
    /// <summary>A statement that names no entity and reads the given Cohort tables, if any.</summary>
    internal static SweepStatement Statement(
        string name,
        string sql,
        SqlParams parameters,
        params RelationalObjectName[] relations
    ) => new(name, null, sql, parameters, relations);

    /// <summary>
    /// The isolation level of the transaction a call joins, which a hold or an erasure in a
    /// caller's transaction checks before it writes.
    /// </summary>
    internal static SweepStatement TransactionIsolation() =>
        Statement(
            "transaction-isolation",
            "SELECT pg_catalog.current_setting('transaction_isolation')",
            new SqlParams()
        );
}
