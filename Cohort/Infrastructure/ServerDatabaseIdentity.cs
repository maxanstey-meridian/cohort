namespace Cohort.Infrastructure;

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
