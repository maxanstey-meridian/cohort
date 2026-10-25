using Cohort.Domain;

namespace Cohort.Application;

/// <summary>
/// Erases one subject's rows now, through the same categories, rules and strategies as a
/// sweep. Scoped: <see cref="ErasureTransaction.Caller"/> runs on this scope's DbContext.
/// </summary>
public interface IRetentionErasureService
{
    public Task<ErasureResult> ExecuteAsync(
        RetentionErasureRequest request,
        CancellationToken ct = default
    );

    public Task<ErasureResult> EraseAsync(
        TenantContext tenant,
        ErasureScope scope,
        CancellationToken ct = default
    ) => ExecuteAsync(RetentionErasureRequest.Tenanted(tenant, scope), ct);
}

/// <summary>Whose transaction an erasure runs in.</summary>
public enum ErasureTransaction
{
    /// <summary>
    /// Cohort's own: a fresh scope and DbContext, a durable <c>Started</c> row, and batches
    /// that commit independently.
    /// </summary>
    Owned = 0,

    /// <summary>
    /// The transaction open on the calling scope's DbContext. The erasure and its ledger
    /// commit or roll back with the caller's own changes; Cohort commits nothing. Audit
    /// observers run only once the caller commits, and a rollback leaves no ledger row.
    /// </summary>
    Caller = 1,
}

public abstract record RetentionErasureRequest
{
    private RetentionErasureRequest(
        ErasureScope scope,
        DateTimeOffset? at,
        ErasureTransaction transaction
    )
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (!Enum.IsDefined(transaction))
        {
            throw new ArgumentOutOfRangeException(
                nameof(transaction),
                transaction,
                "Erasure transaction mode must be defined."
            );
        }

        Scope = scope;
        // Npgsql only writes UTC DateTimeOffsets to timestamptz.
        At = at?.ToUniversalTime();
        Transaction = transaction;
    }

    public ErasureScope Scope { get; }

    /// <summary>
    /// The time the erasure treats as now. Null uses the database clock. A real (non-dry)
    /// erasure refuses a time later than the database clock.
    /// </summary>
    public DateTimeOffset? At { get; }

    public ErasureTransaction Transaction { get; }

    /// <summary>The tenant the run is scoped and audited under.</summary>
    internal abstract TenantContext RunTenant { get; }

    /// <summary>Erases the subject from the tenant's tenanted entities.</summary>
    public static RetentionErasureRequest Tenanted(
        TenantContext tenant,
        ErasureScope scope,
        DateTimeOffset? at = null,
        ErasureTransaction transaction = ErasureTransaction.Owned
    ) => new TenantedRequest(tenant, scope, at, transaction);

    /// <summary>
    /// Erases the subject from <see cref="RetentionTenantlessAttribute"/> entities, which no
    /// tenant owns. A tenanted request never reaches them. Always
    /// <see cref="ErasureTransaction.Owned"/>: tenantless rows are erased under Cohort's own
    /// <c>System(TenantlessPass)</c> execution scope, never under one the caller set up.
    /// </summary>
    public static RetentionErasureRequest Tenantless(ErasureScope scope, DateTimeOffset? at = null) =>
        new TenantlessRequest(scope, at);

    public sealed record TenantedRequest : RetentionErasureRequest
    {
        internal TenantedRequest(
            TenantContext tenant,
            ErasureScope scope,
            DateTimeOffset? at,
            ErasureTransaction transaction
        )
            : base(scope, at, transaction)
        {
            ArgumentNullException.ThrowIfNull(tenant);
            Tenant = tenant;
        }

        public TenantContext Tenant { get; }

        internal override TenantContext RunTenant => Tenant;
    }

    public sealed record TenantlessRequest : RetentionErasureRequest
    {
        internal TenantlessRequest(ErasureScope scope, DateTimeOffset? at)
            : base(scope, at, ErasureTransaction.Owned) { }

        internal override TenantContext RunTenant => TenantContext.Tenantless;
    }
}
