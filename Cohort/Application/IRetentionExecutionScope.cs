namespace Cohort.Application;

/// <summary>
/// Tells the host whose work each DI scope Cohort creates is doing, and lets it wrap that work,
/// so the host's tenant isolation (for example PostgreSQL row-level security) covers Cohort's
/// own sweeps, previews, erasures, row-handler deliveries, pruning and readiness checks.
/// </summary>
/// <remarks>
/// <para>
/// Cohort resolves this service from every scope it creates and calls <see cref="RunAsync"/>
/// once, with all of that scope's work: Cohort resolves nothing that touches the database in the
/// scope until the host runs the work. Register it scoped, so it can configure that scope's
/// services (typically its DbContext's tenant). Anything the host sets up around the call to
/// the work, including an <see cref="AsyncLocal{T}"/> value or a <c>using</c> scope, encloses
/// all of it. Hosts without tenant isolation register <c>RetentionExecutionScope.Unisolated</c>;
/// with no registration at all, Cohort refuses to start.
/// </para>
/// <para>
/// A scope's context never changes: Cohort runs each scope's work once and creates a new scope
/// for different work. Some work runs inside another scope's work, so calls nest: each worker
/// pass runs inside the worker's <see cref="RetentionSystemWork.WorkerCoordination"/> work, and
/// readiness's <see cref="RetentionSystemWork.Readiness"/> check can run inside any operation.
/// Restore whatever you set when the work returns. Cohort creates its own savepoints only after
/// its transactions start, so isolation applied when a connection opens or a transaction
/// begins sits outside every Cohort savepoint and is never rolled back by one.
/// </para>
/// <para>
/// Tenanted work only ever runs as <see cref="RetentionExecutionContext.Tenant"/>: row
/// handlers' <c>OnBeforeAsync</c> runs in its tenant's sweep scope and every
/// <c>OnAfterAsync</c> delivery gets its own scope run as the row's tenant. No host code
/// runs under <see cref="RetentionExecutionContext.System"/> against tenanted host rows.
/// </para>
/// <para>
/// Cohort runs some statements through ADO.NET directly on the scope's DbContext connection,
/// outside EF's command pipeline. Apply isolation when the connection opens (an EF
/// <c>DbConnectionInterceptor</c>), not per EF command.
/// </para>
/// </remarks>
public interface IRetentionExecutionScope
{
    /// <summary>
    /// Runs <paramref name="work"/>, all of the current scope's work, for
    /// <paramref name="context"/>. Configure the scope, then await <paramref name="work"/>
    /// exactly once and let its exceptions propagate; Cohort refuses an implementation that
    /// returns without running it, runs it twice or returns before it finishes, and rethrows a
    /// failure of the work that the implementation swallowed.
    /// </summary>
    public Task RunAsync(RetentionExecutionContext context, Func<Task> work, CancellationToken ct);
}

/// <summary>
/// Whose work a Cohort-created scope does: one tenant's, or a named kind of system work that
/// deliberately spans tenants or touches only tenantless data.
/// </summary>
public abstract record RetentionExecutionContext
{
    private RetentionExecutionContext() { }

    /// <summary>One tenant's work. The host restricts the scope to that tenant's rows.</summary>
    public sealed record Tenant : RetentionExecutionContext
    {
        public Tenant(Guid tenantId)
        {
            if (tenantId == Guid.Empty)
            {
                throw new ArgumentException(
                    "A tenant execution context needs a tenant ID; tenantless work runs as System(TenantlessPass).",
                    nameof(tenantId)
                );
            }

            TenantId = tenantId;
        }

        public Guid TenantId { get; }
    }

    /// <summary>
    /// System work. The host lets the scope read and write across tenants, and decides how
    /// that is authorised and audited; <see cref="Work"/> names what the work is.
    /// </summary>
    public sealed record System : RetentionExecutionContext
    {
        public System(RetentionSystemWork work)
        {
            if (!Enum.IsDefined(work))
            {
                throw new ArgumentOutOfRangeException(nameof(work), work, "System work must be defined.");
            }

            Work = work;
        }

        public RetentionSystemWork Work { get; }
    }
}

/// <summary>The kinds of system work Cohort does.</summary>
public enum RetentionSystemWork
{
    /// <summary>
    /// Startup and runtime readiness: catalog checks and cross-tenant integrity checks.
    /// </summary>
    Readiness = 0,

    /// <summary>
    /// The scheduled worker's coordination: its advisory lock, occurrence ledger and tenant
    /// source.
    /// </summary>
    WorkerCoordination = 1,

    /// <summary>
    /// Sweeps, previews, erasures and row-handler deliveries for <c>[RetentionTenantless]</c>
    /// entities, which have no tenant column.
    /// </summary>
    TenantlessPass = 2,

    /// <summary>
    /// Row-handler bookkeeping across every tenant's queued work: claim, settle, recover and
    /// scrub.
    /// </summary>
    RowDispatch = 3,

    /// <summary>Pruning Cohort's own run history and inactive holds.</summary>
    HistoryPruning = 4,
}
