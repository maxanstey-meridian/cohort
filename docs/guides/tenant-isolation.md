# Tenant Isolation

If your host isolates tenants inside the database, for example with PostgreSQL row-level
security, Cohort's own work has to run under that isolation too. Cohort creates its own DI
scopes for sweeps, previews, erasures, row-handler deliveries, pruning and readiness, so it
tells you whose work each one does through `IRetentionExecutionScope`. Your hook configures
the scope, typically by telling its DbContext which tenant it serves.

## Register a hook

Every host registers one. A host without tenant isolation registers the built-in no-op:

```csharp
builder.Services.AddSingleton(RetentionExecutionScope.Unisolated);
builder.Services.AddCohort<AppDbContext>();
```

A host with isolation registers its own, scoped, so it can reach the scope's DbContext:

```csharp
builder.Services.AddScoped<IRetentionExecutionScope, AppExecutionScope>();
builder.Services.AddCohort<AppDbContext>();
```

With neither, Cohort refuses to start. Forgetting the hook is a startup error rather than a
silent run as the table owner. See [Startup Validation](/reference/startup-validation).

## What Cohort tells you

Cohort resolves your hook from every scope it creates and calls `RunAsync` once, with all of
that scope's work: nothing in the scope touches the database until you run it. Set up the
scope, then await the work exactly once. Whatever you put around the call encloses everything
Cohort does in the scope, including an `AsyncLocal` value or a `using` block (a Sluice flow,
say). Cohort refuses a hook that returns without running the work, runs it twice, or returns
before it finishes, and rethrows a failure of the work the hook swallowed. The context is
either one tenant's work or a named kind of system work:

```csharp
RetentionExecutionContext.Tenant(Guid TenantId)
RetentionExecutionContext.System(RetentionSystemWork Work)
```

| Where | Context |
|---|---|
| `IRetentionSweep`, `IRetentionPreview` | `Tenant` for a tenanted request, `System(TenantlessPass)` for a tenantless one |
| `IRetentionErasureService` | `Tenant` for a tenanted request, `System(TenantlessPass)` for a tenantless one. Not called for the erasure itself with `ErasureTransaction.Caller`: that erasure runs in your scope, which you already set up (its row-handler batches, below, still are) |
| Each batch's `OnBeforeAsync` handlers and dependents step, in their own scope on the run's connection and transaction, `Caller` erasures included | `Tenant` of the run, or `System(TenantlessPass)` for a tenantless one |
| Scheduled worker iteration: its lock, occurrence ledger, tenant source and tenant coverage check (which reads the tenant column of every retained tenanted table, across tenants) | `System(WorkerCoordination)` |
| A refused scheduled pass (a tenant missing from the source or returned with conflicting contexts), recorded as a `Failed` run | `Tenant` of the refused tenant |
| Each scheduled pass, in its own scope | `Tenant`, or `System(TenantlessPass)` for the tenantless pass |
| Row-handler dispatch: claim, settle, recovery and payload scrub | `System(RowDispatch)` |
| Each `OnAfterAsync` delivery, in its own scope | `Tenant` of the row, or `System(TenantlessPass)` for a tenantless row |
| History pruning | `System(HistoryPruning)` |
| Startup readiness | `System(Readiness)` |
| Readiness's cross-tenant hold check, whichever operation runs readiness first | `System(Readiness)` |
| The check, by `IRetentionDeletion` and by each batch with a dependents step, that its scope sees every hold on a tenantless row, in a model with tenantless entities | `System(Readiness)` |

The first operation can be any of the above, `IRetentionHoldsRepository`,
`IRetentionDeletion` or a `Caller` erasure. Apart from those `System(Readiness)` scopes,
`IRetentionHoldsRepository` and `IRetentionDeletion` create no scope of their own. They run in
your scope, under whatever isolation you already applied to it.

## System work

System work deliberately spans tenants, or touches tables that have no tenant:

| `RetentionSystemWork` | What it is |
|---|---|
| `Readiness` | Catalog checks, and integrity checks across every tenant |
| `WorkerCoordination` | The worker's advisory lock, occurrence ledger, tenant source and tenant coverage check |
| `TenantlessPass` | Sweeps, previews, erasures and handler deliveries for `[RetentionTenantless]` entities |
| `RowDispatch` | Claiming, settling, recovering, requeuing and scrubbing every tenant's queued handler work |
| `HistoryPruning` | Pruning Cohort's run history and inactive holds |

How you carry out `System` is up to you, as long as the scope can read and write across
tenants. Map it to a named, audited system role, and use the `Work` value to name and audit
each kind. If a context with no tenant sees no rows in your host, `System` must not mean "no
tenant".

## Rules Cohort keeps

- **One context per scope.** Cohort runs each scope's work once and never changes its
  context. Different work gets a new scope.
- **Calls nest.** Each scheduled pass runs inside the worker's `WorkerCoordination` work,
  readiness's `System(Readiness)` check can run inside any operation's work, and each batch
  with row handlers or a dependents step runs its own scope, for the run's tenant, inside the sweep's or erasure's
  work (inside your own scope for a `Caller` erasure). Each `OnAfterAsync` delivery runs its
  own scope, for the row's tenant, inside the dispatcher's `System(RowDispatch)` work for its
  batch, which holds the batch's delivery locks. An inner call's
  context wholly replaces the outer one for its work: set it, don't merge it. Restore anything
  you change outside the scope's own services when the work returns, as a `using` block or
  `try`/`finally` does; an `AsyncLocal` set in the hook is restored for you.
- **Tenanted work only runs as `Tenant`.** Each batch's `OnBeforeAsync` handlers and
  dependents step get their own scope, run as the batch's tenant, and are resolved inside that
  work. Each `OnAfterAsync` delivery gets its own scope, run as the row's tenant, and the
  handler is resolved inside that work. No host code runs under `System` against tenanted rows. Handlers
  and steps for tenantless entities run under `System(TenantlessPass)`, against rows no tenant owns.
- **A row-handler or dependents scope's context uses the run's connection.** If your hook opened the
  scope's context, Cohort closes that connection and points the context at the run's, which
  your hook already isolated for the same tenant (or your own, for `ErasureTransaction.Caller`).
  Isolation you apply when a connection opens covers both.
- **Savepoints sit inside your isolation.** Cohort only creates savepoints after its
  transactions start, so a setting you apply when the connection opens or the transaction
  begins is never rolled back by one.

## Example: row-level security

The hook tells the DbContext whose work it does:

```csharp
public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public RetentionExecutionContext? RowSecurity { get; set; }
}

public sealed class AppExecutionScope(AppDbContext db) : IRetentionExecutionScope
{
    public Task RunAsync(RetentionExecutionContext context, Func<Task> work, CancellationToken ct)
    {
        db.RowSecurity = context;
        return work();
    }
}
```

An EF connection interceptor applies it every time the context's connection opens:

```csharp
public sealed class RowSecurityInterceptor : DbConnectionInterceptor
{
    public override void ConnectionOpened(DbConnection connection, ConnectionEndEventData eventData)
    {
        using var command = CreateCommand(connection, eventData);
        command.ExecuteNonQuery();
    }

    public override async Task ConnectionOpenedAsync(
        DbConnection connection,
        ConnectionEndEventData eventData,
        CancellationToken ct = default)
    {
        await using var command = CreateCommand(connection, eventData);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static DbCommand CreateCommand(DbConnection connection, ConnectionEndEventData eventData)
    {
        var command = connection.CreateCommand();
        // Set the full state on every open, so a pooled session never keeps the last tenant.
        command.CommandText = (eventData.Context as AppDbContext)?.RowSecurity switch
        {
            RetentionExecutionContext.Tenant tenant =>
                $"SET ROLE app_tenant; SELECT set_config('app.tenant_id', '{tenant.TenantId}', false)",
            RetentionExecutionContext.System =>
                "SET ROLE app_system; SELECT set_config('app.tenant_id', '', false)",
            _ => "SET ROLE app_tenant; SELECT set_config('app.tenant_id', '', false)",
        };
        return command;
    }
}
```

```csharp
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(connectionString).AddInterceptors(new RowSecurityInterceptor()));
```

Your own requests set `RowSecurity` the same way, from the signed-in tenant. Write the
policies against Cohort's tenant columns as described in
[Row-level security](/reference/schema#row-level-security).

### Why when the connection opens

Cohort runs some statements through ADO.NET directly on the context's connection, outside
EF's command pipeline and sometimes outside a transaction: preview counts, the worker's
ledger check and readiness, for example. An EF command interceptor, or a `SET LOCAL` issued
only when EF starts a transaction, misses those statements, and they would run with the
connection's own rights. A connection interceptor sees every connection Cohort opens.

Session-level settings need a direct connection or session-mode pooling. Behind a
transaction-pooling PgBouncer they would leak between clients, and Cohort's session advisory
locks would not hold either. Npgsql resets session state when a connection returns to its
pool.

Each scope uses its own connection, except a batch's row-handler and dependents scope, which
uses the run's connection and transaction. The worker holds its coordination connection for the
whole iteration and opens another for each pass, and the first readiness check can take one
more in its own `System(Readiness)` scope. A pool of one connection hangs the worker; leave
headroom.

## Grants

Readiness asks PostgreSQL to plan, without running (`EXPLAIN`), the statements each operation
sends, grouped by operation, under the role
of the scope that sends them, the first time that role reaches readiness. So each scope's role
needs only the privileges of the groups it runs, on Cohort's five tables and on your retained
tables, and a role that lacks one is refused before the operation writes anything:

| Group | Sent in | Needs on Cohort's tables |
|---|---|---|
| Sweeps, dry runs, and scheduled passes | `Tenant`, or `System(TenantlessPass)` | `SELECT`, `INSERT`, `UPDATE` on `sweep_run` and `sweep_run_entity_summary`; `SELECT`, `INSERT` on `sweep_run_row_detail`; `SELECT` on `retention_holds`; `INSERT` on `sweep_row_handler_status` with a row handler or dependents step |
| Erasures | `Tenant`, or `System(TenantlessPass)`; your own scope for `ErasureTransaction.Caller` | The same as sweeps |
| Previews | `Tenant`, or `System(TenantlessPass)` | `SELECT` on `retention_holds` |
| `IRetentionHoldsRepository` | Your scope | `SELECT`, `INSERT`, `UPDATE` on `retention_holds` |
| `IRetentionDeletion` | Your scope | `SELECT` on `retention_holds` |
| Readiness's own reads | `System(Readiness)` | `SELECT` on `retention_holds` |
| Payload scrub | `System(RowDispatch)` | `SELECT`, `UPDATE` on `sweep_run_row_detail` |
| Row-handler dispatch | `System(RowDispatch)` | `SELECT`, `UPDATE` on `sweep_run`, `sweep_run_row_detail`, and `sweep_row_handler_status` |
| The scheduled worker's lock, ledger, and tenant coverage check | `System(WorkerCoordination)` | `SELECT` on `sweep_run` |
| History pruning | `System(HistoryPruning)` | Per retention set. Run retention: `SELECT`, `UPDATE`, `DELETE` on `sweep_run`; `SELECT`, `DELETE` on `sweep_run_row_detail` and `sweep_run_entity_summary`; `SELECT` on `sweep_row_handler_status`. Hold retention: `SELECT`, `UPDATE`, `DELETE` on `retention_holds` |

On your retained tables, the roles of sweeps, erasures, previews, holds, and `IRetentionDeletion`
need `SELECT`; sweeps, erasures, and holds also need `UPDATE`, where they lock rows. Sweeps and
erasures need `DELETE` where `Purge` is declared. `SELECT` on the child table of a foreign key
that blocks a purge is optional: with it, a purge leaves out the rows that child still
references, unless the entity has a dependents step; without it, a sweep or an erasure tries
the row and the constraint refuses the delete, and the row is counted as blocked (a sweep still
succeeds and the next run tries it again; an erasure settles `PartiallyFailed` with
`rows-blocked`, as it does with `SELECT`). A preview or a dry run counts such a row as due, not
blocked. The worker needs only `SELECT` on each tenanted retained table. Readiness's own reads, the payload scrub, row-handler
dispatch, and history pruning need nothing on your retained tables. Every role needs `USAGE` on
the schemas of the tables it touches. See [Startup Validation](/reference/startup-validation#statements) for the
full list, including `EXECUTE` on the PostgreSQL functions Cohort calls, which PUBLIC holds by
default.

A tenant's or your own scope's role is checked at its first use, not at startup: a second
tenant's role, or a role a scope hook switches to later, is checked when it first reaches
readiness. Startup does check the payload scrub, the dispatcher (while the kill switch is
off), the worker (when it runs), and pruning (for the retentions set), each in its own scope
under its own role. The identity
columns behind `sweep_run_row_detail.Id` and `sweep_row_handler_status.Id` need no separate
sequence grant; a serial column in their place needs `USAGE` on its sequence, which readiness
checks. See [Upgrading to 0.12.0](/reference/startup-validation#upgrading-to-0-12-0).
