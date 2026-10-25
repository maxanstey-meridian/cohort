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

Cohort resolves your hook from every scope it creates and calls `EnterAsync` once, before
anything in that scope touches the database. The context is either one tenant's work or a
named kind of system work:

```csharp
RetentionExecutionContext.Tenant(Guid TenantId)
RetentionExecutionContext.System(RetentionSystemWork Work)
```

| Where | Context |
|---|---|
| `IRetentionSweep`, `IRetentionPreview` | `Tenant` for a tenanted request, `System(TenantlessPass)` for a tenantless one |
| `IRetentionErasureService` | `Tenant` for a tenanted request, `System(TenantlessPass)` for a tenantless one. Not called for `ErasureTransaction.Caller`: that erasure runs in your scope, which you already entered |
| Scheduled worker iteration: its lock, occurrence ledger and tenant source | `System(WorkerCoordination)` |
| Each scheduled pass, in its own scope | `Tenant`, or `System(TenantlessPass)` for the tenantless pass |
| Row-handler dispatch: claim, settle, recovery and payload scrub | `System(RowDispatch)` |
| Each `OnAfterAsync` delivery, in its own scope | `Tenant` of the row, or `System(TenantlessPass)` for a tenantless row |
| History pruning | `System(HistoryPruning)` |
| Startup readiness | `System(Readiness)` |
| Readiness's cross-tenant hold check, whichever operation runs readiness first | `System(Readiness)` |

The first operation can be any of the above, `IRetentionHoldsRepository`,
`IRetentionDeletion` or a `Caller` erasure. Apart from that readiness scope,
`IRetentionHoldsRepository` and `IRetentionDeletion` create no scope of their own. They run in
your scope, under whatever isolation you already applied to it.

## System work

System work deliberately spans tenants, or touches tables that have no tenant:

| `RetentionSystemWork` | What it is |
|---|---|
| `Readiness` | Catalog checks, and integrity checks across every tenant |
| `WorkerCoordination` | The worker's advisory lock, occurrence ledger and tenant source |
| `TenantlessPass` | Sweeps, previews, erasures and handler deliveries for `[RetentionTenantless]` entities |
| `RowDispatch` | Claiming, settling, recovering and scrubbing every tenant's queued handler work |
| `HistoryPruning` | Pruning Cohort's run history and inactive holds |

How you carry out `System` is up to you, as long as the scope can read and write across
tenants. Map it to a named, audited system role, and use the `Work` value to name and audit
each kind. If a context with no tenant sees no rows in your host, `System` must not mean "no
tenant".

## Rules Cohort keeps

- **One context per scope.** Cohort enters each scope exactly once and never changes it.
  Different work gets a new scope.
- **Tenanted work only runs as `Tenant`.** `OnBeforeAsync` runs inside its tenant's sweep
  scope. Each `OnAfterAsync` delivery gets its own scope, entered as the row's tenant before
  the handler is resolved. No host code runs under `System` against tenanted rows. Handlers
  for tenantless entities run under `System(TenantlessPass)`, against rows no tenant owns.
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
    public ValueTask EnterAsync(RetentionExecutionContext context, CancellationToken ct)
    {
        db.RowSecurity = context;
        return ValueTask.CompletedTask;
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

Each scope uses its own connection. The worker holds its coordination connection for the
whole iteration and opens another for each pass, and the first readiness check can take one
more in its own `System(Readiness)` scope. A pool of one connection hangs the worker; leave
headroom.

## Grants

The tenant role needs `SELECT, INSERT, UPDATE, DELETE` on the five Cohort tables, as well as
on your retained tables. The identity columns behind `sweep_run_row_detail.Id` and
`sweep_row_handler_status.Id` need no separate sequence grant.
