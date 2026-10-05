# Running Retention

There are two ways to run retention. The hosted worker runs it on a cron schedule. You can
also resolve Cohort's services and run it yourself, from a job, an admin endpoint or a
test. Both go through the same execution model.

## The services

| Service | What it does |
|---|---|
| `IRetentionPreview` | Counts what a sweep would change. Writes nothing. |
| `IRetentionSweep` | Runs the real sweep and records it in the ledger. |
| `IRetentionErasureService` | Erases one subject's rows within the same rules. See [Right-to-Erasure](/guides/erasure). |
| `IRetentionDeletion` | Lets your own deletes take part in hold protection. See [Legal Holds](/guides/legal-holds#deleting-around-holds). |

```csharp
var now = DateTimeOffset.UtcNow;

await services.GetRequiredService<IRetentionPreview>().PreviewAsync(tenant, now, ct);
await services.GetRequiredService<IRetentionSweep>().SweepAsync(tenant, now, ct);
await services.GetRequiredService<IRetentionErasureService>()
    .EraseAsync(tenant, new ErasureScope(subjectId), now, ct);
```

`SweepAsync(tenant, ...)` sweeps that tenant's tenanted entities. Tenantless entities hold
one shared row set, so they're swept with an explicit tenantless request:

```csharp
var sweep = services.GetRequiredService<IRetentionSweep>();

await sweep.ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, now), ct);
await sweep.ExecuteAsync(RetentionSweepRequest.Tenantless(now), ct);
await sweep.ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, now, dryRun: true), ct);
```

A dry run counts what it would change and writes the same run and entity audit trail, with
`sweep_run.DryRun` set, without changing any row.

### Scopes and transactions

`IRetentionSweep`, `IRetentionPreview` and `IRetentionErasureService` are singletons that own
their scope. Every call runs in a fresh DI scope, so Cohort's raw SQL never shares your
tracked `DbContext` or your ambient transaction.

- Pass operation context through the request. Your caller-scoped services and transactions
  don't flow into the operation.
- Singleton decorators can wrap these services, for metrics or an approval gate. A scoped
  decorator mustn't depend on its own scope taking part in Cohort's work.
- Caller times (`now`, `createdAt`, `asOf`, ...) can carry any offset. Cohort normalises them
  to UTC at the boundary.

These services also check PostgreSQL, model and schema readiness themselves, so they're safe
to call from a host that never ran Cohort's startup validation.

## The scheduled worker

```json
{
  "Cohort": {
    "Schedule": "0 2 * * *",
    "DryRun": false,
    "KillSwitch": false
  }
}
```

`Schedule` is a cron expression evaluated in **UTC**. Leave it `null` and the worker stays
idle. Sweeps the worker starts are audited as `Scheduled`, and direct `SweepAsync` calls are
audited as `Manual`.

### Tenants

The worker sweeps every tenant returned by `IRetentionTenantSource`. The default source
adapts a singleton `TenantContext` registration, which suits a single-tenant host.
Multi-tenant hosts register their own source:

```csharp
public sealed class AllTenants(AppDbContext db) : IRetentionTenantSource
{
    public async Task<IReadOnlyList<TenantContext>> GetTenantsAsync(CancellationToken ct) =>
        await db.Tenants
            .Select(t => new TenantContext(t.Id, t.Jurisdiction, new Dictionary<string, string>()))
            .ToListAsync(ct);
}
```

- Tenant passes run one after another, in the order the source returned them.
- Duplicate tenant IDs run once. If duplicates disagree on jurisdiction or tags, Cohort logs a
  warning and uses the first.
- One tenant's failure is logged and the remaining tenants still run.
- After the tenant passes, one tenantless pass sweeps any tenantless entities.

### Occurrences

Each cron occurrence runs once, even across replicas:

- Replicas of one install coordinate through a Postgres advisory lock keyed on the install's
  schema-qualified `sweep_run` table. Installs in different schemas of one database sweep
  independently.
- Under the lock, a worker skips an occurrence that already has a `Scheduled` run of the same
  kind (dry or real) started at or after it. A dry-run replica never stands in for the real
  sweep.
- The exception is an interrupted run. A run still `Started` lost its owner, because the lock
  went with its session, and a `Cancelled` run was stopped. Either way the occurrence is
  swept again, including passes that had already finished.
- A run that settled `Failed` or `PartiallyFailed` still claims its occurrence. Its failures
  are retried at the next one.
- Occurrence times are compared against other replicas' clocks, so keep replica clocks in
  sync.

Missed occurrences are skipped, not caught up. The next occurrence is always computed from
the current time. A failed iteration, such as a database outage or a runtime
misconfiguration, is logged and the worker tries again at the next occurrence. It never stops
the host.

### Dry run and the kill switch

- `DryRun: true` makes **scheduled** sweeps count-only, audited runs. Only the worker reads
  it. Explicit requests carry their own `dryRun` flag and are honoured as written.
- `KillSwitch: true` stops between tenant passes. The tenant in progress finishes, and the
  remaining tenants and future occurrences are skipped. It also stops
  [history pruning](/guides/history-pruning) after the batch in progress.

Both are read live from configuration, so flipping them doesn't need a restart.

## Before you start the host

Apply EF Core migrations first. Startup validates the installed Cohort schema but never
changes it. See [Schema & Migrations](/reference/schema).
