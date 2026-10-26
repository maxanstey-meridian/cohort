# Running Retention

There are two ways to run retention. The hosted worker runs it on a cron schedule. You can
also resolve Cohort's services and run it yourself, from a job, an admin endpoint or a
test. Both go through the same execution model.

## The services

| Service | What it does |
|---|---|
| `IRetentionPreview` | Counts what a sweep would change. Writes nothing. |
| `IRetentionSweep` | Runs the real sweep and records it in the ledger. |
| `IRetentionErasureService` | Erases one subject's rows within the same rules, in Cohort's transaction or yours. See [Right-to-Erasure](/guides/erasure). |
| `IRetentionDeletion` | Lets your own deletes take part in hold protection. See [Legal Holds](/guides/legal-holds#deleting-around-holds). |

```csharp
await services.GetRequiredService<IRetentionPreview>().PreviewAsync(tenant, ct: ct);
await services.GetRequiredService<IRetentionSweep>().SweepAsync(tenant, ct);

// The erasure service is scoped.
await using var scope = services.CreateAsyncScope();
await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>()
    .EraseAsync(tenant, new ErasureScope("user", userId), ct);
```

`SweepAsync(tenant, ...)` sweeps that tenant's tenanted entities. Tenantless entities hold
one shared row set, so they're swept with an explicit tenantless request:

```csharp
var sweep = services.GetRequiredService<IRetentionSweep>();

await sweep.ExecuteAsync(RetentionSweepRequest.Tenanted(tenant), ct);
await sweep.ExecuteAsync(RetentionSweepRequest.Tenantless(), ct);
await sweep.ExecuteAsync(RetentionSweepRequest.Tenanted(tenant, dryRun: true), ct);
```

A dry run counts what it would change and writes the same run and entity audit trail, with
`sweep_run.DryRun` set, without changing any row.

### Which "today"

Previews, sweeps and erasures take "today" from the database's clock
(`statement_timestamp()`), the clock that legal holds expire by, so an application server
whose clock runs fast can't delete a row before its anniversary. Leave the time out.

- You may pass a time (`PreviewAsync(tenant, at)`, `RetentionSweepRequest.Tenanted(tenant, at)`,
  `RetentionErasureRequest.Tenanted(tenant, scope, at)`). An earlier time is allowed, because
  it keeps more.
- A run that changes rows refuses a time later than the database's with an
  `ArgumentOutOfRangeException`, before any run is recorded.
- Dry runs and previews change no rows, so they accept any time and can look ahead.

### Scopes and transactions

`IRetentionSweep` and `IRetentionPreview` are singletons that own their scope. Every call runs
in a fresh DI scope, so Cohort's raw SQL never shares your tracked `DbContext` or your
ambient transaction.

`IRetentionErasureService` is **scoped**. An ordinary erasure (`ErasureTransaction.Owned`)
behaves like a sweep and runs in a fresh scope of its own. An erasure with
`ErasureTransaction.Caller` runs on your scope's `DbContext` and joins the transaction you
have open on it. See
[Erasing inside your own transaction](/guides/erasure#erasing-inside-your-own-transaction).

- Pass operation context through the request. Your caller-scoped services and transactions
  don't flow into the operation, except into a `Caller` erasure.
- Decorators can wrap these services, for metrics or an approval gate. Sweep and preview
  decorators can be singletons. An erasure decorator is scoped, because the service it wraps
  is.
- Each fresh scope runs its work through your `IRetentionExecutionScope` as the request's tenant
  (`System(TenantlessPass)` for tenantless rows and requests). See [Tenant Isolation](/guides/tenant-isolation).
- Caller times (`now`, `createdAt`, `expiresAt`, `removedAt`, ...) can carry any offset.
  Cohort normalises them to UTC at the boundary.

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
idle. Sweeps the worker starts are audited as `Scheduled`, and explicit sweeps (`SweepAsync`
or `ExecuteAsync`) are always audited as `Manual`.

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
- Duplicate tenant IDs with the same jurisdiction and tags run once.
- One tenant's failure is logged and the remaining tenants still run.
- After the tenant passes, one tenantless pass sweeps any tenantless entities.

Each occurrence the worker also checks the source against the data, with one query per
retained tenanted table:

- A tenant that owns rows a sweep could still act on, but isn't returned by the source, isn't
  swept. Without its jurisdiction and tags Cohort could apply a shorter rule. It gets a
  `Failed` scheduled run with `code=cohort:tenant-not-in-source` instead.
- Rows count when any strategy their category declares could still act on them: every row if
  the category may purge; otherwise rows not yet soft-deleted or not yet anonymised. Rows of a
  category that declares only `Exempt` never count. Held rows and rows not yet due count,
  because the tenant still owns them. A tenant whose remaining rows are all retired (an
  offboarded customer, say) needs no place in the source.
- A tenant the source returns more than once with different jurisdictions or tags isn't swept
  either, because Cohort won't guess which context applies. It gets a `Failed` scheduled run
  with `code=cohort:conflicting-tenant-contexts`, and the other tenants still run.
- An empty source is fine when there's no tenanted data. With tenanted data, each tenant that
  owns rows a sweep could still act on gets a `Failed` run as above.
- Rows whose tenant column holds `Guid.Empty` can't be swept by any tenant pass. If a sweep
  could still act on any of them, the worker logs an error naming the entity at each
  occurrence; give those rows their real tenant.
- These runs reach audit observers as `Started` then `Failed`, like any failed run, so your
  existing alerting sees them. They're retried like any failed pass (see below): until the
  source returns the tenant, each occurrence gives it up to `OccurrenceMaxAttempts` `Failed`
  runs, `OccurrenceRetryDelay` apart, and then the worker's "gave up" error. Fixing the source
  mid-occurrence gets the tenant swept at the next retry.
- The check reads each retained tenanted table once per iteration, in one scan whatever its
  indexes, and keeps only the tenants the source didn't return and only rows a sweep could
  still act on.

### Occurrences

The worker treats each cron occurrence as an obligation, not an instant. A sweep covers
everything due at any earlier occurrence, so the worker only ever catches up the most recent
occurrence at or before now. It reads what that occurrence still owes from `sweep_run`:

- An occurrence owes one **pass** per tenant in the source, one per refused tenant, and the
  tenantless pass. A pass is keyed by its tenant ID (`Guid.Empty` for the tenantless pass).
- A pass is **complete** once it has a `Scheduled` run of the same kind (dry or real) that
  `Succeeded`, started at or after the occurrence. A dry-run replica never stands in for the
  real sweep.
- A pass is **exhausted** once it has had `OccurrenceMaxAttempts` runs (default 3) without
  succeeding. Runs the kill switch or a shutdown cancelled don't count. The worker logs an
  error naming the occurrence and the tenants, and leaves them to the next occurrence.
- Every other pass is **pending**: it has no run yet, a `Failed` or `PartiallyFailed` run, or
  a run still `Started` whose owner died, which the worker tells by the pass lock being free.
- Each pass runs under its own advisory lock (install, tenant, dry or real), and re-reads its
  ledger under it. A pass another replica is running, even its last attempt, makes the worker
  stop and check again after the delay, so no later pass runs first; a pass that finished
  meanwhile isn't rerun.
- The worker runs only the pending passes, in plan order. Passes that succeeded never rerun,
  so their row handlers don't fire twice. [History pruning](/guides/history-pruning) keeps the
  current occurrence's scheduled runs, so this holds with a short retention too.
- While any pass is pending, the worker retries after `OccurrenceRetryDelay` (default 15
  minutes), or at the next occurrence if that comes sooner and supersedes it.

So no occurrence is lost:

- A host that was down at the occurrence, or a fresh install, sweeps it on start.
- A failed iteration, such as a database outage or a runtime misconfiguration, is logged and
  retried after the delay. It writes no run, so it uses no attempt, and it never stops the
  host.
- A replica that died mid-occurrence leaves its unfinished passes pending, including ones it
  never started. A run the row dispatcher recovered as `Failed` is pending too.

Replicas of one install coordinate through Postgres advisory locks keyed on the install's
schema-qualified `sweep_run` table. The worker lock only keeps replicas from planning an
occurrence side by side: a replica that finds it taken checks again after the delay, by which
time the passes are usually complete. What guarantees one run per pass is the pass lock, held
on the connection the pass's run writes on, so it holds even when a replica loses its
worker-lock session (a timeout, a failover) mid-occurrence. `Started`, every batch and every
per-entity summary check that the session still holds its locks before they commit, so a pass
whose session was lost, dry run or not, records nothing, or commits nothing more and settles
`Failed`. Installs in different schemas of one database sweep independently. Occurrences are
counted on the database clock, the clock that stamps each run's `StartedAt`, so a replica's
own clock never decides which occurrence is due.

### Dry run and the kill switch

- `DryRun: true` makes **scheduled** sweeps count-only, audited runs. Only the worker reads
  it. Explicit requests carry their own `dryRun` flag and are honoured as written.
- `KillSwitch: true` cancels the pass in progress: its current batch rolls back and the run
  settles `Cancelled`. The rest of that occurrence and future occurrences are skipped. It also
  stops [history pruning](/guides/history-pruning) after the batch in progress, and the
  [row handler](/guides/row-handlers) dispatcher's background poller after the batch it's
  running. `FlushAsync` and `RequeueDeadLetteredAsync` still run.
- Turning `KillSwitch` off catches up the most recent occurrence straight away. The cancelled
  run used no attempt.

Both are read live from configuration, so flipping them doesn't need a restart.

## Upgrading to 0.11.0

::: warning Breaking change
0.11.0 takes "today" from the database clock and runs every scope it creates through a hook
you register.

- `SweepAsync(tenant, now)` becomes `SweepAsync(tenant)`.
- `SweepAsync(tenant, now, trigger)` is removed. Use
  `ExecuteAsync(RetentionSweepRequest.Tenanted(tenant))`.
- `RetentionSweepRequest` has no `trigger`: explicit sweeps are always `Manual`, and only the
  worker writes `Scheduled` runs.
- `RetentionSweepRequest.At` is optional and defaults to the database clock. A real (non-dry)
  sweep refuses a later time; see [Which "today"](#which-today).
- `RetentionPreviewRequest.At` and `PreviewAsync`'s time are optional and default to the
  database clock. Drop the `DateTimeOffset.UtcNow` you passed; keep a time only to look ahead.
- Register an `IRetentionExecutionScope`. Startup refuses a host without one. Without
  row-level security, register `RetentionExecutionScope.Unisolated`. See
  [Tenant Isolation](/guides/tenant-isolation).
- Erasure changes are listed under
  [Right-to-Erasure](/guides/erasure#upgrading-to-0-11-0).
- The worker catches up missed and unfinished occurrences, including on its first start, and
  records a `Failed` scheduled run for each tenant your source misses or returns with
  conflicting contexts. See [Occurrences](#occurrences) and [Tenants](#tenants).
- `KillSwitch: true` now cancels the pass in progress instead of letting it finish.
- Don't run an older worker beside this one: an older build doesn't take the pass lock, so
  the two could run the same pass. Turn the kill switch on, or scale to one replica, while
  the rollout runs.
:::

## Before you start the host

Apply EF Core migrations first. Startup validates the installed Cohort schema but never
changes it. See [Schema & Migrations](/reference/schema).
