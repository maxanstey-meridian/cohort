# Row Handlers

Deleting a row is often only half the job. There's a blob in storage, a search index entry,
or a downstream system that needs to hear about it. Row handlers let you hook into each
row Cohort mutates.

## Writing a handler

```csharp
[RowHandlerId("0b6f5e43-3d0e-4c2b-9a51-6f7f2a8c1e90")]
public sealed class DeleteAttachmentBlob(BlobContainerClient blobs) : IRetentionHandler<Attachment>
{
    public Task OnBeforeAsync(Attachment row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["BlobName"] = row.BlobName;
        return Task.CompletedTask;
    }

    public async Task OnAfterAsync(RetentionAfterContext<Attachment> ctx, CancellationToken ct)
    {
        var blobName = (string)ctx.Snapshot["BlobName"]!;
        await blobs.DeleteBlobIfExistsAsync(blobName, cancellationToken: ct);
    }
}
```

```csharp
builder.Services.AddRowHandler<Attachment, DeleteAttachmentBlob>();
```

`AddRowHandler` is the only way to register a handler. Cohort constructs it from each batch's
DI scope, so it can't be a singleton, transient, an instance, a factory, keyed or an open
generic. Startup refuses an `IRetentionHandler<T>` service in any other shape, including a
singleton or instance you registered before calling `AddRowHandler` (which makes its own
registration a no-op), or one registered twice, and names the handler type. On a host that
was never started, every Cohort operation refuses it instead.

Cohort supports handlers registered through `IServiceCollection` only. These checks read
the service collection, so a handler registered straight into a third-party container's own
builder (Autofac's `ContainerBuilder`, say) is out of their reach. For an entity that has no
`AddRowHandler` registration, it would never run while its rows are swept or erased.
Register handlers with `AddRowHandler`, even when a third-party container builds the
provider.

Both methods are optional. The interface supplies no-op defaults.

- **`OnBeforeAsync`** runs in a read-only savepoint inside the sweep transaction, before the
  row is mutated. Use it to capture what you'll need later into `ctx.Snapshot`. Handlers are
  resolved from a DI scope Cohort creates for each batch, run as the batch's tenant, and the
  `DbContext` they inject there is on the sweep's own connection and transaction. Their
  reads see the row as it's about to be mutated, along with any related rows, inside your
  transaction in a [caller-transaction erasure](/guides/erasure#erasing-inside-your-own-transaction).
  A write through that context fails the handler (`sqlstate:25006`), and so does any
  database error, without harming the rest of the batch: the savepoint is always rolled
  back. So does returning with unsaved changes on that context (an `Add`, a changed entity
  or key, or a navigation change never saved, recorded as `reason=on-before-unsaved-changes`).
  Cohort checks with an explicit `DetectChanges`, so a context with automatic change
  detection turned off is checked too. Whatever a handler leaves tracked, a failed
  `SaveChanges` included, is discarded after each row. The context that runs the sweep, your
  own in a caller-transaction erasure, is never given to a handler, so nothing a handler
  tracks reaches it. The handler's other scoped services come from that scope too: they're
  fresh instances, not the ones in the scope that started the run, so per-request state such
  as a scoped "current user" isn't there. State your
  [execution scope](/guides/tenant-isolation) sets up, or an `AsyncLocal` flow, still is. A
  cancellation the handler raises itself, such as an `HttpClient` timeout, is a failure like
  any other; only the run's own token stops the run. Writes that must commit with the row,
  such as deleting or blanking what depends on it, belong in a
  [dependents step](#retiring-a-rows-dependents), which runs after every `OnBeforeAsync`.
  External side effects still belong in `OnAfterAsync`: they survive a rollback,
  they run while the batch holds its row locks, and they can run for a row that's then
  withheld, for example by a hold created mid-sweep or a constraint that refuses the change.
  A write to the row being swept through another connection waits on the sweep's own row
  lock. If the handler throws, the row is left unmutated, the work is dead-lettered, and the
  row counts in `SkippedCount`. A run that
  skipped rows settles `PartiallyFailed`, with a
  `reason=rows-skipped;entityType=…;count=n` line in `EntityFailures` and the run's `Error`.
  The next run tries the row again.
- **`OnAfterAsync`** runs after commit, from a durable queue, with the snapshot you captured.
  This is where side effects belong.

Every handler carries `[RowHandlerId("uuid")]`, a permanent UUID that its queued work is
persisted and delivered under. `AddRowHandler` refuses a handler without one, with a malformed
one, or with one another handler class already uses, whatever its entity (a requeue by handler
id would otherwise reach both). Renaming or moving the class is safe; changing the id isn't.
Work queued under an id no registered handler carries is dead-lettered with
`handler id … is not registered; re-register it and requeue`, and work for an entity whose
`[RetentionEntityId]` the model no longer declares with
`entity id … is not registered; restore its [RetentionEntityId] and requeue`. Either keeps its
snapshot, so once the handler or the id is back, [requeue it](#requeuing-dead-lettered-work).

Upgrading from 0.10, a handler that passed `identity` to `AddRowHandler` keeps its queued work
by carrying the same UUID in `[RowHandlerId]`. A class registered for several entities with a
different `identity` each now carries one id, so the other identities' work dead-letters as
unregistered: split it into one class per entity, each with its old UUID, or re-key the other
identities' rows to the class's id (the `UPDATE` in the 0.11.0 changelog entry). Two classes
that shared one `identity` on different entities can't share it now: give one a new id and
move only its entity's work (the entity-scoped `UPDATE` in the same entry). Work queued
under a type name, by a handler registered without `identity`, must be drained on 0.10 or
re-keyed the same way.

When an entity has several handlers, they run in `[RowHandlerPriority(n)]` order, lowest
first. Handlers without the attribute run last, ordered by type name.

## Retiring a row's dependents

Some rows have dependents that must go with them, in the same transaction: child rows a
foreign key ties to the row, or copies of its values in an audit table. `OnBeforeAsync` is
read-only and `OnAfterAsync` runs after commit, so neither can do it. A dependents step can.

```csharp
[RowHandlerId("6c0e2b9a-41f7-4c3e-8d25-0b7a9f1e6d34")]
public sealed class RetireCaseDependents(AppDbContext db) : IRetentionDependents<Case>
{
    public async Task RetireAsync(Case row, RetentionDependentsContext ctx, CancellationToken ct)
    {
        await db.ExportDeliveries.Where(delivery => delivery.CaseId == row.Id).ExecuteDeleteAsync(ct);
        await db.CaseAuditEvents
            .Where(audit => audit.CaseId == row.Id)
            .ExecuteUpdateAsync(set => set.SetProperty(audit => audit.FieldChanges, (string?)null), ct);
    }
}
```

```csharp
builder.Services.AddRetentionDependents<Case, RetireCaseDependents>();
```

- **One step per entity.** `AddRetentionDependents` refuses a second class for an entity, so
  compose several kinds of dependents in one class. It's the only way to register a step:
  startup refuses an `IRetentionDependents<T>` service in any other shape, as it does a row
  handler's. The class carries a `[RowHandlerId]`, in the same id space as row handlers: no
  other handler or step class may use its id, though one class keeps its one id as a row
  handler and as a step, for every entity.
- **Order.** For each row: every `OnBeforeAsync` captures first, so captures see the database
  before anything is retired; then the step; then Cohort's own change to the row. The step
  runs read-write on the batch scope's `DbContext`, the one handlers get, on the run's
  connection and transaction and as the batch's tenant. Write only through that injected
  context, never another context or connection: those write outside the batch's transaction,
  so their writes commit even when the row is blocked or skipped, the hold check can't see
  them, and a delete of a row that references the swept row waits forever on the sweep's own
  lock on it. An entity with a step is always changed row by row, never in one bulk statement.
- **One savepoint.** The step and the row's change share a savepoint. If a constraint then
  refuses the change, for example because the row is still referenced, the rollback undoes
  the step too and the row counts as blocked (`BlockedCount`), so dependents never go without
  their row. The next run tries the row, and its step, again.
- **Failures skip the row.** Its writes are rolled back, and the row counts in `SkippedCount`
  (`rows-skipped`, so the run settles `PartiallyFailed`), with a row detail and a dead-lettered
  status under the step's id whose `LastError` says why. The step fails when it throws,
  including a database error or a constraint its own statements hit; when it returns with
  unsaved changes on its context (`reason=dependents-unsaved-changes`); when it changes a held
  row (`reason=dependents-changed-held-row`, below); or when it takes its own row out of the
  run's scope, so Cohort's change finds nothing (`reason=dependents-changed-row`). Only the
  run's own token stops the run. A step queues no work, so there is nothing to requeue: the
  next run tries the row again.
- **Holds.** Any change to, or removal of, a row under an active hold fails the step, whatever
  the row's entity, even if the step ends the hold first. An `UPDATE` that writes the same
  value counts as a change; a hold that merely expires or is released while the step runs
  doesn't fail it. Cohort compares
  every visible held row's physical version before and after the step, so the check is
  fail-safe rather than exact: a hold created anywhere the batch can see while the step runs,
  or another session's committed update to a held row, also fails it, and the next run retries
  the row. A Warning names the holds' ids, never the rows. Dependents' legal minimums aren't
  enforced; a hold is the row-specific protection the step honours, as
  [`IRetentionDeletion`](/guides/legal-holds#deleting-around-holds) does. When the model has a tenantless retained entity,
  a tenant's batch must be able to see the active `NULL`-tenant holds, or the entity fails
  rather than run a step that can't check them (see [Tenant Isolation](/guides/tenant-isolation)).
- **Foreign keys and previews.** A purge normally leaves out rows still referenced through a
  `RESTRICT` or `NO ACTION` foreign key. An entity with a step doesn't: its referenced rows are
  tried, the step runs, and a row still referenced after it is blocked by the constraint
  itself. Previews and dry runs, a dry-run erasure's `Complete` included, never run the step,
  so they predict that it clears the references: referenced rows count as eligible, not
  blocked. A row whose references the step can't clear runs the step once per run, rolled
  back each time.
- **Caller-transaction erasures.** In an erasure that joins your transaction, the step runs
  inside it, on its own context, never yours. Cohort doesn't set `SET CONSTRAINTS ALL
  IMMEDIATE` in your transaction, so a deferred constraint the step violates fails your
  `COMMIT`, as one Cohort's own change violates does. In Cohort's own batches constraints are
  immediate, so the same violation fails the step's statement (a skip) or the row's change (a
  block).
- **Locks.** The step runs while the batch holds its row and entity locks, so a slow step
  makes hold creation on those rows wait. A deadlock or lock timeout in the step fails that
  row's step, not the batch.

## Delivery

`OnAfterAsync` is delivered **at least once**. Make it idempotent: when `ctx.Attempt` is
greater than 1, the work might already have been done.

The dispatcher polls the queue, runs up to `MaxParallelism` rows at once, and keeps each
row's handlers in order. A failed attempt retries with exponential backoff: 5 seconds
(`BaseBackoff`), doubling up to 1 hour (`MaxBackoff`). After `MaxAttempts`, 25 by default or
up to about 17.5 hours of retries, the work is dead-lettered with a sanitized `LastError`, and the
row's later handlers with it. Each dead-letter logs an Error. The work keeps its snapshot, so
you can [requeue it](#requeuing-dead-lettered-work). All of these are
[configurable](/reference/configuration). The kill switch stops the background poller after
the batch it's running; `FlushAsync` and requeues still run.

Each delivery runs in its own DI scope, run as the row's tenant
(`System(TenantlessPass)` for tenantless rows); the handler is resolved and run inside that
work. `OnBeforeAsync` runs the same way, in a scope per batch run as the batch's tenant. So a
host's tenant isolation applies to handler code too. See [Tenant Isolation](/guides/tenant-isolation).

### Claim timeout

Each delivery holds its claim for `RowHandlerDispatch:ClaimTimeout`, five minutes by default.

- A handler must finish within nine tenths of the timeout. The rest covers claim latency and
  clock skew between hosts. At that point its cancellation token fires, and the delivery
  counts as a failed attempt.
- While a delivery runs, its dispatcher holds a PostgreSQL session advisory lock for it. A
  handler that ignores its token and outlives its claim isn't reclaimed while that lock is
  held: it keeps the row, and the row's later handlers wait, until it returns. Each pass logs
  a Warning naming it. Honour the token.
- Work left `InFlight` by a crash is reclaimed once the timeout passes and the crashed
  dispatcher's session has gone, and the reclaim counts as an attempt. A process that exits
  closes its session at once, but if the host dies outright (power loss, a VM gone) the server
  holds the idle session until its TCP keepalive gives up, about 2 hours with Linux defaults.
  Set the server's `tcp_keepalives_idle`, `tcp_keepalives_interval` and `tcp_keepalives_count`
  to shorten that; the same applies to recovering a crashed run.
- If the dispatcher's database session is lost while the handler still runs (a failover, an
  idle-session timeout), the lock goes with it, and the work can be reclaimed and run again.
  No lock can rule that out; fence downstream writes on `ctx.Attempt` if it matters.
- Session advisory locks need direct connections or session-mode pooling, not
  transaction-mode pooling or Npgsql `Multiplexing`, which readiness refuses. Each draining
  dispatcher holds one extra connection per batch.
- A delivery whose claim was reclaimed can't settle the row. Its outcome is discarded and the
  newer delivery settles it.
- If recording a delivery's outcome fails, for example on a transient database error, the
  delivery isn't treated as a handler failure and the rest of the batch carries on. The row
  stays `InFlight` with that attempt spent until its claim times out and is reclaimed.

### Dispatch phases

```csharp
services.AddRowHandler<Attachment, ArchiveAttachment>(RowHandlerDispatchPhase.AfterSweepSettled);
```

`Immediate` (the default) work dispatches as soon as its batch commits. `AfterSweepSettled`
work waits until its whole run records completion or failure. That suits work that should
only happen once the sweep's outcome is final. A run left `Started` by a process crash is
marked failed after `RowHandlerDispatch:SweepSettleTimeout`, which releases its work.

### Draining in tests and jobs

`IRetentionRowDispatcher.FlushAsync()` runs the work that's due now, without waiting for the
poller. It's handy in tests and one-shot jobs. It honours retry backoff: a failed attempt
waits out its backoff, so a flush during an outage spends one attempt per row, not the whole
budget. The result reports what's left: `PendingRemaining` (waiting out a backoff, a run that
hasn't settled, or a sibling), `InFlightRemaining` (held by a running delivery) and
`DeadLetteredRequeueable`. Because a flush runs only what's due, looping on it until `Settled`
spins through every backoff wait. To reach a final state, wait out the backoff, or in tests set
a short `BaseBackoff` and `MaxBackoff`, or `MaxAttempts` of 1.

### Requeuing dead-lettered work

Dead-lettered work keeps its snapshot until `PayloadRetention`, so once you've fixed the
cause (the downstream is back, or the handler is registered again) you can run it again:

```csharp
var requeued = await dispatcher.RequeueDeadLetteredAsync();                     // everything
await dispatcher.RequeueDeadLetteredAsync(sweepId: run.SweepId);                // one run
await dispatcher.RequeueDeadLetteredAsync(handlerId: Guid.Parse("0b6f5e43-…")); // one handler
```

- Every dead-lettered handler of each matching row is requeued, so the row's handlers still
  run in order. Filtering on a later handler requeues the earlier one it was waiting on too.
- A requeue resumes each handler's remaining attempts, or gives one more delivery to a
  handler that used them all. `Attempt` carries on, so `ctx.Attempt` still tells the handler
  the work may already be done.
- Work whose snapshot the backstop scrub already cleared can't be requeued.
  `FlushAsync`'s `DeadLetteredRequeueable` counts only work that can.
- The kill switch doesn't stop a requeue; the requeued work runs on the next flush, or once
  the poller is back on.

## Snapshots and personal data

Snapshots can hold pre-anonymisation personal data, so Cohort keeps them as briefly as it
can:

- A row's `CapturedPayload` is cleared as soon as every handler for that row succeeds.
  Dead-lettered work keeps its snapshot until `PayloadRetention`, so it can be requeued, and
  history pruning keeps the run until then.
- A backstop scrub clears anything older than `RowHandlerDispatch:PayloadRetention`, 30 days
  by default, counted from the row's mutation. Startup, and every options reload, refuses a
  `PayloadRetention` shorter than one handler's retry schedule, and, with handlers
  registered, shorter than the entity with the most handlers needs for all of them to run
  their schedules one after another, each attempt up to a `PollInterval` late (see
  [Configuration](/reference/configuration#row-handler-dispatch)). The scrub can still end
  work held up outside that schedule: `AfterSweepSettled` work waiting for a run that takes
  long to settle, and work paused by the kill switch. It still runs while the kill switch
  is on. Queued work whose snapshot the backstop scrubbed can never complete, so it
  dead-letters immediately, naming the scrub as the reason, instead of burning its retries.
- Dead-letter `LastError` stores a [sanitized diagnostic](/guides/audit-trail#failure-diagnostics):
  exception type, safe machine code when there is one, and a diagnostic ID. Never exception
  text or stack traces.

### What a snapshot can hold

Snapshot values must round-trip exactly: `OnAfterAsync` receives an equal value of the same
type. Supported values are:

- `null`
- well-known scalars
- enums and value-equal types from the allow-list below
- one-dimensional arrays and `List<T>` of those, with the element type preserved
- `object?[]`, `List<object?>`, and nested `IDictionary<string, object?>`

Anything else fails the handler that stashed it at capture time, before the row is mutated.
It takes the same path as a throwing `OnBeforeAsync`.

Persisted payloads name CLR types, so deserialisation is allow-listed. Values round-trip only
as well-known scalars, property types of the swept entity, or types declared in the entity's
or its registered handlers' assemblies. A tampered payload naming anything else dead-letters
instead of materialising an arbitrary type. The persisted entity type resolves only against
registered retained entities.
