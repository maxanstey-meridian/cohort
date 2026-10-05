# How It Works

Cohort is a reflection-driven library over EF Core metadata and raw PostgreSQL. It doesn't
load entities into memory to decide their fate. Eligibility, tenancy and holds are all
expressed in SQL.

## At startup

1. **Scan.** Reflection finds `[Retain]` entities in your `DbContext` model, resolves their
   key, tenant, anchor and marker columns from attributes and conventions, and reads table and
   column names from EF metadata.
2. **Validate.** The scanned model is checked against the strategies your rule provider
   declares, the mapping shapes Cohort supports, and the installed schema. See
   [Startup Validation](/reference/startup-validation).

## A run

1. **Lock and start.** The run takes its own advisory lock, then commits a `Started` row in
   `sweep_run`, with `SettledAt = NULL`, before any mutation.
2. **Order.** Retained entities are processed in foreign-key order, dependents before their
   principals, so a retained child is purged before its retained parent.
3. **Resolve.** For each entity, the category's rule is resolved for this tenant and time.
   The cutoff is `now - max(Period, LegalMin)`.
4. **Batch.** Candidate rows past the cutoff, in the tenant and not held, are selected with
   `FOR UPDATE SKIP LOCKED` in batches of `SweepBatchSize`. Each batch runs any `OnBeforeAsync`
   handlers, mutates, writes its audit rows and queues handler work, then **commits on its
   own**. A large backlog never sits in one unbounded transaction, and a failure loses only
   the current batch.
5. **Settle.** A terminal event sets `Status` to `Succeeded`, `PartiallyFailed`, `Failed` or
   `Cancelled` and records `SettledAt`.

Along the way:

- One entity's failure is recorded on the run (`PartiallyFailed` plus `Error`, and in
  `EntityFailures` on the result), and the run carries on with the remaining entities.
- Rows skipped by a failing `OnBeforeAsync` are excluded from the run's remaining batches.
  They're dead-lettered once and left for the next run, so a failing row can't spin the batch
  loop or block the eligible rows behind it. A batch that makes no progress at all stops the
  loop for that entity.
- `HeldCount` is measured directly, by counting rows past the cutoff with an active hold. It
  isn't inferred from candidate arithmetic.

## Crash recovery

A run left `Started` by a crashed process is marked failed after
`RowHandlerDispatch:SweepSettleTimeout`, by the dispatcher. It only does that once it can take
the run's lock, so a live run is never recovered.

## After commit

Two things happen once a batch commits:

- **Audit observers** receive the committed events. This is best effort and in-process. See
  [Audit Trail](/guides/audit-trail).
- **The dispatcher** claims queued handler work from `sweep_row_handler_status` and runs
  `OnAfterAsync`. This is durable and at-least-once. See [Row Handlers](/guides/row-handlers).

## Hosted services

`AddCohort` registers three background services:

| Service | Does |
|---|---|
| Worker | Runs scheduled sweeps when `Schedule` is set |
| Dispatcher | Delivers row-handler work, recovers abandoned runs, and scrubs expired snapshots |
| History pruner | Prunes old runs and inactive holds when [configured](/guides/history-pruning) |

Each one is idle until its feature is configured or has work to do.
