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
   The cutoff is the start of the day `Period` or `LegalMin` (whichever reaches further back)
   before today, counted in calendar years, months and days in the host's `TimeZone`.
4. **Batch.** Candidate rows past the cutoff, in the tenant and not held, are selected with
   `FOR UPDATE SKIP LOCKED` in batches of `SweepBatchSize` (an erasure waits for locked rows
   instead). A purge also leaves out rows still referenced through a `RESTRICT` or
   `NO ACTION` foreign key, read from `pg_catalog`. Each batch runs any `OnBeforeAsync`
   handlers, each in a read-only savepoint that is always rolled back, mutates, writes its
   audit rows and queues handler work, then **commits on its own**. A large backlog never
   sits in one unbounded transaction, and a failure loses only the current batch. A batch
   checks its constraints immediately, so a deferred constraint fails at its statement, not
   at `COMMIT`.
5. **Settle.** A terminal event sets `Status` to `Succeeded`, `PartiallyFailed`, `Failed` or
   `Cancelled` and records `SettledAt`.

Along the way:

- One entity's failure is recorded on the run (`PartiallyFailed` plus `Error`, and in
  `EntityFailures` on the result), and the run carries on with the remaining entities.
- Rows skipped by a failing `OnBeforeAsync` are excluded from the run's remaining batches.
  They're dead-lettered once and left for the next run, so a failing row can't spin the batch
  loop or block the eligible rows behind it. A batch that makes no progress at all stops the
  loop for that entity. Skipped rows make the run `PartiallyFailed`, with a `rows-skipped`
  reason in `Error` and `EntityFailures`.
- A row an integrity constraint refuses (any SQLSTATE class 23 error: a foreign key, a check,
  a unique index, a constraint trigger) is blocked, not fatal. The batch's statement is
  bisected under savepoints until the refused rows are found, or on the row-by-row path each
  row's change has its own savepoint. The rest of the batch commits, blocked rows are left out
  of the run's remaining batches, and a Warning names the constraint (never the row). Rows
  left out up front because a foreign key still references them get one Warning per entity
  naming the keys that can block it. A full
  batch in which every row is refused fails the entity instead, since the cause is then the
  rule or the schema, unless the refusals are foreign-key refusals (`23503`, `23001`: they
  depend on each row's children) or differ between rows. Those rows are only blocked, and
  the run moves on to the next batch.
- `HeldCount` and `BlockedCount` are measured directly: rows past the cutoff with an active
  hold, and rows still referenced through a blocking foreign key, plus the rows a constraint
  refused during the run. Neither is inferred from candidate arithmetic. A sweep's blocked
  rows leave it `Succeeded`, like held rows, and the next run tries them again; an erasure
  with blocked rows settles `PartiallyFailed` with a `rows-blocked` reason.
- Once an entity's batches are done, the rows still due that the run neither changed nor
  skipped are counted once, in `DeferredCount`: rows another transaction had locked (a sweep
  skips them), rows the loop stopped before reaching, and rows that came into scope
  mid-run. A sweep's next run retires them, so they leave it `Succeeded`; an erasure that
  missed rows settles `PartiallyFailed` with a `rows-missed` reason.
- An erasure also reports what its rule keeps in `RetainedCount` (rows younger than the
  legal minimum, or every row under an `Exempt` rule), and `ErasureResult.Complete` says
  whether anything of the subject was left. See
  [What still blocks erasure](/guides/erasure#what-still-blocks-erasure).

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
