# Changelog

Breaking changes come with upgrade steps in the guide that covers them.

## 0.11.0 (unreleased, breaking)

Cohort tells the host whose work it is doing, and closes paths where rows were deleted or left
unprotected without an error.

- Hosts register an `IRetentionExecutionScope`; startup refuses a host without one. Cohort
  runs every scope's work through its `RunAsync(context, work, ct)`, with `Tenant(id)` or a
  named `System(work)`, so a host's row-level security covers sweeps, previews, erasures,
  row-handler delivery, pruning and readiness. Whatever the host sets around the work (its
  tenant, an `AsyncLocal` such as a Sluice flow) encloses all of it; a hook that skips the work,
  runs it twice or doesn't await it is refused, and a failure it swallows is rethrown. Each scheduled worker pass runs in its own scope, as its
  tenant. Hosts without isolation register `RetentionExecutionScope.Unisolated`. Apply
  isolation when a connection opens: Cohort runs some statements outside EF's command pipeline.
- `schema.md` documents Cohort's tenant columns and sentinels (`Guid.Empty` runs, `NULL`
  holds) as a schema contract for host row-level-security policies. Tenant scopes must be
  able to read `NULL`-tenant holds; `IRetentionDeletion` refuses to run in a scope that can't
  see an active one `System(Readiness)` sees.
- `IRetentionErasureService` is scoped and takes a `RetentionErasureRequest`
  (`Tenanted`/`Tenantless`); `EraseAsync(tenant, scope, ct)` no longer takes `now`. Subject
  columns on `[RetentionTenantless]` entities are erased by a tenantless request instead of
  never. A kind only tenantless entities declare is refused for a tenanted erasure.
- An erasure can join the caller's open READ COMMITTED transaction
  (`ErasureTransaction.Caller`): the erasure and its ledger commit or roll back with the
  caller's changes, a rollback (including to a savepoint taken before the erasure) leaves no
  ledger row, and observers run only after commit. A tenantless request always runs in its
  own transaction. `AddCohort` adds a transaction interceptor to the context's options.
- Sweeps and erasures use the database clock unless given a time, and refuse a later time for a
  run that changes rows. `RetentionSweepRequest.At` and `RetentionErasureRequest.At` are
  nullable; `SweepAsync(tenant, now)` becomes `SweepAsync(tenant)`, and
  `SweepAsync(tenant, now, trigger)` is removed (use `ExecuteAsync` with a request). A run's
  event times and the `sweep_run`, summary and row-detail times come from the database clock
  too; `DeletedAt`/`AnonymisedAt` markers keep the run's time.
- `RetentionPreviewRequest.At` and `PreviewAsync`'s time are optional and default to the database
  clock; a preview may still look ahead.
- `ListActiveAsync` and `HasActiveHoldAsync` lose `asOf` and answer for now on the database
  clock, the clock mutations use. Point-in-time answers could be wrong once history pruning
  had removed holds.
- `RetentionHoldRequest` refuses an `expiresAt` that is not after `createdAt`, and
  `CreateAsync` refuses one the database clock has already reached.
- `IRetentionDeletion` refuses a context with unsaved changes, a callback that returns without
  saving, and a callback that deletes any held row (unlisted, cascaded or raw SQL). A refusal
  rolls back and clears the change tracker.
- Readiness refuses to run while an active hold names a `RetentionEntityId` the model no longer
  registers; such holds used to stop protecting their rows silently. Re-point or end the holds
  (see `schema.md`), or restore the identity. One Cohort table set per EF model: give separate
  models their own schema with `ConfigureCohortTables(schema)`.
- Every transaction in which Cohort changes rows or checks holds states READ COMMITTED
  instead of relying on the driver.
- The worker catches up. On start, and after a failed pass or iteration, it reruns the
  unfinished passes of the most recent occurrence, `OccurrenceRetryDelay` apart (default 15
  minutes), until each has succeeded or used `OccurrenceMaxAttempts` runs (default 3; a
  cancelled run doesn't count). A missed occurrence, including on a fresh install, is swept
  when the host starts. Passes that succeeded never rerun. A `Failed` or `PartiallyFailed`
  run no longer claims its occurrence, and a crash between tenant passes no longer leaves
  the later tenants unswept until the next occurrence. An occurrence is due once the
  database clock reaches it, so a replica whose clock runs ahead no longer sweeps early and
  then reruns passes its ledger can't see.
- The worker checks its tenant source against the data each occurrence. A tenant that owns
  retained rows but isn't returned by `IRetentionTenantSource`, or that the source returns
  with conflicting contexts, gets a `Failed` scheduled run (`code=cohort:tenant-not-in-source`
  or `code=cohort:conflicting-tenant-contexts`) instead of being skipped or swept under the
  first context. Rows under a `Guid.Empty` tenant in a tenanted table log an error. The
  empty-source and conflicting-context warnings are gone.
- The kill switch now cancels the scheduled pass in progress instead of letting it finish:
  its current batch rolls back, the run settles `Cancelled`, and the rest of the occurrence is
  skipped. Turning it off catches the occurrence up straight away. It also stops the
  row-handler dispatcher's background poller after the batch it's running; `FlushAsync`,
  `RequeueDeadLetteredAsync` and the payload-retention scrub still run.
- Results report what a run left: `EntitySweepCount` and `SweepEvent.EntitySummary` gain
  `RetainedCount` (kept by a legal minimum or an Exempt rule), `DeferredCount` (still due but
  not reached) and `BlockedCount`; `SweepEvent.EntityProgress` gains `BlockedCount`. Every
  count on `EntitySweepCount` is now a required constructor argument. `ErasureResult.Complete`
  is true only when nothing of the subject is left.
- `sweep_run_entity_summary` gains `RetainedCount`, `DeferredCount` and `BlockedCount`
  (`bigint NOT NULL`). Hosts add a migration and hand-add a step that drops the defaults EF's
  scaffold backfills with, so a 0.10 host refuses to start against it instead of recording 0 for counts it
  never measured; readiness refuses to run until it's applied, and while any of the three
  still has a default.
- A run whose `OnBeforeAsync` skipped rows settles `PartiallyFailed`, with a `rows-skipped`
  reason in `Error` and `EntityFailures`. An erasure that left rows deferred or blocked settles
  `PartiallyFailed` with `rows-missed` or `rows-blocked`. A sweep whose rows another
  transaction had locked counts them in `DeferredCount` and stays `Succeeded`.
  `EntityFailures` is empty exactly when the run succeeded.
- A row that can't be retired yet is blocked and counted, not fatal. A purge leaves out rows
  still referenced through a `RESTRICT` or `NO ACTION` foreign key (read from `pg_catalog`,
  so database-only keys count). Any other integrity-constraint rejection (SQLSTATE class 23)
  blocks only that row: Cohort splits the batch under savepoints, the rest commits, and a
  Warning names the constraint. A full batch in which every row is refused by the same
  non-foreign-key constraint fails the entity; rows foreign keys refuse, or a mix of
  constraints, are blocked and the run moves on, unless the key refuses the value an
  anonymise writes (a static factory's missing key, say), which fails the entity.
  Batches Cohort owns check constraints immediately, so a deferred constraint blocks its row
  rather than failing the commit.
- `OnBeforeAsync` runs in a read-only savepoint that is always rolled back. A database error
  in a handler skips only its row instead of failing the batch, and a handler that writes
  through its `DbContext` now fails (`sqlstate:25006`). Handlers get a `DbContext` of their
  own, from a DI scope Cohort creates for each batch that has handlers, on the run's
  connection and transaction; whatever they track is discarded after each row, and the
  context running the sweep (your own in a caller-transaction erasure) is never given to a
  handler. A handler that returns with unsaved changes (an `Add`, a changed entity or key, a
  navigation change, a failed `SaveChanges`) fails its row
  (`reason=on-before-unsaved-changes`), even with automatic change detection off.
- In a caller-transaction erasure, a handler's other scoped services are fresh instances
  from the handler scope, not the caller's, so caller-scoped state such as a scoped "current
  user" isn't there; what your `IRetentionExecutionScope` sets up, or an `AsyncLocal`, still
  flows. The execution scope now also runs for each such batch.
- Registering an `IRetentionHandler<T>` that `AddRowHandler` didn't (by type without it; as
  a singleton, transient or instance, even before `AddRowHandler`; by factory, key or open
  generic `IRetentionHandler<>`), or registering one handler twice, fails startup and names the handler; on
  a host that was never started, every Cohort operation refuses it. An open-generic handler
  that used to run for every entity is refused: register it per entity.
- Row handlers need `[RowHandlerId("uuid")]`, and `AddRowHandler` loses its `identity`
  parameter: it refuses a handler without the attribute, or one whose id another handler of
  the same entity carries. Queued work is persisted and delivered under that id, never the
  class name, so renaming a handler no longer dead-letters its work; work under an id no
  handler carries is dead-lettered as `handler id … is not registered; re-register it and
  requeue`. Upgrade step: work a 0.10 host queued under a type name (any handler registered
  without `identity`) must be drained or re-keyed. Either run `FlushAsync` on 0.10 until the
  queue is settled, or run
  `UPDATE <schema>.sweep_row_handler_status SET "HandlerType" = '<uuid>' WHERE "HandlerType" = '<persisted type name>' AND "State" IN (0, 1, 3);`
  for each such handler: re-keyed pending work then runs as usual. Work a 0.11 dispatcher
  already dead-lettered as unregistered keeps its snapshot, so after the same `UPDATE`,
  `IRetentionRowDispatcher.RequeueDeadLetteredAsync(handlerId: <uuid>)` delivers it. Work
  0.10 dead-lettered has usually lost its snapshot and can't be requeued. A handler that
  already passed `identity` keeps its work by putting the same UUID in its attribute.
- Dead-lettered `OnAfterAsync` work keeps its snapshot until
  `RowHandlerDispatch:PayloadRetention`, and the new
  `IRetentionRowDispatcher.RequeueDeadLetteredAsync(sweepId, handlerId)` requeues it: every
  dead-lettered handler of each matching row, so they still run in order, for one more
  delivery each, with `Attempt` carried on. Each dead-letter logs an Error.
  `RowDispatcherFlushResult` gains a third positional value, `DeadLetteredRequeueable`, the
  dead-lettered work that can still be requeued. History pruning keeps a run until none of
  its rows holds a snapshot.
- `FlushAsync` honours retry backoff: it runs what's due now, so one flush spends one attempt
  per row instead of retrying back-to-back until the work dead-letters. Re-flush until
  `Settled` where you relied on the old behaviour. Retries back off to a new
  `RowHandlerDispatch:MaxBackoff`, and the defaults change: `BaseBackoff` 5 seconds (was 1),
  `MaxBackoff` 1 hour, `MaxAttempts` 25 (was 10), about 15 hours of retries. Startup refuses
  a `PayloadRetention` shorter than a handler's retry schedule (every backoff wait plus
  `MaxAttempts` times `ClaimTimeout`), since the scrub would end work still retrying.
- A row-handler delivery whose claim expires is reclaimed only once its dispatcher's session
  has gone: the dispatcher holds a session advisory lock for each delivery, so a handler that
  ignores cancellation no longer overlaps its own retry or its row's next handler. Session
  advisory locks need direct connections or session-mode pooling, not transaction-mode
  pooling, and each draining dispatcher holds one extra connection per batch.
- `ErasureTransaction.Caller` and `IRetentionDeletion` detect changes before refusing a
  context with unsaved changes, so an edit made with automatic change detection off is
  refused too.
- A handler's or rule provider's own cancellation (an `HttpClient` timeout, say) fails only
  its row or entity; only the run's own cancellation stops the run.
- A dry-run erasure doesn't settle `PartiallyFailed` for rows it predicts would be left; read
  `Complete`. Rows a foreign key keeps from a purge get one Warning per entity naming the keys.
- Readiness refuses a retained entity whose table or record-id column the database doesn't
  have, whatever its strategy: every run would fail.
- Readiness refuses an anonymise its column can never take: an anonymised column the
  database no longer has; a model store type the column type can't be assigned to without an
  explicit cast (any method, `Null` included); a literal or empty string, after its value
  converter, that the model's type or the column type rejects (length, type or domain CHECK),
  or that the converter throws on; `Null` into a column the database declares NOT NULL,
  directly or through nested domains; one shared value (`FixedLiteral`, `EmptyString`, a
  `Static` factory, or `Null` under `NULLS NOT DISTINCT`) on a unique index with no filter
  whose key lacks the record id (`INCLUDE` columns don't count); a literal on a foreign-key
  column; and any anonymise of a column a foreign key references, or a soft-delete flag,
  deleted-at or anonymised-at marker one references. A host whose anonymise
  failed on every run now fails at startup.
- Cohort requires PostgreSQL 16 or later and refuses an older server with its own message.

Upgrade: [Running Retention](https://maxanstey-meridian.github.io/cohort/guides/running-retention#upgrading-to-0-11-0),
[Right-to-Erasure](https://maxanstey-meridian.github.io/cohort/guides/erasure#upgrading-to-0-11-0),
[Legal Holds](https://maxanstey-meridian.github.io/cohort/guides/legal-holds#upgrading-to-0-11-0),
[Schema](https://maxanstey-meridian.github.io/cohort/reference/schema#upgrading-to-0-11-0)
and [Tenant Isolation](https://maxanstey-meridian.github.io/cohort/guides/tenant-isolation).

## 0.10.0 (2026-10-22, breaking)

Retention periods are calendar periods.

- `RetentionRule.Period` and `LegalMin` are a `RetentionPeriod(Years, Months, Days)`
  instead of a `TimeSpan`, so "7 years" means seven calendar years rather than 2,555 days.
- Cutoffs fall at the start of a day in the new `Cohort:TimeZone` (default `UTC`). A row is
  kept at least through its anniversary day. Periods shorter than a day are gone.
- `SweepEvent.EntitySummary/EntityProgress.ResolvedPeriod` are `RetentionPeriod`.
- `sweep_run_entity_summary.ResolvedPeriod` is replaced by `ResolvedMonths` and
  `ResolvedDays`. Hosts add a migration; readiness refuses to run until it's applied, and
  refuses any required column on a Cohort table that Cohort doesn't write.

Upgrade: [Retention Rules](https://maxanstey-meridian.github.io/cohort/guides/retention-rules#upgrading-to-0-10-0)
and [Audit Trail](https://maxanstey-meridian.github.io/cohort/guides/audit-trail#upgrading-to-0-10-0).

## 0.9.0 (2026-10-09, breaking)

Erasure subjects have a kind.

- `[ErasureSubject(kind)]` and `new ErasureScope(kind, subject, …)` take a required kind. An
  erasure matches only columns of its kind.
- Startup requires one CLR type per kind. Erasure refuses an undeclared kind or a wrong-type
  subject before a run exists.
- `sweep_run.ErasureSubjectKind` records the kind, never the subject. Hosts add a migration.

Upgrade: [Right-to-Erasure](https://maxanstey-meridian.github.io/cohort/guides/erasure#upgrading-to-0-9-0).
