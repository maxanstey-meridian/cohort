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
- Results report what a run left: `EntitySweepCount` and `SweepEvent.EntitySummary` gain
  `RetainedCount` (kept by a legal minimum or an Exempt rule), `DeferredCount` (still due but
  not reached) and `BlockedCount`; `SweepEvent.EntityProgress` gains `BlockedCount`. Every
  count on `EntitySweepCount` is now a required constructor argument. `ErasureResult.Complete`
  is true only when nothing of the subject is left.
- `sweep_run_entity_summary` gains `RetainedCount`, `DeferredCount` and `BlockedCount`
  (`bigint NOT NULL`). Hosts add a migration; readiness refuses to run until it's applied.
- A run whose `OnBeforeAsync` skipped rows settles `PartiallyFailed`, with a `rows-skipped`
  reason in `Error` and `EntityFailures`. An erasure that left rows deferred or blocked settles
  `PartiallyFailed` with `rows-missed` or `rows-blocked`. A sweep whose rows another
  transaction had locked counts them in `DeferredCount` and stays `Succeeded`.
  `EntityFailures` is empty exactly when the run succeeded.
- A row that can't be retired yet is blocked and counted, not fatal. A purge leaves out rows
  still referenced through a `RESTRICT` or `NO ACTION` foreign key (read from `pg_catalog`,
  so database-only keys count). Any other integrity-constraint rejection (SQLSTATE class 23)
  blocks only that row: Cohort splits the batch under savepoints, the rest commits, and a
  Warning names the constraint. A full batch in which every row is refused fails the entity.
  Batches Cohort owns check constraints immediately, so a deferred constraint blocks its row
  rather than failing the commit.
- `OnBeforeAsync` runs in a read-only savepoint that is always rolled back. A database error
  in a handler skips only its row instead of failing the batch, and a handler that writes
  through the sweep's `DbContext` now fails (`sqlstate:25006`). A handler that returns with
  unsaved changes (an `Add`, a changed entity or key, a navigation change, a failed
  `SaveChanges`) fails its row (`reason=on-before-unsaved-changes`), even with automatic
  change detection off. Cohort then clears its own context's change tracker. On a
  caller-transaction erasure's context it detaches what the handler added or loaded and
  restores what the caller tracked. If it can't undo the change completely, it clears the
  caller's change tracker and the erasure throws `InvalidOperationException`, so the caller's
  next save can't commit it.
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
  column; and any anonymise of a column a foreign key references. A host whose anonymise
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
