# Changelog

Breaking changes come with upgrade steps in the guide that covers them.

## 0.11.0 (unreleased, breaking)

Cohort tells the host whose work it is doing, and closes paths where rows were deleted or left
unprotected without an error.

- Hosts register an `IRetentionExecutionScope`; startup refuses a host without one. Cohort
  calls it with `Tenant(id)` or a named `System(work)` before any database work in every scope
  it creates, so a host's row-level security covers sweeps, previews, erasures, row-handler
  delivery, pruning and readiness. Each scheduled worker pass runs in its own scope, entered as
  its tenant. Hosts without isolation register `RetentionExecutionScope.Unisolated`. Apply
  isolation when a connection opens: Cohort runs some statements outside EF's command pipeline.
- `schema.md` documents Cohort's tenant columns and sentinels (`Guid.Empty` runs, `NULL`
  holds) as a schema contract for host row-level-security policies. Tenant scopes must be
  able to read `NULL`-tenant holds, or a held tenantless row looks unheld.
- `IRetentionErasureService` is scoped and takes a `RetentionErasureRequest`
  (`Tenanted`/`Tenantless`); `EraseAsync(tenant, scope, ct)` no longer takes `now`. Subject
  columns on `[RetentionTenantless]` entities are erased by a tenantless request instead of
  never. A kind only tenantless entities declare is refused for a tenanted erasure.
- An erasure can join the caller's open READ COMMITTED transaction
  (`ErasureTransaction.Caller`): the erasure and its ledger commit or roll back with the
  caller's changes, a rollback (including to a savepoint taken before the erasure) leaves no
  ledger row, and observers run only after commit. A tenantless request can't join the
  caller's transaction. `AddCohort` adds a transaction interceptor to the context's options.
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

Upgrade: [Running Retention](https://maxanstey-meridian.github.io/cohort/guides/running-retention#upgrading-to-0-11-0),
[Right-to-Erasure](https://maxanstey-meridian.github.io/cohort/guides/erasure#upgrading-to-0-11-0)
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
