# Changelog

Breaking changes come with upgrade steps in the guide that covers them.

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
