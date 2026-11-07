# History Pruning

Cohort's ledger and hold tables need retention too. Pruning is opt-in. Each retention is
independent, defaults to `null` (keep forever), and must be at least one day when set:

```json
{
  "Cohort": {
    "HistoryPruning": {
      "SucceededRunRetention": "90.00:00:00",
      "FailedRunRetention": "365.00:00:00",
      "InactiveHoldRetention": "90.00:00:00"
    }
  }
}
```

A hosted pruner runs a pass when the host starts and then every hour.

## Runs

A settled run is deleted, with its `sweep_run_entity_summary` and `sweep_run_row_detail`
rows, once its `SettledAt` is older than its retention:

- A run that settled `Succeeded` uses `SucceededRunRetention`.
- A run that settled `Failed`, `PartiallyFailed` or `Cancelled` uses `FailedRunRetention`.
  That lets you keep failure evidence longer, or shorter, than routine history. A run whose
  [dependents step](/guides/row-handlers#retiring-a-rows-dependents) skipped rows settles
  `PartiallyFailed`, so the row details that record why are kept this long too.

A run still `Started` is never deleted; abandoned-run recovery settles an abandoned run as
`Failed` first (see [Crash recovery](/misc/how-it-works#crash-recovery)).

## Holds

A hold is deleted once the earlier of its `RemovedAt` and `ExpiresAt` is older than
`InactiveHoldRetention`. Active holds are never deleted, including holds with no expiry.
`ListActiveAsync` and `HasActiveHoldAsync` only answer for now, so pruning never changes what
they report.

## How it runs

- Each batch of up to 100 runs, or 100 holds, commits on its own. Batches are claimed with
  `FOR UPDATE SKIP LOCKED`, so replicas prune different batches.
- A failed pass is logged and retried an hour later.
- `KillSwitch` stops pruning after the batch in progress.
- With a `Schedule` set, scheduled runs started at or after its most recent occurrence (on the
  database clock) are kept whatever their age: they're the worker's record of what that
  occurrence still owes, and pruning them would rerun passes that already succeeded.
- Pruning is otherwise independent of `Schedule` and `DryRun`.

## Privileges

The pruner runs as `System(HistoryPruning)`, and that role needs exactly the privileges the
statements of the retentions you set send:

- `SucceededRunRetention` or `FailedRunRetention`: `SELECT`, `UPDATE`, and `DELETE` on
  `sweep_run` (the batch is claimed `FOR UPDATE SKIP LOCKED`); `SELECT` and `DELETE` on
  `sweep_run_row_detail` and `sweep_run_entity_summary`.
- `InactiveHoldRetention`: `SELECT`, `UPDATE`, and `DELETE` on `retention_holds`.

A host that sets only `InactiveHoldRetention` needs no `DELETE` on the run tables: the pruner
prunes holds and leaves runs alone. Readiness doesn't check these privileges. A pass whose
role lacks one fails at the statement with PostgreSQL's `42501`, logged as a failed pass and
retried an hour later, while sweeps and the other operations carry on. See
[Startup Validation](/reference/startup-validation#statements).
