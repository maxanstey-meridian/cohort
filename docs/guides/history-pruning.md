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

A settled run is deleted, with its `sweep_run_entity_summary`, `sweep_run_row_detail` and
`sweep_row_handler_status` rows, once its `SettledAt` is older than its retention:

- A run that settled `Succeeded`, with every handler delivery succeeded, uses
  `SucceededRunRetention`.
- A run that settled `Failed`, `PartiallyFailed` or `Cancelled`, or a succeeded run with
  dead-lettered handler work, uses `FailedRunRetention`. That lets you keep failure evidence
  longer, or shorter, than routine history.

A run is **never** deleted while any of its handler work is `Pending` or `InFlight`, however
old it is. A run still `Started` is never deleted either; the dispatcher recovers abandoned
runs as failed first.

## Holds

A hold is deleted once the earlier of its `RemovedAt` and `ExpiresAt` is older than
`InactiveHoldRetention`. Active holds are never deleted, including holds with no expiry.

`ListActiveAsync` and `HasActiveHoldAsync` with an `asOf` older than `InactiveHoldRetention`
can therefore answer "not held" for a hold that has since been pruned.

## How it runs

- Each batch of up to 100 runs, or 100 holds, commits on its own. Batches are claimed with
  `FOR UPDATE SKIP LOCKED`, so replicas prune different batches.
- A failed pass is logged and retried an hour later.
- `KillSwitch` stops pruning after the batch in progress.
- Pruning is otherwise independent of `Schedule` and `DryRun`.
