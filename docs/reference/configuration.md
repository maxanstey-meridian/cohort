# Configuration

Cohort binds the `Cohort` configuration section to `CohortOptions`. Invalid options fail
startup.

```json
{
  "Cohort": {
    "Schedule": "0 2 * * *",
    "DryRun": false,
    "KillSwitch": false,
    "TimeZone": "Europe/London",
    "SweepBatchSize": 5000,
    "SweepSettleTimeout": "01:00:00",
    "AuditObservers": {
      "Timeout": "00:00:05"
    },
    "HistoryPruning": {
      "SucceededRunRetention": "90.00:00:00"
    }
  }
}
```

## Worker and sweeps

| Key | Default | Description |
|---|---|---|
| `Schedule` | `null` | Cron expression, evaluated in **UTC**. `null` disables the worker. |
| `DryRun` | `false` | Run **scheduled** sweeps as count-only audited runs instead of mutating data. They keep the same run and entity audit trail, with `sweep_run.DryRun` set. Only the worker reads it: explicit requests carry their own flag (`RetentionSweepRequest.Tenanted(..., dryRun: true)`, `new ErasureScope(..., dryRun: true)`) and are honoured as written. |
| `KillSwitch` | `false` | Cancel the pass in progress (its current batch rolls back and the run settles `Cancelled`) and skip the remaining passes and future occurrences. Turning it off catches up the most recent occurrence straight away. It also stops [history pruning](/guides/history-pruning) after the batch in progress, and the background [abandoned-run recovery](/misc/how-it-works#crash-recovery) (an explicit `IRetentionRunRecovery.RecoverAsync` call still runs). |
| `OccurrenceRetryDelay` | `00:15:00` | How long the worker waits before retrying an occurrence's unfinished passes, or the next occurrence if that comes sooner. See [Occurrences](/guides/running-retention#occurrences). Greater than zero, at most 1 day. |
| `OccurrenceMaxAttempts` | `3` | Runs one pass of an occurrence gets before the worker leaves it to the next occurrence. Cancelled runs don't count. 1 to 100. |
| `TimeZone` | `UTC` | Time zone id (IANA, such as `Europe/London`; Windows ids are accepted where ICU is available) whose calendar [retention periods](/guides/retention-rules#periods-are-calendar-periods) are counted in. Cohort counts each period back from today's date in this zone and keeps rows anchored on or after the day that lands on. Scheduled sweeps, explicit sweeps, preview and erasure all use it. Must be a zone the host knows (Linux images need tzdata). The schedule stays UTC. |
| `SweepBatchSize` | `5000` | Maximum rows selected, locked and mutated per transaction. Each batch commits independently. At least 1. |
| `SweepSettleTimeout` | `01:00:00` | Age after which a `Started` run whose owner is gone (no process still holds its run lock) is recovered as `Failed`. See [Crash recovery](/misc/how-it-works#crash-recovery). At least 1 minute. |
| `AuditObservers:Timeout` | `00:00:05` | Maximum time Cohort waits for each observer to handle one committed event. Each observer has its own timeout. At most 1 hour. |

See [Running Retention](/guides/running-retention) for how the worker uses them.

## History pruning

| Key | Default | Description |
|---|---|---|
| `HistoryPruning:SucceededRunRetention` | `null` | How long a succeeded run is kept after it settled. `null` keeps it forever. |
| `HistoryPruning:FailedRunRetention` | `null` | How long a `Failed`, `PartiallyFailed`, or `Cancelled` run is kept after it settled. `null` keeps it forever. |
| `HistoryPruning:InactiveHoldRetention` | `null` | How long a removed or expired hold is kept after it stopped protecting. `null` keeps it forever. |

Each must be at least 1 day when set. See [History Pruning](/guides/history-pruning).

## Conventions

Cohort finds an entity's key, tenant and soft-delete columns by name. The defaults are:

| Key | Default | Column |
|---|---|---|
| `Conventions:RecordIdPropertyName` | `Id` | Record ID |
| `Conventions:TenantPropertyName` | `TenantId` | Tenant |
| `Conventions:SoftDeletePropertyName` | `IsDeleted` | Soft-delete flag |
| `Conventions:DeletedAtPropertyName` | `DeletedAt` | Soft-delete timestamp |
| `Conventions:AnonymisedAtPropertyName` | `AnonymisedAt` | Anonymisation marker |

```json
{
  "Cohort": {
    "Conventions": {
      "TenantPropertyName": "OrganisationId"
    }
  }
}
```

A [marker attribute](/reference/attributes#convention-overrides) on a property beats the
global convention, and the global convention beats the built-in default.
