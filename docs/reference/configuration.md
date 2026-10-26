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
    "AuditObservers": {
      "Timeout": "00:00:05"
    },
    "RowHandlerDispatch": {
      "ClaimTimeout": "00:05:00"
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
| `KillSwitch` | `false` | Cancel the pass in progress (its current batch rolls back and the run settles `Cancelled`) and skip the remaining passes and future occurrences. Turning it off catches up the most recent occurrence straight away. It also stops [history pruning](/guides/history-pruning) after the batch in progress, and the [row handler](/guides/row-handlers) dispatcher's background poller after the batch it's running (`FlushAsync` and `RequeueDeadLetteredAsync` still run). |
| `OccurrenceRetryDelay` | `00:15:00` | How long the worker waits before retrying an occurrence's unfinished passes, or the next occurrence if that comes sooner. See [Occurrences](/guides/running-retention#occurrences). Greater than zero, at most 1 day. |
| `OccurrenceMaxAttempts` | `3` | Runs one pass of an occurrence gets before the worker leaves it to the next occurrence. Cancelled runs don't count. 1 to 100. |
| `TimeZone` | `UTC` | Time zone id (IANA, such as `Europe/London`; Windows ids are accepted where ICU is available) whose calendar [retention periods](/guides/retention-rules#periods-are-calendar-periods) are counted in. Cohort counts each period back from today's date in this zone and keeps rows anchored on or after the day that lands on. Scheduled sweeps, explicit sweeps, preview and erasure all use it. Must be a zone the host knows (Linux images need tzdata). The schedule stays UTC. |
| `SweepBatchSize` | `5000` | Maximum rows selected, locked and mutated per transaction. Each batch commits independently. At least 1. |
| `AuditObservers:Timeout` | `00:00:05` | Maximum time Cohort waits for each observer to handle one committed event. Each observer has its own timeout. At most 1 hour. |

See [Running Retention](/guides/running-retention) for how the worker uses them.

## Row handler dispatch

| Key | Default | Description |
|---|---|---|
| `RowHandlerDispatch:PollInterval` | `00:00:10` | Delay between dispatcher polling passes. |
| `RowHandlerDispatch:BatchSize` | `50` | Upper bound on queued handler statuses claimed per poll. The dispatcher claims at most `min(MaxParallelism, BatchSize)`, so every claim starts running at once. Valid range: 1 to 10000. |
| `RowHandlerDispatch:MaxParallelism` | `4` | Maximum rows dispatched concurrently. Handlers for one row stay in order. Valid range: 1 to 256. |
| `RowHandlerDispatch:MaxAttempts` | `25` | Maximum delivery attempts before dead-lettering. With the defaults, 25 attempts take up to about 17.5 hours: every backoff wait plus every attempt's `ClaimTimeout`. A [requeue](/guides/row-handlers#requeuing-dead-lettered-work) resumes the attempts a handler has left, or gives one more delivery to a handler that used them all. Valid range: 1 to 1000. |
| `RowHandlerDispatch:BaseBackoff` | `00:00:05` | Wait after the first failed attempt. Each later failure doubles it, up to `MaxBackoff`. |
| `RowHandlerDispatch:MaxBackoff` | `01:00:00` | Longest wait between two attempts. Valid range: `BaseBackoff` to 1 day. |
| `RowHandlerDispatch:ClaimTimeout` | `00:05:00` | Lease on one delivery. The handler is cancelled at nine tenths of it. In-flight work is reclaimed after it only once the dispatcher that claimed it has gone, as after a crash; a handler still running keeps its claim. If the host died outright, the server notices only when its TCP keepalive gives up (`tcp_keepalives_idle`, `tcp_keepalives_interval`, `tcp_keepalives_count`; about 2 hours with Linux defaults). Valid range: 30 seconds to 1 day. |
| `RowHandlerDispatch:SweepSettleTimeout` | `01:00:00` | Age after which an unowned `Started` run is recovered as failed. At least 1 minute. |
| `RowHandlerDispatch:PayloadRetention` | `30.00:00:00` | Backstop retention for captured row snapshots, which can contain personal data, counted from the row's mutation. Dead-lettered work keeps its snapshot until then, so it can be requeued. At least 1 hour, and longer than one handler's retry schedule: every backoff wait plus `MaxAttempts` times `ClaimTimeout` (about 17.5 hours with the defaults). With row handlers registered, it must also be longer than N times (that schedule plus `MaxAttempts` times `PollInterval`), N being the most handlers on one entity, since a row's handlers run one after another. `AfterSweepSettled` work waiting for a slow run, and work paused by the kill switch, can still outlast it. |

See [Row Handlers](/guides/row-handlers).

## History pruning

| Key | Default | Description |
|---|---|---|
| `HistoryPruning:SucceededRunRetention` | `null` | How long a succeeded run whose handler work all succeeded is kept after it settled. `null` keeps it forever. |
| `HistoryPruning:FailedRunRetention` | `null` | How long a `Failed`, `PartiallyFailed` or `Cancelled` run, or a succeeded run with dead-lettered handler work, is kept after it settled. `null` keeps it forever. |
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
