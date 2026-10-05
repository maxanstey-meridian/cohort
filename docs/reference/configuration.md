# Configuration

Cohort binds the `Cohort` configuration section to `CohortOptions`. Invalid options fail
startup.

```json
{
  "Cohort": {
    "Schedule": "0 2 * * *",
    "DryRun": false,
    "KillSwitch": false,
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
| `KillSwitch` | `false` | Stop between tenant passes: the tenant in progress finishes, and the remaining tenants and future occurrences are skipped. It also stops [history pruning](/guides/history-pruning) after the batch in progress. |
| `SweepBatchSize` | `5000` | Maximum rows selected, locked and mutated per transaction. Each batch commits independently. At least 1. |
| `AuditObservers:Timeout` | `00:00:05` | Maximum time Cohort waits for each observer to handle one committed event. Each observer has its own timeout. At most 1 hour. |

See [Running Retention](/guides/running-retention) for how the worker uses them.

## Row handler dispatch

| Key | Default | Description |
|---|---|---|
| `RowHandlerDispatch:PollInterval` | `00:00:10` | Delay between dispatcher polling passes. |
| `RowHandlerDispatch:BatchSize` | `50` | Upper bound on queued handler statuses claimed per poll. The dispatcher claims at most `min(MaxParallelism, BatchSize)`, so every claim starts running at once. Valid range: 1 to 10000. |
| `RowHandlerDispatch:MaxParallelism` | `4` | Maximum rows dispatched concurrently. Handlers for one row stay in order. Valid range: 1 to 256. |
| `RowHandlerDispatch:MaxAttempts` | `10` | Maximum delivery attempts before dead-lettering. Valid range: 1 to 1000. |
| `RowHandlerDispatch:BaseBackoff` | `00:00:01` | Base delay for exponential retry backoff. |
| `RowHandlerDispatch:ClaimTimeout` | `00:05:00` | Lease on one delivery. The handler is cancelled at nine tenths of it, and in-flight work abandoned by a crash is reclaimed after it. Valid range: 30 seconds to 1 day. |
| `RowHandlerDispatch:SweepSettleTimeout` | `01:00:00` | Age after which an unowned `Started` run is recovered as failed. At least 1 minute. |
| `RowHandlerDispatch:PayloadRetention` | `30.00:00:00` | Backstop retention for captured row snapshots, which can contain personal data. At least 1 hour. |

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
