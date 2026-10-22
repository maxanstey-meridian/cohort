# Audit Trail

Every sweep and erasure writes an authoritative ledger to Cohort's own tables. You can't
switch it off or replace it. It commits in the same transaction as the mutation it describes,
so the ledger and the data never disagree.

## The ledger

| Table | One row per |
|---|---|
| `sweep_run` | Run: trigger kind, erasure subject kind (erasures only, never the subject), status, dry-run flag, tenant, start and settle times, error |
| `sweep_run_entity_summary` | Entity, category, tenant and strategy within a run |
| `sweep_run_row_detail` | Mutated row, when per-row detail is on |

A run commits its `Started` row before any mutation. A terminal event then sets `Status` to
`Succeeded`, `PartiallyFailed`, `Failed` or `Cancelled` and records `SettledAt`.

Summary rows carry:

- category
- strategy
- affected count
- held count
- skipped count
- null-anchor count
- resolved period: whichever of the period and legal minimum set the cutoff (the legal
  minimum alone for an erasure, 0 and 0 when there is none), in two `integer` columns, `ResolvedMonths` and
  `ResolvedDays`. Years fold into months, so "7 years" is 84 months and 0 days.
- optional provenance, in `RuleSource` and `RuleReason`

Within a run, a summary is uniquely identified by its
[retention entity ID](/reference/schema#identities), category, tenant and strategy. Audit
rows also keep the human-readable CLR `EntityType`, but only as diagnostic metadata.
Tenantless runs are attributed to `Guid.Empty`, so summary keys stay unique.

### Per-row detail

Per-row detail is opt-in. Set it on the rule:

```csharp
new RetentionRule(new RetentionPeriod(Days: 30), Strategy.Purge, AuditRowDetail: AuditRowDetail.PerRow)
```

or on the entity, which overrides the rule:

```csharp
[Retain("session-notes", nameof(CreatedAt), AuditRowDetail = AuditRowDetail.PerRow)]
```

An entity left at the default `Inherit` follows its rule. A rule defaults to `SummaryOnly`.
Each row detail records the row's canonical `RecordId` and `TenantId`.

## Audit observers

To export the ledger elsewhere, such as a SIEM, a data warehouse or a compliance system,
register any number of `IRetentionAuditObserver`s:

```csharp
public sealed class AuditExporter : IRetentionAuditObserver
{
    public Task OnCommittedAsync(SweepEvent evt, CancellationToken ct) => evt switch
    {
        SweepEvent.EntitySummary summary => Export(summary, ct),
        _ => Task.CompletedTask,
    };
}
```

```csharp
services.AddSingleton<IRetentionAuditObserver, AuditExporter>();
services.AddCohort<AppDbContext>();
```

Observers receive `Started`, `EntityProgress`, `RowDetail`, `EntitySummary`, and then the
terminal `Completed`, `PartiallyFailed`, `Failed` or `Cancelled` event, in committed
lifecycle order.

- Events are delivered after commit. A batch's `RowDetail` and `EntityProgress` events wait
  until its mutation transaction commits, and rolled-back events are never delivered.
- Each observer is isolated and bounded by `AuditObservers:Timeout`. Exceptions and timeouts
  are logged, but never change committed data, the result or the run status.
- Delivery is best effort, with no durable outbox. A process crash can lose a notification
  after the database commit. Integrations that must not miss anything should poll the ledger
  tables, use CDC, or keep their own outbox.

`RowDetail` events contain the row's `RecordId` and `TenantId`. Treat observers and whatever
they send to as sensitive-data processors, and protect access, logs, queues and exports
accordingly.

## Failure diagnostics

When a failure comes from an external exception, Cohort persists and returns a sanitized
envelope rather than the exception text. It contains:

- the exception type
- a safe machine code when there is one (for example a PostgreSQL SQLSTATE)
- a random diagnostic ID

Cohort's own machine-safe reasons may be stored as plain text instead.

Only diagnostic `Error` and `LastError` text is sanitized. Exception messages, stack traces,
SQL values and subject identifiers are kept out of it, but the deliberately identifying
`RecordId` and `TenantId` fields stay in row-detail events. Structured logs keep the original
exception under the same diagnostic ID, so you can still diagnose it from protected logs.

The ledger grows forever unless you opt in to [history pruning](/guides/history-pruning).

## Upgrading to 0.10.0

::: warning Breaking change
`sweep_run_entity_summary.ResolvedPeriod` (`interval`) is replaced by two `integer` columns,
`ResolvedMonths` and `ResolvedDays`, because a period now carries months. Add a migration:
`dotnet ef migrations add` picks the change up from your model, and readiness refuses to run
until it's applied.

EF scaffolds it as a drop and two adds, which loses the period on existing summaries. Reorder
it to add the two columns, copy each period across, then drop `ResolvedPeriod`:

```csharp
migrationBuilder.Sql(
    """
    UPDATE "public"."sweep_run_entity_summary"
    SET "ResolvedMonths" = EXTRACT(YEAR FROM justify_hours("ResolvedPeriod"))::integer * 12
                         + EXTRACT(MONTH FROM justify_hours("ResolvedPeriod"))::integer,
        "ResolvedDays" = EXTRACT(DAY FROM justify_hours("ResolvedPeriod"))::integer;
    """);
```

Use your Cohort schema if it isn't `public`. Old summaries keep their day count as recorded:
a 0.9 rule of `TimeSpan.FromDays(2555)` becomes 0 months and 2,555 days, while the same rule
written as `Years: 7` records 84 months and 0 days from now on. Any part of a day an old period
carried is dropped.

Apply the migration before starting 0.10.0. A 0.9 host can't write summaries once it has
run, so don't mix versions across a rolling deploy. The code changes are listed under
[Retention Rules](/guides/retention-rules#upgrading-to-0-10-0).
:::
