# Cohort

Annotation-driven data retention for .NET and EF Core.

Cohort gives you a consistent way to say:

- this entity is retained
- this is how old it has to be before action is allowed
- this category should be purged, soft-deleted, or anonymised

From there it handles the awkward bits for you:

- finding eligible rows by age
- applying tenant predicates automatically
- respecting legal holds
- running purge, soft-delete, or anonymise mutations
- supporting immediate right-to-erasure with legal-minimum and active-hold blockers
- writing an audit trail of what happened

Postgres-only.

## What it is for

Use Cohort when you want retention to be part of your application model instead of a pile of ad hoc SQL jobs.

The core idea is simple:

1. annotate EF entities with retention metadata
2. map retention categories to rules
3. run preview, sweep, or erasure through Cohort

Annotations declare membership. Category rules declare policy. Cohort executes that policy safely.

## Example

Two cases:

1. purge short-lived operational data after 30 days
2. keep a business record, but anonymise personal fields after 365 days

```csharp
using Cohort.Application;
using Cohort.Domain;
using Cohort.Hosting;
using Cohort.Infrastructure.Migrations;

[Retain("session-notes", nameof(CreatedAt))]
[RetentionEntityId("a3f467fe-c5d0-4f17-9897-83c373cc1dc8")]
public sealed class SessionNote
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Body { get; set; } = "";
}

[Retain("case-contacts", nameof(CreatedAt))]
[RetentionEntityId("b7316df4-7db5-46ad-aea7-f65c4b430f73")]
public sealed class CaseContact
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? AnonymisedAt { get; set; }

    [Anonymise(AnonymiseMethod.Null)]
    public string? Email { get; set; }

    [Anonymise(AnonymiseMethod.EmptyString)]
    public string FullName { get; set; } = "";
}

public sealed class RetentionRules : IRetentionRuleProvider
{
    public RetentionCategoryCapabilities? GetCapabilities(string category) => category switch
    {
        "session-notes" => new([Strategy.Purge]),
        "case-contacts" => new([Strategy.Anonymise]),
        _ => null,
    };

    public Task<RetentionRule?> ResolveAsync(
        RetentionResolutionContext context,
        CancellationToken ct)
    {
        RetentionRule? rule = context.Category switch
        {
            "session-notes" => new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge),
            "case-contacts" => new RetentionRule(TimeSpan.FromDays(365), Strategy.Anonymise),
            _ => null,
        };

        return Task.FromResult(rule);
    }
}
```

Register Cohort and add its infrastructure tables to your EF model:

```text
builder.Services.AddSingleton<IRetentionRuleProvider, RetentionRules>();
builder.Services.AddCohort<MyDbContext>();
```

```text
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    base.OnModelCreating(modelBuilder);
    modelBuilder.ConfigureCohortTables();
}
```

After adding `ConfigureCohortTables()`, generate a host-owned EF Core migration containing all
five Cohort tables and apply it before startup or any direct Cohort operation. Cohort validates
the installed schema but never creates or upgrades it at runtime. Readiness requires the
columns, keys, unique indexes (not deferrable) and named CHECK constraints Cohort relies on; a
missing performance-only index is logged as a warning.

What happens:

- old `SessionNote` rows are deleted
- old `CaseContact` rows stay in place, but marked fields are scrubbed
- tenant filtering is applied automatically
- held rows are skipped
- audit rows are written to Cohort tables

Once registered, Cohort can preview, sweep, and right-to-erasure retained entities using the rules you mapped. You can let the hosted worker run scheduled sweeps, or resolve the application services yourself when you want to trigger retention explicitly.

## Quick start

### 1. Mark retained entities

Every retained entity also requires a stable UUID identity:

```text
[Retain("session-notes", nameof(CreatedAt))]
[RetentionEntityId("a3f467fe-c5d0-4f17-9897-83c373cc1dc8")]
public sealed class SessionNote { /* ... */ }
```

`[Retain("category", nameof(Anchor))]` says:

- this entity participates in retention
- it belongs to the given category
- age it using the given anchor column

`[RetentionEntityId]` is durable correlation metadata, not display metadata. Never change it
when renaming or moving the CLR type. Startup rejects missing, empty, malformed, or duplicate
identities. It identifies per-row audit detail and handler work independently of the CLR name.
Audit rows retain the human-readable CLR `EntityType` separately; within a run,
`sweep_run_entity_summary` rows are uniquely identified by the durable retention entity ID
together with category, tenant, and strategy.

Complete the configuration by mapping the category through `IRetentionRuleProvider`,
registering Cohort, and adding Cohort's infrastructure tables to the EF model as shown below.

Unannotated entities are implicitly exempt. Use `[ExemptFromRetention("reason")]` if you want that exemption to be explicit in code.

Guardrails enforced at startup validation:

- `Period` and `LegalMin` must be non-negative (`RetentionRule` rejects negative values —
  a negative period would compute a future cutoff and sweep everything).
- Owned retained types, entities sharing a table with another EF type, and entities split
  across multiple tables are rejected. Cohort mutates one independently owned table per
  retained entity and will not guess which columns or rows belong to it.
- Retained entities must not participate in an EF inheritance hierarchy (TPH/TPT/TPC):
  sweep SQL targets the table without a discriminator.
- No `ON DELETE CASCADE` foreign key may lead from a purgeable retained entity into
  another retained entity — a cascade would bypass the dependent's retention window,
  holds, and audit trail.
- No cascade may lead from a purgeable retained entity back into a retained type, including
  itself through a self-reference.
- Convention marker attributes (`[RetentionRecordId]`, `[RetentionTenant]`,
  `[RetentionSoftDelete]`, `[RetentionDeletedAt]`, `[RetentionAnonymisedAt]`) may each appear
  on at most one property.
- A record id that is not the primary key must be unique through an alternate key or an
  unfiltered unique index; a filtered index does not make it unique.
- Anchor, `DeletedAt`, and `AnonymisedAt` columns must map to `timestamp with time zone`
  (`timestamptz` and precision variants are fine): Cohort compares and writes retention
  timestamps as UTC instants, and a naive `timestamp without time zone` column silently drifts
  with the session time zone.
- Tenant, anchor, soft-delete, deleted-at and anonymised-at properties must not use EF value
  converters: Cohort filters and writes those columns in SQL, where a converter would not apply.

Rows whose anchor column is `NULL` never match a cutoff comparison and are retained
indefinitely; prefer non-nullable anchors for purge categories.

Retained entities are tenant-scoped by default. They must expose a `TenantId` property, or mark an alternative property with `[RetentionTenant]`, unless they are intentionally global and explicitly marked with `[RetentionTenantless]`. Declaring `[RetentionTenantless]` on an entity that also exposes a tenant property fails startup validation — the tenant property would win and the marker would be silently ignored.

### 2. Map categories to rules

Each category resolves directly through `IRetentionRuleProvider` to a `RetentionRule`:

- `Period`
- `Strategy`
- optional `LegalMin`
- optional per-rule audit detail
- optional provenance

The entity annotation does not decide whether a row is purged or anonymised. The resolved `RetentionRule` does.

`GetCapabilities(category)` synchronously declares every strategy that `ResolveAsync` can
return for that category. Capabilities are not a cache of the current rule: startup validates
the union of every declared strategy against each retained entity. A runtime rule whose
strategy was not declared is rejected. `ResolveAsync` receives category, tenant, and logical
time, so a host provider can apply tenant- and time-specific policy without hiding its possible
model requirements. Category aliasing, if a host wants it, stays inside its provider.

### 3. Register Cohort

Register your `IRetentionRuleProvider` before `AddCohort<TDbContext>()`, declare every strategy each category can return, and call `ConfigureCohortTables()` in `OnModelCreating`. Startup validates the union of declared strategy requirements.

`[ErasureSubject]` metadata is also validated at startup: marked properties must be mapped
to physical columns and have compatible effective/provider types. Invalid erasure metadata
therefore prevents startup instead of failing the first subject request.

Map all Cohort-owned tables explicitly. The parameterless overload maps them to `public`;
the schema overload maps all five tables together:

```text
modelBuilder.ConfigureCohortTables("retention");
```

All retained and Cohort-owned SQL identifiers are schema-qualified. Missing retained-table
schema resolves to the EF model default and then `public`; runtime behavior does not depend on
PostgreSQL `search_path`.

### 4. Choose how to run it

- `IRetentionPreview` gives you a count-only preview
- `IRetentionSweep` performs the real sweep
- `IRetentionErasureService` runs subject erasure inside the same retention rules
- `IRetentionDeletion` lets host deletion participate atomically in Cohort hold protection

`IRetentionDeletion` is scoped. Resolve it and your registered `TDbContext` from the same DI
scope, identify every row that the host callback will delete, and persist the callback's changes
through that context. Cohort canonicalises the record IDs, takes the same transaction-scoped
advisory locks used by hold creation and retention mutation, checks all targets for active holds,
then invokes the callback inside the same EF/Postgres transaction. It returns `Protected` without
invoking the callback when any target is held; callback failures propagate and roll back. The
operation owns the transaction, so call it when the scoped context has no current transaction.

```csharp
public static class ReadmeRetentionOperations
{
    public static async Task RunAsync(
        IServiceProvider provider,
        TenantContext tenant,
        Guid subjectId,
        CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        await provider.GetRequiredService<IRetentionPreview>().PreviewAsync(tenant, now, ct);
        await provider.GetRequiredService<IRetentionSweep>().SweepAsync(tenant, now, ct);
        await provider.GetRequiredService<IRetentionErasureService>().EraseAsync(
            tenant,
            new ErasureScope(subjectId),
            now,
            ct);
    }
}
```

## Strategies

| Strategy | What Cohort does | Typical use |
|---|---|---|
| `Purge` | Deletes rows past cutoff | short-lived operational data |
| `SoftDelete` | Sets the soft-delete flag | records you still want to hide rather than remove |
| `Anonymise` | Scrubs marked columns in place | data you still need structurally, but not personally |
| `Exempt` | Leaves rows alone | documented non-retained categories |

## Anonymisation

Anonymise categories require an idempotency marker: a nullable `DateTimeOffset` property
named `AnonymisedAt` by convention (or marked with `[RetentionAnonymisedAt]`). `NULL`
means not yet anonymised; the sweep filters on it and stamps it, so rows are scrubbed
exactly once instead of on every sweep. Startup validation enforces the marker.

For straightforward cases, mark columns with `[Anonymise]`:

```text
[Anonymise(AnonymiseMethod.Null)]
public string? Email { get; set; }

[Anonymise(AnonymiseMethod.EmptyString)]
public string FullName { get; set; } = "";

[Anonymise(AnonymiseMethod.FixedLiteral, "[redacted]")]
public string Phone { get; set; } = "";
```

For custom logic, use `AnonymiseWithAttribute`:

```text
[AnonymiseWith(typeof(MyCustomFactory))]
public string ExternalReference { get; set; } = "";
```

## Right-to-erasure

Mark one or more subject identifiers with `[ErasureSubject]`:

```csharp
[Retain("user-data", nameof(CreatedAt))]
[RetentionEntityId("6b619c19-6e3c-44e8-a87f-975c68fd3988")]
public sealed class UserRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }

    [ErasureSubject]
    public Guid UserId { get; set; }

    [ErasureSubject]
    public Guid? DelegateUserId { get; set; }
}
```

You can mark multiple `[ErasureSubject]` properties on the same entity.

Any marked subject column equals the requested subject is treated as an erasure match.

Cohort erases rows that satisfy these conditions:

1. any marked subject column equals the requested subject
2. the row belongs to the requested tenant
3. the resolved strategy is eligible for erasure
4. no active hold covers the row

Ordinary `Period` never delays subject erasure. With no positive `LegalMin`, Cohort emits no
anchor predicate, so null and future anchors are eligible. A positive `LegalMin` adds the
strict predicate `anchor < now - LegalMin`; an exact-boundary or null anchor remains blocked,
and blocked null anchors are reported in `NullAnchorCount`. Active holds always block erasure.

Erasure refuses categories that resolve to the SoftDelete strategy: setting a flag leaves
personal data in the row, which rarely satisfies an erasure request. If it genuinely does,
opt in explicitly with `new ErasureScope(subject, allowSoftDeleteAsErasure: true)`.
Pass `new ErasureScope(subject, dryRun: true)` to count what an erasure would change and write
its audit trail without changing any row; `ErasureResult.DryRun` reports which it was.

Erasure runs on the same execution model as sweeps (see [Execution model](#execution-model)):
the `Started` audit row commits before any mutation, rows are erased in independently
committed batches of `SweepBatchSize`, one entity's failure is recorded
(`ErasureResult.EntityFailures`, plus the run row's `PartiallyFailed` status and `Error`)
while erasure
continues for the subject's data in the remaining entities, and held counts are measured
directly — in dry runs too. One difference: erasure candidate selects wait on locked rows
(`FOR UPDATE`) instead of skipping them, because an erasure request must not silently miss
a row a concurrent transaction happens to hold.

Internally, Cohort builds a relational predicate for the marked subject columns.

## Conventions and overrides

By default Cohort assumes common EF names:

- record id: `Id`
- tenant id: `TenantId`
- soft-delete flag: `IsDeleted`
- deleted-at column: `DeletedAt`
- anonymised-at marker: `AnonymisedAt`

You can override those globally:

```json
{
  "Cohort": {
    "Conventions": {
      "RecordIdPropertyName": "Id",
      "TenantPropertyName": "OrganisationId",
      "SoftDeletePropertyName": "IsDeleted",
      "DeletedAtPropertyName": "DeletedAt",
      "AnonymisedAtPropertyName": "AnonymisedAt"
    }
  }
}
```

Or per entity with marker attributes:

- `[RetentionRecordId]`
- `[RetentionTenant]`
- `[RetentionSoftDelete]`
- `[RetentionDeletedAt]`
- `[RetentionAnonymisedAt]`

Priority is:

- attribute
- global config
- built-in default

## Row handlers

If you need side effects around mutated rows, register handlers with `AddRowHandler<TEntity, THandler>()`.

Give long-lived handlers an explicit stable UUID through the `identity` argument. Without one,
queued work uses the handler's CLR type name; renaming that class dead-letters work still queued
under the old name.

Handlers run through the dispatcher surface (`IRetentionRowDispatcher` backed by `RetentionRowDispatcher`) and let you do things like:

- purge related files or blobs
- emit domain or integration events
- capture original values before mutation

The execution contract:

- `OnBeforeAsync` runs inside the open sweep transaction, before the row mutation. Treat it
  as capture-only: external side effects performed here survive a transaction rollback, and
  it can run for a row that is subsequently withheld (for example by a hold created mid-sweep).
- `OnAfterAsync` is dispatched after commit with at-least-once delivery. Handlers must be
  idempotent; `ctx.Attempt` greater than 1 means a possible retry of completed work.
- Each delivery holds its claim for `RowHandlerDispatch:ClaimTimeout` (default 5 minutes).
  A handler must finish within nine tenths of it (the rest covers claim latency and clock
  skew between hosts): then its cancellation token fires and the delivery is recorded as a
  failed attempt. A handler that ignores the token may overlap its own retry, and the
  row's next handler may start before it finishes. Work left `InFlight` by a crash is
  reclaimed once the timeout elapses, and the reclaim counts as an attempt. A delivery whose
  claim was reclaimed cannot settle the row; its outcome is discarded and the newer delivery
  settles it.
- `AfterSweepSettled` work runs only once its run records completion or failure. A run
  left `Started` by a process crash is marked failed after
  `RowHandlerDispatch:SweepSettleTimeout`, allowing that work to dispatch.
- Captured row snapshots (`CapturedPayload`) can contain pre-anonymisation personal data.
  They are cleared as soon as every handler for the row reaches a terminal state, with a
  backstop scrub after `RowHandlerDispatch:PayloadRetention` (default 30 days). Queued
  handler work whose snapshot the backstop scrubbed can never complete, so it dead-letters
  immediately with the scrub named as the reason instead of burning its retry budget.
  Dead-letter `LastError` stores a sanitized diagnostic containing the exception type, safe
  machine code when available, and a diagnostic ID. It never stores arbitrary exception text
  or stack traces.
- Persisted payloads name CLR types, so deserialisation is allow-listed: snapshot values
  round-trip only as well-known scalars, property types of the swept entity, or types
  declared in the entity's or its registered handlers' assemblies. A tampered payload
  naming anything else dead-letters instead of materialising an arbitrary type, and the
  persisted entity type resolves only against registered retained entities.
- Snapshot values must round-trip exactly: `OnAfterAsync` receives an equal value of the
  same type. Supported values are `null`, well-known scalars, enums and value-equal types
  from the allow-list above, one-dimensional arrays and `List<T>` of those (element type
  preserved), `object?[]`, `List<object?>`, and nested `IDictionary<string, object?>`.
  Anything else fails the handler that stashed it at capture time, before the row is
  mutated, through the same path as a throwing `OnBeforeAsync`.

## Configuration

```json
{
  "Cohort": {
    "Schedule": "0 2 * * *",
    "DryRun": false,
    "KillSwitch": false,
    "AuditObservers": {
      "Timeout": "00:00:05"
    }
  }
}
```

| Key | Default | Description |
|---|---|---|
| `Schedule` | `null` | Cron expression, evaluated in **UTC**. `null` means the worker is disabled. |
| `DryRun` | `false` | Run **scheduled** sweeps as count-only audited runs instead of mutating data, with the same run and entity audit trail and `sweep_run.DryRun` set. Only the worker reads it: explicit requests carry their own flag (`RetentionSweepRequest.Tenanted(..., dryRun: true)`, `new ErasureScope(..., dryRun: true)`) and are honoured as written. |
| `KillSwitch` | `false` | Stop between tenant passes: the tenant in progress finishes, the remaining tenants and future ticks are skipped. |
| `SweepBatchSize` | `5000` | Maximum rows selected, locked, and mutated per transaction. Each batch commits independently. |
| `AuditObservers:Timeout` | `00:00:05` | Maximum time Cohort waits for each observer to handle one committed event. Each observer has an independent timeout. |
| `RowHandlerDispatch:PollInterval` | `00:00:10` | Delay between dispatcher polling passes. |
| `RowHandlerDispatch:BatchSize` | `50` | Upper bound on queued handler statuses claimed per poll; the dispatcher claims at most `min(MaxParallelism, BatchSize)` so every claim starts running at once. Valid range: 1 to 10000. |
| `RowHandlerDispatch:MaxParallelism` | `4` | Maximum rows dispatched concurrently. Handlers for one row remain ordered. Valid range: 1 to 256. |
| `RowHandlerDispatch:MaxAttempts` | `10` | Maximum delivery attempts before dead-lettering. Valid range: 1 to 1000. |
| `RowHandlerDispatch:BaseBackoff` | `00:00:01` | Base delay for exponential retry backoff. |
| `RowHandlerDispatch:ClaimTimeout` | `00:05:00` | Lease on one delivery. The handler is cancelled at nine tenths of it, and in-flight work abandoned by a crash is reclaimed after it. Valid range: 30 seconds to 1 day. |
| `RowHandlerDispatch:SweepSettleTimeout` | `01:00:00` | Age after which an unowned `Started` run is recovered as failed. |
| `RowHandlerDispatch:PayloadRetention` | `30.00:00:00` | Backstop retention for potentially sensitive captured row snapshots. |

Worker semantics worth knowing:

- Hosts must apply EF Core migrations before starting Cohort. Startup validates the
  installed Cohort schema but does not mutate it.
- A failed iteration (database outage, runtime misconfiguration) is logged and the worker
  retries at the next scheduled occurrence. It does not stop the host.
- The worker sweeps every tenant returned by `IRetentionTenantSource`. The default source
  adapts a singleton `TenantContext` registration; multi-tenant hosts register their own
  source enumerating all tenants.
- Tenant passes execute sequentially in first-seen order. Duplicate tenant IDs execute once;
  if duplicate entries disagree on jurisdiction or tags, Cohort logs a warning and uses the
  first context.
- Each occurrence runs once. Replicas coordinate through a Postgres advisory lock, and under it
  a worker skips an occurrence that already has a `Scheduled` run of the same kind (dry or real)
  started at or after it, so a dry-run replica never stands in for the real sweep. (This
  compares with other replicas' clocks, so keep replica clocks in sync.)
- Missed occurrences are skipped, not caught up: the next occurrence is always computed
  from the current time.
- Sweeps triggered by the worker are audited as `Scheduled`; direct calls to
  `IRetentionSweep.SweepAsync` are audited as `Manual`.

## Identities

`RetentionEntityId` and `RecordId` are deliberately different identities:

- `RetentionEntityId` is the stable UUID assigned to a retained entity type with
  `[RetentionEntityId]`. It survives CLR and table renames and correlates rules, holds,
  summaries, row details, and handler work. `EntityType` on audit rows is readable diagnostic
  metadata only.
- `RecordId` is one row's canonical PostgreSQL text identity, `CAST(key AS text)`. Cohort stores
  it in holds and row details and compares it with the row's own key cast, so UUID, integer,
  string, `numeric`, `citext` and provider-converted keys all match consistently.

Record-id types whose text form depends on session settings (`timestamp`/`timestamptz`,
`date`, `time`, `interval`, `real`/`double precision`, `money`, `bytea`) are rejected at
startup: their canonical text could change between sessions and silently detach holds.

## Execution model

Sweeps and erasure are batched and incremental:

- Candidate rows are selected with `FOR UPDATE SKIP LOCKED` in batches of `SweepBatchSize`;
  each batch mutates and **commits independently**, so a large backlog never sits in one
  unbounded transaction and a failure loses only the current batch.
- A run first takes its own advisory lock, then commits the `Started` audit row before any
  mutation, with `Status = Started` and `SettledAt = NULL`. A terminal event sets `Status` to `Succeeded`, `PartiallyFailed`,
  `Failed`, or `Cancelled` and records `SettledAt`. The dispatcher marks stale `Started`
  rows as failed after `RowHandlerDispatch:SweepSettleTimeout`, but only when it can take the
  run's lock, so a live run is never recovered.
- One entity's failure is recorded (run row `Status = PartiallyFailed` and `Error`, plus
  `RetentionSweepResult.EntityFailures` / `ErasureResult.EntityFailures`) and the run
  continues with the remaining entities.
- Rows skipped by a failing `OnBeforeAsync` handler are excluded from the remaining
  batches of the run (they are dead-lettered once and stay behind for the next run), so a
  failing row neither spins the batch loop nor blocks the eligible rows behind it. A batch
  that makes no progress at all stops the loop for that entity.
- `HeldCount` in summaries is measured directly (rows past cutoff with an active hold),
  not inferred from candidate arithmetic.
- Caller times (`now`, `createdAt`, `asOf`, ...) may carry any offset; Cohort normalises them to
  UTC at the public boundary.
- `IRetentionSweep`, `IRetentionPreview`, and `IRetentionErasureService` are singleton,
  scope-owning ports. Every call runs in a fresh DI scope so Cohort's raw SQL cannot share
  the caller's tracked `DbContext` or ambient transaction. Pass operation context through
  the request contracts; caller-scoped services and transactions do not flow into the
  operation. Singleton decorators may wrap these ports, but scoped decorators must not
  depend on their own scope participating in Cohort's execution.

## Legal holds

```csharp
public static class ReadmeLegalHolds
{
    public static Task CreateAsync(
        IRetentionHoldsRepository holds,
        Guid noteId,
        Guid tenantId,
        DateTimeOffset now,
        CancellationToken ct) =>
        holds.CreateAsync(
            new RetentionHoldRequest(
                holdId: Guid.NewGuid(),
                retentionEntityId: Guid.Parse("a3f467fe-c5d0-4f17-9897-83c373cc1dc8"),
                recordId: noteId.ToString(),
                tenantId: tenantId,
                reason: "Litigation hold - case #12345",
                createdAt: now,
                expiresAt: now.AddYears(1)),
            ct);
}
```

Held records survive all strategies. Holds are checked in SQL via a `NOT EXISTS` subquery, not via an in-memory row pass. Hold activity is evaluated against the **database wall clock**, not the sweep's logical `now`: a hold created yesterday protects its row even from a backdated sweep. A hold is active from the moment it commits: its stored `CreatedAt` is the earlier of the requested value and the database clock, so an application clock running ahead cannot open a window where a created hold does not yet protect.

`CreateAsync` validates its input so a hold cannot silently protect nothing:

- `RetentionEntityId` must identify a retained entity in the current EF model, or creation
  throws.
- `RecordId` is parsed and canonicalized using the mapped key's PostgreSQL store type. For a
  Guid key, malformed Guid values are rejected; integer, string, and provider-converted keys
  retain their own canonical PostgreSQL representation.
- For retained tenant-scoped tables, if the target row already exists its tenant must
  match `TenantId` — sweeps only honour holds whose tenant matches the row's, so a
  mis-scoped hold would persist while protecting nothing.
- The stored `RecordId` is the row's own canonical text, so `citext`, `numeric` and similar
  keys match what sweeps compare even when the request spells the ID differently.
- The canonical `RecordId` must identify an existing row, read under `FOR SHARE`: in a
  caller's `REPEATABLE READ` transaction a row deleted since the snapshot raises a
  serialization failure rather than taking a hold that protects nothing. Cohort acquires the same
  entity/tenant/record advisory lock used by mutation before checking existence and inserting
  the hold, so a concurrent Cohort sweep cannot pass the hold between validation and creation.
- Tenantless entities require a null `TenantId`; typed and provider-converted record IDs are
  canonicalized before existence and hold matching.
- `IRetentionHoldsRepository` joins the scoped context's current transaction when there is
  one. An unparsable record id is rejected under a savepoint, so the caller's transaction stays
  usable.

## Audit trail

Every sweep unconditionally writes its authoritative ledger to Cohort-managed tables:

- `sweep_run`
- `sweep_run_entity_summary`
- `sweep_run_row_detail`

Summary rows carry:

- category
- strategy
- affected count
- held count
- skipped count
- resolved period
- optional provenance via `RuleSource` and `RuleReason`

Per-row detail is opt-in through `AuditRowDetail.PerRow`.

Tenantless runs are attributed to `Guid.Empty` in the ledger, so summary keys stay unique; a hold
on a tenantless row has a null `TenantId`.

The EF audit writer is internal and cannot be replaced through dependency injection. Mutation,
row detail, and entity progress commit in the same transaction. Consumers can register any
number of `IRetentionAuditObserver` implementations for post-commit export:

```text
services.AddSingleton<IRetentionAuditObserver, AuditExporter>();
services.AddCohort<MyDbContext>();
```

Observers receive `Started`, `EntityProgress`, `RowDetail`, `EntitySummary`, and the terminal
`Completed`, `PartiallyFailed`, `Failed`, or `Cancelled` event in committed lifecycle order.
Batch `RowDetail` and `EntityProgress` events are not delivered until their mutation transaction
has committed, and rolled-back events are never delivered. Each observer is isolated and bounded
by `AuditObservers:Timeout`; exceptions and timeouts are logged but never alter committed data,
the retention result, or run status.

`RowDetail` events contain the canonical `RecordId` and `TenantId`. Treat observer implementations
and their downstream transport as sensitive-data processors: protect access, logs, queues, and
exports accordingly.

Observer delivery is best effort and has no durable outbox. A process crash can lose a
notification after the database commit. Guaranteed integrations must poll the authoritative
tables, use CDC, or implement a durable host-owned outbox.

## Failure diagnostics

Failures derived from external exceptions persist and return a sanitized envelope containing the
exception type, safe machine code when available (for example PostgreSQL SQLSTATE), and a random
diagnostic ID. Cohort-defined machine-safe reasons may be stored as plain `Error` or `LastError`
values instead. Only diagnostic `Error` and `LastError` text is privacy-sanitized: exception
messages, stack traces, SQL values, and subject identifiers are excluded there, but the deliberately
identifying `RecordId` and `TenantId` fields remain in row-detail events. Structured logs retain the
original exception with the same diagnostic ID for protected operational diagnosis.

## License

[MIT](LICENSE)
