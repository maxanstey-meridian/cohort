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
- supporting right-to-erasure without bypassing retention windows
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

[Retain("session-notes", nameof(CreatedAt))]
public sealed class SessionNote
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Body { get; set; } = "";
}

[Retain("case-contacts", nameof(CreatedAt))]
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

public sealed class RetentionCategories : IRetentionCategoryRepository
{
    public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
    {
        IRetentionRuleResolver? resolver = category switch
        {
            "session-notes" => new StaticRetentionRuleResolver(
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)),
            "case-contacts" => new StaticRetentionRuleResolver(
                new RetentionRule(TimeSpan.FromDays(365), Strategy.Anonymise)),
            _ => null,
        };

        return Task.FromResult(resolver);
    }
}
```

Register Cohort and add its infrastructure tables to your EF model:

```csharp
builder.Services.AddSingleton<IRetentionCategoryRepository, RetentionCategories>();
builder.Services.AddCohort<MyDbContext>();
```

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    base.OnModelCreating(modelBuilder);
    modelBuilder.ConfigureCohortTables();
}
```

What happens:

- old `SessionNote` rows are deleted
- old `CaseContact` rows stay in place, but marked fields are scrubbed
- tenant filtering is applied automatically
- held rows are skipped
- audit rows are written to Cohort tables

Once registered, Cohort can preview, sweep, and right-to-erasure retained entities using the rules you mapped. You can let the hosted worker run scheduled sweeps, or resolve the application services yourself when you want to trigger retention explicitly.

## Quick start

### 1. Mark retained entities

`[Retain("category", nameof(Anchor))]` says:

- this entity participates in retention
- it belongs to the given category
- age it using the given anchor column

Unannotated entities are implicitly exempt. Use `[ExemptFromRetention("reason")]` if you want that exemption to be explicit in code.

Guardrails enforced at startup validation:

- `Period` and `LegalMin` must be non-negative (`RetentionRule` rejects negative values —
  a negative period would compute a future cutoff and sweep everything).
- Retained entities must not participate in an EF inheritance hierarchy (TPH/TPT/TPC):
  sweep SQL targets the table without a discriminator.
- Retained entities must live in the default `public` schema; Cohort SQL does not
  schema-qualify identifiers.
- No `ON DELETE CASCADE` foreign key may lead from a purgeable retained entity into
  another retained entity — a cascade would bypass the dependent's retention window,
  holds, and audit trail.
- Convention marker attributes (`[RetentionRecordId]`, `[RetentionTenant]`,
  `[RetentionSoftDelete]`, `[RetentionDeletedAt]`) may each appear on at most one property.
- Anchor, `DeletedAt`, and `AnonymisedAt` columns must map to `timestamp with time zone`:
  Cohort compares and writes retention timestamps as UTC instants, and a naive
  `timestamp without time zone` column silently drifts with the session time zone.

Rows whose anchor column is `NULL` never match a cutoff comparison and are retained
indefinitely; prefer non-nullable anchors for purge categories.

Retained entities are tenant-scoped by default. They must expose a `TenantId` property, or mark an alternative property with `[RetentionTenant]`, unless they are intentionally global and explicitly marked with `[RetentionTenantless]`. Declaring `[RetentionTenantless]` on an entity that also exposes a tenant property fails startup validation — the tenant property would win and the marker would be silently ignored.

### 2. Map categories to rules

Each category resolves to a `RetentionRule`:

- `Period`
- `Strategy`
- optional `LegalMin`
- optional per-rule audit detail
- optional provenance

The entity annotation does not decide whether a row is purged or anonymised. The resolved `RetentionRule` does.

### 3. Register Cohort

Register your `IRetentionCategoryRepository` before `AddCohort<TDbContext>()`, and call `ConfigureCohortTables()` in `OnModelCreating`.

### 4. Choose how to run it

- `IRetentionPreview` gives you a count-only preview
- `RetentionSweepEngine` performs the real sweep
- `IRetentionErasureService` runs subject erasure inside the same retention rules

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

```csharp
[Anonymise(AnonymiseMethod.Null)]
public string? Email { get; set; }

[Anonymise(AnonymiseMethod.EmptyString)]
public string FullName { get; set; } = "";

[Anonymise(AnonymiseMethod.FixedLiteral, "[redacted]")]
public string Phone { get; set; } = "";
```

For custom logic, use `AnonymiseWithAttribute`:

```csharp
[AnonymiseWith(typeof(MyCustomFactory))]
public string ExternalReference { get; set; } = "";
```

## Right-to-erasure

Mark one or more subject identifiers with `[ErasureSubject]`:

```csharp
[Retain("user-data", nameof(CreatedAt))]
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

Cohort only erases rows that satisfy both conditions:

1. any marked subject column equals the requested subject
2. the row is already past the effective retention cutoff for its category

Active holds still block erasure, and tenant-scoped entities still keep the tenant predicate in the SQL.

Erasure refuses categories that resolve to the SoftDelete strategy: setting a flag leaves
personal data in the row, which rarely satisfies an erasure request. If it genuinely does,
opt in explicitly with `new ErasureScope(subject, allowSoftDeleteAsErasure: true)`.
`ErasureResult.DryRun` tells you whether the call previewed (under `Cohort:DryRun`) or
actually mutated.

Erasure runs on the same execution model as sweeps (see [Execution model](#execution-model)):
the `Started` audit row commits before any mutation, rows are erased in independently
committed batches of `SweepBatchSize`, one entity's failure is recorded
(`ErasureResult.EntityFailures`, plus the run row's `FailedAt`/`Error`) while erasure
continues for the subject's data in the remaining entities, and held counts are measured
directly — in dry runs too. One difference: erasure candidate selects wait on locked rows
(`FOR UPDATE`) instead of skipping them, because an erasure request must not silently miss
a row a concurrent transaction happens to hold.

Internally, the erasure contract passes an `ErasureSubjectPredicate`.

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
      "DeletedAtPropertyName": "DeletedAt"
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
- Work stuck `InFlight` after a crash is reclaimed once `RowHandlerDispatch:ClaimTimeout`
  (default 5 minutes) elapses; the reclaim counts as an attempt.
- `AfterSweepSettled` work runs once its run records completion or failure, or after
  `RowHandlerDispatch:SweepSettleTimeout` (default 6 hours) if the run crashed mid-sweep.
- Captured row snapshots (`CapturedPayload`) can contain pre-anonymisation personal data.
  They are cleared as soon as every handler for the row reaches a terminal state, with a
  backstop scrub after `RowHandlerDispatch:PayloadRetention` (default 30 days). Queued
  handler work whose snapshot the backstop scrubbed can never complete, so it dead-letters
  immediately with the scrub named as the reason instead of burning its retry budget.
  Dead-letter `LastError` stores the root-cause exception type and message only — no
  stack traces.
- Persisted payloads name CLR types, so deserialisation is allow-listed: snapshot values
  round-trip only as well-known scalars, property types of the swept entity, or types
  declared in the entity's or its registered handlers' assemblies. A tampered payload
  naming anything else dead-letters instead of materialising an arbitrary type, and the
  persisted entity type resolves only against registered retained entities.

## Configuration

```json
{
  "Cohort": {
    "Schedule": "0 2 * * *",
    "DryRun": false,
    "KillSwitch": false,
    "ApplyMigrations": false
  }
}
```

| Key | Default | Description |
|---|---|---|
| `Schedule` | `null` | Cron expression, evaluated in **UTC**. `null` means the worker is disabled. |
| `DryRun` | `false` | Run sweeps as preview/count-only instead of mutating data. While enabled, the worker routes ticks through `IRetentionPreview`, `RetentionSweepEngine.SweepAsync` refuses to run, and `EraseAsync` returns counts without mutating (`ErasureResult.DryRun` is `true`). |
| `KillSwitch` | `false` | Finish the current iteration, then skip future ticks. |
| `ApplyMigrations` | `false` | Run `MigrateAsync()` on startup. Cannot combine with `DryRun` or `KillSwitch`. |
| `SweepBatchSize` | `5000` | Maximum rows selected, locked, and mutated per transaction. Each batch commits independently. |

Worker semantics worth knowing:

- A failed iteration (database outage, runtime misconfiguration) is logged and the worker
  retries at the next scheduled occurrence. It does not stop the host.
- The worker sweeps every tenant returned by `IRetentionTenantSource`. The default source
  adapts a singleton `TenantContext` registration; multi-tenant hosts register their own
  source enumerating all tenants.
- Replicas coordinate through a Postgres advisory lock: only one instance sweeps a given
  occurrence (the others skip and log), and startup migrations cannot race.
- Missed occurrences are skipped, not caught up: the next occurrence is always computed
  from the current time.
- Sweeps triggered by the worker are audited as `Scheduled`; direct calls to
  `RetentionSweepEngine.SweepAsync` are audited as `Manual` unless you pass a trigger kind.

## Execution model

Sweeps and erasure are batched and incremental:

- Candidate rows are selected with `FOR UPDATE SKIP LOCKED` in batches of `SweepBatchSize`;
  each batch mutates and **commits independently**, so a large backlog never sits in one
  unbounded transaction and a failure loses only the current batch.
- The `Started` audit row commits immediately, before any mutation. `Completed` (and
  `Failed`, when an entity failed) commit at the end. A run row with both `CompletedAt`
  and `FailedAt` set is a partial failure; `CompletedAt` still `NULL` long after
  `StartedAt` means the process died mid-run.
- One entity's failure is recorded (run row `FailedAt`/`Error`, plus
  `RetentionSweepResult.EntityFailures` / `ErasureResult.EntityFailures`) and the run
  continues with the remaining entities.
- Rows skipped by a failing `OnBeforeAsync` handler are excluded from the remaining
  batches of the run (they are dead-lettered once and stay behind for the next run), so a
  failing row neither spins the batch loop nor blocks the eligible rows behind it. A batch
  that makes no progress at all stops the loop for that entity.
- `HeldCount` in summaries is measured directly (rows past cutoff with an active hold),
  not inferred from candidate arithmetic.
- The mutating sweep is exposed as the `IRetentionSweep` port (mirroring
  `IRetentionPreview`/`IRetentionErasureService`) so hosts can decorate it.

## Legal holds

```csharp
await holdsRepo.CreateAsync(new RetentionHoldRequest(
    HoldId: Guid.NewGuid(),
    TableName: "session_notes",
    RecordId: noteId.ToString(),
    TenantId: tenantId,
    Reason: "Litigation hold - case #12345",
    CreatedAt: DateTimeOffset.UtcNow,
    ExpiresAt: DateTimeOffset.UtcNow.AddYears(1)
));
```

Held records survive all strategies. Holds are checked in SQL via a `NOT EXISTS` subquery, not via an in-memory row pass. Hold activity is evaluated against the **database wall clock**, not the sweep's logical `now`: a hold created yesterday protects its row even from a backdated sweep.

`CreateAsync` validates its input so a hold cannot silently protect nothing:

- `TableName` must exactly match a table mapped by the EF model, or creation throws.
- For tables with a Guid primary key, `RecordId` is normalised to the canonical lowercase
  hyphenated form the sweep compares against; non-Guid values are rejected.
- For retained tenant-scoped tables, if the target row already exists its tenant must
  match `TenantId` — sweeps only honour holds whose tenant matches the row's, so a
  mis-scoped hold would persist while protecting nothing. A row that does not exist yet
  is allowed (holds may be created ahead of their row).

## Audit trail

Every sweep writes to Cohort-managed tables:

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
