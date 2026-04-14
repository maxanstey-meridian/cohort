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

Retained entities are tenant-scoped by default. They must expose a `TenantId` property, or mark an alternative property with `[RetentionTenant]`, unless they are intentionally global and explicitly marked with `[RetentionTenantless]`.

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

Internally, the erasure contract passes an `ErasureSubjectPredicate`.

## Conventions and overrides

By default Cohort assumes common EF names:

- record id: `Id`
- tenant id: `TenantId`
- soft-delete flag: `IsDeleted`
- deleted-at column: `DeletedAt`

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
| `Schedule` | `null` | Cron expression. `null` means the worker is disabled. |
| `DryRun` | `false` | Run sweeps as preview/count-only instead of mutating data. |
| `KillSwitch` | `false` | Finish the current iteration, then skip future ticks. |
| `ApplyMigrations` | `false` | Run `MigrateAsync()` on startup. Cannot combine with `DryRun` or `KillSwitch`. |

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

Held records survive all strategies. Holds are checked in SQL via a `NOT EXISTS` subquery, not via an in-memory row pass.

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
