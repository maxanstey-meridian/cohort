# Plan: Cohort — declarative retention for .NET / EF Core

## Context

GDPR retention is currently nothing in casebridge and will keep being nothing until the framework is in place. The dominant enterprise approaches are commercial (OneTrust, Microsoft Purview) and require six-figure procurement plus a connector still owned by the consuming app — they don't replace the need for an in-app deletion mechanism. The dominant homegrown approaches are either ad-hoc cleanup scripts (rots, drifts, no audit trail) or YAML-configured workers (boring but loses code-locality).

The right move for our stack is **annotation-driven, category-tagged retention with a lifted resolver contract**, packaged as a standalone .NET 9 library so it can be tested in isolation against synthetic entities and reused across casebridge, perch, and any future apps. The lifted resolver shape costs ~30 lines on day one and is the seam that prevents a future rewrite when a council eventually wants conditional, effectful, or external-source-driven rules.

The packaging decision is deliberate: in-repo would be cheaper to iterate but conflates package correctness with casebridge correctness. Standalone forces the package to prove itself against a synthetic test surface, which is the whole point.

**Name: Cohort.** Captures the "entities in the same retention group age out together" semantics.

## Scope

**In:** standalone .NET 9 class library in a new repo, sequenced across three milestones. Milestone A proves the package shape end-to-end with the smallest possible surface (purge only, no holds, no persisted audit). Milestone B adds the destructive strategies, holds, and the persisted audit log. Milestone C adds the operational surface (worker, killswitch, dry-run scheduling, erasure API, migration host-merging).

**Out:**
- **Casebridge consumption** — separate planning brief, follow-up after Cohort v1 (all three milestones) lands. Tagging every casebridge entity is its own focused PR with the load-bearing decision-per-entity work.
- **Admin UI for editing category rules** — casebridge concern. Cohort ships the `IRetentionCategoryRepository` interface; casebridge implements it against a `retention_category_config` table and builds the UI.
- **Conditional / Alias / Effectful / Caching resolver implementations** — interface ships, those concrete classes do not. Hosts build them when they need them.
- **Hash and format-preserving anonymisation** — explicitly deferred to a v2 brief that has to think properly about KMS, uniqueness preservation, key rotation, and reversibility risk. v1 anonymisation is `Null` / `EmptyString` / `FixedLiteral` only. No hashing means no key management means no HMAC config in v1.
- **Source generator** — reflection is fine for a daily sweep. Source-gen is a v3 nice-to-have if startup validation becomes a hot path.
- **OneTrust / Purview adapters** — only build when a council asks. The lifted resolver contract makes this a one-class addition later.
- **Row-level resolution context** — `RetentionResolutionContext` ships with `(category, tenant, now, aliasPath)` only. No entity instance, no parent reference. Add fields when a real consumer requires them.
- **Shadow-property and owned-type anchors** — anchor member must be a direct CLR property of the root entity. Documented as a known limitation in v1.

---

## Milestone A — Package shape proven

The smallest possible end-to-end proof: types, attributes, registry, lifted resolver contract, startup validator, purge sweep (no holds, no persisted audit), dry-run preview, sample consumer, integration tests. If this works, the lifted contract is validated, the registry is validated, the sweep query shape is validated, and Milestone B can build on a proven foundation.

### A.1 Repo bootstrap

```
~/Sites/cohort/
├── Cohort/                          # main library project
├── Cohort.Tests/                    # synthetic-entity unit tests
├── Cohort.Sample/                   # sample consumer (dogfood)
├── Cohort.Sample.Tests/             # end-to-end tests against the sample consumer
├── Cohort.sln
├── README.md
└── .editorconfig                    # match casebridge conventions
```

`dotnet new classlib --framework net9.0` for `Cohort/`. Single dependency: `Microsoft.EntityFrameworkCore`. No Newtonsoft, no Serilog, no MediatR, nothing else. Version-pin EF to a recent stable.

`Cohort.Sample.Tests` uses xUnit + FluentAssertions + Testcontainers.PostgreSql. Synthetic test entities live in `Cohort.Sample/Entities/`.

### A.2 Core types

**File:** `Cohort/Model/`

- `RetentionRule.cs` — `sealed record RetentionRule(TimeSpan Period, Strategy Strategy, TimeSpan? LegalMin, AuditRowDetail AuditRowDetail)`. The `AuditRowDetail` field is wired in A even though A doesn't persist any audit (defaults to `SummaryOnly`); B starts honouring it.
- `Strategy.cs` — `enum Strategy { Purge, SoftDelete, Anonymise, Exempt }`. Only `Purge` and `Exempt` are exercised in A.
- `AuditRowDetail.cs` — `enum AuditRowDetail { SummaryOnly, PerRow }`. Default `SummaryOnly`.
- `RetentionResolutionContext.cs` — `sealed record RetentionResolutionContext(string Category, TenantContext Tenant, DateTimeOffset Now, IReadOnlyList<string> AliasPath)`.
- `TenantContext.cs` — `sealed record TenantContext(Guid Id, string? Jurisdiction, IReadOnlyDictionary<string, string> Tags)`.
- `AnonymiseMethod.cs` — `enum AnonymiseMethod { Null, EmptyString, FixedLiteral }`. Hash is *deliberately not in this enum* — it would be a security footgun without per-tenant key management, format-preserving guarantees, and uniqueness-constraint handling. Defer to v2.
- `RetentionSweepResult.cs` — `sealed record RetentionSweepResult(Guid SweepId, DateTimeOffset StartedAt, DateTimeOffset CompletedAt, IReadOnlyList<EntitySweepCount> Counts)` plus `EntitySweepCount(Type EntityType, string Category, Guid TenantId, Strategy Strategy, int Affected)`. This is what the sweep returns in A. B refactors the engine to feed an audit writer, but the result object stays for caller ergonomics.

These are the public vocabulary. Anything in this folder is a public API contract — keep it small and additive.

### A.3 Attributes

**File:** `Cohort/Attributes/`

- `RetainAttribute(string category, string anchor)` — class-targeting, not inheritable, single-instance. The `anchor` parameter is the **CLR property name** (a "member"), not a column name. Always pass via `nameof(...)`. The registry resolves the column name from EF metadata at scan time.
- `AnonymiseAttribute(AnonymiseMethod method, string? literal = null)` — property-targeting. `literal` is required if `method == FixedLiteral`. Used only when the resolved strategy is `Anonymise` (B).
- `ExemptFromRetentionAttribute(string reason)` — class-targeting. Required if `[Retain]` is absent. The arch test enforces "exactly one of the two."

**Deliberately not in v1:** `SoftDeleteColumnAttribute`. SoftDelete uses a CLR-level convention (`bool IsDeleted` property required, `DateTimeOffset? DeletedAt` optional), validated by the registry. If a real host later needs an override, add `SoftDeletePropertyAttribute(string propertyName)` — CLR-facing, never accepting raw column names.

### A.4 Lifted resolver contract

**File:** `Cohort/Resolution/`

- `IRetentionRuleResolver.cs`:
  ```csharp
  public interface IRetentionRuleResolver
  {
      Task<RetentionRule> ResolveAsync(
          RetentionResolutionContext ctx,
          CancellationToken ct);

      // Optional startup hook. Static resolvers return the rule immediately.
      // Effectful resolvers may return null to opt out of startup validation.
      RetentionRule? TryResolveAtStartup() => null;
  }
  ```
- `StaticRetentionRuleResolver.cs`:
  ```csharp
  public sealed class StaticRetentionRuleResolver(RetentionRule rule) : IRetentionRuleResolver
  {
      public Task<RetentionRule> ResolveAsync(
          RetentionResolutionContext ctx, CancellationToken ct)
          => Task.FromResult(rule);
      public RetentionRule? TryResolveAtStartup() => rule;
  }
  ```
- `IRetentionCategoryRepository.cs`:
  ```csharp
  public interface IRetentionCategoryRepository
  {
      Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct);
      Task<IReadOnlyList<string>> ListCategoriesAsync(CancellationToken ct);
  }
  ```
- `RetentionAliasCycleException.cs` — exception type ships in A even though no alias resolver implementation does, so consumer-built alias resolvers throw a recognisable type from day one.

**This is the slice that earns its keep.** Once the contract returns resolvers instead of rules, every fancier rule shape (conditional, alias, effectful, cached, OneTrust-backed) is a host-side decoration. The package never has to ship a DSL.

### A.5 Registry + reflection scan

**File:** `Cohort/Registry/`

- `RetentionRegistry<TContext>.cs` where `TContext : DbContext`. Built once at startup. Walks `dbContext.Model.GetEntityTypes()`, reads `[Retain]` / `[ExemptFromRetention]` per entity, validates the anchor *member* exists on the CLR type and is `DateTime` or `DateTimeOffset`, and produces an immutable `IReadOnlyDictionary<Type, RetentionEntry>`.
- `RetentionEntry.cs`:
  ```csharp
  public sealed record RetentionEntry(
      Type EntityType,
      string TableName,
      string Category,
      string AnchorMember,    // CLR property name — the developer's vocabulary
      string AnchorColumn,    // resolved DB column — used only by SQL strategies
      IReadOnlyList<AnonymiseField> AnonymiseFields,
      bool HasSoftDeleteConvention);
  ```
  The split between `AnchorMember` and `AnchorColumn` is deliberate: nothing outside the SQL layer should ever speak in column names. The registry does the translation once.
- `RetentionEntryBuilder.cs` — internal helper, separated for testability.

### A.6 Startup validator

**File:** `Cohort/Startup/RetentionStartupValidator.cs`

Runs once during host startup, after the registry is built. Asserts:

1. Every entity has either `[Retain]` or `[ExemptFromRetention]` (no implicit fall-through).
2. Every `[Retain].anchor` resolves to an existing date-typed CLR property on the entity.
3. Every `[Retain].category` resolves to a non-null `IRetentionRuleResolver` via the repository.
4. Every static resolver's `TryResolveAtStartup()` returns a non-null `RetentionRule`. Effectful resolvers returning null are logged at INFO ("deferred validation for category X") but don't fail boot.

In A, validation only covers `Purge` and `Exempt` strategies. B extends the validator with:
- For each `Anonymise` category, every entity in that category has at least one `[Anonymise]` field marker, and `FixedLiteral` markers carry a non-null literal.
- For each `Anonymise` field, the property type matches the method (`Null` requires nullable, `EmptyString` and `FixedLiteral` require `string`-typed).
- For each `SoftDelete` category, every entity has the convention `IsDeleted` property and (optionally) the `DeletedAt` property.

Failures throw `RetentionConfigurationException` listing **all** problems found, not first-only — give the developer the full picture in one boot attempt.

### A.7 Sweep engine — Purge only, hold-free, no persisted audit

**File:** `Cohort/Sweep/`

- `RetentionSweepEngine.cs` — top-level coordinator. Walks the registry, resolves the rule for each entity's category, computes the cutoff timestamp, builds the query, executes inside a transaction, and aggregates per-entity counts into a `RetentionSweepResult`.
- `PurgeSweepStrategy.cs` — strategy-specific query builder. The A query shape is deliberately the bare minimum:
  ```sql
  DELETE FROM "{table}"
   WHERE "{anchor_column}" < @cutoff
     AND "{tenant_column}" = @tenantId
  RETURNING "id";
  ```
  No hold subquery (B adds it), no per-row audit insert (B adds it). Parameter binding only — column names come from the trusted registry, never from external input.
- `IRetentionSweepStrategy.cs` — interface anchored on `Task<int> SweepAsync(RetentionEntry entry, RetentionRule rule, RetentionResolutionContext ctx, IDbConnection conn, CancellationToken ct)`. Only `PurgeSweepStrategy` implements it in A.

### A.8 Dry-run preview API

**File:** `Cohort/Hosting/IRetentionPreview.cs` (interface ships in A; default implementation also in A)

```csharp
public interface IRetentionPreview
{
    Task<RetentionPreviewReport> WhatWouldBeDeletedAsync(
        DateTimeOffset asOf,
        CancellationToken ct);
}

public sealed record RetentionPreviewReport(
    DateTimeOffset AsOf,
    IReadOnlyList<EntityPreviewCount> Counts);

public sealed record EntityPreviewCount(
    Type EntityType, string Category, Guid TenantId, Strategy Strategy, int CandidateCount);
```

Implemented by running the sweep query as `SELECT COUNT(*)` instead of `DELETE`. Hosts can call this from their own scheduling code without depending on Cohort's worker (which doesn't ship until C).

### A.9 Sample consumer + end-to-end tests

**File:** `Cohort.Sample/` and `Cohort.Sample.Tests/`

- `Cohort.Sample` — a tiny standalone consumer project: a `SampleDbContext` with three synthetic entities exercising A's surface (one `Purge`, one `Exempt`, one with deliberately-missing annotations to fail the validator), a `SampleCategoryRepository` returning hardcoded `StaticRetentionRuleResolver`s, the wiring in a `Program.cs`. **This is the dogfood project that proves the package works in isolation.** SoftDelete and Anonymise entities arrive in B alongside the strategies that need them.
- `Cohort.Sample.Tests` — exercises A's stack against Testcontainers Postgres:
  - Registry scan finds all annotated entities with correct anchor members.
  - Startup validator passes for the well-formed sample, fails with specific errors when fixture entities are deliberately mis-annotated.
  - Purge sweep deletes only rows past the cutoff, returns a `RetentionSweepResult` with correct counts per entity.
  - Multi-tenant isolation: a sweep for tenant A doesn't affect tenant B's rows.
  - Dry-run preview returns counts without modifying any rows.
  - Lifted resolver contract: a custom `IRetentionRuleResolver` implementation in the test project successfully overrides static resolution.
  - Cycle detection: a deliberately-cyclic alias resolver written by the test project throws `RetentionAliasCycleException`.

### A — Critical files

| File | Action |
|---|---|
| `Cohort/Model/RetentionRule.cs` + siblings | New |
| `Cohort/Attributes/RetainAttribute.cs` + siblings | New |
| `Cohort/Resolution/IRetentionRuleResolver.cs` + `StaticRetentionRuleResolver.cs` + `IRetentionCategoryRepository.cs` + `RetentionAliasCycleException.cs` | New |
| `Cohort/Registry/RetentionRegistry.cs` + `RetentionEntry.cs` + builder | New |
| `Cohort/Startup/RetentionStartupValidator.cs` + `RetentionConfigurationException.cs` | New |
| `Cohort/Sweep/RetentionSweepEngine.cs` + `IRetentionSweepStrategy.cs` + `PurgeSweepStrategy.cs` | New |
| `Cohort/Hosting/IRetentionPreview.cs` + default implementation | New |
| `Cohort.Sample/` (Program.cs, SampleDbContext, three entities, SampleCategoryRepository) | New |
| `Cohort.Sample.Tests/` (full A end-to-end matrix above) | New |

### A — Verification

1. `dotnet build Cohort.sln` — clean.
2. `dotnet test Cohort.Tests` — sub-second unit tests against the registry, validator, query builder, resolver default, using in-memory `DbContext` fixtures.
3. `dotnet test Cohort.Sample.Tests` — Testcontainers Postgres spins up, applies the sample's own migrations, runs the A end-to-end matrix.
4. **Manual smoke** — `dotnet run --project Cohort.Sample`, seed some rows, call the sweep manually, verify counts and that no rows past cutoff remain.

---

## Milestone B — Destructive strategies, holds, persisted audit

A is proof-of-shape. B adds the surfaces that get harder at scale: SoftDelete, Anonymise, the legal-hold join, and the persisted audit log with run-level/summary/optional-per-row split. The audit interface uses a discriminated-union event type so adding new event kinds in the future is open/closed — no method-additions to the interface.

### B.1 SoftDelete sweep strategy

**File:** `Cohort/Sweep/SoftDeleteSweepStrategy.cs`

- Query shape:
  ```sql
  UPDATE "{table}"
     SET "{is_deleted_column}" = true,
         "{deleted_at_column}" = @now      -- omitted if convention column absent
   WHERE "{anchor_column}" < @cutoff
     AND "{tenant_column}" = @tenantId
     AND "{is_deleted_column}" = false      -- don't double-soft-delete
     AND NOT EXISTS (
       SELECT 1 FROM "retention_holds"
        WHERE "entity_type" = @entityType
          AND "entity_id"   = "{table}"."id"
          AND ("expires_at" IS NULL OR "expires_at" > @now)
     )
  RETURNING "id";
  ```
- Convention validator extension: in B, the startup validator asserts that every entity in a `SoftDelete` category has a `bool IsDeleted` CLR property. The `DateTimeOffset? DeletedAt` property is optional but checked for type if present.
- Tests: convention satisfied → soft-deletes correctly; convention missing → startup fails with a clear error; double-sweep doesn't re-touch already-soft-deleted rows.

### B.2 Anonymise sweep strategy

**File:** `Cohort/Sweep/AnonymiseSweepStrategy.cs`

- Query shape (built dynamically per entity from its `[Anonymise]` field markers):
  ```sql
  UPDATE "{table}"
     SET "{field1}" = NULL,                              -- AnonymiseMethod.Null
         "{field2}" = '',                                -- AnonymiseMethod.EmptyString
         "{field3}" = @field3_literal                    -- AnonymiseMethod.FixedLiteral
   WHERE "{anchor_column}" < @cutoff
     AND "{tenant_column}" = @tenantId
     AND NOT EXISTS (...holds subquery...)
  RETURNING "id";
  ```
- Per-field literals are bound as parameters, not interpolated.
- Validator extension: every `Anonymise` category requires at least one `[Anonymise]` field marker on every member entity. Each marker is type-checked against its method (`Null` ↔ nullable, `EmptyString`/`FixedLiteral` ↔ `string`).
- Tests: each method works correctly on appropriate field types; validator rejects type mismatches at startup; idempotency (sweeping twice doesn't break anything because the WHERE clause filters by the anchor cutoff, not by field state).

### B.3 Holds repository + sweep query refactor

**File:** `Cohort/Holds/`

- `IRetentionHoldsRepository.cs`:
  ```csharp
  public interface IRetentionHoldsRepository
  {
      Task<bool> IsHeldAsync(string entityType, Guid entityId, CancellationToken ct);
      Task<RetentionHold> CreateAsync(RetentionHoldRequest req, CancellationToken ct);
      Task RemoveAsync(Guid holdId, CancellationToken ct);
      Task<IReadOnlyList<RetentionHold>> ListActiveAsync(string entityType, CancellationToken ct);
  }
  ```
- `RetentionHold.cs` + `RetentionHoldRequest.cs` records.
- `EfRetentionHoldsRepository.cs` — default implementation against the package's `retention_holds` table (created via `ConfigureCohortTables(ModelBuilder)` — see C.5).
- The sweep query refactor: A's purge query gets the `AND NOT EXISTS (...)` hold subquery added. B.1 and B.2 strategies include it from the start. The integration is at SQL level — no per-row C# check.
- The repository interface exists for the *host*'s benefit (creating holds, listing them in admin UIs), not for the sweep's benefit.
- Tests: held entities are not deleted/soft-deleted/anonymised; expired holds don't block sweeps; removing a hold makes the entity eligible on the next sweep.

### B.4 Audit writer with discriminated-union event interface

**File:** `Cohort/Audit/`

The audit writer interface is a single method taking a discriminated-union event. This is the open/closed shape — adding a new event kind in v2 (e.g. `Errored`, `Skipped`, `HoldRespected`) is a non-breaking addition because writers handle events via pattern matching, not method overrides.

```csharp
public interface IRetentionAuditWriter
{
    Task WriteAsync(SweepEvent evt, CancellationToken ct);
}

public abstract record SweepEvent
{
    public sealed record Started(
        Guid SweepId,
        DateTimeOffset At,
        bool DryRun,
        Guid TenantId) : SweepEvent;

    public sealed record EntitySummary(
        Guid SweepId,
        DateTimeOffset At,
        Type EntityType,
        string Category,
        Guid TenantId,
        Strategy Strategy,
        TimeSpan ResolvedPeriod,
        int Affected,
        int HeldCount) : SweepEvent;

    public sealed record RowDetail(
        Guid SweepId,
        DateTimeOffset At,
        Type EntityType,
        Guid EntityId,
        string Category,
        Strategy Strategy,
        Guid TenantId) : SweepEvent;

    public sealed record Completed(
        Guid SweepId,
        DateTimeOffset At,
        TimeSpan Duration,
        int TotalAffected) : SweepEvent;
}
```

**Per-row detail is opt-in per category** via the `AuditRowDetail` field on `RetentionRule`. The engine only emits `RowDetail` events for categories whose resolved rule has `AuditRowDetail.PerRow`. Default is `SummaryOnly` for all strategies — hosts opt in explicitly when they want forensic detail (typically on `Purge` categories with low row volume).

**Default writer + tables:**

- `EfRetentionAuditWriter.cs` — pattern-matches the event union and writes to:
  - `sweep_run` — one row per sweep, written on `Started` and updated on `Completed`.
  - `sweep_run_entity_summary` — one row per `EntitySummary` event, bounded by entities × tenants per sweep.
  - `sweep_run_row_detail` — one row per `RowDetail` event, only populated for categories that opted in.
- All three tables ship via `ConfigureCohortTables(ModelBuilder)` (see C.5) so they live in the host's migration history.
- Tests: writers receive every event in the right order; per-row detail is suppressed for `SummaryOnly` categories; `EntitySummary.HeldCount` correctly reflects how many candidate rows were skipped due to holds; `Completed.TotalAffected` matches the sum of summary counts.

### B.5 Sweep engine refactor to feed the audit writer

**File:** `Cohort/Sweep/RetentionSweepEngine.cs` (refactor)

A's engine returns `RetentionSweepResult` directly. B's engine still returns the result object (caller ergonomics — useful for tests, useful for hosts that want to react to sweep outcomes), but the result is *derived from* the audit events emitted during the sweep, not computed independently. The engine emits `Started` → per-entity `EntitySummary` (and conditionally `RowDetail`) → `Completed` to the audit writer, and aggregates the same data into the result.

Hosts who don't want persisted audit can register a no-op writer; the result object still works.

### B — Critical files

| File | Action |
|---|---|
| `Cohort/Sweep/SoftDeleteSweepStrategy.cs` | New |
| `Cohort/Sweep/AnonymiseSweepStrategy.cs` | New |
| `Cohort/Sweep/RetentionSweepEngine.cs` | Edit (audit-writer integration + hold subquery in purge) |
| `Cohort/Sweep/PurgeSweepStrategy.cs` | Edit (add hold subquery) |
| `Cohort/Startup/RetentionStartupValidator.cs` | Edit (extend for SoftDelete + Anonymise) |
| `Cohort/Holds/IRetentionHoldsRepository.cs` + records + EF impl | New |
| `Cohort/Audit/IRetentionAuditWriter.cs` + `SweepEvent.cs` + `EfRetentionAuditWriter.cs` | New |
| `Cohort.Sample/Entities/` | Edit (add SoftDelete, Anonymise, Held entities) |
| `Cohort.Sample.Tests/` | Edit (full B end-to-end matrix) |

### B — Verification

1. `dotnet build Cohort.sln` — clean.
2. `dotnet test Cohort.Tests` — extended unit suite for the new strategies + validator branches.
3. `dotnet test Cohort.Sample.Tests` — Testcontainers integration matrix:
   - SoftDelete sweep correctly tombstones rows past cutoff and only those rows.
   - Anonymise sweep correctly scrubs each method's fields, leaves non-annotated fields untouched.
   - Held entities survive every strategy.
   - Per-row audit emitted only for opted-in categories; summary-only categories produce zero `sweep_run_row_detail` rows.
   - `sweep_run` row is written on Started and updated on Completed; `Duration` and `TotalAffected` accurate.
   - The result object returned by the engine matches the audit event aggregate.

---

## Milestone C — Operational surface, erasure, migration polish

The package is functionally complete after B. C adds the operational sugar that makes it usable in production: scheduled worker, killswitch, dry-run mode, the Article 17 erasure trigger, the host-merged migration story, and the single-entry-point DI registration.

### C.1 Hosted service worker

**File:** `Cohort/Hosting/RetentionWorker.cs`

- `RetentionWorker : BackgroundService` — runs on a cron schedule from config. Default schedule disabled (host must opt in).
- Reads `Cohort:Schedule`, `Cohort:DryRun`, `Cohort:KillSwitch` from `IOptionsMonitor<CohortOptions>` so they can be flipped without restart.
- Killswitch behaviour: when flipped on, the worker finishes its current iteration cleanly and skips subsequent ticks until flipped off. No mid-transaction abort.
- Dry-run behaviour: when flipped on, the engine runs the same code path but uses `SELECT COUNT(*)` instead of `DELETE`/`UPDATE`. Audit events still fire, with `DryRun: true`, so the audit log shows what *would* have happened. This is what an admin UI shows as "preview the next sweep."

### C.2 Strongly-typed options + validation

**File:** `Cohort/Hosting/CohortOptions.cs`

```csharp
public sealed class CohortOptions
{
    public string? Schedule { get; init; }       // cron expression, null = no schedule
    public bool DryRun { get; init; }
    public bool KillSwitch { get; init; }
    public bool ApplyMigrations { get; init; }   // see C.5
}
```

Validated via `IValidateOptions<CohortOptions>` with `ValidateOnStart()`, matching the casebridge JWT pattern. Cron expression parsed and validated at startup, not at first tick.

### C.3 Single DI entry point

**File:** `Cohort/Hosting/ServiceCollectionExtensions.cs`

```csharp
public static IServiceCollection AddCohort<TContext>(
    this IServiceCollection services,
    Action<CohortOptions> configure)
    where TContext : DbContext;
```

Wires the registry, the validator, the strategies, the audit writer, the holds repo, the preview API, and the worker. Host can override any default via fluent helpers:

```csharp
services.AddCohort<CaseBridgeDbContext>(opts => { opts.Schedule = "0 2 * * *"; })
        .UseAuditWriter<MyAuditWriter>()
        .UseHoldsRepository<MyHoldsRepo>()
        .UseCategoryRepository<MyCategoryRepo>();
```

### C.4 Right-to-erasure (Art.17) trigger

**File:** `Cohort/Erasure/`

- `RetentionErasureService.cs`:
  ```csharp
  public interface IRetentionErasureService
  {
      Task<ErasureResult> HandleErasureRequestAsync(
          string subjectKey,
          ErasureScope scope,
          CancellationToken ct);
  }
  ```
- `ErasureSubjectAttribute.cs` — property-targeting. Marks a CLR property as the subject identifier for erasure requests. Examples: a `UserId` property on entities related to a citizen's data.
- The service walks the registry, finds every entity with an `[ErasureSubject]` property whose value matches `subjectKey`, and applies the entity's category strategy as if a sweep had matched it.
- Same audit event stream (`Started` event has a new `ErasureRequestId` field via a discriminated `Trigger` union — `Scheduled | Erasure(Guid requestId)`), same hold check, same transaction discipline.
- Returns an `ErasureResult` with per-entity counts and a `Guid` request id for the host's response packet to the regulator.
- This slice is *small* because everything it needs already exists from B. The work is the new attribute, the entry point, the trigger union extension on `SweepEvent.Started`, and the tests proving cascade behaviour and hold-respect.

### C.5 Migration host-merging

**File:** `Cohort/Migrations/CohortModelBuilder.cs`

- `public static ModelBuilder ConfigureCohortTables(this ModelBuilder modelBuilder)` — adds the `retention_holds`, `sweep_run`, `sweep_run_entity_summary`, and `sweep_run_row_detail` entity configurations to the host's `DbContext` model. The host calls this from `OnModelCreating` and Cohort's tables become part of the host's existing migration history. No second migration history table.
- Decision #3 from the planning brief: **host-merged** is the recommended path, baked into C.5. Hosts who prefer Cohort to own its own history can use `dbContext.Database.Migrate()` against a separate migration assembly — the package supports both, but documents host-merged as the primary path.
- Tests: a sample host that calls `ConfigureCohortTables` produces the expected EF model; the resulting migration includes Cohort's tables in the host's history; the tables match the schema expected by the EF default implementations of the audit writer and holds repo.

### C — Critical files

| File | Action |
|---|---|
| `Cohort/Hosting/RetentionWorker.cs` | New |
| `Cohort/Hosting/CohortOptions.cs` + validator | New |
| `Cohort/Hosting/ServiceCollectionExtensions.cs` | New |
| `Cohort/Erasure/IRetentionErasureService.cs` + impl + `ErasureSubjectAttribute.cs` + `ErasureResult.cs` | New |
| `Cohort/Audit/SweepEvent.cs` | Edit (add `Trigger` union to `Started` event) |
| `Cohort/Migrations/CohortModelBuilder.cs` | New |
| `Cohort.Sample/Program.cs` | Edit (wire `AddCohort<>` + `ConfigureCohortTables`) |
| `Cohort.Sample.Tests/` | Edit (worker schedule + killswitch + dry-run + erasure end-to-end) |

### C — Verification

1. `dotnet build Cohort.sln` — clean.
2. `dotnet test` — full unit + integration suites green.
3. **Manual smoke**: run the sample with the worker enabled on a fast schedule (`*/1 * * * *`), watch sweeps fire, flip the killswitch via config hot-reload, watch the worker honour it.
4. **Erasure end-to-end**: the sample test seeds a citizen with data across multiple entities, calls `HandleErasureRequestAsync`, asserts every entity touched matches the category strategy and the audit log records the erasure request id.
5. **Migration end-to-end**: the sample's migrations include Cohort's tables in a single history, and `dotnet ef migrations script` produces a coherent SQL output.

---

## Decisions to confirm before starting

1. **Repo location.** New repo at `~/Sites/cohort` with its own GitHub remote? Or under an org? Affects nothing structurally, just where the `git clone` lives.
2. **`Cohort.Sample` location.** In the same repo as the package (recommended — dogfood is the whole point) or as a separate sibling repo. Recommend **same repo, separate solution folder**.

(Decision #3 from the original brief — migration ownership — has been resolved in C.5: host-merged via `ConfigureCohortTables`. Decision #5 — HMAC key source — has been deleted because no hashing in v1. Decisions #1 and #4 — name and soft-delete attribute — have been resolved in the body of the plan.)

## Anti-scope guardrails

The temptation with a clean greenfield package is to ship every clever shape on day one. **Resist.** The lifted contract is the only thing that has to be right; everything past `StaticRetentionRuleResolver` is a host concern that proves itself by being needed. Specifically *not* in v1 across any milestone:

1. **`ConditionalRetentionRuleResolver`** — wait until a council needs jurisdiction-specific rules.
2. **`AliasRetentionRuleResolver`** — same. The exception type ships, the implementation does not.
3. **`CachingRetentionRuleResolver`** — premature; static resolvers don't need caching, no effectful resolvers exist yet.
4. **OneTrust / Purview adapters** — not until a real council is on one of those tools.
5. **Source generator** — reflection is fine for daily sweeps. Revisit only if startup validation becomes a measurable hot path.
6. **Row-level rules** ("retain this specific submission for X years because the case is in litigation") — that's what `retention_holds` is *for*. Resist the urge to invent a per-row rule system; use holds.
7. **Hash, format-preserving pseudonymisation, per-subject keyed pseudonymisation** — all defer to a v2 brief that does the KMS / uniqueness / reversibility thinking properly.
8. **Multi-database support beyond Postgres** — package will probably work on SQL Server with minor query tweaks, but don't *test* against SQL Server until a real consumer needs it.
9. **Threshold-based per-row audit** ("only log row detail if count < N") — too clever for v1. Per-category boolean opt-in via `AuditRowDetail` is enough; smarter throttling can be a host-side `IRetentionAuditWriter` decoration if a real consumer needs it.

## Open / not in plan

- **Casebridge consumption brief** — the follow-up planning pass after Cohort v1 (all three milestones) lands. Will cover: defining `CaseBridgeRetentionCategories`, tagging every entity in `CaseBridgeDbContext`, building the `retention_category_config` table + admin UI, wiring the worker, adding the casebridge-side arch test, exposing the Art.17 endpoint. Probably 6-8 slices of its own. Should NOT be bundled with Cohort v1.

- **Cross-category cascade FK design.** When a child entity outlives its parent because they're in different categories, the FK from child to parent must be `ON DELETE SET NULL` or stored as a soft reference. **Cohort doesn't enforce this** — it's a host schema decision. The casebridge consumption brief should include an arch test that detects FK constraints between entities in categories with different periods.

- **Perch consumption.** Useful second consumer to validate that Cohort's contract isn't accidentally casebridge-shaped. Defer until Cohort has a stable v1 — adding a second consumer too early biases the design.

- **Compliance officer sign-off on the strategy enum vocabulary.** Worth running `Purge` / `SoftDelete` / `Anonymise` / `Exempt` past whoever ends up owning the GDPR conversation with councils. UK ICO uses "erasure", "pseudonymisation", "restriction of processing". Aligning the enum names to their vocabulary makes the audit conversation smoother. Not a code blocker.

- **Hash / pseudonymisation v2 brief.** Once a real consumer asks for pseudonymisation that survives joins (e.g. "anonymise the user but keep the analytics correlating their actions"), the v2 brief has to think through: per-subject keys, KMS integration, format-preserving algorithms (FF1/FF3), uniqueness constraint preservation, key rotation, and what the audit log records when a key rotates. Substantial work. Out of scope for v1.

- **Shadow-property and owned-type anchors.** v1 requires the anchor to be a direct CLR property. Hosts using shadow properties (`OnModelCreating` `b.Property<DateTimeOffset>("CreatedAt")`) or owned-type embedded timestamps will need to either expose them as CLR properties or wait for v2. Documented as a known limitation in the v1 README.
