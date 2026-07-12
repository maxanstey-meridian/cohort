# Cohort Pre-1.0 Refactor

## Purpose

This document defines the coordinated pre-1.0 refactor for Cohort. It is the implementation contract for the work: settled decisions, required outcomes, migration constraints, phased execution, verification, and definition of done.

Final closure and sign-off moved to `SIGNABLES.md`. This file remains the original design and regression baseline; where its checked Definition of Done conflicts with second-pass evidence or closure status, `SIGNABLES.md` is authoritative.

The refactor must land as one coherent contract release. Public contracts, schema identity, audit guarantees, erasure semantics, and startup validation intersect too heavily to ship as unrelated behavioral changes.

Baseline before this work:

- 194 unit tests pass in `Cohort.Tests`.
- 339 PostgreSQL tests pass in `Cohort.Sample.Tests`.
- The worktree is clean.

## Settled Decisions

- Erasure ignores ordinary `RetentionRule.Period`.
- Active legal holds block erasure.
- Positive `LegalMin` blocks erasure until the minimum age has elapsed.
- A configured retention period alone is not a legal reason to refuse erasure.
- Holds are host decisions and must represent a defensible, case-specific reason to continue processing.
- Soft delete remains invalid as erasure unless the caller explicitly opts in.
- Existing persisted exception text will not be redacted during upgrade.
- All newly persisted failure diagnostics must be sanitized.
- PostgreSQL table references must be explicitly schema-qualified.
- Runtime SQL must not depend on PostgreSQL `search_path`.
- The category repository/resolver pair will be replaced by one direct rule-provider port.
- Dynamic rule providers must declare their possible strategies for startup validation.
- The authoritative Cohort audit ledger cannot be replaced by consumers.
- Consumers may register best-effort post-commit audit observers.
- This is intentionally a breaking pre-1.0 API release.

## Legal Interpretation

Article 17 requires erasure without undue delay where the right applies. Article 17(3) permits continued processing where necessary for, among other things, compliance with a legal obligation or the establishment, exercise, or defence of legal claims.

Cohort therefore treats `LegalMin` and active holds as configured blockers, but does not treat ordinary `Period` as a blocker. Cohort cannot determine whether a host's configured legal minimum or hold is legally valid. Hosts remain responsible for assessing each request and configuring only defensible exceptions.

References:

- [UK GDPR Article 17](https://www.legislation.gov.uk/eur/2016/679/article/17/data.html)
- [ICO: Right to erasure](https://ico.org.uk/for-organisations/uk-gdpr-guidance-and-resources/individual-rights/individual-rights/right-to-erasure/)

This design records the technical default and is not legal advice.

## Required Outcomes

### Compliance corpus

- Cohort's complete supported compliance contract is enumerated in one versioned manifest.
- Every supported requirement has a stable ID, source or product provenance, an explicit behavioral statement, and one or more executable corpus tests.
- Every corpus test maps back to a manifest requirement.
- Unsupported regulatory responsibilities are explicitly recorded as non-goals rather than silently omitted.
- Corpus tests exercise the real public API and PostgreSQL-backed runtime path at the boundary where each requirement becomes observable.
- The corpus remains stable throughout the refactor; implementation wiring may change, but settled expected outcomes may not be rewritten to match implementation drift.
- Existing mechanism-specific tests remain in place and complement rather than replace the corpus.

### Erasure

- Subject erasure acts immediately regardless of ordinary retention period.
- Active holds still block matching rows.
- Positive `LegalMin` still blocks rows until its strict cutoff has elapsed.
- With no positive `LegalMin`, no anchor predicate is applied.
- Nullable and future-dated anchors are eligible when there is no positive `LegalMin`.
- Nullable anchors are blocked and reported when a positive `LegalMin` prevents establishing minimum age.
- Preview, dry run, live mutation, candidate discovery, locking, handler-aware execution, and final mutation revalidation use identical eligibility rules.
- Erasure audit summaries report `LegalMin ?? TimeSpan.Zero` as the resolved period.
- Purge, anonymise, and explicitly approved soft delete share the same erasure eligibility.
- `Strategy.Exempt` remains non-mutating and visible in per-entity results; it must not be presented as proof of a statutory exemption.

### Domain contracts

- `RetentionRule` cannot be constructed or copied into an invalid state.
- `RetentionRule` has get-only properties and no public positional deconstruction surface.
- Rule-level `AuditRowDetail.Inherit` is rejected.
- `RetentionEntityId` always means the stable UUID identifying a retained entity type.
- `RecordId` always means the canonical text identity of a row.
- Public and persisted row-level uses of `EntityId` are renamed to `RecordId`.

### Rule resolution and startup validation

- One `IRetentionRuleProvider` replaces `IRetentionCategoryRepository` and `IRetentionRuleResolver`.
- Rule resolution remains asynchronous and receives category, tenant, logical time, and alias path.
- Every category synchronously declares a non-empty set of possible strategies.
- Startup validates the union of all possible strategies for each retained entity.
- Runtime rejects a strategy that was not declared in category capabilities.
- Invalid `[ErasureSubject]` metadata fails startup rather than the first erasure request.
- Host-owned aliases and alias-cycle reporting remain supported.

### Schema safety

- Every retained table reference is schema-qualified.
- Every Cohort-owned table reference is schema-qualified.
- Missing EF schema resolves explicitly to `public`.
- `ConfigureCohortTables(ModelBuilder)` remains available and maps Cohort tables to `public`.
- `ConfigureCohortTables(ModelBuilder, string schema)` maps all Cohort tables to the supplied schema.
- Table adoption compares both schema and table name.
- Schema validation resolves objects through `pg_namespace` and `pg_class`, not `search_path`.
- A same-named decoy table in another schema cannot be read, validated, or mutated accidentally.
- One internal schema contract defines required columns, types, indexes, constraints, and foreign keys for both model construction and validation.

### Audit ownership

- The EF audit writer is an internal, unconditional part of Cohort execution.
- Consumers cannot replace or suppress authoritative audit persistence.
- Mutation and its required audit evidence remain in the same transaction.
- Public audit observers run only after commit.
- Observer failures never roll back retention or alter run status.
- Rolled-back events are never sent to observers.
- Observer delivery is explicitly best effort, not a durable integration guarantee.

### Failure safety

- Exception messages and stack traces are never persisted in new audit or handler rows.
- Persisted failures contain a safe exception type, machine code where available, and diagnostic ID.
- Result objects and observer events expose only sanitized failure details.
- Structured logs retain the original exception and the same diagnostic ID.
- Existing historical `Error` and `LastError` values remain unchanged.

### Runtime readiness

- Every public database operation validates provider, model configuration, and installed Cohort schema.
- Public operations work correctly even when invoked without starting the generic host.
- Successful readiness is cached safely across concurrent callers.
- Failure and cancellation are never cached.
- Readiness failures occur before a `Started` audit event is written.

### Operational behavior

- One tenant failure does not prevent later tenants or tenantless entities from running.
- Duplicate tenant IDs execute once per scheduled occurrence.
- Tenant execution remains sequential.
- Failed `OnBeforeAsync` rows do not create an unbounded process-memory list or SQL parameter array.
- Committed row-detail state prevents a failed row from spinning within the same run.
- Rolled-back row-detail state never hides a row from retry.
- Dispatcher numeric options have explicit safe ceilings.
- Dispatcher time arithmetic saturates rather than overflowing.
- Long poll and heartbeat delays are chunked safely.
- Advisory-lock release or connection-close failure does not mask an existing primary exception.

### Public package surface

- The complete public API is allowlisted, including public members, nested types, constants, and enum members.
- Reflection and ordering helpers used only by Cohort are internal.
- The sample exercises the real public ports directly without a pass-through facade.

## Phase 0: Executable Compliance Corpus

Create a versioned compliance manifest and a dedicated PostgreSQL-backed acceptance-test area before changing production contracts.

Recommended structure:

```text
corpus/
└── compliance-manifest.json

Cohort.Sample.Tests/
└── ComplianceCorpus/
    ├── ComplianceCorpusManifestTests.cs
    ├── RetentionScopeCorpusTests.cs
    ├── StorageLimitationCorpusTests.cs
    ├── StrategySemanticsCorpusTests.cs
    ├── LegalMinimumCorpusTests.cs
    ├── HoldsCorpusTests.cs
    ├── ErasureCorpusTests.cs
    ├── AnonymisationCorpusTests.cs
    ├── AccountabilityCorpusTests.cs
    ├── IsolationCorpusTests.cs
    ├── FailureSafetyCorpusTests.cs
    └── OperationalReadinessCorpusTests.cs
```

The manifest is the inventory of Cohort's claimed support. It must not claim that Cohort provides complete GDPR compliance or adjudicates legal requests.

Each supported requirement must record at least:

```json
{
  "id": "STORAGE-001",
  "source": "GDPR Article 5(1)(e)",
  "requirement": "Personal data is not retained beyond its resolved retention period",
  "tests": [
    "StorageLimitationCorpusTests.Expired_rows_are_processed"
  ],
  "status": "supported"
}
```

The manifest may cite a statutory source, regulator guidance, or an explicit Cohort product invariant. A citation explains provenance; it is not a certification claim.

### Corpus contract areas

#### Retention scope

- `[Retain]` entities participate in retention.
- Unannotated entities are implicitly exempt.
- `[ExemptFromRetention]` entities are explicitly exempt.
- Conflicting annotations fail startup.
- Unsupported EF model shapes fail before mutation.
- Owned, shared, and table-split mappings cannot cause accidental mutation.

#### Storage limitation

- Expired rows are processed.
- Unexpired rows survive.
- Rows exactly on the cutoff survive.
- Zero retention means immediate scheduled eligibility.
- Null anchors survive scheduled sweeps and are reported.
- Dynamic tenant- and time-specific policy resolution is respected.
- Ordinary sweeps use the greater of `Period` and `LegalMin`.
- `Strategy.Exempt` performs no mutation.

#### Strategy semantics

- Purge physically removes only eligible rows.
- Soft delete changes only configured deletion markers.
- Anonymise changes only declared fields and preserves non-personal fields.
- Already-anonymised rows are not anonymised repeatedly.
- Factory-backed and provider-converted values retain correct CLR/store semantics.
- Invalid strategy-specific models fail before execution.

#### Legal minimums

- Ordinary sweeps apply `max(Period, LegalMin)`.
- Exact legal-minimum boundaries remain protected.
- Contextual legal minima are resolved for the correct tenant and logical time.
- Null anchors cannot demonstrate that a positive legal minimum elapsed.
- Audit evidence records the policy actually applied.

#### Holds and restrictions

- Active holds prevent mutation.
- Removed and expired holds no longer prevent mutation.
- Holds are scoped to the correct retention entity, record, and tenant.
- Holds affect purge, soft delete, anonymise, and erasure consistently.
- Hold creation validates target existence and tenant ownership.
- Concurrently created holds are rechecked before mutation.
- Cohort enforces configured holds but does not adjudicate their legal validity.

#### Erasure

- Accepted erasure requests ignore ordinary retention period.
- Positive legal minimum and active holds remain configured blockers.
- Subject and tenant matching are exact.
- Primary and alternate subject identifiers work.
- Null anchors are immediately eligible without a positive legal minimum.
- Purge, anonymise, and explicitly approved soft delete apply the same eligibility.
- Soft delete without explicit erasure approval is refused.
- Preview, dry run, and live execution agree.
- Concurrent eligibility changes are rechecked before mutation.

#### Anonymisation and data minimisation

- Only properties marked for anonymisation are changed.
- Null, empty-string, fixed-literal, and factory-backed methods obey their declared semantics.
- The anonymised-at marker is written atomically with anonymisation.
- Original values supplied to factories use the expected CLR representation.
- Anonymised output is converted to the configured provider representation.
- Data outside the declared anonymisation scope remains unchanged.

#### Accountability

- Mutating runs have coherent started and terminal evidence.
- Audit totals agree with committed mutations.
- Per-row evidence exists when configured.
- Rolled-back mutations do not leave committed row evidence.
- Rule provenance is preserved.
- Held, skipped, null-anchor, and failed work is visible at the appropriate level.
- Authoritative audit evidence cannot be disabled by consumer registration.
- Persisted diagnostics do not contain arbitrary personal data.
- Observer failure cannot compromise authoritative evidence.

#### Tenant, subject, and schema isolation

- A tenant cannot mutate another tenant's rows.
- Tenanted and tenantless runs cannot leak into one another.
- A subject request cannot mutate another subject's rows.
- Record IDs of different CLR and store types remain correctly scoped.
- Same-named tables in another schema cannot be read, validated, or mutated.
- Caller-tracked EF entities cannot overwrite Cohort mutations.

#### Failure safety

- Partial batch failure does not over-report affected rows.
- Committed batches remain durable.
- Rolled-back batches remain retryable.
- Failed row handlers do not cause an in-run processing loop.
- Stale discovery cannot mutate newly ineligible rows.
- Lock contention cannot weaken final policy predicates.
- Cancellation produces coherent terminal evidence.
- Cleanup failure cannot mask the primary failure.
- One entity or tenant failure does not suppress unrelated work.

#### Operational readiness

- Unsupported providers fail before mutation.
- Missing or malformed Cohort schema fails before mutation.
- Every possible dynamic strategy is validated before execution.
- Direct public invocation enforces the same readiness as hosted startup.
- Migration upgrades preserve holds and audit history.
- Kill switches and advisory locks prevent unintended scheduled execution.

### Corpus mechanics

- Use stable requirement and scenario IDs so failures name the contract that broke.
- Use typed C# arrangements for executable scenarios; do not force EF entities, timestamps, holds, or strategies into an untyped JSON fixture language.
- Use the JSON manifest only for inventory, provenance, status, and test linkage.
- Exercise public Cohort ports through the sample host and real PostgreSQL.
- Assert observable entity state, public results, audit evidence, and isolation boundaries.
- Do not assert internal call order.
- Keep canonical policy cases in the corpus and detailed mechanism permutations in the existing focused suites.
- A known behavior being intentionally changed may make its new corpus scenario red before implementation. Unrelated corpus scenarios must remain green.

### Manifest completeness gate

`ComplianceCorpusManifestTests` must prove:

- Requirement IDs are unique.
- Every requirement has a non-empty source or product provenance.
- Every supported requirement names at least one executable acceptance test. Any manifest-linked executable acceptance test is part of the corpus regardless of its folder.
- Every named test exists and is an executable xUnit test.
- Every test under `ComplianceCorpus` is named by at least one manifest requirement.
- Status values come from a closed supported vocabulary.
- Unsupported and out-of-scope requirements cannot name passing support tests.

### Relationship to existing tests

The test layers have different jobs:

| Layer | Responsibility |
| --- | --- |
| Compliance corpus | Defines what Cohort promises through every manifest-linked executable acceptance test, regardless of folder |
| Other end-to-end tests | Prove specific SQL, transaction, batching, handler, audit, and concurrency mechanisms preserve the promise |
| Pure unit tests | Prove calculations and invariant validation |
| Migration and package tests | Prove deployability and consumer compatibility |

Do not delete detailed tests merely because the same high-level outcome appears in the corpus.

### Phase gate

- The manifest inventories every compliance capability Cohort claims to support.
- Explicit non-goals cover adjacent GDPR responsibilities Cohort does not implement.
- Manifest completeness tests pass.
- Corpus scenarios execute through public APIs and real PostgreSQL.
- Existing green behavior remains green except for explicitly red target-state scenarios.
- The immediate-erasure scenario is red for the intended reason before production behavior changes.
- The corpus is reviewed as product and compliance intent, not merely as test implementation.

## Phase 1: Public Contract Gate

Replace the Infrastructure-only API check in `PackageReleaseContractTests.cs` with a whole-assembly contract.

The gate must compare:

- Exported top-level and nested types
- Public constructors
- Public methods and extension methods
- Public properties
- Public fields and constants
- Enum members

Planned removals:

- `IRetentionCategoryRepository`
- `IRetentionRuleResolver`
- `StaticRetentionRuleResolver`
- `IRetentionAuditWriter`
- `ReflectionMemberResolver`
- `RowHandlerPriorityAttribute.GetPriority`
- `RowHandlerPriorityAttribute.DefaultPriority`

Planned additions:

- `IRetentionRuleProvider`
- `RetentionCategoryCapabilities`
- `IRetentionAuditObserver`
- Public `RecordId` properties replacing row-level `EntityId`

### Phase gate

- The allowlist describes the intended post-refactor API.
- The test fails for any unplanned public type or member.
- The clean-consumer package test still compiles against the intended API.

## Phase 2: Domain and Identity Contracts

### `RetentionRule`

Refactor `Cohort/Domain/RetentionRule.cs` into a non-positional `sealed record` with one explicit constructor and get-only properties.

Validate:

- `Period >= TimeSpan.Zero`
- `LegalMin` is null or non-negative
- `Strategy` is defined
- `AuditRowDetail` is defined
- `AuditRowDetail.Inherit` is not accepted on a resolved category rule

### Identity vocabulary

Rename:

- `RetentionEntry.EntityId` to `RetentionEntityId`
- `RetentionAfterContext<TEntity>.EntityId` to `RecordId`
- `SweepEvent.RowDetail.EntityId` to `RecordId`
- `sweep_run_row_detail."EntityId"` to `"RecordId"`

Add `RetentionEntityId` to `EntitySweepCount` where necessary to preserve unambiguous correlation.

### Migration

Add a forward sample migration that renames the row-detail column without losing audit history. Do not rewrite historical migrations.

Document the equivalent host migration requirement.

### Phase gate

- `RetentionRule` contract tests cover every invariant.
- Reflection proves rule properties are get-only.
- No public `Deconstruct` exists.
- Migration-chain tests prove historical row-detail data survives the rename.
- All production and test code uses the settled identity vocabulary.

## Phase 3: Direct Rule Provider

Replace the existing category repository and resolver interfaces with:

```csharp
public interface IRetentionRuleProvider
{
    RetentionCategoryCapabilities? GetCapabilities(string category);

    Task<RetentionRule?> ResolveAsync(
        RetentionResolutionContext context,
        CancellationToken ct
    );
}
```

`RetentionCategoryCapabilities` must be immutable, invariant-safe, and contain a non-empty defensively copied strategy set.

Runtime flow:

1. Build `RetentionResolutionContext`.
2. Resolve the rule directly from the provider.
3. Reject an unknown category or unresolved runtime rule.
4. Reject a returned strategy not declared by capabilities.
5. Verify an internal execution strategy exists unless the result is `Exempt`.

Startup flow:

1. Load capabilities for every retained category.
2. Reject missing or empty capabilities.
3. Validate the union of all declared strategy requirements.
4. Aggregate failures across entities and categories.

Strategy-specific startup validation:

- `Purge`: relational dependency and restrictive-FK behavior
- `SoftDelete`: flag, deleted-at metadata, types, and mappings
- `Anonymise`: fields, methods, factories, provider conversion, and marker
- `Exempt`: no mutation-specific metadata

Extract `[ErasureSubject]` metadata resolution into a shared internal component. Startup must validate marked properties, EF mapping, physical columns, compatible subject types, and provider conversion metadata. Runtime must only validate and convert the supplied subject value.

Aliases remain host-owned. Alias resolution must preserve tenant, logical time, and alias path. `RetentionAliasCycleException` remains public.

### Phase gate

- All repository/resolver production types and registrations are removed.
- Dynamic tenant- and time-dependent providers still work.
- A provider returning an undeclared strategy fails deterministically.
- Multi-strategy categories validate every possible shape.
- Invalid erasure metadata fails startup.
- Alias traversal and cycle tests remain green.

## Phase 4: Schema Qualification

Introduce one infrastructure-local physical identifier:

```csharp
internal sealed record RelationalObjectName(string Schema, string Name);
```

Introduce one PostgreSQL formatter that correctly quotes each identifier component. Callers must never pass prequoted identifiers.

Resolve retained-table schema using:

```csharp
entityType.GetSchema()
    ?? db.Model.GetDefaultSchema()
    ?? "public"
```

Create an internal `CohortStoreTables` value containing the qualified identity of all five Cohort-owned tables, derived from finalized EF metadata.

Thread qualified names through:

- `RelationalSweepStrategyCore`
- `PurgeSweepStrategy`
- `SoftDeleteSweepStrategy`
- `AnonymiseSqlBuilder` and executors
- `EfRetentionHoldsRepository`
- `RetentionHoldSql`
- `EfRetentionAuditWriter`
- `RetentionHandlerSupport`
- `RetentionRowDispatcher`
- `CohortSchemaValidator`

Add the model-builder overload:

```csharp
ConfigureCohortTables(ModelBuilder modelBuilder, string schema)
```

Keep the original binary signature and map it explicitly to `public`.

Extract an internal `CohortSchemaContract` consumed by model construction and schema validation. The host EF model provides physical identifiers but is not the authority for required runtime capabilities.

Replace unqualified `to_regclass` lookup with explicit `pg_namespace` and `pg_class` matching. Error messages must include qualified table names.

### Phase gate

- No application-table SQL is emitted with an unqualified relation name.
- Explicit schema tests pass with that schema absent from `search_path`.
- Same-named decoy tables in `public` remain untouched.
- Holds, audit, handlers, preview, sweep, and erasure all use mapped schemas.
- Mixed-case, whitespace, and embedded-quote identifiers are safe.
- Schema adoption distinguishes same-named tables in different schemas.

## Phase 5: Immediate Erasure

Add erasure-specific cutoff functions to `CutoffCalculator` rather than overloading sweep semantics.

Suggested API:

```csharp
TimeSpan ResolveErasureMinimumAge(TimeSpan? legalMin);
DateTimeOffset? ComputeErasureCutoff(DateTimeOffset now, TimeSpan? legalMin);
```

Interpretation:

- Null or zero `LegalMin` returns no cutoff and emits no anchor predicate.
- Positive `LegalMin` returns `now - LegalMin`.
- Ordinary `Period` is never used by erasure.

No-positive-`LegalMin` eligibility:

```text
subject match
AND tenant match
AND strategy eligibility
AND no active hold
```

Positive-`LegalMin` eligibility:

```text
subject match
AND anchor < legal-minimum cutoff
AND tenant match
AND strategy eligibility
AND no active hold
```

Apply this predicate consistently during discovery, locking, handler loading, final mutation, preview, held counting, dry run, and live execution.

Add erasure null-anchor counting for positive legal minima. Include it in `EntitySweepCount` and audit summaries.

Set erasure summary `ResolvedPeriod` to `LegalMin ?? TimeSpan.Zero`.

### Phase gate

- A fresh matching row inside ordinary `Period` is erased.
- Positive `LegalMin` blocks a fresh row.
- A row exactly at the legal-minimum cutoff remains blocked.
- A row older than the cutoff by one tick is eligible.
- Null anchors are eligible without positive `LegalMin`.
- Null anchors are blocked and reported with positive `LegalMin`.
- Active holds block otherwise eligible rows.
- Preview and dry run predict the same rows as live mutation.
- Purge, anonymise, and opted-in soft delete behave identically.
- Handler-aware and bulk paths behave identically.
- Final mutation rechecks holds, subject, tenant, and legal-minimum eligibility after lock waits.

## Phase 6: Authoritative Audit and Observers

Remove the public replaceable `IRetentionAuditWriter` port.

Make `EfRetentionAuditWriter` the internal authoritative writer. Inject the sealed concrete type directly into run orchestration and register it unconditionally.

Add:

```csharp
public interface IRetentionAuditObserver
{
    Task OnCommittedAsync(SweepEvent evt, CancellationToken ct);
}
```

Add an internal notifier that resolves all registered observers.

Observer rules:

- Invoke only after the relevant database transaction commits.
- Isolate each observer from other observers.
- Bound observer execution time.
- Log observer failure.
- Never change retention result, audit status, or committed data.
- Never emit an event for rolled-back work.

Guaranteed external export is out of scope. Hosts needing guaranteed export must poll the authoritative tables, use CDC, or build a durable host-owned outbox.

### Phase gate

- A consumer cannot replace or suppress authoritative audit persistence.
- Audit evidence remains atomic with mutation where required.
- Multiple observers may be registered.
- Observer failures do not fail or partially fail a run.
- Rolled-back row details and progress events are never observed.
- Committed lifecycle events are observed in order.
- Package tests expose observers but not the authoritative writer.

## Phase 7: Sanitized Failure Diagnostics

Add a central internal `RetentionFailureDiagnostic`.

Persist only:

- Root exception type
- Safe machine code, such as PostgreSQL SQLSTATE or bounded `HResult`
- Random diagnostic ID

Do not persist:

- `Exception.Message`
- Stack trace
- SQL text containing values
- Subject identifiers
- Arbitrary handler or provider text

Use sanitized diagnostics in:

- `sweep_run.Error`
- `sweep_row_handler_status.LastError`
- `RetentionSweepResult.EntityFailures`
- `ErasureResult.EntityFailures`
- Observer events

Log the original exception with the same diagnostic ID and safe structural context.

Static Cohort-authored messages may remain where they cannot contain external values.

Do not redact historical database values as part of this refactor.

### Phase gate

- Tests force failures containing unique personal-data sentinels.
- Sentinels are absent from persisted rows, results, and observer events.
- Persisted diagnostics contain type, safe code, and diagnostic ID.
- Captured structured logs contain the original exception and matching diagnostic ID.
- No production path sends raw `ex.Message` to an audit or handler persistence command.

## Phase 8: Runtime Readiness

Add `RetentionRuntimeReadinessValidator` with a shared concurrency gate.

It validates:

- Npgsql provider
- Retention model and rule-provider configuration
- Installed Cohort schema

Successful validation is cached. Failed or cancelled validation is retryable.

Invoke readiness from:

- `IRetentionSweep`
- Audited dry-run execution
- `IRetentionPreview`
- `IRetentionErasureService`
- Every `IRetentionHoldsRepository` operation
- `IRetentionRowDispatcher.FlushAsync`
- Dispatcher hosted iteration
- Worker iteration
- Startup hosted validation

Public argument validation should occur before creating a scope. Runtime readiness must occur before writing a durable `Started` event.

### Phase gate

- Public operations invoked without `IHost.StartAsync` fail with a clear configuration exception when not ready.
- Missing migrations do not leak raw PostgreSQL errors from public operations.
- Applying migrations after a failed call allows the next call to succeed.
- Concurrent first callers share one successful validation execution.
- Failure and cancellation are not cached.
- Hosted startup and direct invocation report the same readiness failures.

## Phase 9: Operational Hardening

### Tenant isolation

Update `RetentionWorker` to materialize and deduplicate tenant IDs while preserving first-seen order.

Requirements:

- Duplicate IDs run once.
- Conflicting duplicate contexts produce a warning and use the first context.
- Null source entries are rejected explicitly.
- Each tenant pass catches and logs non-cancellation failures independently.
- Cancellation stops the iteration immediately.
- Tenantless execution still runs after a tenant failure.
- Kill switch is rechecked between passes.
- Execution remains sequential.

### Persisted failed-row exclusion

Remove run-wide `excludedRecordIds` collections and parameters.

Candidate discovery must exclude committed row details for the current stable run identity using `NOT EXISTS` over:

- `SweepId`
- `RetentionEntityId`
- `RecordId`
- `Category`
- `Strategy`
- `TenantId`

Keep batch-local attempted IDs where needed for lock races. Keep batch-local skipped IDs for progress reporting.

### Dispatcher bounds and time safety

Enforce:

- `BatchSize <= 10_000`
- `MaxParallelism <= 256`
- `MaxAttempts <= 1_000`

Add saturating subtraction for payload, claim, and settlement cutoffs.

Chunk long poll and heartbeat delays. Do not pass arbitrary configured `TimeSpan` values directly to `Task.Delay`.

### Cleanup exception precedence

Lock release and owned-connection closure must follow this precedence:

1. Preserve the primary operation exception.
2. Log unlock and close failures as secondary when a primary exception exists.
3. Surface unlock failure when no primary exception exists.
4. Surface close failure when neither primary nor unlock failure exists.

Cleanup must use an independent bounded cancellation token.

### Phase gate

- One tenant failure does not starve later tenants or tenantless entities.
- Duplicate tenant IDs execute once.
- High-volume `OnBeforeAsync` failures do not create run-sized in-memory lists or parameter arrays.
- Healthy rows behind failing rows still complete.
- Rolled-back exclusions do not hide rows from retry.
- Extreme duration values do not overflow timestamps or fail delay creation.
- Invalid numeric option values fail startup validation.
- Lock cleanup does not mask the primary exception.

## Phase 10: Cleanup and Documentation

- Remove `SampleRetentionStartupService`.
- Resolve and invoke public ports directly in the sample and test host.
- Move `ReflectionMemberResolver` into Infrastructure and make it internal.
- Move row-handler priority lookup and default ordering into Infrastructure.
- Remove stale milestone comments.
- Replace sample repository/resolver implementations with a direct provider.
- Update `CLAUDE.md` for the new public contracts and identity vocabulary.
- Rewrite README examples and explanations.

README must cover:

- Immediate subject erasure
- Legal minimum and hold blockers
- Direct rule provider and declared capabilities
- Startup validation of all possible strategies
- Startup validation of erasure subjects
- Explicit schema mapping and migration ownership
- Authoritative audit tables and best-effort observers
- Sanitized persisted diagnostics
- `RetentionEntityId` versus `RecordId`
- Breaking API and database migration notes

Do not broadly rearrange existing tests solely because files are large. Add focused files for new concerns and move existing tests only when their fixtures move cleanly without creating a test framework.

Recommended focused test files:

- `SchemaQualificationEndToEndTests.cs`
- `RuntimeReadinessEndToEndTests.cs`
- `RetentionWorkerTenantIsolationEndToEndTests.cs`
- `OnBeforeFailureProgressEndToEndTests.cs`
- `AdvisoryLockCleanupEndToEndTests.cs`
- `RetentionRowDispatcherOperationalEndToEndTests.cs`

### Phase gate

- Sample uses only intended consumer-facing APIs.
- Documentation contains no removed type or old identity name.
- Stale milestone comments are gone.
- Package README and clean-consumer tests describe and compile against the final contract.

## Migration Requirements

The release must include explicit host guidance for these changes:

- Rename `sweep_run_row_detail."EntityId"` to `"RecordId"` without losing data.
- Map Cohort tables explicitly to `public` or a chosen schema through `ConfigureCohortTables`.
- Move all five Cohort tables together when changing schema.
- Preserve the foreign keys from summaries and row details to `sweep_run` and from handler status to row detail.
- Replace category repository/resolver registrations with one rule provider.
- Replace custom audit writers with audit observers or database-based export.
- Declare all possible strategies for every category.
- Correct newly surfaced startup failures before deployment.

Changing schema must be an EF migration owned by the host. PostgreSQL `ALTER TABLE ... SET SCHEMA` should be preferred because it preserves table identity and dependent objects.

Historical migrations must not be rewritten.

## Test Strategy

Follow the repository test rules in `CLAUDE.md`.

- Pure calculations and invariant validation belong in `Cohort.Tests`.
- Anything involving EF, SQL, DI, options binding, hosted services, migrations, or PostgreSQL belongs in `Cohort.Sample.Tests`.
- Use the real Npgsql provider and Testcontainers PostgreSQL.
- Do not add a mocking framework.
- New behavior starts with a failing end-to-end test unless it is a genuinely pure function.
- Tests assert observable state, persisted evidence, and public results rather than internal call order.

Required verification commands must be inferred from the checked-in projects and run without invented flags. At minimum, the final verification includes:

```sh
dotnet test Cohort.Tests/Cohort.Tests.csproj
dotnet test Cohort.Sample.Tests/Cohort.Sample.Tests.csproj
dotnet build Cohort.slnx
```

The package contract and clean-consumer test must run as part of `Cohort.Tests`.

## Execution Order

1. Executable compliance corpus and manifest
2. Public contract gate
3. `RetentionRule` and identity contracts
4. Direct rule provider and capability-driven startup validation
5. Schema qualification and row-detail migration
6. Immediate erasure
7. Authoritative audit and post-commit observers
8. Sanitized failure diagnostics
9. Runtime readiness
10. Operational hardening
11. Sample, tests, documentation, and release guidance
12. Full build, test, package, corpus, and migration verification

Each phase must be green before work begins on the next phase. If a phase cannot be independently verified, its boundary is wrong and must be redrawn rather than leaving the repository partially migrated.

## Definition of Done

### Compliance corpus

- [x] A versioned manifest inventories every compliance capability Cohort claims to support.
- [x] Every supported requirement has a stable ID, provenance, explicit behavioral statement, and executable corpus coverage.
- [x] Every corpus test maps back to one or more manifest requirements.
- [x] Manifest completeness is mechanically enforced.
- [x] Adjacent GDPR responsibilities outside Cohort's scope are explicit non-goals.
- [x] Corpus tests run through public APIs and real PostgreSQL.
- [x] Corpus assertions cover entity state, public results, audit evidence, and isolation where applicable.
- [x] Settled corpus outcomes were not weakened to make the refactor pass.
- [x] Existing detailed tests remain responsible for mechanism-specific permutations.

### Behavior

- [x] Ordinary retention period never delays subject erasure.
- [x] Active holds block subject erasure.
- [x] Positive legal minimum blocks subject erasure until its strict cutoff elapses.
- [x] Null-anchor behavior is correct with and without a positive legal minimum.
- [x] Preview, dry run, bulk mutation, and handler-aware mutation agree exactly.
- [x] Concurrent hold, subject, tenant, and anchor changes are rechecked before mutation.
- [x] Soft delete cannot silently claim to be erasure.

### Contracts

- [x] `RetentionRule` cannot represent invalid state.
- [x] The direct rule-provider contract is the only category-resolution port.
- [x] Every category declares possible strategies.
- [x] Runtime rules cannot exceed declared capabilities.
- [x] Identity vocabulary is consistent across public API, infrastructure, SQL, tests, and documentation.
- [x] The whole public package surface is mechanically allowlisted.
- [x] No accidental utility or infrastructure seam remains public.

### Persistence and schema

- [x] Every raw SQL relation is schema-qualified.
- [x] Runtime behavior is independent of `search_path`.
- [x] Custom schemas work for retained and Cohort-owned tables.
- [x] Decoy tables in other schemas remain untouched.
- [x] Schema validation targets the exact mapped objects.
- [x] The row-detail identity column migration preserves existing data.
- [x] Historical migrations remain unchanged.

### Audit and privacy

- [x] Authoritative audit persistence cannot be replaced or disabled.
- [x] Required audit evidence remains transactionally consistent with mutation.
- [x] Observers receive only committed events.
- [x] Observer failure cannot alter retention outcomes.
- [x] Newly persisted failures contain no arbitrary exception messages or stack traces.
- [x] Results and observer events expose only sanitized diagnostics.
- [x] Full errors are emitted only through structured logging with a correlated diagnostic ID; hosts remain responsible for sink protection and retention.
- [x] Historical failure text is not modified.

### Validation and operations

- [x] All public database operations enforce runtime readiness.
- [x] Failed readiness remains retryable.
- [x] One tenant failure does not stop later work.
- [x] Duplicate tenant IDs execute once.
- [x] Failed row-handler work cannot create unbounded run-sized memory or SQL parameters.
- [x] Dispatcher options and time calculations are bounded safely.
- [x] Cleanup failures do not mask primary exceptions.

### Quality gates

- [x] The compliance corpus and manifest completeness gate pass.
- [x] `Cohort.Tests` passes.
- [x] `Cohort.Sample.Tests` passes against PostgreSQL.
- [x] `Cohort.slnx` builds with warnings treated as errors.
- [x] Package creation succeeds.
- [x] A clean external consumer restores, builds, and runs against the packed package.
- [x] The complete sample migration chain succeeds from an empty database.
- [x] Upgrade migration tests preserve existing audit data.
- [x] Every public README C# example compiles against the final public API.
- [x] `CLAUDE.md`, README, sample code, and package tests contain no stale contract names.
- [x] No unrelated compatibility shim or deprecated duplicate API is introduced.

## Non-Goals

- Claiming that Cohort or its corpus certifies complete GDPR compliance
- Implementing GDPR responsibilities outside Cohort's declared retention scope
- Determining whether a host's legal hold or legal minimum is legally justified
- Automatically adjudicating Article 17 requests
- Redacting historical error rows
- Guaranteed delivery to external audit observers
- SQL Server or SQLite support
- Rewriting historical migrations
- Adding built-in conditional, alias, or caching rule providers
- Parallel tenant execution
- Broad test-framework abstraction or mock-library adoption
