# Cohort Architectural Review

## Summary

Cohort's retention mechanics are fundamentally sound: PostgreSQL behavior is tested against a real engine, mutation and audit writes are transactionally aligned, legal-hold races are handled deliberately, and tenant isolation is broadly covered.

The main architectural defect is context ownership. Cohort performs out-of-band SQL through the host's scoped `DbContext`, while exposing sweep and erasure as directly callable scoped services. That creates stale tracking and ambient transaction hazards. Fix that before structural refactoring.

The test suite is not mock soup. It is unusually strong and near-metal, but several named tests manufacture confidence because they do not exercise the behavior in their names.

Review confidence: high.

## Action Status

Actioned on 2026-07-11 against the reviewed working tree.

### Completed

- Public `IRetentionSweep` and `IRetentionErasureService` calls now execute through scope-owning facades, isolating raw SQL from the caller's tracked `DbContext` and ambient transaction.
- Real PostgreSQL regressions prove caller-tracked entities cannot overwrite soft-delete or anonymisation changes.
- Hold removal now participates in the current EF transaction. Create/remove commit and rollback behavior is covered end to end.
- PostgreSQL store-type metadata is validated before it can be rendered into SQL; unsafe metadata falls back to column-to-text matching.
- The false null-tenant tests now insert actual `NULL` tenant rows and prove sweep/preview exclusion.
- Real PostgreSQL `ON DELETE RESTRICT` coverage proves child-before-parent sweep execution.
- Audited dry-run cancellation and partial-failure settlement are covered against PostgreSQL.
- Audit settlement that intentionally ignores caller cancellation is bounded to 30 seconds.
- Migration application is now a dedicated startup service that completes before validation, dispatcher polling, and scheduling.
- Convention names, `[Retain]` arguments, hold requests, and tenant tag ownership now enforce their invariants.
- Anonymisation factory capabilities are represented by one closed execution mode instead of contradictory booleans.
- Metadata-driven reflection consistently uses the shadow-safe member resolver in execution paths identified by the review.
- Concrete worker, options-validator, and EF audit implementations are internal package details.
- Exact hold activity boundaries, deferred handler settle timeout, hold removal failures, and transaction composition have real PostgreSQL tests.
- Behavioral tests no longer depend on the sample model's global entity count. Genuine completeness tests assert the complete expected set.
- The worker failure probe is explicitly armed after startup instead of depending on a magic resolver-call count.

### Deliberate Dispositions

- `RetentionSweepEngine`, `RetentionErasureService`, and `RetentionRowDispatcher` were not mechanically split by line count. Their complexity is real, but splitting them without changing behavior would be a broad internal rewrite with no independently failing contract. Meridian requires seams to be earned; the correctness, lifecycle, and testability issues were fixed directly instead.
- Public renames (`Strategy`, `IRetentionCategoryRepository`, row-level `EntityId`, and `AuditRowDetail`) were not performed. They are source-breaking naming changes and need a planned major-version API migration rather than being mixed into correctness work.
- Holds remain addressed by physical table name. Moving persisted holds to stable retention entity IDs requires a storage migration and an explicit compatibility policy for existing host databases.
- `ErasureScope` remains non-generic. Replacing its subject `object` with a generic public contract is a source-breaking API redesign; runtime model validation remains the current compatibility boundary.
- `RetentionModelConventions` remains public because direct metadata construction is part of the existing test and extension surface. Removing it requires redesigning that construction API rather than only changing visibility.
- The single-project layer layout remains intentional. Architecture tests enforce dependency direction without introducing project/package boundaries that do not correspond to runtime ownership.

### Verification After Actioning

- `dotnet test Cohort.slnx --no-restore`: **402 passed**, 0 failed.
- `~/Sites/plumb/plumb . --json`: no findings.
- `git diff --check`: clean.
- `dotnet format Cohort.slnx --verify-no-changes --no-restore` is not a current repository gate; it reports broad pre-existing whitespace and naming-policy violations across untouched files.

## Critical Findings

### 1. Retention SQL can be undone by the host's EF change tracker

**Evidence**

- `Cohort/Hosting/ServiceCollectionExtensions.cs:60` aliases the host `TContext` to scoped `DbContext`.
- `Cohort/Hosting/ServiceCollectionExtensions.cs:76-81` builds preview, erasure, and sweep services in that same scope.
- `Cohort/Infrastructure/Sweep/RelationalSweepStrategyCore.cs:188-207` mutates rows through raw commands.
- `Cohort/Infrastructure/Sweep/AnonymiseMutationExecutor.cs:85-102` and `:123-147` do the same for anonymisation.

EF does not reconcile raw SQL with tracked entities. A direct caller can:

1. Load and track an entity.
2. Run Cohort in the same scope.
3. See stale values in memory.
4. Call `SaveChangesAsync`.
5. Restore anonymised or soft-deleted values from the stale tracked entity.

A purged entity also remains tracked and can cause concurrency failures or accidental reinsertion depending on subsequent state changes.

**Fix**

Cohort must own a dedicated `DbContext` for every retention operation. Use `IDbContextFactory<TContext>` or a Cohort-owned scope. Do not call `ChangeTracker.Clear()` because that would silently discard consumer work.

This is the highest-priority architectural correction.

### 2. Ambient host transactions invalidate audit durability and break batching

**Evidence**

- `Cohort/Infrastructure/RetentionSweepEngine.cs:69-75` says `Started` commits immediately without an ambient transaction.
- `Cohort/Infrastructure/Audit/EfRetentionAuditWriter.cs:394-398` attaches audit commands to `db.Database.CurrentTransaction`.
- `Cohort/Infrastructure/RetentionSweepEngine.cs:484-499` starts a batch transaction.
- Erasure has the equivalent behavior in `Cohort/Infrastructure/RetentionErasureService.cs:58-70` and `:290-307`.

When called inside a host transaction:

- `Started` is not independently durable.
- Cohort attempts to start another transaction and Npgsql rejects it.
- Failure settlement can disappear when the host rolls back.

**Fix**

The dedicated-context correction resolves this properly. Until then, fail immediately when `CurrentTransaction` is non-null, before writing `Started`.

## High Findings

### 3. Legal-hold removal does not participate in ambient transactions

`CreateAsync` deliberately joins the current transaction:

- `Cohort/Infrastructure/Holds/EfRetentionHoldsRepository.cs:33-37`
- `Cohort/Infrastructure/Holds/EfRetentionHoldsRepository.cs:60-62`

`RemoveAsync` does not assign `command.Transaction`:

- `Cohort/Infrastructure/Holds/EfRetentionHoldsRepository.cs:270-294`

On a connection with an active Npgsql transaction, removal can fail. It also prevents atomic "application update plus remove hold" operations despite creation supporting that contract.

**Fix**

Assign the current EF transaction to the command and add commit/rollback integration tests.

### 4. The null-tenant regression tests never insert a null tenant

The named behavior is not tested:

- `Cohort.Sample.Tests/RetentionSweepEndToEndTests.cs:121-186`
- `Cohort.Sample.Tests/RetentionPreviewEndToEndTests.cs:271-336`

Both insert tenant A and tenant B. Neither inserts `TenantId = NULL`. They prove ordinary tenant isolation only.

**Fix**

Insert a null tenant through raw SQL if the current CLR model prevents it. Assert that targeted sweep and preview exclude both the null tenant and other tenant.

This is a coverage-honesty failure, not merely a missing edge case.

### 5. FK execution ordering is only metadata-tested

The production engines depend on child-before-parent ordering:

- `Cohort/Infrastructure/RetentionSweepEngine.cs:95-101`
- `Cohort/Infrastructure/RetentionErasureService.cs:117-123`
- `Cohort/Infrastructure/RetentionExecutionPlanOrderer.cs:44-71`

`Cohort.Sample.Tests/RetentionExecutionPlanOrdererTests.cs` explicitly executes no SQL.

**Fix**

Add a real PostgreSQL model with retained parent and child rows under `ON DELETE RESTRICT`. Exercise sweep and erasure through production composition. Include a held child blocking parent deletion and assert the intended partial-failure behavior.

### 6. Arbitrary EF store-type text is interpolated into SQL

**Evidence**

- `Cohort/Infrastructure/Sweep/RecordIdSql.cs:16-29`
- `Cohort/Infrastructure/Holds/EfRetentionHoldsRepository.cs:247-255`
- Metadata source: `Cohort/Infrastructure/RetentionEntryBuilder.cs:137`

`GetColumnType()` text is executable SQL metadata. A malformed host `HasColumnType(...)` configuration can break sweeps and potentially alter generated SQL.

**Fix**

Resolve canonical Npgsql type mappings or validate a strict PostgreSQL type grammar. If validation fails, use the existing column-to-text fallback.

## Medium Findings

### 7. Sweep and erasure duplicate an invariant-heavy run coordinator

- `RetentionSweepEngine` is 764 lines.
- `RetentionErasureService` is 623 lines.
- Both implement started/failed/cancelled settlement, execution planning, batching, per-entity continuation, audit progress, and result assembly independently.

This duplication is now dangerous because it contains transactional and audit lifecycle invariants, not incidental procedural similarity.

**Fix**

Extract internal concrete components, not new public ports:

- `RetentionRunCoordinator`
- `RetentionExecutionPlanBuilder`
- `RetentionEntityExecutor`
- `RetentionAuditSession`

Keep top-level use cases explicit and readable. Do not replace them with a mediator or event bus.

### 8. `RetentionRowDispatcher` is an entire subsystem collapsed into one class

`Cohort/Infrastructure/Handlers/RetentionRowDispatcher.cs` is 1,072 lines and owns:

- polling
- claiming
- leases and heartbeats
- transaction management
- retry and dead-letter behavior
- serialization and payload scrubbing
- handler discovery and reflection invocation
- CLR identity compatibility

**Fix**

Split it into internal concrete responsibilities:

- queue repository
- work claimer
- invocation executor
- retry policy
- payload store
- polling coordinator

These are earned internal boundaries. They do not all need ports.

### 9. Startup migration and dispatcher execution can race

Hosted services are registered in this order:

- validation
- dispatcher
- worker

See `Cohort/Hosting/ServiceCollectionExtensions.cs:83-93`.

Migrations run inside `RetentionWorker`:

- `Cohort/Hosting/RetentionWorker.cs:318-377`

The dispatcher can query Cohort tables before migrations complete. It survives by logging polling errors, but startup ordering is accidental.

**Fix**

Create a migration startup service registered before validation and both background workers:

1. Migrations
2. Configuration/model validation
3. Dispatcher
4. Scheduled worker

Migration ownership does not belong in the scheduler.

### 10. Cancellation settlement can hang indefinitely

Sweep and erasure deliberately use `CancellationToken.None` for audit settlement:

- `RetentionSweepEngine.cs:71-75`, `:143-165`, `:643-648`
- `RetentionErasureService.cs:60-70`, `:169-191`, `:544-549`

Best-effort settlement after cancellation is correct. Unbounded settlement is not. A blocked database can prevent host shutdown indefinitely.

**Fix**

Use an independent bounded token from `CancellationTokenSource.CancelAfter(...)`, then preserve the original cancellation or failure.

### 11. Execution metadata is repeatedly reduced to strings and reflected back

`RetentionEntry` stores property names, column names, CLR types, and store types. Execution then repeatedly calls `Type.GetProperty`, sometimes bypassing `ReflectionMemberResolver`:

- `RelationalSweepStrategyCore.cs:463-467`
- `AnonymiseAssignmentResolver.cs:119-123`
- `AnonymiseHandlerAwareMutationExecutor.cs:79-83`
- `SoftDeleteSweepStrategy.cs:172-180`

This recreates shadowed-property ambiguity that the custom resolver was intended to solve.

**Fix**

Carry resolved `PropertyInfo` and extracted EF mapping metadata in the immutable entry. Resolve once while building the registry.

### 12. The public hold contract leaks physical table names

`RetentionHoldRequest` uses `TableName`, and `EfRetentionHoldsRepository` resolves exact physical table names at `:176-195`.

Table renaming therefore breaks external hold producers. Cohort already has stable retained-entity identities through `[RetentionEntityId]`.

**Fix**

Address holds by stable retention entity ID. Persist that stable ID and keep table name only as diagnostic metadata.

## Public API Findings

### 13. Several names obscure capability ownership

- `Strategy` should be `RetentionStrategy`.
- `IRetentionCategoryRepository` returns a resolver, not a category aggregate. Use `IRetentionRuleResolverProvider` or `IRetentionPolicyProvider`.
- Row-level `EntityId` should be `RecordId`; reserve `RetentionEntityId` for entity-type identity.
- `AuditRowDetail` should be retention-specific.

These are breaking changes and belong in one deliberate API revision, not piecemeal churn.

### 14. `IAnonymiseValueFactory` permits contradictory capability states

`RequiresPerRowExecution` and `RequiresOriginalValue` can be overridden into incompatible combinations.

Replace the booleans with a closed mode:

```csharp
public enum AnonymiseFactoryExecutionMode
{
    Static,
    PerRow,
    PerRowWithOriginalValue,
}
```

### 15. Public records permit invalid states

Examples:

- blank `[Retain]` category or anchor
- blank hold reason/table
- expiry before creation
- negative result counts
- undefined enum values
- mutable tenant tags
- `ErasureScope` carrying an untyped `object`

The immediate fixes are constructor validation and collection copying. `ErasureScope` needs a stronger subject-value model rather than an `object` carrier.

### 16. Public implementation detail is broader than necessary

Candidates for internal visibility:

- `RetentionWorker`
- `CohortOptionsValidator`
- `EfRetentionAuditWriter`
- `RetentionModelConventions`
- `ReflectionMemberResolver`
- `RetentionAliasCycleException`, which currently has no production throw site

## Testing Assessment

The test design aligns well with Meridian:

- Real PostgreSQL 16 via Testcontainers: `PostgresFixture.cs:10-25`
- No EF InMemory provider
- No mocking framework dependencies
- Raw SQL, migrations, transactions, locks, `SKIP LOCKED`, concurrency, audit persistence, and value conversion are genuinely executed
- Handwritten doubles are narrow probes, not interaction-heavy mock soup

The important remaining test backlog is:

1. Actual null-tenant sweep and preview tests.
2. Restrictive-FK sweep and erasure tests.
3. Dry-run partial failure and cancellation durability.
4. Hold creation/removal inside caller-owned transactions.
5. Missing, repeated, and concurrent hold removal.
6. Deferred handler release only after the sweep records a terminal status.
7. Worker kill switch between tenant passes.
8. Exact hold-time boundaries.
9. Connection ownership after success, failure, and cancellation.
10. Replace unrelated global `HaveCount(10)` assertions with behavior-specific assertions.

The repeated `HaveCount(10)` assertions couple unrelated behavioral tests to sample model cardinality. They break when a new retained sample entity is added while proving nothing about the additional summaries.

## What Is Correct

These areas should be preserved:

- Mutation batches and audit progress commit atomically.
- Failed batches roll back without erasing prior committed batches.
- Candidate predicates are reapplied during mutation.
- Legal-hold creation and sweep use deterministic advisory locking.
- Concurrent sweep work uses `FOR UPDATE SKIP LOCKED`.
- Tenantless entities are not swept once per tenant.
- Values are parameterized; identifiers are quoted.
- Entity failures are isolated and surfaced as partial failures.
- Dispatcher claim fencing, leases, retries, ordering, and payload scrubbing are substantially tested.
- Architecture direction is enforced by tests and currently has no inward dependency violation.

The single-project layer layout is acceptable under Meridian. Splitting each layer into a separate project would add package/project ceremony without creating a real runtime or module boundary. Strengthen the architecture tests instead of project-splitting.

## Recommended Order

1. Give Cohort dedicated `DbContext` ownership and prohibit ambient host transactions.
2. Fix hold removal transaction participation.
3. Repair the false null-tenant tests and add real FK execution tests.
4. Separate migration startup from scheduling.
5. Bound audit settlement time.
6. Extract shared sweep/erasure coordination.
7. Split the dispatcher internally.
8. Tighten metadata and public API types in a planned breaking release.
9. Complete stable entity identity across holds and audit keys.
10. Remove brittle model-count assertions and timing-dependent worker setup.

## Verification

- `dotnet test Cohort.slnx`: **327 passed**, 0 failed.
- `plumb . --json`: no findings.
- GitNexus was unavailable because this repository is not indexed.
- Review was performed against a heavily modified working tree, not commit `c45f7df`; no production files were changed during the review.
