# Cohort Signable Closeout

## Purpose

This document is the authoritative implementation and evidence contract for the second pass over the Cohort pre-1.0 refactor.

`REFACTOR.md` records the original design and implementation sequence. Its settled product behavior remains the regression baseline, but its checked Definition of Done is not evidence that the work is complete. Where this document and `REFACTOR.md` differ about closure, evidence, or sign-off, this document wins.

The second pass is complete only when every requirement in this document is implemented, mapped to appropriate evidence, independently reviewable, and checked in the Definition of Done. A green aggregate test count is necessary but is not sufficient.

## Why This Pass Exists

The first pass established broad behavioral coverage and implemented the intended public contract. A subsequent audit found that several closure claims were stronger than their evidence:

- PostgreSQL application and Cohort table names are qualified, but built-in functions and system catalog relations are not consistently qualified through `pg_catalog`.
- Runtime behavior can therefore still depend on an explicitly ordered hostile `search_path`.
- `CohortSchemaContract` validates one hand-authored schema description while `CohortModelBuilder` constructs another.
- Some manifest-linked acceptance tests invoke internal engines, validators, or cleanup helpers rather than the public boundary named by the requirement.
- The manifest does not inventory several supported product invariants that are part of the release contract.
- Several corpus assertions can pass without proving row survival, mutation-free refusal, committed totals, or coherent persisted evidence.
- Some migration, concurrency, and operational claims have representative or structural evidence but no explicit primary evidence at the boundary where the claim is observable.

This document closes those gaps without redesigning the retention model or weakening any settled behavior.

## Authority and Change Control

- This file is the source of truth for second-pass scope, invariants, evidence, phase gates, and sign-off.
- Existing stable manifest requirement IDs and settled outcomes must not be renamed or weakened merely to make tests pass.
- A requirement may be clarified only if the clarification preserves or strengthens its externally observable guarantee.
- A requirement may be removed or converted to a non-goal only through an explicit product decision recorded in this file and the compliance manifest.
- Every supported requirement must have primary evidence of the correct kind. Secondary mechanism tests may complement primary evidence but cannot replace it.
- Checkboxes are changed only after implementation, targeted evidence, the full regression suite, and review of the resulting diff all pass.
- No item is complete because the implementation appears correct by inspection alone.
- No item is complete because another broader test happens to execute the same code path without asserting the requirement.

## Settled Product Invariants

The following behavior remains binding throughout this pass:

- Ordinary sweeps use the greater of `RetentionRule.Period` and positive `LegalMin`.
- Subject erasure ignores ordinary `RetentionRule.Period`.
- Active holds block ordinary retention and subject erasure.
- Positive `LegalMin` blocks erasure until the anchor is strictly older than the legal-minimum cutoff.
- With no positive `LegalMin`, erasure applies no anchor predicate; null and future anchors are eligible.
- Soft delete is not accepted as erasure without explicit caller opt-in.
- Purge, soft delete, anonymise, preview, dry run, handlers, candidate locking, and final mutation use the eligibility appropriate to their operation.
- Tenant, subject, retention-entity, record, category, and strategy boundaries remain exact.
- Required audit evidence is authoritative and transactionally consistent with mutation.
- Audit observers are post-commit, bounded, ordered per observer, isolated, and best effort.
- Newly produced persisted, result, and observer failure diagnostics are sanitized.
- Historical persisted failure text is preserved unchanged.
- Every public database operation enforces runtime readiness before durable `Started` evidence.
- Public operations work without generic-host startup when their dependencies and schema are ready.
- Unsupported retained EF mappings fail before mutation.
- Tenant execution is sequential; duplicate tenants execute once; one tenant failure does not suppress later or tenantless work.
- Persisted row-detail progress prevents same-run failed-handler loops without hiding rolled-back work from retry.
- The public package surface is an explicit allowlist.
- PostgreSQL is the only supported database provider.
- Existing detailed tests remain regression obligations even when equivalent high-level behavior appears in the corpus.

## Evidence Model

### Primary evidence kinds

Every supported manifest requirement must name one or more evidence entries. Each entry has a test identity, an evidence kind, and whether it is primary or secondary.

Allowed evidence kinds:

- `public-postgresql`: invokes a public Cohort application port against real PostgreSQL and asserts observable state, public results, and persisted evidence as applicable.
- `hosted-postgresql`: starts or executes the real hosted path against PostgreSQL and asserts startup, worker, scheduling, or dispatcher behavior.
- `migration-postgresql`: applies real migrations or representative host-owned migration SQL against PostgreSQL and verifies preserved data and schema capabilities.
- `package-consumer`: packs Cohort and proves a clean external consumer restores, compiles, and runs against the intended public API.
- `pure-contract`: proves a deterministic invariant with no I/O boundary.
- `architecture`: proves assembly visibility, dependency direction, public API shape, or another static structural contract.
- `internal-mechanism`: proves an internal algorithm or failure-precedence mechanism that cannot reasonably be induced through a public boundary without adding a production seam solely for testing.

### Evidence rules

- User-observable runtime behavior requires at least one primary `public-postgresql` or `hosted-postgresql` test.
- Migration behavior requires primary `migration-postgresql` evidence.
- Public API and visibility claims require primary `architecture` and, where consumer compatibility is claimed, `package-consumer` evidence.
- Pure domain invariants require primary `pure-contract` evidence.
- `internal-mechanism` evidence can be primary only for an explicitly internal invariant such as cleanup exception precedence. It cannot support a claim that a public operation behaved correctly unless a public test also observes that behavior.
- Tests that directly resolve `RetentionSweepEngine`, `RetentionStartupValidator`, `RetentionRegistry`, `EfRetentionAuditWriter`, `RetentionRunAdvisoryLock`, or `OperationalConnectionCleanup` are not public acceptance evidence.
- Every primary PostgreSQL test must execute real SQL. Metadata-only model tests are not substitutes for SQL behavior.
- Representative tests are acceptable only when production paths demonstrably share the same implementation and the shared mechanism has focused evidence. Distinct SQL builders or transaction paths require distinct evidence.
- Assertions must fail if the protected row disappears, a refused operation partially mutates, totals over-report, evidence is attached to the wrong identity, or a required event is absent.
- The manifest linkage gate proves inventory shape and linkage. The successful full test run proves execution. The gate must not claim that reflection alone proves xUnit execution or semantic adequacy.

### Manifest evidence shape

The compliance manifest advances to a new major manifest version and replaces untyped test-name lists with typed evidence, for example:

```json
{
  "id": "SCHEMA-SEARCHPATH-001",
  "source": "Cohort product invariant: runtime SQL is independent of search_path",
  "requirement": "PostgreSQL catalog relations and built-in functions used by Cohort cannot be shadowed by an earlier writable search_path schema.",
  "evidence": [
    {
      "test": "SchemaQualificationEndToEndTests.Hostile_search_path_cannot_shadow_catalogs_holds_or_locks",
      "kind": "public-postgresql",
      "primary": true
    }
  ],
  "status": "supported"
}
```

The exact DTO names may follow repository conventions, but the information and validation rules are mandatory.

## Required Outcomes

### 1. PostgreSQL Name Resolution

#### Invariants

- Every application-table and Cohort-owned relation reference is formatted as a separately quoted schema and relation name.
- Every PostgreSQL system catalog relation is explicitly qualified with `pg_catalog`.
- Every PostgreSQL built-in function or operator-like function invoked by raw Cohort SQL is explicitly qualified with `pg_catalog` where PostgreSQL permits qualification.
- This includes, at minimum, `statement_timestamp`, advisory-lock functions, `hashtextextended`, `unnest`, and `pg_get_expr`.
- SQL must not rely on PostgreSQL's implicit insertion of `pg_catalog` into `search_path`.
- Identifier values derived from EF metadata remain quoted as identifiers, never interpolated as SQL values or accepted prequoted.
- A same-named user relation or function in a schema before `pg_catalog` cannot alter readiness, hold activity, candidate selection, mutation, audit persistence, dispatch, or lock ownership.

#### Required evidence

- Add a hostile schema before `pg_catalog` in `search_path`.
- Create shadow functions with the signatures Cohort invokes, including a false `statement_timestamp`, advisory-lock functions, and hash/array helpers where PostgreSQL allows an exact overload.
- Create shadow catalog-shaped relations or views sufficient to detect unqualified catalog access.
- Execute public hold creation/listing, preview, sweep, erasure, dispatcher flush, and scheduled advisory-lock behavior through the mapped custom schema.
- Prove active holds still block, real advisory locks still serialize, schema validation still inspects the exact mapped objects, and decoys remain unused.
- Retain mixed-case, whitespace, and embedded-quote schema/table coverage.
- Add a deterministic architecture or SQL-generation guard that detects newly introduced unqualified `pg_*` catalog/function references. This guard is secondary to runtime evidence.

### 2. One Cohort Schema Contract

#### Invariants

- One infrastructure-local schema description is authoritative for all five Cohort tables.
- It defines table roles, columns, CLR/store types, nullability, generated values, primary keys, alternate or unique keys, indexes, predicates, checks, foreign keys, and delete behavior.
- `ConfigureCohortTables` consumes that description when constructing the EF model.
- `CohortSchemaValidator` consumes the same description when checking installed PostgreSQL objects.
- Table adoption consumes the same expected key and role information.
- Constraint names and normalized expressions have one owner.
- There is no second hand-authored list of Cohort schema requirements in `CohortModelBuilder` or the validator.
- Strongly typed runtime entities may retain typed EF expressions, but schema names, compositions, and capabilities must come from the shared contract rather than duplicated literals.

#### Required evidence

- A model-contract test enumerates all five finalized EF mappings and compares every required capability to the contract.
- PostgreSQL validation accepts a schema generated from the model.
- For each capability family, a malformed real schema is rejected with the exact qualified table and missing or incompatible capability.
- Adoption tests prove same table name in a different schema is not adopted.
- An architecture test fails if model construction or validation defines an independent schema requirement inventory.

### 3. Complete and Honest Manifest

#### Invariants

- The manifest is the inventory of supported Cohort product and compliance invariants, not merely a sample of GDPR-adjacent scenarios.
- Existing requirement IDs remain stable.
- Existing behavioral statements are not weakened.
- Each requirement records provenance, an explicit statement, status, and typed evidence.
- Supported requirements have primary evidence of an appropriate kind.
- Non-goals have no passing support evidence.
- Every dedicated corpus test maps to at least one requirement.
- Every evidence target resolves uniquely by namespace, type, and method or by another collision-proof identity.
- Duplicate evidence entries within one requirement are rejected.
- The linkage gate describes reflected targets as structurally linked, not proven executable.
- Full test execution, not reflection, is the execution gate.

#### Missing requirement families to add

The manifest must add explicit requirements for at least the following currently supported contracts:

- Invalid `RetentionRule` states, get-only properties, and absence of public positional deconstruction.
- Immutable, non-empty, defensively copied `RetentionCategoryCapabilities`.
- Runtime rejection of unresolved categories and strategies outside startup-validated capabilities.
- Tenant, logical time, and alias-path preservation, including alias-cycle reporting.
- Default `public` schema mapping and custom schema mapping of all five Cohort tables.
- Exact catalog resolution and hostile-`search_path` resistance.
- Single schema-contract ownership.
- Authoritative audit writer package visibility and non-replaceability.
- Multiple-observer delivery, committed lifecycle ordering, timeout isolation, and explicitly non-durable delivery.
- Preservation of historical `Error` and `LastError` values.
- Duplicate-tenant deduplication, first-context behavior, conflicting-context warning, sequential execution, and tenantless continuation.
- Bounded failed-handler progress without run-sized record-ID collections or SQL parameter arrays.
- Dispatcher numeric ceilings, saturating time arithmetic, and chunked delays.
- Cleanup exception precedence and independent bounded cleanup cancellation.
- Whole-package public API allowlisting and internal-only infrastructure helpers.
- Row-detail identity migration, complete historical migration chain, and custom-schema move guidance/evidence.

#### Existing entries to correct

- Requirements currently supported only by direct internal engine invocation must gain public-boundary primary evidence or be reclassified as internal mechanism requirements with a separate public requirement.
- `ACCOUNT-004` must distinguish public API non-replaceability from observer failure isolation and provide evidence for both.
- `HOLD-002` must prove that equal canonical record and tenant identities in different retained entity types do not cross-match.
- `ERASURE-006` and `ERASURE-007` must use precise terminology for public preview versus audited dry run.
- `FAILURE-002` must distinguish loop prevention from the separate bounded-memory and bounded-parameter guarantee.
- `FAILURE-004` must be classified as internal exception-precedence evidence unless a real public fault path is exercised.
- `READY-002` must distinguish direct validator execution from actual hosted startup validation.
- `READY-009` must not stand in for saturating arithmetic or delay chunking.

### 4. Corpus Assertion Integrity

#### Required corrections

- Protected soft-delete and anonymisation rows are asserted by exact identity and expected count before field assertions. Empty collections must fail.
- A refused soft-delete erasure is followed immediately by assertions that every candidate row and audit table remains unchanged before any accepted erasure runs.
- Partial-failure scenarios assert public counts, terminal event totals, persisted run status, persisted `TotalAffected`, entity summaries, row details, and source rows against the same committed aggregate.
- Accountability scenarios assert summary and row-detail retention entity ID, record ID, tenant, category, strategy, affected count, resolved period, and provenance, not only row existence.
- Positive-`LegalMin` null-anchor scenarios assert the persisted summary for the nullable-anchor entity itself.
- Hold scenarios include unheld positive controls for purge, soft delete, anonymise, and erasure so unchanged held rows cannot pass because a strategy is globally broken.
- Factory scenarios assert invocation, original CLR value, tenant, member, and provider-side stored value where those semantics are claimed.
- Readiness tests include an invalid or absent schema path so they prove validation ran; a happy path against an already valid fixture is compatibility evidence only.
- Public operation success or failure is asserted where isolation tests currently ignore the returned failure aggregate.
- Manifest duplicate evidence is rejected.

#### Mutation-free refusal

Every fail-before-mutation requirement must assert all applicable surfaces immediately after the refused call:

- source rows;
- deletion/anonymisation markers;
- authoritative run rows;
- entity summaries;
- row details;
- handler statuses;
- observer events.

Assertions performed only after a later successful operation do not prove mutation-free refusal.

### 5. Public Boundary Discipline

#### Invariants

- Public behavioral acceptance tests resolve and invoke `IRetentionSweep`, `IRetentionPreview`, `IRetentionErasureService`, `IRetentionHoldsRepository`, or `IRetentionRowDispatcher`, or execute the real hosted service path.
- Tests do not resolve internal engines to avoid readiness, scope ownership, startup snapshots, transaction ownership, or public result assembly.
- Provider-conversion behavior claimed for ordinary sweeps is exercised through `IRetentionSweep`.
- Hosted-startup claims start the real host or invoke the actual hosted validation service through host startup.
- Direct-public-operation claims invoke a public port without first calling a private test validation helper.
- Internal mechanism tests remain allowed but are labeled honestly and do not substitute for public behavior.

#### Required replacements or reclassification

- Replace internal-engine provider-conversion acceptance targets with public sweep tests.
- Replace internal startup-validator acceptance targets with real hosted startup or direct public-operation tests, depending on the requirement.
- Keep cleanup precedence tests as focused internal-mechanism evidence unless an end-to-end production fault can be induced without adding a test-only production seam.
- Keep schema/model metadata tests as architecture or narrow integration evidence, not public SQL acceptance evidence.

### 6. Hold and Concurrency Safety

#### Invariants

- Hold creation and mutation acquire the same stable advisory key derived from retention entity, tenant scope, and canonical record identity.
- Hold target existence and tenant ownership are checked after acquiring the shared lock.
- Final mutation rechecks hold activity after row/advisory lock waits.
- Stable retention-entity identity prevents cross-entity hold collisions.
- The lock protocol cannot be bypassed by `search_path` shadowing.
- Concurrent public engines do not double-mutate or double-report one row.

#### Required evidence

- Public hold creation racing purge preserves the row.
- Public hold creation racing soft delete preserves all deletion markers.
- Public hold creation racing anonymisation preserves every anonymised field and marker.
- Public hold creation racing erasure preserves the row without direct SQL insertion of the hold.
- A hold for one retained entity does not block another retained entity with the same canonical record ID and tenant.
- At least one concurrent-engine test covers each distinct mutation implementation: relational bulk, handler-aware relational, and anonymisation.
- Every race test has a timeout, proves the intended wait occurred, and asserts public counts plus final database state.

### 7. Audit and Privacy Evidence

#### Invariants

- Consumers cannot register a replacement authoritative writer through the public package.
- Registering zero, one, or multiple observers cannot suppress authoritative persistence.
- Observers receive only committed events in lifecycle order.
- One observer's failure, timeout, or cancellation non-cooperation does not block later observers or alter run status.
- Rolled-back row details and progress are not observed.
- Public results, observers, `sweep_run.Error`, and handler `LastError` expose only sanitized diagnostics for new failures.
- Protected structured logs retain the original exception and matching diagnostic ID.
- Existing historical `Error` and `LastError` values survive all forward migrations byte-for-byte.

#### Required evidence

- The API allowlist and clean consumer prove the authoritative writer is unavailable and observers are available.
- Multiple recording observers assert identical committed lifecycle order.
- Mutation rollback asserts no corresponding observer event or persisted progress.
- Failure sentinels are absent from every public/persisted/observer surface and present only in the correlated structured exception log.
- Migration tests seed distinct historical `Error` and `LastError` sentinel values before upgrade and assert exact preservation afterward.
- Corpus accountability tests assert coherent identities and totals, not merely the presence of evidence rows.

### 8. Operational Bounds and Failure Safety

#### Invariants

- Duplicate tenant IDs execute once in first-seen order.
- Conflicting contexts for a duplicate tenant log a warning and use the first context.
- Tenant execution remains sequential.
- A tenant failure does not suppress later tenants or tenantless execution.
- Cancellation stops the occurrence immediately.
- Kill switches are rechecked between passes.
- Failed `OnBeforeAsync` rows are excluded through committed persisted row detail, not a run-sized process collection.
- Candidate SQL parameter count is bounded by batch-local work, not total failed work in the run.
- Rolled-back progress never excludes a row from a later run.
- `BatchSize`, `MaxParallelism`, and `MaxAttempts` enforce their documented ceilings through real configuration binding and hosted validation.
- Timestamp subtraction and retry scheduling saturate at representable bounds.
- Dispatcher poll and heartbeat delays are split into `Task.Delay`-safe chunks; observer timeout uses its own validated ceiling.
- Cleanup uses an independent bounded cancellation token.
- Primary exceptions outrank unlock and close exceptions; unlock outranks close when no primary exception exists.

#### Required evidence

- Existing tenant isolation and deduplication tests become typed manifest evidence.
- Add or retain a high-volume PostgreSQL scenario with persistent handler failures and healthy rows beyond them.
- Observe command parameter counts, or another deterministic boundary, and prove they remain batch-bounded as total failures grow.
- Prove the same failed row does not spin in one run and remains retryable after rollback.
- Pure tests cover minimum/maximum timestamps, `TimeSpan.MaxValue`, retry overflow, and every delay chunk boundary.
- Hosted binding tests cover values at each numeric ceiling and one value above each ceiling.
- Focused cleanup tests assert all precedence combinations and bounded independent cancellation.

### 9. Migration and Custom-Schema Safety

#### Invariants

- Historical migrations remain unchanged.
- The row-detail `EntityId` to `RecordId` transition is an in-place preserving rename.
- Stable retention-entity identity migration preserves holds, summaries, and row details.
- Historical diagnostics are not rewritten.
- Hosts changing Cohort schema move all five tables together in a host-owned EF migration.
- Moving tables with `ALTER TABLE ... SET SCHEMA` preserves table identity, data, indexes, checks, and foreign keys.
- The runtime model and readiness validator target the post-move schema exactly.

#### Required evidence

- Empty-database migration to current succeeds.
- Legacy-to-current migration succeeds with historical holds, summaries, row details, handler statuses, `Error`, and `LastError` populated.
- Forward/back/forward coverage for the row-detail rename preserves record identity and related evidence where downgrade is supported by the sample migration test.
- A representative host-owned custom-schema move test starts with populated public tables, moves all five tables, then verifies data, indexes, constraints, foreign keys, readiness, holds, sweep, erasure, audit, and dispatcher behavior in the destination schema.
- Public decoys and a hostile `search_path` remain irrelevant after the move.

### 10. Package and Documentation Contract

#### Invariants

- The whole exported assembly surface remains allowlisted, including nested types, constructors, methods, properties, fields, constants, enum members, generic constraints, nullable metadata, inheritance, interfaces, operators, and attribute usage.
- Internal engines, schema descriptors, SQL helpers, reflection helpers, ordering helpers, and the authoritative audit writer remain unavailable to consumers.
- The package exposes public ports, rule-provider contracts, observers, attributes, domain values, and hosting registration intentionally.
- Every public README C# example compiles in the clean consumer.
- Documentation describes observer delivery as best effort and database audit tables as authoritative.
- Documentation distinguishes `RetentionEntityId` from `RecordId` and describes custom-schema migration ownership.
- Documentation does not claim that Cohort certifies GDPR compliance or adjudicates legal validity.

#### Required evidence

- Existing whole-assembly API baseline passes.
- Package creation succeeds.
- A clean external consumer restores, builds, and runs against the packed package.
- Removed ports and internal infrastructure types fail consumer compilation or are absent from the approved API baseline.
- README snippets used by the package test compile against the final package.

## Implementation Phases

### Phase 0: Reopen Claims and Establish Red Evidence

1. Change the affected `REFACTOR.md` completion claims back to incomplete or add an explicit pointer stating that final closure moved to `SIGNABLES.md`.
2. Advance the manifest schema and add typed evidence without weakening existing requirement text.
3. Add failing tests for hostile `search_path`, vacuous protected-row assertions, mutation-free erasure refusal, committed totals, and historical diagnostic preservation.
4. Reclassify internal-only evidence before changing production code.

Gate:

- Every identified gap has a stable requirement ID and a red or explicitly structural evidence target.
- Existing settled behavior remains green.
- No production fix lands before its intended failure is demonstrated, except a pure architecture refactor whose current duplication is itself the failure.

### Phase 1: PostgreSQL Resolution Hardening

1. Qualify all system catalog relations and PostgreSQL built-ins.
2. Add hostile-schema decoys and exact overloads.
3. Exercise holds, locking, readiness, sweep, erasure, audit, and dispatch through public paths.

Gate:

- Hostile `search_path` tests pass.
- No unqualified raw SQL relation or relevant PostgreSQL built-in remains.
- Existing custom-schema and decoy tests remain green.

### Phase 2: Single Schema Authority

1. Refactor the schema contract into the shared source consumed by model construction, adoption, and validation.
2. Remove duplicated schema requirement literals.
3. Retain the public `ConfigureCohortTables` signatures and generated database shape.

Gate:

- The generated migration model is unchanged except for intentional metadata changes.
- Contract/model/validator tests pass.
- Malformed-schema diagnostics still identify exact qualified capabilities.

### Phase 3: Corpus and Manifest Integrity

1. Complete the manifest inventory.
2. Add typed evidence and collision-proof test identities.
3. Correct assertion blind spots.
4. Replace or reclassify internal acceptance targets.
5. Ensure wording distinguishes public behavior, package shape, migration behavior, and internal mechanisms.

Gate:

- Every supported requirement has appropriate primary evidence.
- Every dedicated corpus test is linked.
- No internal mechanism test is labeled as public acceptance evidence.
- The manifest gate makes no claim stronger than it can mechanically prove.

### Phase 4: Concurrency, Audit, and Operational Closure

1. Complete the hold-race matrix.
2. Complete committed-total and evidence-coherence assertions.
3. Add historical diagnostic migration preservation.
4. Add high-volume parameter-bound evidence and register existing time/tenant/observer tests in the manifest.
5. Complete cleanup cancellation and precedence evidence.

Gate:

- All race tests prove the wait and final state.
- Public, observer, and persisted totals agree with committed work.
- Historical text remains exact.
- Parameter counts remain batch-bounded as total failures grow.

### Phase 5: Migration, Package, and Final Review

1. Add representative custom-schema move evidence.
2. Update README and migration guidance only where the second pass changes or clarifies the contract.
3. Run package and clean-consumer verification.
4. Review every requirement/evidence mapping manually for semantic adequacy.
5. Run the full verification matrix and inspect the final diff.

Gate:

- Every Definition of Done item below is supported by named evidence.
- No checkbox is inferred from aggregate test count alone.
- No unresolved audit finding remains hidden in a `supported` manifest entry.

## Verification Matrix

At minimum, final verification includes the commands inferred from the checked-in solution and project files:

```sh
dotnet test Cohort.Tests/Cohort.Tests.csproj
dotnet test Cohort.Sample.Tests/Cohort.Sample.Tests.csproj
dotnet build Cohort.slnx
```

In addition:

- The package and clean-consumer test must execute as part of `Cohort.Tests`.
- Manifest linkage and corpus tests must execute as part of `Cohort.Sample.Tests`.
- Empty and populated upgrade migration paths must execute against PostgreSQL.
- The custom-schema move scenario must execute against PostgreSQL.
- `git diff --check` must pass.
- The final diff must be reviewed for unrelated public API or migration changes.
- Any formatter or static-analysis failure must be either fixed or recorded here with an explicit scope decision; it cannot be silently omitted from the final report.

## Definition of Done

### PostgreSQL safety

- [x] Every application, Cohort, and system-catalog relation in raw SQL is explicitly schema-qualified.
- [x] Every relevant PostgreSQL built-in invoked by raw SQL is explicitly `pg_catalog`-qualified.
- [x] A writable schema before `pg_catalog` cannot shadow hold, lock, readiness, mutation, audit, or dispatcher behavior.
- [x] Hostile `search_path`, custom schema, mixed identifier, and public-decoy tests pass through public APIs.

### Schema authority

- [x] One schema contract owns all required table capabilities.
- [x] Model construction, table adoption, and runtime validation consume that contract.
- [x] No duplicated hand-authored schema inventory remains.
- [x] Generated model shape and malformed-schema diagnostics are fully tested.

### Manifest and evidence

- [x] The manifest uses typed, collision-proof evidence entries.
- [x] Existing requirement IDs and behavioral strength are preserved.
- [x] Every supported product/compliance invariant is inventoried.
- [x] Every supported requirement has primary evidence of the correct kind.
- [x] Every dedicated corpus test maps back to a requirement.
- [x] Duplicate or unresolved evidence targets fail the linkage gate.
- [x] Internal mechanism tests are not represented as public acceptance evidence.
- [x] The manifest gate describes its mechanical guarantee honestly.

### Corpus assertions

- [x] Protected rows are asserted to exist by identity and count.
- [x] Refused operations are proven mutation-free before any later operation.
- [x] Public, observer, and persisted totals agree with committed mutations.
- [x] Summary and row-detail identity, policy, strategy, tenant, and provenance are coherent.
- [x] Hold tests have positive controls for every mutation strategy.
- [x] Factory and provider-conversion claims assert both invocation context and stored representation.
- [x] Readiness tests prove validation ran by exercising invalid and repaired states.

### Public boundaries and concurrency

- [x] User-observable requirements are exercised through public ports or real hosted paths.
- [x] Provider-conversion acceptance tests use `IRetentionSweep`.
- [x] Hosted-startup acceptance tests start the real host.
- [x] Public hold creation races safely with purge, soft delete, anonymise, and erasure.
- [x] Cross-entity canonical record collisions do not leak holds.
- [x] Distinct bulk, handler-aware, and anonymisation concurrency paths are covered.

### Audit and privacy

- [x] Authoritative audit persistence remains internal and non-replaceable.
- [x] Multiple observers receive committed lifecycle events in order.
- [x] Observer failure, timeout, and non-cooperation cannot alter retention outcomes.
- [x] Rolled-back work is neither persisted as progress nor observed.
- [x] New diagnostics remain sanitized across every public and persisted surface.
- [x] Original exceptions remain correlated only through structured logging with a diagnostic ID; sink protection is a host responsibility.
- [x] Historical `Error` and `LastError` values survive upgrade unchanged.

### Operations and migrations

- [x] Tenant deduplication, first-context selection, warning behavior, sequential execution, and failure isolation are manifest-backed.
- [x] Failed-handler processing is proven loop-free, retry-safe, memory-bounded, and parameter-bounded.
- [x] Numeric ceilings, saturating arithmetic, and chunked delays are manifest-backed.
- [x] Cleanup precedence and independent bounded cancellation are proven.
- [x] Empty, legacy, populated-history, row-detail rename, and custom-schema move migration scenarios pass.
- [x] Holds, audit evidence, diagnostics, indexes, checks, and foreign keys survive applicable upgrades and moves.

### Package and final quality

- [x] The complete public API allowlist passes.
- [x] Package creation and clean-consumer execution pass.
- [x] Every public README C# example compiles against the packed package.
- [x] All unit, PostgreSQL, corpus, migration, package, and build gates pass.
- [x] `git diff --check` passes.
- [x] Static-analysis or formatting status is explicitly reported.
- [x] A final manual traceability review confirms that every checked item is supported by evidence that asserts the named invariant.
- [x] No known audit finding remains represented as complete without an explicit supported requirement or non-goal decision.

## Non-Goals

- Reopening immediate-erasure, legal-minimum, hold, or soft-delete policy decisions.
- Claiming that Cohort or its corpus certifies complete GDPR compliance.
- Adjudicating whether a host's legal minimum, hold, or erasure request is legally valid.
- Adding SQL Server or SQLite support.
- Rewriting historical migrations.
- Redacting historical failure text.
- Providing guaranteed external observer delivery.
- Adding test-only production ports, replaceable infrastructure seams, or mocks solely to force otherwise internal failures.
- Exhaustively testing every theoretically possible PostgreSQL identifier or concurrency interleaving when distinct implementations and boundary conditions are already covered.
- Broadly reorganizing existing test files unrelated to closing a named requirement.

## Sign-Off Record

This section is completed only after every Definition of Done item is checked.

- Implementation reviewer: OpenCode renewed implementation, concurrency, resource-bound, and diff review
- Contract/corpus reviewer: OpenCode renewed clause-by-clause traceability review; independent final evidence audit found no remaining unsupported claims
- Migration/package reviewer: OpenCode exact migration-shape, historical-hash, README extraction, and clean-consumer review
- Final unit test result: 236 passed, 0 failed, 0 skipped
- Final PostgreSQL test result: 443 passed, 0 failed, 0 skipped
- Final build result: succeeded with 0 warnings and 0 errors
- Final package consumer result: passed as part of `Cohort.Tests`; every public README C# example was extracted from the packed README and compiled
- Final migration result: empty, legacy, populated-history, row-detail rename, historical diagnostics, exact historical-source hashes, and exact custom-schema move scenarios passed
- Known accepted warnings: `dotnet format Cohort.slnx --verify-no-changes --no-restore` reports the repository-wide pre-existing `IDE1006` private-field underscore baseline; this second pass does not perform an unrelated naming migration. Meridian `plumb` reports `MER-TO-010` because `global.json` does not set `rollForward: latestFeature`; Cohort intentionally remains a .NET 9 library and SDK-policy migration is outside this retention closeout.
- Sign-off date: 2026-07-12
