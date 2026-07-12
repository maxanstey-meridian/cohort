# CLAUDE.md — Cohort

Project-specific agent instructions. Read this before writing tests or adding features. Global preferences live in `~/.claude/CLAUDE.md`.

## What Cohort is

Standalone .NET 9 class library. Annotation-driven retention for EF Core consumers (Postgres-only SQL). Hosts annotate entities with `[Retain(category, anchor)]`, register an `IRetentionRuleProvider` with declared strategy capabilities, and Cohort previews, sweeps, or erases rows through public application ports. Ordinary retention uses `max(Period, LegalMin)`; subject erasure ignores `Period` but remains blocked by positive `LegalMin` and active holds.

## Test-writing rules — READ BEFORE WRITING ANY TEST

Three patterns. Each has a header comment in its exemplar file. Pick one, copy the exemplar, edit the seed and assertions.

1. **End-to-end test in `Cohort.Sample.Tests/`** — the default. Use whenever the code under test touches a port: `DbContext`, `IOptions<T>` with real config binding, `IHostedService`, file/HTTP I/O, anything that crosses an I/O boundary. Feed real data in the front, run the real code path, assert what comes out. **Copy `RegistryEndToEndTests.cs`.** Do not abstract. Do not share a base class beyond `IntegrationTestBase`. Do not add mocks.

2. **Pure unit test in `Cohort.Tests/`** — only when the code under test is a static function with no I/O, no DbContext, no time source beyond parameters, no randomness. `[Theory]` + `[InlineData]` rows. **Copy `CutoffCalculatorTests.cs`.** No async. No fixtures. No DI. No `IClock` abstraction — never invent an abstraction to test a pure function.

3. **Narrow integration test in `Cohort.Sample.Tests/` (the middle ground)** — only when the code under test crosses one boundary that Npgsql model metadata fully serves without opening a connection (reflection over `Model.GetEntityTypes()` and friends). If there is **any** SQL involved, skip this and write pattern #1 instead. **Copy `RegistryScanTests.cs`. If in doubt, don't — write pattern #1.**

### The mock ban is structural, not policy

`Cohort.Tests` and `Cohort.Sample.Tests` deliberately do not reference NSubstitute (or Moq, or FakeItEasy). Adding the package is not the solution to a difficult test — it is the symptom of a missing end-to-end test. If you find yourself wanting `Substitute.For<IFoo>()`:

1. Stop.
2. The thing you're testing has an I/O boundary.
3. Write the end-to-end test instead. It will be shorter than the mock setup. Promise.

### When you add a port

A new `IFoo` interface ships in the same PR as an end-to-end test that exercises the **real** implementation against `PostgresFixture`. An `IFoo` without a real-implementation end-to-end test is not merged. This is the rule that prevents mock hell.

### When you add a feature

Start in `Cohort.Sample.Tests/` with a red end-to-end test. Then make it pass. The sample is the dogfood project — if the feature can't be exercised through the sample, either the feature's shape is wrong or the sample needs a new entity. Both are worth the friction.

## Project layout

```
Cohort/                 — the library. No tests of its own.
├── Domain/             — pure types. Depends on NOTHING (not even EF Core).
├── Application/        — ports + orchestration. Depends on Domain only.
├── Infrastructure/     — EF defaults, raw SQL adapters. Depends on Application + Domain.
│   ├── Sweep/          — RelationalSweepStrategyCore (shared purge/soft-delete SQL), Purge/SoftDelete/AnonymiseSweepStrategy
│   ├── Holds/          — EfRetentionHoldsRepository, RetentionHoldSql
│   ├── Audit/          — EfRetentionAuditWriter
│   └── Migrations/     — CohortModelBuilder (ConfigureCohortTables extension)
└── Hosting/            — CohortOptions, RetentionWorker, AddCohort<TContext>()
Cohort.Tests/           — fast unit suite. No Docker. No NSubstitute.
Cohort.Sample/          — dogfood console consumer. The thing the e2e tests exercise.
Cohort.Sample.Tests/    — e2e suite. Testcontainers Postgres. The default test home.
```

`Cohort/` has no tests of its own. All testing routes through `Cohort.Tests` (pure) or `Cohort.Sample.Tests` (everything else).

## Layer rules — `Cohort/` folder = layer

The dependency rule is enforced by `using` statements, not project boundaries. Read your imports before committing.

- **`Domain/`** depends on nothing. If a type needs `using Microsoft.EntityFrameworkCore;`, it isn't Domain. Pure functions, attributes, value records.
- **`Application/`** depends on Domain only. Defines ports as interfaces. Orchestrates. May reference `DbContext` as a port-shaped dependency (it's the host's "here is my model" contract), but never `DbSet<T>.FromSqlRaw`, never raw SQL, never Npgsql-specific types.
- **`Infrastructure/`** depends on Application + Domain. Implements ports. Owns raw SQL, EF query expressions, provider-specific types. Contains sweep strategies, holds repository, audit writer, and model builder.
- **`Hosting/`** depends on Application + Infrastructure. The DI entry point (`AddCohort<TContext>`), options, and the background worker. Consumer-facing.

**Layer placement test**: if a type has no dependency on a port, an external system's shape, or a framework — it's Domain. If it does — it's Application (defines/orchestrates the port) or Infrastructure (implements one).

**Worked example**: `IRetentionRuleProvider` lives in Application because it is a host I/O boundary, while its invariant-safe `RetentionCategoryCapabilities` value lives in Domain. The layer is determined by what a type binds to, not by how much code it contains.

## Conventions

- `sealed record` for data, `sealed class` for behaviour
- Primary constructors, file-scoped namespaces, 4 spaces, always braces, nullable enable
- `ILogger<T>` injected directly — no facades
- Strongly-typed options with `IValidateOptions<T>` + `ValidateOnStart()` — never read config strings inline
- `Directory.Packages.props` is the package allowlist. Adding a package requires justification in the PR description.
- No MediatR, no AutoMapper, no ceremony.

## Convention override attributes

Property names for record ID, tenant, soft-delete flag, and deletion timestamp are resolved by convention (`Id`, `TenantId`, `IsDeleted`, `DeletedAt`). Override any of them with a property-level marker attribute:

- `[RetentionRecordId]` — marks the record ID property (replaces `Id` convention)
- `[RetentionTenant]` — marks the tenant property (replaces `TenantId` convention)
- `[RetentionSoftDelete]` — marks the soft-delete flag (replaces `IsDeleted` convention)
- `[RetentionDeletedAt]` — marks the deletion timestamp (replaces `DeletedAt` convention)
- `[RetentionAnonymisedAt]` — marks the anonymised-at marker (replaces `AnonymisedAt` convention; mandatory for Anonymise categories)

These follow the same pattern as `[Anonymise]` — property-level, discovered by reflection, attribute wins over convention.

## Record ID types

Cohort is PK-type-agnostic. Entity record IDs can be `Guid`, `int`, `long`, `string`, or any other type. Cohort stores canonical record IDs as `text` in its own infrastructure tables (`retention_holds.RecordId`, `sweep_run_row_detail.RecordId`). Hold-table joins cast the column to text (`hold."RecordId" = CAST(target."pk_col" AS text)`); candidate-id matching casts the **parameter** to the column's store type when EF metadata exposes one (`RecordIdSql`, index-friendly), falling back to the column-to-text cast otherwise.

`RetentionEntityId` always means the stable UUID assigned to a retained entity type. `RecordId` always means one row's canonical text identity. Hold creation canonicalizes the mapped ID, requires the target row to exist, validates tenant ownership, and uses the same advisory-lock key as mutation. Tenantless targets require a null tenant ID.

## Entity annotation

- Entities annotated with `[Retain]` are subject to retention sweeps.
- Entities annotated with `[ExemptFromRetention]` are explicitly documented as exempt (optional sugar).
- **Unannotated entities are implicitly exempt.** No annotation required to opt out.
- Entities with **both** `[Retain]` and `[ExemptFromRetention]` fail startup validation.
- Retained owned types, shared-table mappings, entity/table splitting, and inheritance mappings are unsupported and fail startup before mutation.
- All raw relation names are schema-qualified. `ConfigureCohortTables()` maps Cohort tables to `public`; `ConfigureCohortTables(schema)` maps all five together.

## Runtime contracts

- `IRetentionRuleProvider.GetCapabilities` declares every possible strategy; startup validates their union and runtime rejects undeclared strategies.
- Startup validates `[ErasureSubject]` mapping and provider conversion metadata.
- `IRetentionSweep`, `IRetentionPreview`, `IRetentionErasureService`, `IRetentionHoldsRepository`, and row dispatch enforce PostgreSQL/model/schema readiness even without host startup.
- The EF audit ledger is internal and unconditional. `IRetentionAuditObserver` is post-commit, bounded, isolated, and best effort.
- Externally-derived failures use a sanitized type/code/diagnostic-ID envelope; Cohort-defined safe machine reasons may be plain. Diagnostic error text excludes subject/raw exception data, but row-detail observers deliberately receive sensitive `RecordId` and `TenantId`. Original exceptions remain only in correlated structured logs.

## Out of scope

- No built-in conditional, alias, or caching rule providers — hosts build these when needed
- No hash/format-preserving anonymisation — v1 is `Null`/`EmptyString`/`FixedLiteral` only
- No SQL Server or SQLite support — Postgres-only SQL (`RETURNING`, `= ANY()`, `FOR UPDATE`)
- No source generator — reflection is fine for a daily sweep
- No OneTrust/Purview adapters
- CI: `.github/workflows/publish.yml` packs and publishes; there is no separate test workflow
