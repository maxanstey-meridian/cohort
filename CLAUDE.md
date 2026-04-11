# CLAUDE.md — Cohort

Project-specific agent instructions. Read this before writing tests or adding features. Global preferences live in `~/.claude/CLAUDE.md`.

## What Cohort is

Standalone .NET 9 class library. Annotation-driven retention for EF Core consumers. Hosts annotate entities with `[Retain(category, anchor)]`, register an `IRetentionRuleResolver`, and Cohort sweeps rows past their cutoff. Three milestones A/B/C — see [`.plans/COHORT1.md`](.plans/COHORT1.md). This skeleton is pre-A, see [`.plans/COHORT0.md`](.plans/COHORT0.md).

## Test-writing rules — READ BEFORE WRITING ANY TEST

Three patterns. Each has a header comment in its exemplar file. Pick one, copy the exemplar, edit the seed and assertions.

1. **End-to-end test in `Cohort.Sample.Tests/`** — the default. Use whenever the code under test touches a port: `DbContext`, `IOptions<T>` with real config binding, `IHostedService`, file/HTTP I/O, anything that crosses an I/O boundary. Feed real data in the front, run the real code path, assert what comes out. **Copy `RegistryEndToEndTests.cs`.** Do not abstract. Do not share a base class beyond `IntegrationTestBase`. Do not add mocks.

2. **Pure unit test in `Cohort.Tests/`** — only when the code under test is a static function with no I/O, no DbContext, no time source beyond parameters, no randomness. `[Theory]` + `[InlineData]` rows. **Copy `CutoffCalculatorTests.cs`.** No async. No fixtures. No DI. No `IClock` abstraction — never invent an abstraction to test a pure function.

3. **Narrow integration test in `Cohort.Sample.Tests/` (the middle ground)** — only when the code under test crosses one boundary that EF Core's InMemory provider fully serves (reflection over `Model.GetEntityTypes()` and friends). If there is **any** SQL involved, skip this and write pattern #1 instead. **Copy `RegistryScanTests.cs`. If in doubt, don't — write pattern #1.**

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
└── Infrastructure/     — EF defaults, raw SQL adapters. Depends on Application + Domain.
                          Empty in skeleton — fills up in Milestone A/B.
Cohort.Tests/           — fast unit suite. No Docker. No NSubstitute.
Cohort.Sample/          — dogfood console consumer. The thing the e2e tests exercise.
Cohort.Sample.Tests/    — e2e suite. Testcontainers Postgres. The default test home.
```

`Cohort/` has no tests of its own. All testing routes through `Cohort.Tests` (pure) or `Cohort.Sample.Tests` (everything else).

## Layer rules — `Cohort/` folder = layer

The dependency rule is enforced by `using` statements, not project boundaries. Read your imports before committing.

- **`Domain/`** depends on nothing. If a type needs `using Microsoft.EntityFrameworkCore;`, it isn't Domain. Pure functions, attributes, value records.
- **`Application/`** depends on Domain only. Defines ports as interfaces. Orchestrates. May reference `DbContext` as a port-shaped dependency (it's the host's "here is my model" contract), but never `DbSet<T>.FromSqlRaw`, never raw SQL, never Npgsql-specific types.
- **`Infrastructure/`** depends on Application + Domain. Implements ports. Owns raw SQL, EF query expressions, provider-specific types. Empty until Milestone A's strategies and Milestone B's EF defaults land.

**Layer placement test**: if a type has no dependency on a port, an external system's shape, or a framework — it's Domain. If it does — it's Application (defines/orchestrates the port) or Infrastructure (implements one).

**Worked example**: `StaticRetentionRuleResolver` has zero runtime dependencies but **implements `IRetentionRuleResolver` (an Application port)** — therefore it lives in `Application/`, not `Domain/`. The layer is determined by what a type *binds to*, not by how much code it contains.

## Conventions

- `sealed record` for data, `sealed class` for behaviour
- Primary constructors, file-scoped namespaces, 4 spaces, always braces, nullable enable
- `ILogger<T>` injected directly — no facades
- Strongly-typed options with `IValidateOptions<T>` + `ValidateOnStart()` — never read config strings inline
- `Directory.Packages.props` is the package allowlist. Adding a package requires justification in the PR description.
- No MediatR, no AutoMapper, no ceremony.

## What is NOT in this repo yet

The skeleton is a trellis. None of the following exist — do not hallucinate code to edit them:

- No `RetentionSweepEngine`, no strategies (`PurgeSweepStrategy`, `SoftDeleteSweepStrategy`, `AnonymiseSweepStrategy`) — Milestone A/B
- No `IRetentionHoldsRepository`, no `retention_holds` table — Milestone B
- No `IRetentionAuditWriter`, no `sweep_run*` tables — Milestone B
- No `RetentionStartupValidator`, no `RetentionConfigurationException` — Milestone A
- No `IRetentionPreview`, no dry-run path — Milestone A
- No `RetentionWorker`, no `CohortOptions`, no `AddCohort<TContext>()` — Milestone C
- No `IRetentionErasureService`, no `[ErasureSubject]` attribute — Milestone C
- No `ConfigureCohortTables(ModelBuilder)` — Milestone C
- No `ExemptFromRetentionAttribute` — Milestone A

Pointer: `.plans/COHORT1.md` for the milestone sequence. `.plans/COHORT0.md` for what this skeleton does (and doesn't) ship.
