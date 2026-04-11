# Cohort — pre-Milestone-A scaffolding

## Context

`~/Sites/medway/cohort/` currently contains only `.plans/COHORT1.md` (the three-milestone Cohort library plan). Before any Milestone A code is written, the repo needs a skeleton that establishes file layout, conventions, DI shape, and — most critically — **exemplar tests in three styles** that future LLM agents will pattern-match against.

The user's load-bearing constraint: **end-to-end integration tests are the default**. Pure functions get unit tests; anything that crosses an I/O boundary gets a real Postgres-backed test via Testcontainers. No mocks. The `Cohort.Tests` and `Cohort.Sample.Tests` projects deliberately do **not** reference NSubstitute — the ban is structural, not policy. Goal: when an LLM later adds Milestone A's sweep engine, the path of least resistance is to copy `RegistryEndToEndTests.cs`.

Reference codebases (already explored, do not re-explore):
- `~/Sites/medway/casebridge/api` — modular monolith, sealed records, primary ctors, `IValidateOptions<T>` + `ValidateOnStart()`, `PostgresFixture` + `IntegrationTestBase` + Respawn pattern, no MediatR/AutoMapper.
- `~/Sites/medway/rivet/Rivet.Tests/{AnnotationRoundTripTests.cs,FormatRoundTripTests.cs}` — round-trip test gold standard: raw-string fixtures, real pipeline through, positive AND negative assertions in one test, shared `CompilationHelper` for boilerplate.

Decisions confirmed: **net9.0**, **`.slnx`**, **Central Package Management on**, repo lives at `/Users/max/Sites/medway/cohort/`.

## Repo layout to create

```
/Users/max/Sites/medway/cohort/
├── .editorconfig                        # 4 spaces, file-scoped namespaces, always braces, _camelCase
├── .gitignore                           # dotnet new gitignore + .idea/ + .vs/
├── global.json                          # SDK pin: 9.0.x
├── Directory.Build.props                # Nullable=enable, ImplicitUsings=enable, TreatWarningsAsErrors=true, LangVersion=latest
├── Directory.Packages.props             # central package management — single version list
├── Cohort.slnx                          # solution
├── README.md                            # one paragraph + links to .plans/COHORT1.md and CLAUDE.md
├── CLAUDE.md                            # project-specific agent instructions (load-bearing — see below)
├── .plans/
│   ├── COHORT1.md                       # already exists
│   └── COHORT0.md                       # NEW — copy of this scaffolding plan committed for traceability
│
├── Cohort/                              # the library (net9.0). Folder = layer. Dependency rule enforced by `using` statements.
│   ├── Cohort.csproj                    # single PackageRef: Microsoft.EntityFrameworkCore. InternalsVisibleTo Cohort.Tests.
│   ├── Domain/                          # pure types. Depends on NOTHING (not even EF Core). Layer placement test:
│   │   │                                #   "no dependency on a port, an external system's shape, or a framework → Domain".
│   │   ├── RetainAttribute.cs           # sealed, [AttributeUsage(Class)], (string category, string anchorMember)
│   │   ├── RetentionRule.cs             # sealed record (TimeSpan Period, TimeSpan? LegalMin) — Strategy/AuditRowDetail deferred to A
│   │   ├── RetentionEntry.cs            # sealed record (Type EntityType, string TableName, string Category, string AnchorMember)
│   │   └── CutoffCalculator.cs          # internal static. Compute(now, period, legalMin) → DateTimeOffset.
│   │                                    #   Returns now - max(period, legalMin ?? period). The pure helper exemplar #1 tests.
│   │                                    #   InternalsVisibleTo Cohort.Tests so the exemplar can hit it.
│   ├── Application/                     # ports + orchestration. Depends on Domain only. No EF queries, no SQL.
│   │   ├── IRetentionRuleResolver.cs    # port: ResolveAsync + default TryResolveAtStartup() returning null
│   │   ├── StaticRetentionRuleResolver.cs  # sealed class, returns constant rule. Lives here (not Domain) because it
│   │   │                                #   implements an Application port — that's the layer placement signal.
│   │   └── RetentionRegistry.cs         # sealed class, primary ctor (DbContext db). Single method: IReadOnlyList<RetentionEntry> Scan().
│   │                                    #   Walks db.Model.GetEntityTypes(), reads [Retain], validates anchor member exists & is date-typed,
│   │                                    #   resolves table name from EF metadata. Throws InvalidOperationException on failure (deliberate
│   │                                    #   crude version — proper RetentionConfigurationException with multi-error aggregation is Milestone A).
│   │                                    #   Note: takes DbContext (an EF type) as a port-shaped dependency. EF is the registry's
│   │                                    #   "external system" — Application is allowed to depend on the abstraction (DbContext is
│   │                                    #   the host's contract for "here is my model"), not on concrete provider types.
│   └── Infrastructure/                  # EF defaults, raw SQL adapters. Depends on Application + Domain. EMPTY in skeleton —
│                                        #   nothing lives here until Milestone A's PurgeSweepStrategy and Milestone B's
│                                        #   EfRetentionAuditWriter / EfRetentionHoldsRepository land. The folder exists as a
│                                        #   placeholder so the LLM sees the three-layer shape from day one.
│       └── .gitkeep
│
├── Cohort.Tests/                        # FAST suite — pure unit tests only, no Docker
│   ├── Cohort.Tests.csproj              # xunit, FluentAssertions, Microsoft.EntityFrameworkCore.InMemory.
│   │                                    #   NO Testcontainers. NO Respawn. NO NSubstitute. The absences are policy.
│   ├── GlobalUsings.cs                  # using Xunit; using FluentAssertions; using Cohort.*;
│   └── CutoffCalculatorTests.cs         # EXEMPLAR #1 — see below
│
├── Cohort.Sample/                       # dogfood console consumer (OutputType=Exe)
│   ├── Cohort.Sample.csproj             # ProjectRef Cohort. PackageRefs: EFCore.Design, Npgsql.EFCore.PostgreSQL, Hosting, Options.ConfigurationExtensions
│   ├── Program.cs                       # Host.CreateApplicationBuilder; AddDbContext<SampleDbContext> via Npgsql; bind SampleOptions via
│   │                                    #   AddOptions<>().BindConfiguration().ValidateDataAnnotations().ValidateOnStart();
│   │                                    #   on run: scope SampleDbContext, build RetentionRegistry, Scan(), log "Found {N} entries", print each.
│   ├── appsettings.json                 # "Cohort": { "ConnectionString": "..." }
│   ├── SampleOptions.cs                 # sealed class with const string SectionName = "Cohort". DataAnnotations on ConnectionString.
│   ├── SampleDbContext.cs               # sealed class, single DbSet<Note>
│   ├── Entities/
│   │   └── Note.cs                      # sealed class. [Retain("short-lived", nameof(Note.CreatedAt))]. Fields: Id, CreatedAt, Body.
│   └── Migrations/                      # initial migration creating notes table — shipped from day one (decision D5: yes)
│
└── Cohort.Sample.Tests/                 # E2E suite — Testcontainers Postgres
    ├── Cohort.Sample.Tests.csproj       # ProjectRefs Cohort, Cohort.Sample. PackageRefs: xunit, FluentAssertions,
    │                                    #   Testcontainers.PostgreSql, Respawn, Microsoft.EFCore.InMemory (for EXEMPLAR #2 only).
    │                                    #   NO NSubstitute.
    ├── GlobalUsings.cs
    ├── _Infrastructure/
    │   ├── PostgresFixture.cs           # IAsyncLifetime. Starts postgres:16-alpine. Builds throwaway SampleDbContext, runs MigrateAsync(),
    │   │                                #   creates Respawner checkpoint. Mirrors casebridge PostgresFixture verbatim (read it first).
    │   ├── IntegrationCollection.cs     # [CollectionDefinition("Integration")] : ICollectionFixture<PostgresFixture>
    │   ├── CohortTestHost.cs            # sealed class — the rivet `CompilationHelper` analogue. Wraps Host.CreateApplicationBuilder,
    │   │                                #   wires SampleDbContext against fixture connection string, exposes CreateDbContext() scope helper.
    │   │                                #   This is the "spin it up in one line" utility every e2e test calls first.
    │   └── IntegrationTestBase.cs       # abstract. [Collection("Integration")]. IAsyncLifetime: respawn DB, build CohortTestHost.
    │                                    #   Exposes one protected property: CohortTestHost Host. No HttpClient (Cohort isn't a web app).
    │                                    #   No seed helpers — tests seed inline so they read top-to-bottom.
    ├── RegistryScanTests.cs             # EXEMPLAR #2 — see below
    └── RegistryEndToEndTests.cs         # EXEMPLAR #3 — see below
```

## The three exemplar tests

Each file opens with a header comment block telling the LLM **which pattern it is and when to copy it**. These headers are load-bearing — they're how the LLM picks the right pattern for the next feature.

### EXEMPLAR #1 — `Cohort.Tests/CutoffCalculatorTests.cs`

Header comment: *"Pattern: pure unit test. Use when the code under test is a static function with no I/O, no DbContext, no time source beyond parameters. If you find yourself writing Substitute.For<...>, you're in the wrong file — move to Cohort.Sample.Tests and write an end-to-end test instead."*

`sealed class CutoffCalculatorTests` with one `[Theory]` + three `[InlineData]` rows:
- 30-day period, no legal min → cutoff = now - 30d
- 30-day period, 90-day legal min → legal min dominates
- 90-day period, 30-day legal min → period dominates

Hits internal `CutoffCalculator` via `InternalsVisibleTo`. No async, no fixtures, no DI, no `IClock` abstraction. The last point is deliberate — don't let the LLM invent `IClock` for a pure function.

### EXEMPLAR #2 — `Cohort.Sample.Tests/RegistryScanTests.cs`

Header comment: *"Pattern: narrow integration test (the middle ground). Use when the code under test crosses one boundary (reflection over an EF model) but doesn't need a real database. Appropriate ONLY when: (a) the thing you're testing has no SQL path, (b) EF Core's InMemory provider fully exercises Model.GetEntityTypes(). If there is ANY SQL involved, skip this pattern and write EXEMPLAR #3 instead. If in doubt, write EXEMPLAR #3."*

Uses `DbContextOptionsBuilder<SampleDbContext>().UseInMemoryDatabase(...)`, instantiates `SampleDbContext` and `RetentionRegistry` directly, calls `Scan()`. Asserts (rivet-style — positive AND negative in the same test):
- `entries.Should().ContainSingle(e => e.Category == "short-lived" && e.AnchorMember == "CreatedAt" && e.EntityType == typeof(Note))`
- `entries.Should().NotContain(e => e.Category == "long-lived")`

Lives in `Cohort.Sample.Tests` not `Cohort.Tests` because it references `SampleDbContext`. The project-reference graph is the architectural guardrail.

### EXEMPLAR #3 — `Cohort.Sample.Tests/RegistryEndToEndTests.cs`

Header comment: *"Pattern: end-to-end test. THIS IS THE PATTERN. Feed data in the front, assert what comes out the back. Use this whenever the code under test touches a port (DbContext, IOptions with real config, IHostedService, file/HTTP I/O). Copy this file, rename, edit the seed and assertions. Do not abstract. Do not share a base class beyond IntegrationTestBase. Do not add mocks."*

```csharp
[Collection("Integration")]
public sealed class RegistryEndToEndTests(PostgresFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Registry_Scan_Finds_Annotated_Entity_Against_Real_Postgres()
    {
        // Arrange — seed two real rows through the real DbContext
        await using (var db = Host.CreateDbContext())
        {
            db.Notes.Add(new Note { Id = Guid.NewGuid(), CreatedAt = DateTimeOffset.UtcNow.AddDays(-10), Body = "one" });
            db.Notes.Add(new Note { Id = Guid.NewGuid(), CreatedAt = DateTimeOffset.UtcNow.AddDays(-1),  Body = "two" });
            await db.SaveChangesAsync();
        }

        // Act — registry against the same real DbContext
        List<RetentionEntry> entries;
        await using (var db = Host.CreateDbContext())
        {
            entries = new RetentionRegistry(db).Scan().ToList();
        }

        // Assert — positive AND negative
        entries.Should().ContainSingle(e =>
            e.Category == "short-lived" &&
            e.AnchorMember == nameof(Note.CreatedAt) &&
            e.EntityType == typeof(Note));
        entries.Should().NotContain(e => e.Category == "long-lived");

        // Sanity — rows actually landed in Postgres, not just in the EF model
        await using var verify = Host.CreateDbContext();
        (await verify.Notes.CountAsync()).Should().Be(2);
    }
}
```

## CLAUDE.md content (project-specific, load-bearing)

The new repo's `CLAUDE.md` is the enforcement mechanism. Required sections:

1. **Test-writing rules** (numbered, READ BEFORE WRITING ANY TEST):
   - I/O of any kind → end-to-end test in `Cohort.Sample.Tests`. Copy `RegistryEndToEndTests.cs`.
   - Pure static function → `[Theory]` unit test in `Cohort.Tests`. Copy `CutoffCalculatorTests.cs`.
   - One boundary that EF InMemory fully serves (reflection only, no SQL) → narrow integration in `Cohort.Sample.Tests`. Copy `RegistryScanTests.cs`. **If in doubt, don't — write end-to-end.**
   - Before writing `Substitute.For<...>` STOP. NSubstitute is intentionally absent from both test projects. If you think you need a mock, the thing has an I/O boundary and belongs in an end-to-end test.
   - New port `IFoo` → same PR adds an end-to-end test exercising the real implementation against `PostgresFixture`. Non-negotiable.
   - New library feature → start in `Cohort.Sample.Tests` with a red end-to-end test, then make it pass.

2. **Project layout** — short map. Reinforce: `Cohort/` has no tests of its own.

3. **Conventions** — `sealed record` for data, `sealed class` for behaviour, primary ctors, file-scoped namespaces, 4 spaces, always braces, nullable enable, `ILogger<T>` direct, `IValidateOptions<T>` + `ValidateOnStart()`, `Directory.Packages.props` is the package allowlist.

4. **Layer rules (Cohort/ folder = layer)** —
   - `Domain/` depends on **nothing** (not even EF Core). If a type needs `using Microsoft.EntityFrameworkCore;` it isn't Domain.
   - `Application/` depends on Domain only. Defines ports as interfaces. Orchestrates. May reference `DbContext` as a port-shaped dependency (it's the host's "here is my model" contract), but never `DbSet<T>.FromSqlRaw`, never SQL strings, never provider-specific types.
   - `Infrastructure/` depends on Application + Domain. Implements ports. Owns raw SQL, EF query expressions, Npgsql-specific types. Empty in the skeleton — fills up in Milestone A/B.
   - **Layer placement test**: if a type has no dependency on a port, an external system's shape, or a framework — it's Domain. If it does — it's Application or Infrastructure (Application if it defines/orchestrates the port, Infrastructure if it implements one).
   - **Worked example**: `StaticRetentionRuleResolver` has zero runtime dependencies but **implements `IRetentionRuleResolver` (an Application port)** — therefore it lives in Application, not Domain. The layer is determined by what a type *binds to*, not by how much code it contains.

5. **What is NOT in this repo yet** — explicit list so the LLM doesn't hallucinate code to edit:
   - No `RetentionSweepEngine`, no strategies (Purge/SoftDelete/Anonymise) — Milestone A/B
   - No `IRetentionHoldsRepository`, no `retention_holds` table — Milestone B
   - No `IRetentionAuditWriter`, no `sweep_run*` tables — Milestone B
   - No `RetentionStartupValidator` — Milestone A
   - No `IRetentionPreview`, no dry-run path — Milestone A
   - No `RetentionWorker`, no `CohortOptions`, no `AddCohort<TContext>()` — Milestone C
   - No `IRetentionErasureService` — Milestone C
   - No `ConfigureCohortTables(ModelBuilder)` — Milestone C
   - No `ExemptFromRetentionAttribute` — Milestone A
   - Pointer to `.plans/COHORT1.md` for the milestone sequence

## Critical files to create

| File | Purpose |
|---|---|
| `/Users/max/Sites/medway/cohort/CLAUDE.md` | Load-bearing — enforces the end-to-end test rule |
| `/Users/max/Sites/medway/cohort/Directory.Build.props` | Nullable, warn-as-error, lang version |
| `/Users/max/Sites/medway/cohort/Directory.Packages.props` | Central package management — single version list |
| `/Users/max/Sites/medway/cohort/global.json` | SDK pin to 9.0.x |
| `/Users/max/Sites/medway/cohort/.editorconfig` | 4 spaces, file-scoped, _camelCase |
| `/Users/max/Sites/medway/cohort/Cohort.slnx` | Solution file |
| `/Users/max/Sites/medway/cohort/Cohort/Application/RetentionRegistry.cs` | The annotation→entry path the e2e test exercises |
| `/Users/max/Sites/medway/cohort/Cohort/Domain/CutoffCalculator.cs` | The pure helper exemplar #1 tests |
| `/Users/max/Sites/medway/cohort/Cohort/Domain/{RetainAttribute,RetentionRule,RetentionEntry}.cs` | Pure types — Domain layer |
| `/Users/max/Sites/medway/cohort/Cohort/Application/{IRetentionRuleResolver,StaticRetentionRuleResolver}.cs` | Resolver port + static impl |
| `/Users/max/Sites/medway/cohort/Cohort/Infrastructure/.gitkeep` | Placeholder so the three-layer shape is visible from day one |
| `/Users/max/Sites/medway/cohort/Cohort.Sample/Entities/Note.cs` | The one annotated entity |
| `/Users/max/Sites/medway/cohort/Cohort.Sample/Migrations/` | Initial migration — fixture calls MigrateAsync() |
| `/Users/max/Sites/medway/cohort/Cohort.Sample.Tests/_Infrastructure/PostgresFixture.cs` | Mirrors casebridge `PostgresFixture` |
| `/Users/max/Sites/medway/cohort/Cohort.Sample.Tests/_Infrastructure/CohortTestHost.cs` | "One line to spin up the whole thing" — the load-bearing helper |
| `/Users/max/Sites/medway/cohort/Cohort.Sample.Tests/_Infrastructure/IntegrationTestBase.cs` | The class every future e2e test inherits |
| `/Users/max/Sites/medway/cohort/Cohort.Sample.Tests/RegistryEndToEndTests.cs` | EXEMPLAR #3 — the file the LLM copies for every new feature |
| `/Users/max/Sites/medway/cohort/Cohort.Tests/CutoffCalculatorTests.cs` | EXEMPLAR #1 |
| `/Users/max/Sites/medway/cohort/Cohort.Sample.Tests/RegistryScanTests.cs` | EXEMPLAR #2 |
| `/Users/max/Sites/medway/cohort/.plans/COHORT0.md` | This plan, committed for traceability |

## Reference patterns to mirror (read first, then write)

- `~/Sites/medway/casebridge/api/CaseBridge.Tests/Integration/_Infrastructure/PostgresFixture.cs` — copy the structure for `Cohort.Sample.Tests/_Infrastructure/PostgresFixture.cs`
- `~/Sites/medway/casebridge/api/CaseBridge.Tests/Integration/_Infrastructure/IntegrationTestBase.cs` — copy the structure for `Cohort.Sample.Tests/_Infrastructure/IntegrationTestBase.cs` (drop the HttpClient — Cohort isn't a web app)
- `~/Sites/medway/casebridge/api/.editorconfig` — copy verbatim, then trim casebridge-specifics
- `~/Sites/medway/rivet/Rivet.Tests/AnnotationRoundTripTests.cs` — match the test-writing style: raw-string fixtures inline, positive+negative assertions in one test, sealed class, `Subject_Does_Thing` naming
- `~/Sites/medway/rivet/Rivet.Tests/CompilationHelper.cs` — analogous role to `CohortTestHost`; read for shape

## Anti-scope (explicitly NOT in this skeleton)

1. No sweep engine, no strategies — Milestone A
2. No holds repo or table — Milestone B
3. No audit writer or `sweep_run*` tables — Milestone B
4. No `AddCohort<TContext>()` DI extension — Milestone C
5. No hosted service / worker — Milestone C
6. No erasure service — Milestone C
7. No `ConfigureCohortTables(ModelBuilder)` — Milestone C
8. No `RetentionConfigurationException` with aggregated errors — Milestone A
9. No `ExemptFromRetentionAttribute` — Milestone A
10. No second sample entity — one is enough for the patterns
11. No GitHub Actions workflow — defer until first Milestone A green run
12. No NSubstitute reference in any project — structural ban
13. No `WebApplicationFactory<Program>` analogue — Cohort is a library, not a web app

## Verification (run after execution)

From `/Users/max/Sites/medway/cohort/`:

1. `dotnet --version` — matches `global.json` (9.0.x). Proves SDK pin.
2. `dotnet restore` — all four projects resolve via `Directory.Packages.props`. Proves CPM wiring.
3. `dotnet build Cohort.slnx` — clean, zero warnings (warn-as-error on). Proves project shapes, nullable, project refs.
4. `dotnet test Cohort.Tests` — sub-second, no Docker, three `[InlineData]` rows green. **The "is my laptop configured" check.**
5. `dotnet test Cohort.Sample.Tests` — Docker required. Spins up `postgres:16-alpine`, applies initial migration, runs `RegistryScanTests` (InMemory) and `RegistryEndToEndTests` (real Postgres). Both green. Proves `PostgresFixture`, `IntegrationTestBase`, `CohortTestHost`, Respawn, the annotation→registry round-trip, and project-reference graph.
6. `dotnet run --project Cohort.Sample` — assumes a reachable Postgres via `appsettings.Development.json` or user secret. Builds the host, scans, logs `Found 1 retention entries`, prints the `Note` entry. Proves the production code path uses the same `RetentionRegistry.Scan()` as the tests.
7. Manual read of `CLAUDE.md` — verify the test-writing rules and "what is NOT in this repo yet" sections are present and accurate.

When all seven pass, the skeleton is alive and Milestone A can start.
