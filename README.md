<p align="center">
  <h1 align="center">Cohort</h1>
  <p align="center">
    <a href="https://www.nuget.org/packages/Cohort"><img src="https://img.shields.io/nuget/v/Cohort?label=Cohort" alt="NuGet" /></a>
    <img src="https://img.shields.io/badge/license-MIT-blue" alt="License" />
  </p>
</p>

**Retention belongs in your model.** Cohort lets you annotate EF Core entities with a
retention category, map each category to a rule, and then preview, sweep or erase rows
through ordinary application services. Cohort handles the awkward parts: tenant scoping,
legal holds, batched Postgres mutations, right-to-erasure and an audit trail of everything
it touched.

You get the same result as a pile of nightly SQL jobs, but the policy sits next to the
entity it governs. Startup refuses to run a retention model it can't execute safely.

## Prerequisites

- .NET 9 or later, with EF Core 9 or later (EF Core 10 hosts are supported).
- PostgreSQL through the Npgsql EF Core provider. Cohort's SQL is Postgres-only.

## Install

```bash
dotnet add package Cohort
```

## Three steps in

### 1. Annotate the entities you retain

```csharp
using Cohort.Domain;

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
```

`[Retain]` names the category and the column to age rows by. `[RetentionEntityId]` is a
stable UUID that survives class and table renames. Never change it. Unannotated entities are
left alone.

### 2. Map categories to rules

The annotation says which category an entity belongs to. Your rule provider says what that
category means:

```csharp
using Cohort.Application;
using Cohort.Domain;

public sealed class RetentionRules : IRetentionRuleProvider
{
    public RetentionCategoryCapabilities? GetCapabilities(string category) => category switch
    {
        "session-notes" => new([Strategy.Purge]),
        "case-contacts" => new([Strategy.Anonymise]),
        _ => null,
    };

    public Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct)
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

`ResolveAsync` sees the tenant and the time, so rules can vary per tenant or jurisdiction.
`GetCapabilities` declares every strategy a category could ever resolve to, which lets
startup check that every entity can actually carry it out.

### 3. Register Cohort and migrate

```csharp
builder.Services.AddSingleton<IRetentionRuleProvider, RetentionRules>();
builder.Services.AddCohort<AppDbContext>();
```

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    base.OnModelCreating(modelBuilder);
    modelBuilder.ConfigureCohortTables();
}
```

Then add an EF Core migration and apply it. Cohort keeps its audit ledger, holds and handler
queue in five tables of your database. It checks that schema at startup but never creates or
changes it, so the migration stays yours.

## Run it

Set a cron schedule and the hosted worker sweeps every tenant:

```json
{ "Cohort": { "Schedule": "0 2 * * *" } }
```

Or call the services yourself:

```csharp
var now = DateTimeOffset.UtcNow;
await services.GetRequiredService<IRetentionPreview>().PreviewAsync(tenant, now, ct);
await services.GetRequiredService<IRetentionSweep>().SweepAsync(tenant, now, ct);
await services.GetRequiredService<IRetentionErasureService>()
    .EraseAsync(tenant, new ErasureScope("user", userId), now, ct);
```

Thirty-day-old session notes are deleted, and year-old case contacts keep their row but
lose their email and name. Every query is scoped to the tenant, held rows are skipped, and
the run is written to Cohort's ledger.

## Also in the box

- [Right-to-erasure](https://maxanstey-meridian.github.io/cohort/guides/erasure): erase one
  subject's rows immediately. Columns name the kind of subject they hold
  (`[ErasureSubject("user")]`), so a user erasure never touches a person column. A positive
  `LegalMin` and active holds still block it.
- [Legal holds](https://maxanstey-meridian.github.io/cohort/guides/legal-holds): a held row
  survives every strategy. `IRetentionDeletion` lets your own deletes respect holds too.
- [Row handlers](https://maxanstey-meridian.github.io/cohort/guides/row-handlers): capture a
  row before mutation, then clean up blobs or publish events after commit, with
  at-least-once delivery and dead-lettering.
- [Audit observers](https://maxanstey-meridian.github.io/cohort/guides/audit-trail): export
  each committed run event to your own systems.
- [History pruning](https://maxanstey-meridian.github.io/cohort/guides/history-pruning):
  Cohort's own ledger has retention too, and it's opt-in.
- Dry runs, a kill switch, per-tenant passes and batched transactions, all covered under
  [Running retention](https://maxanstey-meridian.github.io/cohort/guides/running-retention).

## Documentation

[Getting Started](https://maxanstey-meridian.github.io/cohort/getting-started) ·
[Retention Rules](https://maxanstey-meridian.github.io/cohort/guides/retention-rules) ·
[Configuration](https://maxanstey-meridian.github.io/cohort/reference/configuration) ·
[Startup Validation](https://maxanstey-meridian.github.io/cohort/reference/startup-validation) ·
[Limitations](https://maxanstey-meridian.github.io/cohort/misc/limitations)
(what Cohort deliberately doesn't do)

## License

[MIT](LICENSE)
