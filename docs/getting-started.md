# Getting Started

Cohort reads retention metadata from your EF Core model and runs it against PostgreSQL.
This page walks one entity from annotation to its first sweep.

## Install

```bash
dotnet add package Cohort
```

Cohort targets .NET 9 and EF Core 9, runs on EF Core 10 hosts, and requires the Npgsql EF
Core provider.

## Annotate an entity

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
```

- `[Retain("session-notes", nameof(CreatedAt))]` puts the entity in the `session-notes`
  category and ages its rows by `CreatedAt`.
- `[RetentionEntityId]` is the entity's permanent identity. Holds, audit rows and handler
  work refer to it, so generate a UUID once and never change it.
- `Id` and `TenantId` are found by convention. Other names work too, as described in
  [Attributes](/reference/attributes).

## Write a rule provider

```csharp
using Cohort.Application;
using Cohort.Domain;

public sealed class RetentionRules : IRetentionRuleProvider
{
    public RetentionCategoryCapabilities? GetCapabilities(string category) => category switch
    {
        "session-notes" => new([Strategy.Purge]),
        _ => null,
    };

    public Task<RetentionRule?> ResolveAsync(RetentionResolutionContext context, CancellationToken ct)
    {
        RetentionRule? rule = context.Category switch
        {
            "session-notes" => new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge),
            _ => null,
        };

        return Task.FromResult(rule);
    }
}
```

## Register and migrate

Register the provider before `AddCohort`:

```csharp
builder.Services.AddSingleton<IRetentionRuleProvider, RetentionRules>();
builder.Services.AddCohort<AppDbContext>();
```

Add Cohort's tables to your model:

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    base.OnModelCreating(modelBuilder);
    modelBuilder.ConfigureCohortTables();
}
```

Then generate and apply a migration as usual:

```bash
dotnet ef migrations add AddCohort
dotnet ef database update
```

The migration creates Cohort's five tables. Cohort checks them at startup but never creates
or changes them itself. See [Schema & Migrations](/reference/schema).

## Run a sweep

Preview first. It counts what would change without writing anything:

```csharp
var tenant = new TenantContext(tenantId, "uk", new Dictionary<string, string>());
var now = DateTimeOffset.UtcNow;

var preview = await services.GetRequiredService<IRetentionPreview>().PreviewAsync(tenant, now, ct);
var result = await services.GetRequiredService<IRetentionSweep>().SweepAsync(tenant, now, ct);
```

The sweep deletes this tenant's session notes older than 30 days, skips any under a legal
hold, and records the run in `sweep_run`.

To run on a schedule instead, set a cron expression and the hosted worker sweeps every
tenant:

```json
{ "Cohort": { "Schedule": "0 2 * * *" } }
```

## Next

- [Retention Rules](/guides/retention-rules): categories, strategies, legal minimums and
  tenancy
- [Running Retention](/guides/running-retention): the worker, dry runs and the kill switch
- [Anonymisation](/guides/anonymisation): keep the row, lose the personal data
- [Startup Validation](/reference/startup-validation): everything Cohort checks before it
  will run
