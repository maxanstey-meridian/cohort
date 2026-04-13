# Cohort

Annotation-driven GDPR retention for .NET / EF Core. Declare retention rules on your entities, and Cohort sweeps rows past their retention period — purge, soft-delete, or anonymise. Postgres-only.

## Quick start

### 1. Tag entities and define what happens to them

```csharp
// The entity — [Retain] declares the category and the age anchor
[Retain("short-lived", nameof(CreatedAt))]
public sealed class Note
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Body { get; set; } = "";
}

// The rules — map each category to a strategy and retention period
public sealed class MyCategoryRepository : IRetentionCategoryRepository
{
    public Task<IRetentionRuleResolver?> GetAsync(string category, CancellationToken ct)
    {
        IRetentionRuleResolver? resolver = category switch
        {
            "short-lived" => new StaticRetentionRuleResolver(
                new RetentionRule(TimeSpan.FromDays(30), Strategy.Purge)),
            "pii" => new StaticRetentionRuleResolver(
                new RetentionRule(TimeSpan.FromDays(90), Strategy.Anonymise)),
            _ => null,
        };
        return Task.FromResult(resolver);
    }
}
```

`[Retain("short-lived", nameof(CreatedAt))]` says "this entity belongs to the `short-lived` category, age it by `CreatedAt`." The category repository says "`short-lived` means purge after 30 days." Neither piece does anything without the other.

Unannotated entities are implicitly exempt — no annotation needed to opt out. Use `[ExemptFromRetention("reason")]` if you want to document the exemption explicitly.

Retained entities are tenant-scoped by default. They must expose a `TenantId` property (or mark an alternative property with `[RetentionTenant]`) unless the entity is intentionally global and explicitly marked with `[RetentionTenantless]`.

### 2. Wire it up

```csharp
// Register your category repository BEFORE AddCohort
builder.Services.AddSingleton<IRetentionCategoryRepository, MyCategoryRepository>();
builder.Services.AddSingleton(new TenantContext(tenantId, "uk", new Dictionary<string, string>()));
builder.Services.AddCohort<MyDbContext>();
```

Call `ConfigureCohortTables()` in your `OnModelCreating` to add Cohort's infrastructure tables (`retention_holds`, `sweep_run`, etc.) to your migration history:

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    // your entity config...
    modelBuilder.ConfigureCohortTables();
}
```

## Strategies

| Strategy | Behaviour | Entity requirements |
|---|---|---|
| `Purge` | `DELETE` rows past cutoff | `[Retain]`, anchor property, record ID |
| `SoftDelete` | `SET IsDeleted = true` | Above + `bool IsDeleted` property |
| `Anonymise` | Scrub `[Anonymise]`-marked fields | Above + at least one `[Anonymise]` field |
| `Exempt` | Skip entirely | `[ExemptFromRetention]` or no annotation |

If a retained entity is intentionally tenantless, mark it explicitly:

```csharp
[Retain("global-audit", nameof(CreatedAt))]
[RetentionTenantless]
public sealed class GlobalAuditLog
{
    public Guid Id { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Payload { get; set; } = "";
}
```

Without `[RetentionTenantless]`, Cohort treats a missing tenant property on a retained entity as a startup configuration error. Tenant-scoped entities get `AND "TenantId" = @tenantId` in sweep and erasure SQL automatically.

## Upgrade from `0.1.1`

If you are upgrading an existing host, regenerate and apply your Cohort migration before booting `0.2.0`.

Release notes and verification gates for `0.2.0` live in the version-pinned changelog entry:
[CHANGELOG.md](https://github.com/maxanstey-meridian/cohort/blob/v0.2.0/CHANGELOG.md#020). That checklist includes `dotnet pack Cohort/Cohort.csproj` plus clean-consumer restore checks for `AnonymiseWithAttribute`, `PreviewEraseAsync(...)`, and the row-dispatch surface.

## Anonymise methods

```csharp
[Retain("pii", nameof(CreatedAt))]
public sealed class Contact
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }

    [Anonymise(AnonymiseMethod.Null)]
    public string? Email { get; set; }

    [Anonymise(AnonymiseMethod.EmptyString)]
    public string Name { get; set; } = "";

    [Anonymise(AnonymiseMethod.FixedLiteral, "[redacted]")]
    public string Phone { get; set; } = "";
}
```

## Convention overrides

Property names are resolved by convention (`Id`, `TenantId`, `IsDeleted`, `DeletedAt`). Override globally via config, or per-entity with marker attributes.

**Global** — applies to all entities:

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

**Per-entity** — attribute wins over global config:

```csharp
[Retain("orders", nameof(PlacedAt))]
public sealed class Order
{
    [RetentionRecordId]
    public Guid OrderId { get; set; }

    [RetentionTenant]
    public Guid OrganisationId { get; set; }

    public DateTimeOffset PlacedAt { get; set; }
}
```

Available markers: `[RetentionRecordId]`, `[RetentionTenant]`, `[RetentionSoftDelete]`, `[RetentionDeletedAt]`.

Priority: attribute > global config > built-in default.

## Right-to-erasure (Art. 17)

Mark the subject identifier with `[ErasureSubject]`:

```csharp
[Retain("user-data", nameof(CreatedAt))]
public sealed class UserRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }

    [ErasureSubject]
    public Guid UserId { get; set; }
}
```

Then trigger erasure:

```csharp
var result = await erasureService.EraseAsync(tenant, new ErasureScope(userId), DateTimeOffset.UtcNow);
```

Cohort only erases rows that satisfy both conditions:

1. The row matches the requested `[ErasureSubject]`.
2. The row is already past the effective retention cutoff for its category (`max(Period, LegalMin)`).

Active holds still block erasure, and tenant-scoped entities still keep the tenant predicate in the SQL. Cohort does not use right-to-erasure to bypass the retention window.

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
| `Schedule` | `null` | Cron expression (5 or 6 fields). `null` = worker disabled. |
| `DryRun` | `false` | Run sweeps as `SELECT COUNT(*)` instead of `DELETE`/`UPDATE`. Audit events still fire. |
| `KillSwitch` | `false` | Finish current iteration, skip all subsequent ticks. Hot-reloadable. |
| `ApplyMigrations` | `false` | Run `MigrateAsync()` on startup. Cannot combine with `DryRun` or `KillSwitch`. |

## Legal holds

```csharp
await holdsRepo.CreateAsync(new RetentionHoldRequest(
    HoldId: Guid.NewGuid(),
    TableName: "notes",
    RecordId: noteId.ToString(),
    TenantId: tenantId,
    Reason: "Litigation hold — case #12345",
    CreatedAt: DateTimeOffset.UtcNow,
    ExpiresAt: DateTimeOffset.UtcNow.AddYears(1)
));
```

Held records survive all strategies. Holds are checked at SQL level via a `NOT EXISTS` subquery — no per-row C# check.

## Audit trail

Every sweep writes to three tables (created by `ConfigureCohortTables()`):

- `sweep_run` — one row per sweep (timestamps, trigger, dry-run flag, total affected)
- `sweep_run_entity_summary` — per-entity counts (category, strategy, affected, held)
- `sweep_run_row_detail` — per-row detail, opt-in per category (`AuditRowDetail.PerRow` on the rule) or per entity (`[Retain("cat", nameof(Anchor), AuditRowDetail = AuditRowDetail.PerRow)]`). Entity-level wins over category-level.
