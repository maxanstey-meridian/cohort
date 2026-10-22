# Retention Rules

Cohort splits retention into two halves. **Annotations declare membership**: which entities
are retained, in which category, aged by which column. **Rules declare policy**: how long a
category is kept and what happens to it afterwards. Cohort then executes that policy.

## Categories

```csharp
[Retain("case-contacts", nameof(CreatedAt))]
[RetentionEntityId("b7316df4-7db5-46ad-aea7-f65c4b430f73")]
public sealed class CaseContact { /* ... */ }
```

`[Retain("category", nameof(Anchor))]` says three things:

- this entity takes part in retention
- it belongs to the given category
- its rows are aged by the given anchor column

Several entities can share a category. The annotation never decides whether a row is
purged or anonymised. The rule resolved for the category decides that.

Unannotated entities are implicitly exempt. If you want the exemption visible in code, mark
the entity `[ExemptFromRetention("reason")]`. Marking an entity with both `[Retain]` and
`[ExemptFromRetention]` fails startup.

Rows whose anchor is `NULL` never match a cutoff, so they're kept indefinitely. Prefer
non-nullable anchors for purge categories.

## Entity identity

`[RetentionEntityId]` gives each retained entity a stable UUID. It's durable correlation
metadata, not a display name: holds, audit summaries, row details and handler work all refer
to it, independently of the CLR type name. Never change it when you rename or move the
class. Startup rejects missing, empty, malformed or duplicate identities.

## Rules

Each category resolves through `IRetentionRuleProvider.ResolveAsync` to a `RetentionRule`:

```csharp
new RetentionRule(
    Period: new RetentionPeriod(Years: 1),
    Strategy: Strategy.Anonymise,
    LegalMin: new RetentionPeriod(Months: 6),       // optional
    AuditRowDetail: AuditRowDetail.PerRow,          // optional, default SummaryOnly
    Provenance: new RetentionRuleProvenance(        // optional
        Source: "retention-policy-v4",
        Reason: "Contact data, 12 months"))
```

- `Period` is how long a row is kept after its anchor.
- `Strategy` is what happens to it then.
- `LegalMin` is a legal minimum. Ordinary sweeps use whichever of `Period` and `LegalMin`
  reaches further back from today (see below). [Erasure](/guides/erasure) ignores `Period`
  but still respects a non-zero `LegalMin`.
- `AuditRowDetail` turns on per-row audit records for this rule. See
  [Audit Trail](/guides/audit-trail).
- `Provenance` is copied to the audit summary as `RuleSource` and `RuleReason`.

### Periods are calendar periods

A `RetentionPeriod` is years, months and days, as retention schedules write them:
`new RetentionPeriod(Years: 7)`, `new RetentionPeriod(Months: 18)`,
`new RetentionPeriod(Days: 30)`. Twelve months carry into a year; days never carry into
months, because a month has no fixed length.

Cohort counts a period back from the date it is today in the host's
[`TimeZone`](/reference/configuration#worker-and-sweeps), then cuts off at the start of the
date that lands on. A row anchored on that date or later is kept, so a row is kept at least
through the whole of its anniversary day:

- a row anchored on 1 March 2027 with a 7-year period is kept until the end of 1 March 2034,
  whatever the leap years in between;
- from 31 March, one month back is the last day of February. So a row anchored on the last
  day of a shorter month can stay up to three days past its anniversary: one anchored on
  28 February with a one-month period is kept until the end of 31 March;
- days are counted first, then years and months: the reverse of counting forward from the
  anchor, so a mixed period such as 1 month 28 days never reaches a row before its anniversary.

Where `Period` and `LegalMin` differ, the one that reaches further back from today wins. A
month and 30 days can compare differently on different days of the year.

A period's parts must be non-negative. A negative period would compute a future cutoff and
sweep everything, so `RetentionPeriod` rejects it.

`ResolveAsync` receives the category, the tenant and the logical time. A provider can
therefore apply tenant-, jurisdiction- and time-specific policy. Return `null` when a
category has no rule. Aliasing categories, if you want it, stays inside your provider.

## Capabilities

```csharp
public RetentionCategoryCapabilities? GetCapabilities(string category) => category switch
{
    "case-contacts" => new([Strategy.Anonymise, Strategy.Purge]),
    _ => null,
};
```

`GetCapabilities` synchronously declares **every** strategy `ResolveAsync` could ever return
for that category. It isn't a cache of today's rule. Startup validates the union of declared
strategies against each entity in the category. For example, an entity in a category that
may anonymise must have an `AnonymisedAt` marker. At run time, a rule whose strategy wasn't
declared is rejected.

This keeps a per-tenant provider honest: it can't hide a model requirement behind a rule
that only one tenant ever resolves.

## Strategies

| Strategy | What Cohort does | Typical use |
|---|---|---|
| `Purge` | Deletes rows past the cutoff | Short-lived operational data |
| `SoftDelete` | Sets the soft-delete flag (and `DeletedAt`, when mapped) | Records you want hidden rather than removed |
| `Anonymise` | Scrubs marked columns in place | Data you still need structurally, but not personally |
| `Exempt` | Leaves rows alone | Documented non-retained categories |

Anonymisation has its own page: [Anonymisation](/guides/anonymisation).

## Tenancy

Retained entities are tenant-scoped by default. Every query Cohort runs for a tenant filters
on the entity's tenant column, so one tenant's sweep can't touch another's rows.

- The tenant column is `TenantId` by convention. Mark a different property with
  `[RetentionTenant]`, or rename it globally in [configuration](/reference/configuration).
- An entity that's intentionally global is marked `[RetentionTenantless]`. Declaring that on
  an entity that also exposes a tenant property fails startup, because the tenant property
  would win and the marker would be silently ignored.
- Tenantless entities are swept by tenantless requests (`RetentionSweepRequest.Tenantless`)
  and attributed to `Guid.Empty` in the ledger.

## Upgrading to 0.10.0

::: warning Breaking change
0.10.0 counts periods in calendar years, months and days instead of a `TimeSpan`.

- Replace `TimeSpan.FromDays(n)` in your rule provider with a `RetentionPeriod`. Write
  "7 years" as `new RetentionPeriod(Years: 7)`, not `Days: 2555`: 2,555 days falls short of
  seven years by the leap days in between.
- Periods shorter than a day are gone. A zero period now keeps a row until the end of its
  anchor's day, and every cutoff is the start of a day, so a row can stay up to a day longer
  than under 0.9.
- Set `Cohort:TimeZone` to the zone your retention schedule is written in. It defaults to
  `UTC`. See [configuration](/reference/configuration#worker-and-sweeps).
- Audit observers: `SweepEvent.EntitySummary.ResolvedPeriod` and
  `SweepEvent.EntityProgress.ResolvedPeriod` are now `RetentionPeriod`.
- Add the audit-summary migration described in
  [Audit Trail](/guides/audit-trail#upgrading-to-0-10-0). Readiness refuses to run until it's
  applied.
:::
