# Right-to-Erasure

When a subject asks to be forgotten, you don't wait for their rows to age out.
`IRetentionErasureService` erases one subject's rows now, through the same categories,
rules and strategies as an ordinary sweep.

## Marking the subject

Mark one or more subject identifiers with `[ErasureSubject]`:

```csharp
[Retain("user-data", nameof(CreatedAt))]
[RetentionEntityId("6b619c19-6e3c-44e8-a87f-975c68fd3988")]
public sealed class UserRecord
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }

    [ErasureSubject]
    public Guid UserId { get; set; }

    [ErasureSubject]
    public Guid? DelegateUserId { get; set; }
}
```

A row matches when **any** marked column equals the requested subject. Startup validates the
markers: each must map to a physical column with a compatible effective and provider type.
Bad erasure metadata stops the host from starting, rather than failing the first real
request.

## Erasing

```csharp
var result = await services.GetRequiredService<IRetentionErasureService>()
    .EraseAsync(tenant, new ErasureScope(subjectId), DateTimeOffset.UtcNow, ct);
```

Cohort erases rows that meet all four conditions:

1. a marked subject column equals the requested subject
2. the row belongs to the requested tenant
3. the category's resolved strategy is eligible for erasure
4. no active hold covers the row

## What still blocks erasure

Ordinary `Period` never delays erasure. Legal minimums and holds do:

- With no positive `LegalMin`, Cohort adds no age condition at all. Null and future anchors
  are eligible.
- A positive `LegalMin` adds the strict condition `anchor < now - LegalMin`. A row exactly
  on the boundary, or with a null anchor, stays blocked. Blocked null anchors are reported in
  `NullAnchorCount`.
- Active holds always block erasure.

## Soft delete is not erasure

Erasure refuses categories that resolve to `SoftDelete`. Setting a flag leaves the personal
data in the row, which rarely satisfies an erasure request. If in your case it genuinely
does, opt in explicitly:

```csharp
new ErasureScope(subjectId, allowSoftDeleteAsErasure: true)
```

## Dry runs

```csharp
new ErasureScope(subjectId, dryRun: true)
```

A dry run counts what the erasure would change and writes its audit trail without changing
any row. `ErasureResult.DryRun` tells you which kind of run it was.

## How it runs

Erasure uses the same [execution model](/misc/how-it-works) as sweeps:

- The `Started` audit row commits before any mutation.
- Rows are erased in independently committed batches of `SweepBatchSize`.
- One entity's failure is recorded in `ErasureResult.EntityFailures`, and in the run's
  `PartiallyFailed` status and `Error`. Erasure carries on with the subject's data in the
  remaining entities.
- Held counts are measured directly, in dry runs too.

One difference: a sweep skips rows another transaction has locked, but erasure waits for
them (`FOR UPDATE` rather than `SKIP LOCKED`). An erasure request mustn't silently miss a row
just because something else was holding it at that moment.
