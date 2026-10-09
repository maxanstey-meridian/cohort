# Right-to-Erasure

When a subject asks to be forgotten, you don't wait for their rows to age out.
`IRetentionErasureService` erases one subject's rows now, through the same categories,
rules and strategies as an ordinary sweep.

## Marking the subject

Mark each column that identifies a subject with `[ErasureSubject(kind)]`. The kind names
which sort of subject the column holds, such as `"user"` or `"person"`:

```csharp
[Retain("case-notes", nameof(CreatedAt))]
[RetentionEntityId("6b619c19-6e3c-44e8-a87f-975c68fd3988")]
public sealed class CaseNote
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }

    [ErasureSubject("user")]
    public Guid AuthorUserId { get; set; }

    [ErasureSubject("user")]
    public Guid? DelegateUserId { get; set; }

    [ErasureSubject("person")]
    public Guid? ApplicantPersonId { get; set; }
}
```

An erasure names one kind, and only that kind's columns are matched: erasing a person never
touches a row because its user column happens to hold the same value. A row matches when
**any** column of the requested kind equals the subject. An entity with no column of that
kind is skipped.

Startup validates the markers, so bad erasure metadata stops the host from starting rather
than failing the first real request:

- the kind isn't blank
- every column of one kind holds the same CLR type (after nullable unwrapping), across the
  whole model; give each sort of subject its own kind
- each marked property maps to a physical column with a compatible provider type

Kinds compare exactly (ordinal, case-sensitive), and a property carries at most one kind.

## Erasing

```csharp
var result = await services.GetRequiredService<IRetentionErasureService>()
    .EraseAsync(tenant, new ErasureScope("person", personId), DateTimeOffset.UtcNow, ct);
```

Cohort erases rows that meet all four conditions:

1. a column marked with the requested kind equals the requested subject
2. the row belongs to the requested tenant
3. the category's resolved strategy is eligible for erasure
4. no active hold covers the row

Two requests are refused with an `InvalidOperationException` before any run is recorded:

- a kind no retained entity declares, which would otherwise erase nothing and look like
  success
- a subject that isn't of its kind's CLR type, such as a `string` for a `Guid` kind

The run's audit row records the kind (`sweep_run.ErasureSubjectKind`, and
`SweepEvent.Started.ErasureSubjectKind` for observers). It never records the subject itself.

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
new ErasureScope("person", personId, allowSoftDeleteAsErasure: true)
```

## Dry runs

```csharp
new ErasureScope("person", personId, dryRun: true)
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

## Upgrading to 0.9.0

::: warning Breaking change
0.9.0 gives every erasure subject a kind, with no default.

- Re-mark every `[ErasureSubject]` with its kind: `[ErasureSubject("user")]`.
- Pass the kind to `ErasureScope`: `new ErasureScope("user", userId)`.
- Add a migration: `sweep_run` gains a nullable `ErasureSubjectKind` column and the
  `CK_sweep_run_ErasureSubjectKind_Erasure_Only` check. `dotnet ef migrations add` picks both
  up from your model, and readiness refuses to run until they're applied.

A wrong-type subject is now refused before the run starts, instead of being recorded as an
entity failure in a partially failed run.
:::
