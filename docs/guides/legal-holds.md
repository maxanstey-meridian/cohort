# Legal Holds

A hold freezes one row, and the rows you declare held with it. While it's active, no strategy
touches those rows: no sweep, no erasure, and no host deletion that goes through
`IRetentionDeletion`. See [Holding related rows](#holding-related-rows) to protect a Case's
links, its Submission, and the Submission's uploads with one hold on the Case.

## Creating a hold

```csharp
await holds.CreateAsync(
    new RetentionHoldRequest(
        holdId: Guid.NewGuid(),
        retentionEntityId: Guid.Parse("a3f467fe-c5d0-4f17-9897-83c373cc1dc8"),
        recordId: noteId.ToString(),
        tenantId: tenantId,
        reason: "Litigation hold - case #12345",
        createdAt: now,
        expiresAt: now.AddYears(1)),
    ct);
```

`holds` is the scoped `IRetentionHoldsRepository`. It also offers `RemoveAsync`,
`ListActiveAsync`, `HasActiveHoldAsync`, `ListCoveringAsync`, and `ListCoveringSubjectAsync`,
which report the holds that protect now (see [When a hold protects](#when-a-hold-protects)
and [Asking what is held](#asking-what-is-held)). Pass `expiresAt: null` for a hold that
lasts until it's removed.

`CreateAsync` validates the request, so a hold can't silently protect nothing:

- `retentionEntityId` must identify a retained entity in the current EF model. It must stay
  that way: readiness refuses to run while an active hold names an identity the model no
  longer declares ([remedy](/reference/schema#changed-entity-identities)).
- `expiresAt`, when set, must be after `createdAt` and later than the database clock. An
  already-passed expiry throws `InvalidOperationException` without aborting a transaction you
  own.
- `recordId` is parsed and canonicalised using the key's PostgreSQL store type. For a Guid
  key, malformed values are rejected. Integer, string and provider-converted keys keep their
  own canonical representation.
- The stored `RecordId` is the row's own canonical text, so `citext`, `numeric` and similar
  keys match what sweeps compare, even when the request spells the ID differently.
- The row must exist. Cohort reads it under `FOR SHARE`, so in a caller's `REPEATABLE READ`
  transaction, a row deleted since the snapshot raises a serialization failure instead of
  creating a hold over nothing.
- For tenant-scoped entities, the row's tenant must match `tenantId`. Sweeps only honour holds
  whose tenant matches the row's, so a mis-scoped hold would protect nothing. Tenantless
  entities require a null `tenantId`.
- Cohort takes the same entity, tenant and record advisory lock that mutation uses before
  checking existence and inserting, so a concurrent sweep can't slip past a hold between
  validation and creation.
- When the held entity covers related rows (see [Holding related rows](#holding-related-rows)),
  Cohort also locks each of those rows `FOR SHARE` after it validates the target and before it
  inserts the hold. See [The creation lock](#the-creation-lock).

The repository joins the scoped context's current transaction when there is one. An
unparsable record ID is rejected under a savepoint, so your transaction stays usable. A hold
on an entity that covers related rows is refused in a caller transaction at REPEATABLE READ or
SERIALIZABLE ([why](#the-creation-lock)).

## When a hold protects

A row is protected while an active hold **covers** it: a hold names the row itself, or the
row is linked by a `[HeldWith]` or `[HeldWithReferencing]` relation to a row a hold covers,
through any chain of such relations, all within one tenant. Protected rows are never purged,
soft-deleted, or anonymised by a sweep or erasure, are counted as `HeldCount` and keep an
erasure from being `Complete`, are refused by `IRetentionDeletion`, and are guarded against a
[dependents step](/guides/row-handlers#retiring-a-rows-dependents). Creating a hold locks every
row it will cover, so a sweep or erasure either finishes with a row before the hold commits or
sees the hold. An `IRetentionDeletion` callback fails and rolls back if any hold on a covering
entity appears while it runs (fail-safe, as the dependents step already is). Concurrent hold
creation and erasure or deletion can deadlock across entities; PostgreSQL aborts one side with
SQLSTATE `40P01`, which is retryable, and no hold is ever left over a removed row.

Holds are checked in SQL with a `NOT EXISTS` subquery, not with an in-memory pass over rows.
Previews and dry runs count covered rows the same way, so they report what the real run will.

Mutations (sweeps, erasure and `IRetentionDeletion`) judge `CreatedAt`, `ExpiresAt` and
`RemovedAt` against the **database clock** (`statement_timestamp()`), not the operation's
logical `now`. A hold created yesterday protects its row even from a backdated sweep.

- A hold protects from the moment it commits. Its stored `CreatedAt` is the earlier of the
  requested value and the database clock, so an application clock running ahead can't open a
  gap where a new hold doesn't yet protect.
- `RemovedAt` is stored as given. `ExpiresAt` is stored as given too, but must still be in
  the future when the hold is created. The hold stops protecting once the database clock
  passes either.

`ListActiveAsync`, `HasActiveHoldAsync`, `ListCoveringAsync`, and `ListCoveringSubjectAsync`
answer for now, on the same database clock and with the same rule mutations use, so a report
never disagrees with what a sweep would do. They don't answer for past moments:
[history pruning](/guides/history-pruning) removes holds that stopped protecting long ago, so
the table can't answer those reliably.

## Holding related rows

A hold on a Case should protect its links, the Submission it came from, and that Submission's
uploads, without a hold on each. Declare the relations on the entities whose rows become held,
with two class-level attributes:

```csharp
[Retain("case-links", nameof(CreatedAt))]
[RetentionEntityId("0c1d0b4e-6d3a-4f33-9a53-6f1d8c0a7e21")]
[HeldWith(typeof(Case), nameof(CaseId))]
public sealed class CaseLink
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public Guid? CaseId { get; set; }
}

[Retain("submissions", nameof(CreatedAt))]
[RetentionEntityId("5f7a2d9c-3b8e-4c1a-8d64-2e9b7f0a4c13")]
[HeldWithReferencing(typeof(Case), nameof(Case.SourceSubmissionId))]
public sealed class Submission
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}

[Retain("uploads", nameof(CreatedAt))]
[RetentionEntityId("a8e4c6b2-1d5f-4e07-b3a9-7c2f5d8e1b64")]
[HeldWith(typeof(Submission), nameof(SubmissionId))]
public sealed class Upload
{
    public Guid Id { get; set; }
    public Guid TenantId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public Guid SubmissionId { get; set; }
}
```

- **`[HeldWith(typeof(Case), nameof(CaseId))]`** is for a member of this entity that names the
  related row: a link's `CaseId`, an upload's `SubmissionId`.
- **`[HeldWithReferencing(typeof(Case), nameof(Case.SourceSubmissionId))]`** is for a member of
  the related entity that names this one: a Case's `SourceSubmissionId` names the Submission
  it came from. Several Cases can share one Submission, and it stays held while any of them is.
- Both attributes go on the entity whose rows become held, so everything that protects an
  entity reads on that entity. Each can be repeated for several relations.
- The link always names the record ID of the row it points at (`Id`, or the property
  `[RetentionRecordId]` marks), because that's what a hold stores. You can't declare a link to
  an alternate key. The link must have the same type as that record ID and a
  compatible collation (see [Startup validation](/reference/startup-validation#rules)).
- No database foreign key is needed, so a link with a composite key, or none at all, works.
  Cohort matches the link column to the record ID in its own SQL.
- A `NULL` link covers nothing: a link row with no `CaseId` is swept as usual.
- Coverage flows one way, from the related row to the row that declares it. A hold on an
  upload doesn't cover its Submission, and a hold on the Submission doesn't cover the Cases
  that reference it.
- Relations chain. With the three above, a hold on a Case covers its links, the Submission its
  `SourceSubmissionId` names, and that Submission's uploads. A relation that leads back to
  an entity already on the chain, including from an entity to itself, is a cycle and is
  refused at startup.
- Every hop stays in one tenant: the related row must have the same tenant as the row it
  covers, and the hold's tenant must be the tenant of the row it names. A hold in another
  tenant never protects a row, even when a link names it. Both ends of a relation are tenanted
  or both are `[RetentionTenantless]`.

A bad declaration stops the host from starting, naming the declaring type, the attribute, and
the member. See [Startup Validation](/reference/startup-validation#hold-relations).

A covered row is judged exactly as if it carried its own hold: by sweeps, erasure, previews,
dry runs, `IRetentionDeletion`, and the dependents step.

### Asking what is held

`HasActiveHoldAsync` judges coverage, as the sweep does. Two reads say which holds protect:

```csharp
// The holds covering one row, directly or through relations.
var covering = await holds.ListCoveringAsync(uploadEntityId, upload.Id.ToString(), tenantId, ct);

// The holds covering any row of one erasure subject.
var held = await holds.ListCoveringSubjectAsync("person", personId, tenantId, ct);
```

- `ListCoveringAsync` returns each active hold that covers the row once, ordered like
  `ListActiveAsync`. A page can show "held with Case CB-123" from the hold's
  `RetentionEntityId` and `RecordId`. A hold on the row's own ID counts even when the row is
  gone, as it does for `HasActiveHoldAsync`. `ListActiveAsync` stays direct-only: it lists
  holds, not rows.
- `ListCoveringSubjectAsync` returns the holds covering any row whose `[ErasureSubject(kind)]`
  column equals the subject (see [Right-to-Erasure](/guides/erasure)). A `null` `tenantId`
  reads the `[RetentionTenantless]` entities, as `RetentionErasureRequest.Tenantless` does,
  and `Guid.Empty` is refused. It ignores age, legal minimum, and strategy, so it answers "is
  any of this subject's data held?", not "would an erasure report `HeldCount > 0`": a Person
  on a held Case is held whether or not the data is due yet. That's the question to ask before
  an erase or a merge.
- `ListCoveringSubjectAsync` is one read statement: no run, no ledger row, no observer events,
  and no rule-provider call. Like an erasure, it refuses a kind no retained entity declares and
  a subject of the wrong type, before reading anything.

### The creation lock

`CreateAsync` row-locks, `FOR SHARE`, every row the new hold will cover, after it validates the
target row and before it inserts the hold. The walk takes one statement per covered entity.
Each statement locks every row of that entity the hold will cover, through all the relation
paths that end at the entity (so two relations reaching one entity, as in a diamond, are
locked together), in the entity's sweep order (anchor, then record ID), the order a sweep or
erasure locks a batch in. So the two can't deadlock within one entity. The entities run in an
order where every entity on a path to another comes first, so a row is locked before a later
statement reads through it. A hold can't race a sweep or erasure into removing a row it
covers:

- If a sweep or erasure had already locked a row to change it, hold creation waits for it, and
  the hold covers what is left.
- If hold creation locked the row first, an erasure waits and then sees the hold: it reports the
  row as held and leaves it unchanged. A sweep skips the locked row. It reports the row as held
  if the hold has committed by the time the sweep counts, or as deferred otherwise, and the
  next run sees the hold. A row is never in both counts.
- `IRetentionDeletion` takes no row locks, so a callback that deletes a covered row waits for
  the hold to commit and then fails ([below](#deleting-around-holds)).

Because the lock walk must read the current rows, `CreateAsync` refuses a caller transaction at
`REPEATABLE READ` or `SERIALIZABLE` when the held entity covers related rows (under either, a
row linked after the snapshot wouldn't be locked). `READ UNCOMMITTED` is accepted, because
PostgreSQL runs it as `READ COMMITTED`. The refusal is an `InvalidOperationException`, thrown
before anything changes, saying the hold "needs a READ COMMITTED transaction". A hold on an
entity that covers nothing accepts any isolation, as before.

Deadlocks across entities remain possible. Hold creation holds the target's advisory lock and
then row-locks covered rows entity by entity, while an erasure takes locks entity by entity in
its own plan order, and an `IRetentionDeletion` callback can delete several covered rows in any
order. Either can deadlock with the walk. PostgreSQL detects it and aborts one side with
SQLSTATE `40P01`, which is retryable, and no hold is created over a removed row. What the
aborted side loses is what any error costs it:

- **An erasure** records the entity as failed and rolls it back to its savepoint (in a
  caller transaction) or loses that batch (in its own run). The run settles `PartiallyFailed`,
  and earlier entities and batches stand.
- **Hold creation in a caller transaction** aborts the whole caller transaction, because
  `CreateAsync` uses no savepoint. Retry the transaction.
- **A deletion** rolls back.

Cohort adds no lock ordering across entities: a holder's covered rows and an erasure's plan
order can't share one order.

### Writing links

Cohort judges a link when its own statement runs, and doesn't lock holder rows against your
writes. If your code sets a link (say, a record's `HolderPersonId` to a held Person) while a
sweep is mutating a row further down that chain (one of that record's audit events), the
mutation can commit just after the link made its row covered. The outcome always matches one
valid order: the mutation first, then the link. A link written on the row Cohort is mutating
waits on Cohort's row lock, so that case is safe.

### Removing a declaration

A row is held because a declaration says so. Remove a `[HeldWith]` or `[HeldWithReferencing]`
declaration and the rows it covered are no longer held, even though the hold on the related
row is still active. That's the intended behaviour: the next sweep or erasure treats them as
unheld.

### What it costs

The held-row guard runs for a [dependents step](/guides/row-handlers#retiring-a-rows-dependents)
and for `IRetentionDeletion`. For a step it costs two reads per row of the step's entity, and
each read scales with the rows active holds cover that the scope can see (one tenant's under
row-level security, every tenant's without it), not with table size. Measured on PostgreSQL 16
with default configuration, one read takes about 17 ms for 10,000 covered rows, 175 ms for
100,000, and 2 s for 1,000,000. So holds covering about 100,000 rows slow a sweep of a
dependents-step entity by minutes per 1,000 rows, and about 1,000,000 by an hour. Uncovered
rows cost nothing, even in a table of a million. `IRetentionDeletion` pays one pair of reads
per call: about 35 ms, 350 ms, and 4 s at those sizes.

## Deleting around holds

Your own code deletes retained rows too. `IRetentionDeletion` lets those deletes respect
holds atomically:

```csharp
var outcome = await deletion.ExecuteAsync(
    [new RetentionTarget(Guid.Parse("a3f467fe-c5d0-4f17-9897-83c373cc1dc8"), note.Id.ToString(), tenantId)],
    async ct =>
    {
        db.SessionNotes.Remove(note);
        await db.SaveChangesAsync(ct);
    },
    ct);

if (outcome == RetentionDeletionOutcome.Protected)
{
    // A hold covers at least one target; nothing was deleted.
}
```

Cohort canonicalises the record IDs and takes the same transaction-scoped advisory locks
that hold creation and retention mutation use. It checks every target for an active hold,
then runs your callback inside the same EF and Postgres transaction.

- If any target is held, it returns `Protected` without running the callback. A target is held
  when a hold names it or when a hold covers it through a
  [relation](#holding-related-rows): an upload whose Case is held is `Protected`.
- If the callback throws, the exception propagates and everything rolls back.

Cohort checks the callback's work before it commits:

- The context must have no unsaved changes when you call `ExecuteAsync`, or it throws
  `InvalidOperationException` before doing anything.
- The callback must save its changes. Returning with unsaved changes throws and rolls back.
- If any row an active hold covers disappears inside the transaction, `ExecuteAsync` throws
  and rolls back, naming the hold IDs. A covered row is one the hold names or reaches through
  a [relation](#holding-related-rows), so deleting a held Case's upload fails the same way. That
  covers rows the callback didn't list, EF cascades, database `ON DELETE CASCADE` and raw SQL,
  and a row whose hold the callback ends before deleting it. A hold whose row was already gone
  before the callback ran doesn't count, and neither does a hold that merely expires or is
  released while the callback runs.
- If a hold appears while the callback runs, on an entity that covers related rows (one that a
  `[HeldWith]` or `[HeldWithReferencing]` declaration names as its related type),
  `ExecuteAsync` throws and rolls back, naming the hold ID, whatever the callback touched. Hold
  creation locks the rows it covers, so a callback that deletes one waits for the hold to
  commit and then runs, and the guard can no longer reach the deleted row from the new hold.
  The rule is fail-safe, not exact: a covering hold created anywhere the scope can see while the callback runs fails the
  deletion, even one that covers nothing the callback touched. A retry that lists a covered
  row sees the hold up front and returns `Protected`. Holds on entities that cover nothing keep
  the exact rule above.
- After a refusal or failure once the callback has run, the context's change tracker is
  cleared, so a later `SaveChanges` can't apply the callback's changes outside the hold check.
- On a context with a retrying execution strategy, a transient failure re-runs the whole
  deletion, callback included, with an empty change tracker.

This guards deletion only. A callback that anonymises or soft-deletes a held row leaves the
row in place, so Cohort can't see it.

`IRetentionDeletion` is scoped. Resolve it and your `DbContext` from the same DI scope, and
make every change through that context. Cohort can't see writes made through another
context or connection: they commit on their own, outside the transaction. The operation owns
the transaction, so call it when the context has no transaction open.

## Upgrading to 0.12.0

::: warning Breaking change
0.12.0 lets a hold cover related rows and judges those rows wherever it judges holds. The hold
change itself needs no schema change or migration; 0.12.0 as a whole does: see
[Dependents and After-Commit Work](/guides/row-handlers#upgrading-from-row-handlers).

- `IRetentionHoldsRepository` gains `ListCoveringAsync` and `ListCoveringSubjectAsync`. A host
  that implements the interface (a test double, a decorator) must add both. A host that only
  calls it needs nothing.
- Nothing is covered until you declare a relation with `[HeldWith]` or
  `[HeldWithReferencing]` ([Holding related rows](#holding-related-rows)). Once you do, rows
  that were unheld are held: sweeps leave them and count them in `HeldCount`, an erasure that
  reaches them isn't `Complete`, `IRetentionDeletion` returns `Protected` for them, a
  dependents step that changes one is skipped, and `HasActiveHoldAsync` now answers `true` for
  them. Check code that treats `false` as permission to delete, merge, or erase.
- A bad declaration stops the host from starting, and readiness checks each link column in the
  real table ([Startup Validation](/reference/startup-validation#hold-relations)). Fix the
  message it gives, or remove the declaration.
- Creating a hold on an entity that covers related rows now refuses a caller transaction at
  REPEATABLE READ or SERIALIZABLE, and can deadlock with an erasure or a deletion; PostgreSQL
  aborts one side with `40P01`, which you can retry ([the creation lock](#the-creation-lock)).
- `IRetentionDeletion` fails and rolls back when a hold on an entity that covers related rows
  appears while its callback runs, even if the callback touched none of the covered rows.
- A dependents step now fails on any change to a covered row, and on a row it inserts or links
  under a held row (a new covered row counts, as a new hold already did). Its guard costs more
  as active holds cover more rows ([what it costs](#what-it-costs)).
- Removing a relation declaration releases the rows it covered.
:::

## Upgrading to 0.11.0

::: warning Breaking change
0.11.0 judges holds on the database clock and checks what a deletion actually removed.

- `ListActiveAsync` and `HasActiveHoldAsync` lose `asOf`: they answer for now on the database
  clock. Drop the argument.
- `RetentionHoldRequest` refuses an `expiresAt` that is not after `createdAt`, and
  `CreateAsync` refuses one the database clock has already reached. Seed past holds in tests
  with SQL instead.
- `IRetentionDeletion` refuses a context with unsaved changes, a callback that returns without
  saving, and a callback that deletes any held row, listed or not, even one whose hold it
  ends first. Save or discard pending
  changes before calling it, and save inside the callback.
:::
