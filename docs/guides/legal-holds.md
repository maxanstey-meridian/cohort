# Legal Holds

A hold freezes one row. While it's active, no strategy touches that row: no sweep, no
erasure, and no host deletion that goes through `IRetentionDeletion`.

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
`ListActiveAsync` and `HasActiveHoldAsync`, which report the holds that protect now (see
[When a hold protects](#when-a-hold-protects)). Pass `expiresAt: null` for a hold that lasts
until it's removed.

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

The repository joins the scoped context's current transaction when there is one. An
unparsable record ID is rejected under a savepoint, so your transaction stays usable.

## When a hold protects

Holds are checked in SQL with a `NOT EXISTS` subquery, not with an in-memory pass over rows.

Mutations (sweeps, erasure and `IRetentionDeletion`) judge `CreatedAt`, `ExpiresAt` and
`RemovedAt` against the **database clock** (`statement_timestamp()`), not the operation's
logical `now`. A hold created yesterday protects its row even from a backdated sweep.

- A hold protects from the moment it commits. Its stored `CreatedAt` is the earlier of the
  requested value and the database clock, so an application clock running ahead can't open a
  gap where a new hold doesn't yet protect.
- `RemovedAt` is stored as given. `ExpiresAt` is stored as given too, but must still be in
  the future when the hold is created. The hold stops protecting once the database clock
  passes either.

`ListActiveAsync` and `HasActiveHoldAsync` answer for now, on the same database clock and with
the same rule mutations use, so a report never disagrees with what a sweep would do. They
don't answer for past moments: [history pruning](/guides/history-pruning) removes holds that
stopped protecting long ago, so the table can't answer those reliably.

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

- If any target is held, it returns `Protected` without running the callback.
- If the callback throws, the exception propagates and everything rolls back.

Cohort checks the callback's work before it commits:

- The context must have no unsaved changes when you call `ExecuteAsync`, or it throws
  `InvalidOperationException` before doing anything.
- The callback must save its changes. Returning with unsaved changes throws and rolls back.
- If any row with an active hold disappears inside the transaction, `ExecuteAsync` throws
  and rolls back, naming the hold IDs. That covers rows the callback didn't list, EF
  cascades, database `ON DELETE CASCADE` and raw SQL. A hold whose row was already gone
  before the callback ran doesn't count.
- After a refusal or failure once the callback has run, the context's change tracker is
  cleared, so a later `SaveChanges` can't apply the callback's changes outside the hold check.

This guards deletion only. A callback that anonymises or soft-deletes a held row leaves the
row in place, so Cohort can't see it.

`IRetentionDeletion` is scoped. Resolve it and your `DbContext` from the same DI scope, and
make every change through that context. Cohort can't see writes made through another
context or connection: they commit on their own, outside the transaction. The operation owns
the transaction, so call it when the context has no transaction open.
