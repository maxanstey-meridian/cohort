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
`ListActiveAsync` and `HasActiveHoldAsync`. Pass `expiresAt: null` for a hold that lasts
until it's removed.

`CreateAsync` validates the request, so a hold can't silently protect nothing:

- `retentionEntityId` must identify a retained entity in the current EF model.
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
- `ExpiresAt` and `RemovedAt` are stored as given. The hold stops protecting once the
  database clock passes them.

`ListActiveAsync` and `HasActiveHoldAsync` are reporting queries. They evaluate the same
columns against the `asOf` you pass. If [history pruning](/guides/history-pruning) is on,
asking about a moment older than `InactiveHoldRetention` can answer "not held" for a hold
that has since been pruned.

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

`IRetentionDeletion` is scoped. Resolve it and your `DbContext` from the same DI scope,
list every row the callback will delete, and save the callback's changes through that
context. The operation owns the transaction, so call it when the context has no transaction
open.
