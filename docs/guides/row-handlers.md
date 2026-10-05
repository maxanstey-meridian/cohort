# Row Handlers

Deleting a row is often only half the job. There's a blob in storage, a search index entry,
or a downstream system that needs to hear about it. Row handlers let you hook into each
row Cohort mutates.

## Writing a handler

```csharp
public sealed class DeleteAttachmentBlob(BlobContainerClient blobs) : IRetentionHandler<Attachment>
{
    public Task OnBeforeAsync(Attachment row, RetentionBeforeContext ctx, CancellationToken ct)
    {
        ctx.Snapshot["BlobName"] = row.BlobName;
        return Task.CompletedTask;
    }

    public async Task OnAfterAsync(RetentionAfterContext<Attachment> ctx, CancellationToken ct)
    {
        var blobName = (string)ctx.Snapshot["BlobName"]!;
        await blobs.DeleteBlobIfExistsAsync(blobName, cancellationToken: ct);
    }
}
```

```csharp
builder.Services.AddRowHandler<Attachment, DeleteAttachmentBlob>(
    identity: Guid.Parse("0b6f5e43-3d0e-4c2b-9a51-6f7f2a8c1e90"));
```

Both methods are optional. The interface supplies no-op defaults.

- **`OnBeforeAsync`** runs inside the sweep transaction, before the row is mutated. Use it to
  capture what you'll need later into `ctx.Snapshot`. Treat it as capture-only, for two
  reasons: an external side effect here survives a transaction rollback, and it can run for a
  row that's then withheld, for example by a hold created mid-sweep. If it throws, the row is
  left unmutated and the work is dead-lettered.
- **`OnAfterAsync`** runs after commit, from a durable queue, with the snapshot you captured.
  This is where side effects belong.

Give long-lived handlers a stable `identity`. Without one, queued work is keyed by the
handler's CLR type name, and renaming the class dead-letters work still queued under the old
name.

When an entity has several handlers, they run in `[RowHandlerPriority(n)]` order, lowest
first. Handlers without the attribute run last, ordered by type name.

## Delivery

`OnAfterAsync` is delivered **at least once**. Make it idempotent: when `ctx.Attempt` is
greater than 1, the work might already have been done.

The dispatcher polls the queue, runs up to `MaxParallelism` rows at once, and keeps each
row's handlers in order. A failed attempt retries with exponential backoff from
`BaseBackoff`. After `MaxAttempts`, the work is dead-lettered with a sanitized `LastError`.
All of these are [configurable](/reference/configuration).

### Claim timeout

Each delivery holds its claim for `RowHandlerDispatch:ClaimTimeout`, five minutes by default.

- A handler must finish within nine tenths of the timeout. The rest covers claim latency and
  clock skew between hosts. At that point its cancellation token fires, and the delivery
  counts as a failed attempt.
- A handler that ignores its token can overlap its own retry, and the row's next handler may
  start before it finishes. Honour the token.
- Work left `InFlight` by a crash is reclaimed once the timeout passes, and the reclaim counts
  as an attempt.
- A delivery whose claim was reclaimed can't settle the row. Its outcome is discarded and the
  newer delivery settles it.
- If recording a delivery's outcome fails, for example on a transient database error, the
  delivery isn't treated as a handler failure and the rest of the batch carries on. The row
  stays `InFlight` with that attempt spent until its claim times out and is reclaimed.

### Dispatch phases

```csharp
services.AddRowHandler<Attachment, ArchiveAttachment>(RowHandlerDispatchPhase.AfterSweepSettled);
```

`Immediate` (the default) work dispatches as soon as its batch commits. `AfterSweepSettled`
work waits until its whole run records completion or failure. That suits work that should
only happen once the sweep's outcome is final. A run left `Started` by a process crash is
marked failed after `RowHandlerDispatch:SweepSettleTimeout`, which releases its work.

### Draining in tests and jobs

`IRetentionRowDispatcher.FlushAsync()` runs queued work to completion now, without waiting
for the poller. It's handy in tests and one-shot jobs.

## Snapshots and personal data

Snapshots can hold pre-anonymisation personal data, so Cohort keeps them as briefly as it
can:

- A row's `CapturedPayload` is cleared as soon as every handler for that row reaches a
  terminal state.
- A backstop scrub clears anything older than `RowHandlerDispatch:PayloadRetention`, 30 days
  by default. Queued work whose snapshot the backstop scrubbed can never complete, so it
  dead-letters immediately, naming the scrub as the reason, instead of burning its retries.
- Dead-letter `LastError` stores a [sanitized diagnostic](/guides/audit-trail#failure-diagnostics):
  exception type, safe machine code when there is one, and a diagnostic ID. Never exception
  text or stack traces.

### What a snapshot can hold

Snapshot values must round-trip exactly: `OnAfterAsync` receives an equal value of the same
type. Supported values are:

- `null`
- well-known scalars
- enums and value-equal types from the allow-list below
- one-dimensional arrays and `List<T>` of those, with the element type preserved
- `object?[]`, `List<object?>`, and nested `IDictionary<string, object?>`

Anything else fails the handler that stashed it at capture time, before the row is mutated.
It takes the same path as a throwing `OnBeforeAsync`.

Persisted payloads name CLR types, so deserialisation is allow-listed. Values round-trip only
as well-known scalars, property types of the swept entity, or types declared in the entity's
or its registered handlers' assemblies. A tampered payload naming anything else dead-letters
instead of materialising an arbitrary type. The persisted entity type resolves only against
registered retained entities.
