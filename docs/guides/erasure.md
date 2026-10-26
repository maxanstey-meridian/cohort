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
await using var scope = services.CreateAsyncScope();
var result = await scope.ServiceProvider.GetRequiredService<IRetentionErasureService>()
    .EraseAsync(tenant, new ErasureScope("person", personId), ct);
```

The service is scoped, so resolve it from a scope (in a request handler, inject it).

`EraseAsync` is shorthand for `ExecuteAsync(RetentionErasureRequest.Tenanted(tenant, scope))`.
The request is where the other choices go:

```csharp
await using var scope = services.CreateAsyncScope();
var erasure = scope.ServiceProvider.GetRequiredService<IRetentionErasureService>();
var subject = new ErasureScope("person", personId);

await erasure.ExecuteAsync(RetentionErasureRequest.Tenanted(tenant, subject), ct);
await erasure.ExecuteAsync(RetentionErasureRequest.Tenantless(subject), ct);
await erasure.ExecuteAsync(
    RetentionErasureRequest.Tenanted(tenant, subject, transaction: ErasureTransaction.Caller), ct);
```

Cohort erases rows that meet all four conditions:

1. a column marked with the requested kind equals the requested subject
2. the row belongs to the requested tenant (or, for a tenantless request, the entity is
   `[RetentionTenantless]`)
3. the category's resolved strategy is eligible for erasure
4. no active hold covers the row

The erasure runs as of the database's clock, the clock legal holds expire by. You can pass
an earlier time as `at`. A real erasure refuses a later one with an
`ArgumentOutOfRangeException` before any run is recorded; a dry run accepts any time. See
[which "today"](/guides/running-retention#which-today).

Two requests are refused with an `InvalidOperationException` before any run is recorded:

- a kind no retained entity in the request's scope declares, which would otherwise erase
  nothing and look like success. A kind only `[RetentionTenantless]` entities declare is
  refused for a tenanted request, and the other way round.
- a subject that isn't of its kind's CLR type, such as a `string` for a `Guid` kind

### Tenantless entities

A `[RetentionTenantless]` entity, such as a platform-wide login log, holds personal data that
no tenant owns. A tenanted erasure never touches it: the tenant asking would otherwise erase
shared rows, and the run would be attributed to that tenant. Erase it with a tenantless
request:

```csharp
await erasure.ExecuteAsync(
    RetentionErasureRequest.Tenantless(new ErasureScope("user", userId)), ct);
```

- Rules resolve as for the tenantless sweep, under the tenant ID `Guid.Empty`, and the run is
  recorded under `sweep_run.TenantId = Guid.Empty`.
- Tenantless holds (`TenantId` null) protect its rows.
- To erase a user everywhere, make one tenanted request per tenant plus one tenantless
  request, the same shape as the scheduled worker.
- It always runs in its own transaction: `RetentionErasureRequest.Tenantless` takes no
  `transaction` argument, so it can't join yours.

The run's audit row records the kind (`sweep_run.ErasureSubjectKind`, and
`SweepEvent.Started.ErasureSubjectKind` for observers). It never records the subject itself.

## What still blocks erasure

Ordinary `Period` never delays erasure. Legal minimums, holds and Exempt rules do:

- With no non-zero `LegalMin`, Cohort adds no age condition at all. Null and future anchors
  are eligible.
- A non-zero `LegalMin` adds the strict condition `anchor < cutoff`, where the cutoff is the
  start of the day `LegalMin` before today, in the host's `TimeZone` (see
  [periods](/guides/retention-rules#periods-are-calendar-periods)). A row anchored on or
  after that day, or with a null anchor, stays.
- A category that resolves to `Exempt` keeps every one of the subject's rows.
- A row another table still references through a `RESTRICT` or `NO ACTION` foreign key can't
  be purged, and a row any other integrity constraint refuses (a check, a unique index, a
  constraint trigger) can't be changed. Both count as blocked. Delete or re-point the
  referencing rows, then erase again. A full batch in which every row is refused fails the
  entity, since the cause is then the rule or the schema, unless the refusals differ between
  rows or are a foreign key refusing a purge. A foreign key refusing the value an anonymise
  writes is the rule's, so it still fails the entity. A Purge entity with a
  [dependents step](/guides/row-handlers#retiring-a-rows-dependents) tries its referenced
  rows anyway: the step can retire what references them, and a row still referenced after it
  is blocked.
- Active holds always block erasure.

Each `EntitySweepCount` in the result says what the erasure left of the subject, and why. The
counts don't overlap:

| Count | Rows left because |
|---|---|
| `HeldCount` | an active hold covers them |
| `NullAnchorCount` | their anchor is null, so a legal-minimum cutoff never matches them |
| `RetainedCount` | the rule keeps them: younger than the legal minimum, or every row under an `Exempt` rule |
| `SkippedCount` | a row handler's `OnBeforeAsync` or its dependents step failed, so the row was left unchanged |
| `DeferredCount` | they came into scope mid-run behind the rows the run had already passed (a hold ended, a reference went, an anchor moved earlier), so the next run handles them |
| `BlockedCount` | a database constraint stopped Cohort changing them |

`ErasureResult.Complete` reads them for you. It is `true` only when no entity failed and every
one of those counts is zero, so nothing of the subject remains in the requested scope. A dry
run's `Complete` says whether the real run would be, except that it never runs a
[dependents step](/guides/row-handlers#retiring-a-rows-dependents): it predicts that the step
clears a Purge row's references, so it counts those rows as erased, not blocked.

Skipped, deferred and blocked rows also make the run `PartiallyFailed`: each adds a plain
reason to `EntityFailures` and to the run's `Error`, such as
`reason=rows-skipped;entityType=MyApp.Note;count=1` (`rows-missed` for deferred rows,
`rows-blocked` for blocked ones). Held, null-anchor and retained rows are the rules working
as configured, so they leave the run `Succeeded` and only show in the counts and in
`Complete`.

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
any row. `ErasureResult.DryRun` tells you which kind of run it was. It never settles
`PartiallyFailed` for rows it predicts the real run would leave, blocked ones included: read
`Complete` and the counts. It never runs a
[dependents step](/guides/row-handlers#retiring-a-rows-dependents), so a referenced row of a
Purge entity with one counts as erased, on the prediction that its step clears the
references.

## How it runs

An ordinary erasure (`ErasureTransaction.Owned`, the default) uses the same
[execution model](/misc/how-it-works) as sweeps:

- The `Started` audit row commits before any mutation.
- Rows are erased in independently committed batches of `SweepBatchSize`.
- One entity's failure is recorded in `ErasureResult.EntityFailures`, and in the run's
  `PartiallyFailed` status and `Error`. Erasure carries on with the subject's data in the
  remaining entities.
- Held, null-anchor and retained counts are measured directly, in dry runs too. Deferred
  rows are measured once each entity's batches are done, in real runs only.

One difference: a sweep skips rows another transaction has locked, but erasure waits for
them (`FOR UPDATE` rather than `SKIP LOCKED`). An erasure request mustn't silently miss a row
just because something else was holding it at that moment.

## Erasing inside your own transaction

Sometimes the erasure has to be atomic with your own change: lock the person, check they
can be erased, erase them and mark them erased, all or nothing. Pass
`ErasureTransaction.Caller` and the erasure joins the transaction open on your scope's
`DbContext`:

```csharp
await using var transaction = await db.Database.BeginTransactionAsync(ct);
var person = await db.People.FromSql($"SELECT * FROM people WHERE id = {personId} FOR UPDATE")
    .SingleAsync(ct);
person.MarkErased();
await db.SaveChangesAsync(ct);

var result = await erasure.ExecuteAsync(
    RetentionErasureRequest.Tenanted(tenant, new ErasureScope("person", personId),
        transaction: ErasureTransaction.Caller),
    ct);
if (result.EntityFailures.Count > 0)
{
    await transaction.RollbackAsync(ct);
    return;
}

await transaction.CommitAsync(ct);
```

Only a tenanted request can join your transaction; a tenantless one always runs in its own.
Resolve `IRetentionErasureService` from the same scope as that `DbContext`; the service is
scoped for this reason. The erasure needs:

- an open `READ COMMITTED` transaction, begun through EF (`BeginTransactionAsync` or
  `UseTransaction`). Under `REPEATABLE READ` or `SERIALIZABLE` the hold checks would read a
  snapshot from when your transaction began and could miss a hold committed since, so
  Cohort refuses.
- no unsaved changes on the context. Save or discard them first.
- the context built from the container's options, where `AddCohort<TContext>()` adds its
  commit interceptor. A context whose options you built yourself is refused.

What changes compared with an ordinary erasure:

- **Nothing commits on its own.** `Started`, every batch and the final status are written
  through your transaction. Cohort never commits or rolls it back; on any error, including
  cancellation, the exception reaches you and the transaction is yours to roll back. A
  cancellation mid-entity undoes that entity to its savepoint first, so the run still
  settles `Cancelled` in your transaction.
- **One entity's failure still doesn't stop the rest.** Each entity runs under a savepoint,
  so a failure undoes only that entity's changes. It's reported in
  `ErasureResult.EntityFailures` and the run settles `PartiallyFailed`. Check the result
  before you commit.
- **A rollback leaves no ledger row.** If you roll back, including to a savepoint taken
  before the erasure, the attempt leaves no `sweep_run` row, because the ledger is part of
  the same transaction. Nothing it changed was kept either, so the erasure didn't happen.
  Cohort logs the rollback at Information with the sweep ID when it sees it: an explicit
  `RollbackAsync`, or a commit after a rollback to such a savepoint or of an aborted transaction. A transaction disposed
  without a commit or rollback logs nothing, because EF raises no event for it.
- **Locks are held until you commit.** Row and entity locks last until your transaction
  ends, so a concurrent sweep or hold of the same rows waits for you. Keep the transaction
  short.
- **Observers hear about it after the commit.** Audit observers are told of every event of
  the run, `Started` to `Completed`, only once your commit has succeeded, and never if you
  roll back or the commit fails. Commit through EF so Cohort sees it. Once the commit
  returns, Cohort checks on your context's connection, outside any transaction, that the
  run's `sweep_run` row committed. If it didn't (you rolled back to a savepoint taken before
  the erasure, or committed a transaction a failed statement had already aborted), observers
  are told nothing and the rollback is logged at Information. If the check itself fails, for
  example because your session can no longer read `sweep_run`, Cohort logs a Warning that it
  could not confirm the run and tells observers nothing: a missed event is within best
  effort, a report of a run that may not have happened is not. Your commit is never affected.
  The check runs with your connection's session settings, so row-level security must be
  applied when the connection opens, not with `SET LOCAL` (see
  [Tenant Isolation](/guides/tenant-isolation)). `CommitAsync` is the supported path: a
  synchronous `Commit()` blocks while observers are told, each for up to
  `AuditObservers:Timeout` per event.
- **Row handlers run after the commit; a dependents step runs inside it.** `OnBeforeAsync`
  runs inside your transaction, in a read-only savepoint, and a
  [dependents step](/guides/row-handlers#retiring-a-rows-dependents) runs read-write inside
  it, under the row's own savepoint. Neither runs on your `DbContext`: they get their own
  scope and context, on your connection and transaction, and whatever they track is discarded
  after each row. Your context's change tracker is never touched. Their other scoped services
  are fresh instances, not yours (see [row handlers](/guides/row-handlers)).
  `OnAfterAsync` work is queued in the same transaction, so the dispatcher only sees it once
  you commit, and a rollback discards it.
- **Your deferred constraints stay deferred.** Cohort doesn't change your transaction's
  constraint mode, so a change of Cohort's that breaks a deferred constraint fails your
  `COMMIT`, not the row, and is not counted as blocked.
- **Your change tracker isn't refreshed.** The erasure changes rows with SQL, so entities
  your context already tracks keep their old values. Load and change your own rows before
  erasing, or call `ChangeTracker.Clear()` after.
- **Your execution scope is called only for row handlers or a dependents step.** The erasure
  runs in your scope, under whatever tenant isolation you already applied to it. Each batch
  with row handlers or a dependents step gets its own scope, run as the request's tenant, on your already-open connection. See [Tenant Isolation](/guides/tenant-isolation).

A dry run with `Caller` is allowed. It writes its audit rows into your transaction and
changes nothing else.

## Upgrading to 0.11.0

::: warning Breaking change
0.11.0 reshapes the erasure API around a request and the database clock.

- `EraseAsync(tenant, scope, now, ct)` becomes `EraseAsync(tenant, scope, ct)`. Erasure runs
  as of the database clock.
- `IRetentionErasureService` is scoped. Resolve it from a scope or inject it into scoped
  services; a singleton can't inject it.
- Subject columns on `[RetentionTenantless]` entities are erased only by a tenantless
  request, `RetentionErasureRequest.Tenantless(scope)`. A tenanted request no longer reaches
  them, and a kind only tenantless entities declare is refused for it.
- A tenantless request always runs in its own transaction: `RetentionErasureRequest.Tenantless`
  takes no `transaction` argument.
- `RetentionErasureRequest.At` is optional and defaults to the database clock. A real
  erasure refuses a later time before any run is recorded; a dry run accepts any time.
- Every timestamp the run records or reports (`sweep_run`, summaries, row details, observer
  events, `ErasureResult`) comes from the database clock. `DeletedAt`/`AnonymisedAt` markers
  keep the run's time.
- The result says what the erasure left: `EntitySweepCount` gains `RetainedCount`,
  `DeferredCount` and `BlockedCount`, and `ErasureResult.Complete` is true only when nothing
  of the subject remains. See [What still blocks erasure](#what-still-blocks-erasure).
- An erasure that left rows skipped, deferred or blocked settles `PartiallyFailed`, with a
  `rows-skipped`, `rows-missed` or `rows-blocked` reason in `EntityFailures` and `Error`.
- Add the summary-column migration described under
  [Audit Trail](/guides/audit-trail#upgrading-to-0-11-0).
:::

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
