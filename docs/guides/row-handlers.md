# Dependents and After-Commit Work

Retiring a row is often only half the job. There are child rows a foreign key ties to it,
copies of its values in an audit table, a blob in storage, or a downstream system that needs to
hear about it. An entity's dependents step handles all of them, in the row's own transaction:
it retires what lives in your database directly, and for anything outside it, writes a message
to your own outbox, which commits with the row and is delivered after the commit.

## Retiring a row's dependents {#retiring-a-rows-dependents}

Some rows have dependents that must go with them, in the same transaction: child rows a
foreign key ties to the row, or copies of its values in an audit table. A dependents step
retires them.

```csharp
public sealed class RetireCaseDependents(AppDbContext db) : IRetentionDependents<Case>
{
    public async Task RetireAsync(Case row, RetentionDependentsContext ctx, CancellationToken ct)
    {
        await db.ExportDeliveries.Where(delivery => delivery.CaseId == row.Id).ExecuteDeleteAsync(ct);
        await db.CaseAuditEvents
            .Where(audit => audit.CaseId == row.Id)
            .ExecuteUpdateAsync(set => set.SetProperty(audit => audit.FieldChanges, (string?)null), ct);
    }
}
```

```csharp
builder.Services.AddRetentionDependents<Case, RetireCaseDependents>();
```

`ctx` carries the run's `SweepId`, the row's `Category`, the `Strategy` about to change the
row, the run's `TenantId`, and `At`, the database time the batch stamps its rows with.

- **One step per entity.** `AddRetentionDependents` refuses a second class for an entity, so
  compose several kinds of dependents in one class. It's the only way to register a step:
  startup refuses an `IRetentionDependents<T>` service in any other shape (a singleton,
  transient, instance, factory, keyed or open-generic service, one registered twice, or one
  whose registration was removed or replaced after `AddRetentionDependents` added it), and
  names the step. These checks read the `IServiceCollection`, so register steps with
  `AddRetentionDependents` even when a third-party container builds the provider. Calling it
  again for the same entity and step with a different `retiresReferencingRows` throws.
- **Order.** For each row the step runs first, then Cohort's own change to the row. The step
  is resolved from a DI scope Cohort creates for each batch, run as the batch's tenant, and runs
  read-write on that scope's `DbContext`, on the run's connection and transaction. Its other
  scoped services come from that scope too: they're fresh instances, not the ones in the scope
  that started the run, so per-request state such as a scoped "current user" isn't there.
  State your [execution scope](/guides/tenant-isolation) sets up, or an `AsyncLocal` flow,
  still is. Write only through the injected context, never another context or connection:
  those write outside the batch's transaction, so their writes commit even when the row is
  blocked or skipped, the hold check can't see them, and a delete of a row that references the
  swept row waits forever on the sweep's own lock on it. Whatever the step leaves tracked is
  discarded after each row. An entity with a step is always changed row by row, never in one
  bulk statement.
- **One savepoint.** The step and the row's change share a savepoint. If a constraint then
  refuses the change, for example because the row is still referenced, the rollback undoes
  the step too and the row counts as blocked (`BlockedCount`), so dependents never go without
  their row. The next run tries the row, and its step, again.
- **Failures skip the row.** Its writes are rolled back, and the row counts in `SkippedCount`
  (`rows-skipped`, so the run settles `PartiallyFailed`). Cohort records the skip once, in a
  `sweep_run_row_detail` row whose `Failure` says why, whatever the entity's row-detail audit
  level, and logs an Error. `Failure` holds a [sanitized diagnostic](/guides/audit-trail#failure-diagnostics)
  for an exception (its type, a safe machine code when there is one, and a diagnostic ID that
  matches the logged Error), or `reason=…` for a failure Cohort found itself. It never holds
  exception text. Audit observers aren't told about a skipped row: they see mutated rows only.
  The step fails when it throws, including a database error or a constraint its own
  statements hit; when it returns with unsaved changes on its context
  (`reason=dependents-unsaved-changes`); when it changes a held row
  (`reason=dependents-changed-held-row`, below); or when it takes its own row out of the run's
  scope, so Cohort's change finds nothing (`reason=dependents-changed-row`). A cancellation the
  step raises itself, such as an `HttpClient` timeout, is a failure like any other; only the
  run's own token stops the run. The next run tries the row again.
- **Holds.** A held row runs no step. Any change to, or removal of, a row under an active hold
  fails the step, whatever the row's entity, even if the step ends the hold first. A row is
  under a hold when the hold names it or covers it through a
  [hold relation](/guides/legal-holds#holding-related-rows), so updating a held Case's upload
  fails the step. An `UPDATE` that writes the same value counts as a change; a hold that merely
  expires or is released while the step runs doesn't fail it. Cohort compares every visible
  held row's physical version before and after the step, covered rows included, so the check
  is fail-safe rather than exact: a hold created anywhere the batch can see while the step
  runs, a row that becomes covered while it runs (the step, or another session, inserts or
  links a row under a held row), or another session's committed update to a held row, also
  fails it, and the next run retries the row. A step that cuts a covered row's link and then
  changes the row still fails: Cohort compares the row it saw before. A Warning names the
  holds' ids, never the rows. Dependents' legal minimums aren't enforced; a hold is the
  row-specific protection the step honours, as
  [`IRetentionDeletion`](/guides/legal-holds#deleting-around-holds) does. When the model has a
  tenantless retained entity, a tenant's batch must be able to see the active `NULL`-tenant
  holds, or the entity fails rather than run a step that can't check them (see
  [Tenant Isolation](/guides/tenant-isolation)).
- **What the hold check costs.** Cohort reads the held rows' versions twice per row of an
  entity with a step, and each read scales with the rows active holds cover that the batch can
  see, not with the table's size. One read takes about 17 ms at 10,000 covered rows, 175 ms at
  100,000, and 2 s at 1,000,000 (PostgreSQL 16, default configuration). So holds covering about
  100,000 rows slow a sweep of a step's entity by minutes per 1,000 rows, and about 1,000,000
  by an hour; uncovered rows cost nothing. See
  [what it costs](/guides/legal-holds#what-it-costs).
- **Foreign keys and previews.** Whether a purge tries a row that's still referenced depends on
  what the step retires: see [Rows that reference the entity](#rows-that-reference-the-entity).
- **Caller-transaction erasures.** In an erasure that joins your transaction, the step runs
  inside it, on its own context, never yours, so nothing the step tracks reaches your context.
  Its reads see what you saved but haven't committed. Cohort doesn't set `SET CONSTRAINTS ALL
  IMMEDIATE` in your transaction, so a deferred constraint the step violates fails your
  `COMMIT`, as one Cohort's own change violates does. In Cohort's own batches constraints are
  immediate, so the same violation fails the step's statement (a skip) or the row's change (a
  block).
- **Locks.** The step runs while the batch holds its row and entity locks, so a slow step
  makes hold creation on those rows, or on a row they're held with, wait. A deadlock or lock
  timeout in the step fails that row's step, not the batch.

## Rows that reference the entity {#rows-that-reference-the-entity}

A purge normally leaves out, up front, rows still referenced through a `RESTRICT` or
`NO ACTION` foreign key, and counts them blocked. What it does for an entity with a step
depends on `retiresReferencingRows`:

- **`true`, the default**, says the step retires the rows that reference the entity, as
  `RetireCaseDependents` above does. Referenced rows are tried: the step runs, and a row still
  referenced after it is blocked by the constraint itself. Previews and dry runs, a dry-run
  erasure's `Complete` included, never run the step, so they predict that it clears the
  references: referenced rows count as eligible, not blocked. A row whose references the step
  can't clear runs the step once per run, rolled back each time.
- **`false`** says it doesn't, for a step that only writes to an outbox, say. Purge keeps the
  up-front filter for that entity: a still-referenced row is left out and counted blocked, and
  its step never runs. Previews and dry runs count it blocked too.

```csharp
builder.Services.AddRetentionDependents<Note, NoteOutboxStep>(retiresReferencingRows: false);
```

Pass `false` whenever the step leaves the referencing rows alone. With the default, every due
row that's still referenced would run the step and fail at the database, every run.

### Rows deleted by `ON DELETE CASCADE`

A row that PostgreSQL deletes through an `ON DELETE CASCADE` foreign key gets no hook: no
step runs for it, and nothing reaches your outbox. When a child row owns anything outside the
database (a blob, a record in another system), set its foreign key to `Restrict` and retire
the child in the parent's dependents step, which can then write the child's message too.

## After-commit work through your outbox {#after-commit-work-through-your-outbox}

Work outside the database (delete a blob, tell another system) mustn't run inside the batch:
it would survive a rollback, and could run for a row that's then blocked, skipped or held.
Cohort keeps no work queue for it. Instead, the step writes a message to your own outbox
through the `DbContext` it's handed. The message commits with the row's change and rolls back
with it, and your outbox delivers it once the transaction has committed.

Cohort's sample does this with a plain outbox table. The step, `Cohort.Sample/NoteOutboxStep.cs`:

```csharp
public sealed class NoteOutboxStep(SampleDbContext db) : IRetentionDependents<Note>
{
    public async Task RetireAsync(Note row, RetentionDependentsContext ctx, CancellationToken ct)
    {
        // A message saved outside Cohort's transaction would commit on its own, even when the
        // note it announces is never retired.
        if (db.Database.CurrentTransaction is null)
        {
            throw new InvalidOperationException(
                "NoteOutboxStep writes the outbox only inside the transaction that retires the note."
            );
        }

        db.OutboxMessages.Add(
            new OutboxMessage
            {
                Id = Guid.NewGuid(),
                TenantId = ctx.TenantId,
                Type = "note-retired",
                Payload = row.Id.ToString(),
                CreatedAt = ctx.At,
            }
        );
        await db.SaveChangesAsync(ct);
    }
}
```

The outbox table's mapping, in `SampleDbContext.OnModelCreating`:

```csharp
modelBuilder.Entity<OutboxMessage>(b =>
{
    b.ToTable("outbox_messages");
    b.HasKey(message => message.Id);
    b.Property(message => message.TenantId).IsRequired();
    b.Property(message => message.Type).IsRequired();
    b.Property(message => message.Payload).IsRequired();
    b.Property(message => message.CreatedAt).IsRequired();
});
```

And its registration, with `retiresReferencingRows: false` because the step retires nothing
that references a note (see [Rows that reference the entity](#rows-that-reference-the-entity)):

```csharp
builder.Services.AddRetentionDependents<Note, NoteOutboxStep>(retiresReferencingRows: false);
```

- **The step checks `CurrentTransaction` first.** When Cohort runs the step, its context is
  always in the batch's transaction (or yours, in a caller-transaction erasure). The check
  makes that a precondition rather than an assumption: if the context ever had no
  transaction, `SaveChangesAsync` would commit the message on its own, so the outbox would
  announce a row that was never retired. The step throws instead, which skips the row and
  records why.
- **The step saves.** `SaveChangesAsync` writes inside Cohort's transaction and commits
  nothing; a step that returns with the message still unsaved fails with
  `reason=dependents-unsaved-changes`.
- **The message follows the row.** A row that's purged commits its message. A row a
  constraint refuses, a row whose step fails, and every row of a batch that rolls back leave
  no message. A held row runs no step, so it writes none. In a caller-transaction erasure the
  message commits only if you commit.
- **Delivery is your outbox's.** Whatever relays the outbox (a poller, a library's sweeper)
  runs after the commit, at least once, so make what it calls idempotent. The outbox table is
  yours: leave it unretained, or give it a retention of its own.

### Brighter

[Brighter](https://github.com/BrighterCommand/Brighter)'s outbox fits, given one
requirement: the outbox write must join the context's current transaction, through a
transaction provider that resolves the same scoped context Cohort hands the step. Brighter's
`PostgreSqlEntityFrameworkTransactionProvider<TContext>` takes the context through its
constructor, so the step injects both the context and the provider from the batch's scope.

Cohort neither compiles nor tests this snippet; `AppDbContext`, `Attachment`, and
`DeleteAttachmentBlob` are placeholders. The producer configuration:

```csharp
brighter.AddProducers(producerOptions =>
{
    producerOptions.ProducerRegistry = producerRegistry;
    producerOptions.Outbox = new PostgreSqlOutbox(databaseConfiguration);
    producerOptions.TransactionProvider =
        typeof(PostgreSqlEntityFrameworkTransactionProvider<AppDbContext>);
    producerOptions.ConnectionProvider = typeof(PostgreSqlConnectionProvider);
});
```

The step deposits the message after the same `CurrentTransaction` check:

```csharp
public sealed class DeleteAttachmentBlob(Guid id, string blobName) : Command(id)
{
    public string BlobName { get; } = blobName;
}

public sealed class AttachmentOutboxStep(
    AppDbContext db,
    IAmACommandProcessor commandProcessor,
    IAmABoxTransactionProvider<DbTransaction> transactionProvider
) : IRetentionDependents<Attachment>
{
    public async Task RetireAsync(Attachment row, RetentionDependentsContext ctx, CancellationToken ct)
    {
        if (db.Database.CurrentTransaction is null)
        {
            throw new InvalidOperationException(
                "AttachmentOutboxStep deposits only inside the transaction that retires the attachment."
            );
        }

        await commandProcessor.DepositPostAsync(
            new DeleteAttachmentBlob(Guid.NewGuid(), row.BlobName),
            transactionProvider,
            cancellationToken: ct
        );
    }
}
```

```csharp
builder.Services.AddRetentionDependents<Attachment, AttachmentOutboxStep>(retiresReferencingRows: false);
```

Brighter then publishes the deposited message after the commit, and the handler that deletes
the blob runs wherever your consumers run.

## Recovering abandoned runs {#recovering-abandoned-runs}

A run left `Started` by a crashed process is settled as `Failed` once it's older than
`Cohort:SweepSettleTimeout` (an hour by default) and no process still holds its run lock. A
background service does this when the host starts and then once a minute, and skips while the
kill switch is on. Until a run is recovered it reads as in progress, and history pruning never
deletes it.

A host that doesn't run hosted services (a console job, a function that never starts
`IHost`) calls the port itself, from a scope, where it would have run the service:

```csharp
await scope.ServiceProvider.GetRequiredService<IRetentionRunRecovery>().RecoverAsync(ct);
```

`RecoverAsync` recovers every abandoned run now. It ignores the kill switch, runs as
`System(Recovery)` (see [Tenant Isolation](/guides/tenant-isolation)), and checks readiness
first, as every Cohort operation does. See [Crash recovery](/misc/how-it-works#crash-recovery).

## Upgrading from row handlers {#upgrading-from-row-handlers}

0.12.0 removes row handlers (`IRetentionHandler<T>`, `AddRowHandler`, `[RowHandlerPriority]`,
`[RowHandlerId]`), the dispatcher (`IRetentionRowDispatcher`, `FlushAsync`), the
`Cohort:RowHandlerDispatch` options, the `sweep_row_handler_status` table, and the
`sweep_run_row_detail.CapturedPayload` column. From 0.11.0:

1. On 0.11.0, drain queued handler work (`FlushAsync` until nothing is pending), and requeue
   or export dead-lettered work you still need. Whatever is left is dropped with the table.
2. Move each handler to the entity's dependents step, depositing to your outbox
   ([above](#after-commit-work-through-your-outbox)), and remove its `[RowHandlerId]` (a step
   carries none now). Pass `retiresReferencingRows: false` when the step only deposits. A step
   differs from a handler:
   - an entity has one step, so several handlers, or a handler and a step, compose into one
     class;
   - each row the step runs for reads the held-row versions twice, to refuse a step that
     changes a held row;
   - on a model with a tenantless entity, each batch checks that its scope can read tenantless
     holds, and refuses the batch if it can't.
3. Add a migration. It adds `sweep_run_row_detail.Failure`, copies each skipped row's recorded
   error into it, then drops the table and the column. If `dotnet ef migrations add`
   scaffolds a rename of `CapturedPayload` to `Failure`, or drops the table first, replace its
   `Up()` with these steps, in this order. The sample's migration
   (`Cohort.Sample/Migrations/20261107155318_RemoveRowHandlerQueue.cs`):

   ```csharp
   migrationBuilder.AddColumn<string>(
       name: "Failure",
       schema: "public",
       table: "sweep_run_row_detail",
       type: "text",
       nullable: true);

   migrationBuilder.Sql(
       """
       UPDATE "public"."sweep_run_row_detail" AS detail
       SET "Failure" = status."LastError"
       FROM "public"."sweep_row_handler_status" AS status
       WHERE status."SweepRunRowDetailId" = detail."Id"
         AND status."State" = 3
         AND status."Attempt" = 1
         AND status."ClaimedAt" IS NULL
         AND status."CompletedAt" = status."QueuedAt";
       """);

   migrationBuilder.DropTable(
       name: "sweep_row_handler_status",
       schema: "public");

   migrationBuilder.DropColumn(
       name: "CapturedPayload",
       schema: "public",
       table: "sweep_run_row_detail");
   ```

   Write your Cohort schema where the sample has `public`, if you passed one to
   `ConfigureCohortTables`. The `UPDATE` copies the error of each row a failing dependents
   step skipped: 0.11.0 recorded that as a dead-lettered status (`State` 3) queued and
   completed at one database-clock instant and never claimed. A dead-lettered `OnAfterAsync`
   delivery belongs to a row that was changed, so its row detail keeps `Failure` `NULL`.
4. Move `Cohort:RowHandlerDispatch:SweepSettleTimeout` to `Cohort:SweepSettleTimeout`. The old
   key is ignored.
5. If your host doesn't run hosted services, call `IRetentionRunRecovery.RecoverAsync` where
   you called `FlushAsync` ([above](#recovering-abandoned-runs)).
