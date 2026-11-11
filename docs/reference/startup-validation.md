# Startup Validation

Cohort would rather not start than run a retention model it can't execute safely. Everything
on this page is checked when the host starts, before any row is touched. A failure names the
entity and the problem.

## Required services

- An `IRetentionExecutionScope` is registered. Cohort runs each scope's work through it, so a
  host's tenant isolation covers Cohort's own work. Hosts without
  tenant isolation register `RetentionExecutionScope.Unisolated`. Without either, the host
  fails to start, and a host that skips startup validation fails on its first Cohort call. See
  [Tenant Isolation](/guides/tenant-isolation).

## Annotations and identity

- Every retained entity has a `[RetentionEntityId]`. Missing, empty, malformed, and duplicate
  identities are rejected, naming the entity.
- An entity can't be both `[Retain]` and `[ExemptFromRetention]`.
- Each convention marker attribute (`[RetentionRecordId]`, `[RetentionTenant]`,
  `[RetentionSoftDelete]`, `[RetentionDeletedAt]`, `[RetentionAnonymisedAt]`) appears on at
  most one property.
- `[RetentionTenantless]` isn't combined with a tenant property. Otherwise the tenant property
  would win and the marker would be silently ignored.

## Dependents steps

- An entity has at most one [dependents step](/guides/row-handlers#retiring-a-rows-dependents):
  `AddRetentionDependents` refuses a second step class for an entity when it's called, and a
  repeat call for the same entity and step with a different `retiresReferencingRows`.
- Every `IRetentionDependents<T>` service is registered once, through
  `AddRetentionDependents`, as scoped. A step registered any other way is refused and named,
  and so is one whose service was removed or replaced after `AddRetentionDependents` added it.
- A dependents step registered for an entity that isn't a `[Retain]` entity in the `DbContext`
  model is refused and named: it would never run.
- On a host that was never started, every Cohort operation refuses these too.

## Rules and capabilities

- The union of strategies each category declares in `GetCapabilities` is checked against every
  entity in it. For example, an entity that may be anonymised needs an `AnonymisedAt` marker,
  and one that may be soft-deleted needs a soft-delete flag.
- At run time, a rule whose strategy wasn't declared is rejected.
- `RetentionPeriod` rejects a negative part, which would compute a future cutoff and sweep
  everything.
- `TimeZone` must name a time zone the host knows.
- `[AnonymiseWith]` factories are registered exactly once as `IAnonymiseValueFactory`.
- `[ErasureSubject(kind)]` kinds aren't blank, every column of one kind holds the same CLR
  type across the model, and each marked property maps to a physical column with a
  compatible provider type.

## Mapping shapes

Cohort mutates one independently owned table per retained entity, and won't guess which
columns or rows belong to it. So these are rejected:

- retained owned types
- entities sharing a table with another EF type
- entities split across multiple tables
- entities in an EF inheritance hierarchy (TPH, TPT, or TPC), because sweep SQL targets the
  table without a discriminator

## Columns

- Anchor, `DeletedAt`, and `AnonymisedAt` columns map to `timestamp with time zone`
  (`timestamptz` and precision variants are fine). Cohort compares and writes retention
  timestamps as UTC instants, and a naive `timestamp without time zone` column silently drifts
  with the session time zone. Readiness also reads the database's own column type, through any
  domains, and refuses a naive one ([Rules](#rules)).
- Tenant, anchor, soft-delete, deleted-at, and anonymised-at properties don't use EF value
  converters. Cohort filters and writes those columns in SQL, where a converter wouldn't
  apply.
- A record ID that isn't the primary key is unique through an alternate key or an unfiltered
  unique index. A filtered index doesn't make it unique.
- Record-ID types whose text form depends on session settings are rejected. See
  [Identities](/reference/schema#identities).
- The `AnonymisedAt` marker of an entity whose category may anonymise has no default or value
  generation mapped in the EF model (`HasDefaultValue`, `HasDefaultValueSql`, or
  `ValueGenerated` other than `Never`).
  EF would then leave it out of an insert and the database would fill it in, so every row
  would be born marked as anonymised and never anonymised. `NULL` must mark the rows still to
  anonymise.

### Statements

Readiness asks PostgreSQL to analyse and plan, without running, the statements Cohort sends.
That is the statements against the host's retained tables, for the host's model and the
strategies its categories declare: sweeps, previews, and dry runs; erasures, for each subject
kind an entity maps, with and without a legal minimum; holds and their deletion check; and the
worker's tenant-coverage scan. Some of them also read Cohort's `retention_holds` and
`sweep_run_row_detail` tables. Cohort builds each statement with the code the runs execute, so
what readiness explains is what a run sends, and it refuses what PostgreSQL refuses, for every
column those statements use. Statements against Cohort's own four tables and the PostgreSQL
functions it calls aren't explained: a role that lacks a privilege on them fails at the
statement.

Readiness explains all of these statements together, once for the model. A schema fault in any
of them refuses every operation on the model, not only the one that sends the statement. The
error holds one entry per entity, with PostgreSQL's SQLSTATE and message, the operations that
send the statement, and the tables it reads, in the form `Retained entity MyApp.Order:
PostgreSQL refused statements Cohort sends. 42703: column … does not exist (while sweeping
with Purge, guarding held rows; reading public.orders). Apply the host application's pending EF Core migrations, or
map the entity to what the database has.` A refusal of a statement that spans entities, with
none of an entity's own refused, is one error headed "Across retained entities". Data-only
values are bound as typed `NULL`s, so that text carries no subject data. PostgreSQL refuses:

- a table or column the statements read or write that the database doesn't have (`42P01`,
  `42703`): a retained table, a record-ID, anchor, tenant, soft-delete, deleted-at, or
  anonymised-at column, an anonymised column, an erasure-subject column, or a hold-relation
  link column;
- a comparison with no operator, such as a tenant column that is `text` against a `uuid`, or a
  flag that is `integer` against `boolean` (`42883`), and a value that can't be assigned to the
  column it is written to (`42804`): the model's store type, `Null` and factory values
  included, against the column's type. See [Anonymisation](/guides/anonymisation);
- a column the database generates (a generated column, or an identity column
  `GENERATED ALWAYS`), which takes no `UPDATE` (`428C9`);
- a cast type the database doesn't know, resolved through the session's search path
  (`42704`).

Readiness doesn't refuse a privilege the database role lacks (`42501`): the check needs none,
and a role that can't run a statement fails at that statement, under the role that sends it,
before the statement writes. The statements are explained as the session that first validates
the model sees them, with that session's role and search path, so a fault only another role or
path reaches surfaces at the statement in the same way:

- a missing `USAGE` on a schema is raised while PostgreSQL looks up a name, so that
  statement's columns go unchecked;
- the blocking-key read below keeps only the child tables the role can `SELECT`, so the
  statements explained are that role's;
- a publication's replica-identity refusal (`55000`) is raised after the permission check, so
  a role without the privilege doesn't reveal it;
- an unqualified type name (a domain, `citext`) resolves through the session's search path.

`SELECT` on the child table of a foreign key that blocks a purge changes what a run does, and
no role is refused for lacking it. With it, a purge
leaves out the rows that child still references, unless the entity has a dependents step
registered as retiring the rows that reference it (its rows are always tried, whatever the role
can read; see [Rows that reference the entity](/guides/row-handlers#rows-that-reference-the-entity)).
Without it, that key is left out: a sweep
or an erasure tries the row, the constraint refuses the delete, and the row is counted as
blocked (a sweep still succeeds and the next run tries the row again; an erasure settles
`PartiallyFailed` with `rows-blocked`, as it does with `SELECT`). A preview or a dry run
doesn't try the row: it counts it as due, not blocked.

What each scope's role needs to run its operation, and only that
([Tenant Isolation](/guides/tenant-isolation#grants) says which scope that is), is below: a
preview role needs only `SELECT`, and recovery needs nothing on your tables. On the
retained tables that is `SELECT`; `UPDATE` where a statement locks rows (`FOR UPDATE` and
`FOR SHARE` need it, as `SoftDelete` and `Anonymise` do); `DELETE` where `Purge` is declared;
and `USAGE` on each schema those tables and Cohort's live in.

| Operation | Cohort's tables | Functions | Your retained tables |
|---|---|---|---|
| **Sweep**: sweeps, dry runs, and scheduled passes | `sweep_run` and `sweep_run_entity_summary`: `SELECT`, `INSERT`, `UPDATE`. `sweep_run_row_detail`: `SELECT`, `INSERT`. `retention_holds`: `SELECT` | `pg_advisory_lock`, `pg_advisory_unlock`, `pg_try_advisory_lock` (the pass lock), the entity lock, the clock | `SELECT`, `UPDATE`; `DELETE` where `Purge` is declared |
| **Erasure** | As Sweep | `pg_advisory_lock`, `pg_advisory_unlock`, the entity lock, the clock, `current_setting` | As Sweep |
| **Preview** | `retention_holds`: `SELECT` | The clock | `SELECT` |
| **Holds**: `IRetentionHoldsRepository` | `retention_holds`: `SELECT`, `INSERT`, `UPDATE` | The entity lock, the clock, `current_setting` | `SELECT`, `UPDATE` (the rows a hold locks `FOR SHARE`) |
| **Deletion**: `IRetentionDeletion` | `retention_holds`: `SELECT` | The entity lock, the clock | `SELECT` |
| **Readiness**: the `System(Readiness)` scope's own reads | `retention_holds`: `SELECT` | The clock | None |
| **Recovery**: abandoned-run recovery | `sweep_run`: `SELECT`, `UPDATE` | `pg_try_advisory_lock`, `pg_advisory_unlock` (the run lock), the clock | None |
| **Worker**: the scheduled worker's coordination | `sweep_run`: `SELECT` | `pg_try_advisory_lock`, `pg_advisory_unlock_all`, the clock | `SELECT` on each tenanted retained table |
| **Pruning**: history pruning, for the retentions set | Runs: `sweep_run` `SELECT`, `UPDATE`, `DELETE`; `sweep_run_row_detail` and `sweep_run_entity_summary` `SELECT`, `DELETE`. Holds: `retention_holds` `SELECT`, `UPDATE`, `DELETE` | The clock | None |

In the table, "the entity lock" is
`pg_advisory_xact_lock` over `hashtextextended`; and "the clock" is `statement_timestamp`,
which every read of holds calls inline to decide which are active. The check, in sweeps and
erasures, that a run still holds its locks reads the `pg_locks` view (and calls
`pg_backend_pid`), which PUBLIC can read by default.

Two more grants no table privilege covers:

- `USAGE` on the sequence behind a serial `Id` column of `sweep_run_row_detail`, for sweeps
  and erasures. An identity column, which Cohort's own tables use, needs no grant. A
  serial column's default calls `nextval`, so the insert fails at the statement for a role
  that holds neither `USAGE` nor `UPDATE` on the sequence.
- `EXECUTE` on the functions in the table. PUBLIC holds it by default, and a database that
  revokes it from a role fails at the call.

History pruning needs only what the retentions you set send, so a host that prunes only
inactive holds needs no `DELETE` on `sweep_run`.

At startup, the model pass runs in the `System(Readiness)` scope, then in the scope of each
subsystem that will run, so that scope's own context is checked too: `System(Recovery)` when
the kill switch is off (abandoned-run recovery runs whatever else is configured),
`System(WorkerCoordination)` when the worker runs (the kill switch off and a `Schedule` set),
and `System(HistoryPruning)` when a pruning retention is set and the kill switch is off. A
refusal stops the host. The pass runs once for each model instance and connection string, so a
later scope that shares both does no further work beyond the provider and Multiplexing checks.
A refusal after startup (a scope that reaches a model or connection string startup didn't
check, say) throws a `RetentionConfigurationException` from the call that reached readiness.
The recovery service, worker, and pruner log it and retry, and sweeps, previews, and the other
operations carry on.

A pass records nothing until it completes, so a pass refused partway starts again from its
first check on the next call. A completed pass is recorded on its model instance and connection
string only outside a caller's transaction (one begun through EF, or a `System.Transactions`
transaction your context is enlisted in). The one check recorded on its own is the
stored-identities check, which also records inside a caller's transaction, but only when its
`System(Readiness)` scope ran in a session (a PostgreSQL backend) other than the caller's: on a
host that gives every context one session, whether as one connection object or as several over
one connection, that read would see the caller's uncommitted hold writes. Once
the model has passed, every later call returns with no round trip, except the check for
tenantless holds, which reads the caller's database and its own `System(Readiness)` scope's
before it reads them. Inside a caller's transaction with no pass recorded, each call runs the
pass again, skipping the stored-identities check if that one was recorded. See
[Readiness](/reference/schema#readiness).

### Rules

A short list of rules covers what PostgreSQL accepts but the statements still get wrong:

- Each retained table is an ordinary or partitioned table: not a view, materialized view, or
  foreign table, since Cohort changes rows in place, addressing each by its physical position
  (`ctid`): a view has no `ctid`, a materialized view's rows can't be updated or deleted, and a
  foreign table's `ctid` doesn't reliably identify its rows.
- Each [hold relation](#hold-relations)'s link column has the catalog type, resolved through
  domains, of the record-ID column it names, since hold SQL equates the two. PostgreSQL
  compares `citext` with `text` and `integer` with `numeric` without complaint, so only a
  rule sees them differ. `varchar` and `text` count as one type, so `varchar(36)` links to a
  `text` record ID; any other difference is refused, naming both types. Once the types match
  and both columns are collatable, their collations are checked too: PostgreSQL only fails on
  a collation conflict when the plan compares the two columns. Equal collations pass, and a
  link with the `default` collation always passes, because the record ID's collation governs
  the comparison. Two different non-default collations are refused, since PostgreSQL can't
  choose between them and every hold check across the relation would fail. A non-default
  nondeterministic link collation against a record ID with the `default` collation is refused
  too, because one link value would match several record IDs and a row could be covered by
  holds on several rows. Each message names the declaration and the column. A missing link
  column is the statement check's.
- A record ID's text form doesn't depend on session settings, since hold joins compare it. See
  [Identities](/reference/schema#identities).
- The anchor, deleted-at, and anonymised-at columns are `timestamp with time zone`, seen
  through any chain of domains. A `timestamp without time zone` compares and assigns through a
  cross-type operator, so PostgreSQL accepts it and the value drifts with the session time
  zone.
- A column an update strategy writes isn't referenced by a foreign key: the soft-delete flag
  and deleted-at of an entity that may be soft-deleted, or the anonymised-at marker of one
  that may be anonymised. Changing a referenced key is either refused while a row references it
  or silently rewrites the referencing rows.
- A tenant column is `NOT NULL`, since a `NULL` tenant never matches a tenant's run, erasure,
  or hold. When `SoftDelete` is declared, the soft-delete flag is `NOT NULL`, since
  `= FALSE` skips a `NULL`. When `Anonymise` is declared, the anonymised-at column is nullable,
  since `IS NULL` finds the rows still to anonymise and never matches a `NOT NULL` column. A
  column's own `NOT NULL`, or a `NOT NULL` domain anywhere in its chain, counts.
- Each anonymised column takes what Cohort writes to it, checked against the database
  catalog: an `EmptyString` or `FixedLiteral` value, after the value converter, is valid input
  for both the cast type and the column type (length, type, and domain `CHECK` included);
  `Null` doesn't go into a column the database declares `NOT NULL`, directly or through nested
  domains; a value every row shares (`EmptyString`, `FixedLiteral`, a `Static` factory) isn't
  written to a column in the key of an unfiltered unique index whose key lacks the record ID;
  a literal isn't written to a foreign-key column; and no anonymised column is referenced by a
  foreign key. See [Anonymisation](/guides/anonymisation).

## Hold relations

An entity that carries `[HeldWith]` or `[HeldWithReferencing]` (see
[Holding related rows](/guides/legal-holds#holding-related-rows)) is refused when:

- the attribute's arguments are invalid (a null type or a blank member). The message reads
  "[HeldWith] or [HeldWithReferencing] on `MyApp.CaseLink` is invalid: …".
- the declaring entity isn't `[Retain]`. An attribute on an `[ExemptFromRetention]` or an
  unannotated entity is an error, not ignored.
- the related type isn't a `[Retain]` entity in the `DbContext` model.
- the link member isn't a mapped public property with a table column, on the entity that
  holds the link: the declaring entity for `[HeldWith]`, the related one for
  `[HeldWithReferencing]`.
- the link's provider CLR type (after any value converter, and after unwrapping
  `Nullable<T>`) differs from that of the record ID it names. Startup compares provider CLR
  types only; store types are compared by readiness, against the database catalog
  ([Rules](#rules)).
- one entity is tenanted and the other `[RetentionTenantless]`.
- the same relation is declared twice.
- the relations form a cycle, a relation from an entity to itself included. The message lists
  the entities and each declaration in the cycle.

Each other message, a cycle's included, names the declaration it refuses in the form
`[HeldWith(typeof(MyApp.Case), "CaseId")] on MyApp.CaseLink`: the attribute, the related type,
the member, and the declaring type. A declaration needs no database foreign key, so none is
checked. Readiness also checks each link column against the real table
([Statements](#statements) and [Rules](#rules)).

## Foreign keys

- No `ON DELETE CASCADE` foreign key leads from a purgeable retained entity into another
  retained entity. A cascade would bypass the dependent's retention window, holds and audit
  trail.
- No cascade leads from a purgeable retained entity back into a retained type, including
  itself through a self-reference.
- Retained entities can't form a foreign-key cycle. Cohort sweeps dependents before their
  principals, so it needs an order.
- `RESTRICT` and `NO ACTION` dependents are expected and aren't refused. A purge leaves a row
  they still reference in place and reports it in `BlockedCount`; the next run tries it
  again. A child that never expires, or whose category can't purge, keeps its parent's
  `BlockedCount` from ever shrinking.

## Database

- The connection is PostgreSQL, version 16 or later, and Cohort's `DbContext` doesn't use
  Npgsql `Multiplexing`, which would break the session advisory locks Cohort holds. See
  [Limitations](/misc/limitations#database).
- The installed Cohort schema has the required columns, keys, unique indexes, and `CHECK`
  constraints, and no required column Cohort doesn't write. See
  [Readiness](/reference/schema#readiness).
- No active hold names a `[RetentionEntityId]` that no retained entity in the model declares
  any more. Such a hold would never match its row again, so the row would lose its protection
  without an error. See [Changed entity identities](/reference/schema#changed-entity-identities).
- [Options](/reference/configuration) are in range, on start and on every reload.

## Upgrading to 0.12.0

::: warning Breaking change
0.12.0 asks PostgreSQL to check the statements Cohort sends against your tables, and refuses
more of what it finds at startup and at readiness. Your own tables and EF model may need a
change. Each message names the entity and column it refuses. Cohort's own schema changes too:
readiness refuses a `sweep_run_row_detail` with no `Failure` column until you apply that
migration (see [Dependents and After-Commit Work](/guides/row-handlers#upgrading-from-row-handlers)).

- A tenant column that is nullable in the database: make it `NOT NULL`. A `NULL` tenant never
  matches any tenant's run, erasure, or hold, so those rows were silently never swept.
- A soft-delete flag that is nullable, on an entity whose category may soft-delete: make it
  `NOT NULL`, with a default of `false` if rows exist. A `NULL` flag was silently skipped.
- An anchor, deleted-at, or anonymised-at column the database types as
  `timestamp without time zone`: change it to `timestamp with time zone`, converting existing
  values from the zone they were written in. The naive type drifted with the session time
  zone.
- An anonymised-at column that is `NOT NULL`, on an entity whose category may anonymise: make
  it nullable. `NULL` marks the rows still to anonymise, so a `NOT NULL` column never matched
  any.
- An `AnonymisedAt` marker with `HasDefaultValue`, `HasDefaultValueSql`, or value generation
  mapped in the EF model, on an entity whose category may anonymise: remove the default or the
  generation. With one, every row was born marked as anonymised. Rows already inserted that way
  need their marker set back to `NULL` if they should still be anonymised.
- The text of a refusal for a missing table or column, a mistyped column, a generated column,
  or a missing hold-relation link column is now PostgreSQL's, with its SQLSTATE and message.
  Code or alerts that match the old text need updating.
:::
