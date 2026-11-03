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

## Row handlers and dependents steps

- Every [row handler](/guides/row-handlers) carries a `[RowHandlerId]` holding a non-empty
  UUID, and no two handler classes share an id, whatever their entities. `AddRowHandler`
  refuses a missing, malformed, or shared id when it's called, naming the handler, and an id
  another class uses as a dependents step. One class keeps its one id for every entity it's
  registered for, as a handler or as a step.
- Every `IRetentionHandler<T>` service is registered once, through `AddRowHandler`, as
  scoped. A handler registered any other way is refused and named, and so is one whose
  service was removed or replaced after `AddRowHandler` added it: it would never run.
- Every [dependents step](/guides/row-handlers#retiring-a-rows-dependents) carries a
  `[RowHandlerId]` in the same id space: `AddRetentionDependents` refuses a missing or
  malformed id, an id another step or handler class uses, and a second step for an entity.
- Every `IRetentionDependents<T>` service is registered once, through
  `AddRetentionDependents`, as scoped. A step registered any other way is refused and named,
  and so is one whose service was removed or replaced after `AddRetentionDependents` added it.
- A row handler or dependents step registered for an entity that isn't a `[Retain]` entity in
  the `DbContext` model is refused and named: it would never run.
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
  with the session time zone.
- Tenant, anchor, soft-delete, deleted-at, and anonymised-at properties don't use EF value
  converters. Cohort filters and writes those columns in SQL, where a converter wouldn't
  apply.
- A record ID that isn't the primary key is unique through an alternate key or an unfiltered
  unique index. A filtered index doesn't make it unique.
- Record-ID types whose text form depends on session settings are rejected. See
  [Identities](/reference/schema#identities).
- A retained entity's `AnonymisedAt` marker has no default or value generation mapped in the
  EF model (`HasDefaultValue`, `HasDefaultValueSql`, or `ValueGenerated` other than `Never`).
  EF would then leave it out of an insert and the database would fill it in, so every row
  would be born marked as anonymised and never anonymised. `NULL` must mark the rows still to
  anonymise.

### Statements

Readiness asks PostgreSQL to analyse and plan, without running, every statement Cohort can send
for the host's model and the strategies its categories declare: sweeps, previews, and dry runs;
erasures, for each subject kind an entity maps, with and without a legal minimum; holds and
their deletion check; and the worker's tenant-coverage scan. It builds them with the code the
runs execute, so what readiness explains is what a run sends, and it refuses what PostgreSQL
refuses, for every column those statements use. The error holds one entry per entity, with
PostgreSQL's SQLSTATE and message, the operations that send the statement, the statement text
around the error, and the tables it reads. Data-only values are bound as typed `NULL`s, so
that text carries no subject data. PostgreSQL refuses:

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
  (`42704`);
- a privilege the database role lacks (`42501`), `USAGE` on a schema included. The role
  readiness validates under must be able to run every statement Cohort may send, for every
  operation: a host that splits roles gives the role that reaches readiness the privileges of
  all of them (`SELECT`, `INSERT`, `UPDATE`, and `DELETE` on the retained tables, and row locks
  need `UPDATE`). A role that can't prove it is permitted is refused, not skipped.

The pass is kept per database, role, and search path. See [Readiness](/reference/schema#readiness).

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
- [Options](/reference/configuration) are in range. With row handlers registered,
  `RowHandlerDispatch:PayloadRetention` is longer than the retry schedule of the entity with
  the most handlers (see [Configuration](/reference/configuration#row-handler-dispatch)), on
  start and on every reload.
