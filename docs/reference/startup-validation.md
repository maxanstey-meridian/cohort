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
- Each retained table and its record-ID column exist in the database, and the table is an
  ordinary or partitioned table: not a view, materialized view, or foreign table, since Cohort
  changes rows in place, addressing each by its physical position (`ctid`): a view has no
  `ctid`, a materialized view's rows can't be updated or deleted, and a foreign table's `ctid`
  doesn't reliably identify its rows.
- Each [hold relation](#hold-relations)'s link column exists in the real table (the message
  says the link column "was not found in the database"), and its catalog type, resolved
  through domains, equals the catalog type of the record-ID column it names, since hold SQL
  equates the two. `varchar` and `text` count as one type, so `varchar(36)` links to a `text`
  record ID; any other difference is refused, naming both types. Once the types match and
  both columns are collatable, their collations are checked too. Equal collations pass, and a
  link with the `default` collation always passes, because the record ID's collation governs
  the comparison. Two different non-default collations are refused, since PostgreSQL can't
  choose between them and every hold check across the relation would fail. A non-default
  nondeterministic link collation against a record ID with the `default` collation is refused
  too, because one link value would match several record IDs and a row could be covered by
  holds on several rows. Each message names the declaration and the column.
- Each anonymised column can take what Cohort writes to it, checked against the database
  catalog: the column exists, the database doesn't generate it (a generated column, or an
  identity column `GENERATED ALWAYS`, takes no `UPDATE`), and the model's store type can be assigned to its type without
  an explicit cast; an `EmptyString` or `FixedLiteral` value, after the value converter, is
  valid input for both types (length, type, and domain `CHECK` included); `Null` doesn't go
  into a column the database declares `NOT NULL`, directly or through nested domains; a value
  every row shares (`EmptyString`, `FixedLiteral`, a `Static` factory) isn't written to a
  column in the key of an unfiltered unique index whose key lacks the record ID; a literal
  isn't written to a foreign-key column; and no anonymised column is referenced by a foreign
  key. See [Anonymisation](/guides/anonymisation).
- Each column an update strategy always writes exists, isn't generated by the database, and
  isn't referenced by a foreign key: the soft-delete flag and deleted-at of an entity that may
  be soft-deleted, or the anonymised-at marker of one that may be anonymised. A missing one
  would fail every such update (the message says the column "was not found in the database"),
  and so would a generated column or an identity column `GENERATED ALWAYS`, which takes no
  `UPDATE` (the message says the database "generates" it). Changing a referenced key is either
  refused while a row references it or silently rewrites the referencing rows.

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
  ([Columns](#columns)).
- one entity is tenanted and the other `[RetentionTenantless]`.
- the same relation is declared twice.
- the relations form a cycle, a relation from an entity to itself included. The message lists
  the entities and each declaration in the cycle.

Each other message, a cycle's included, names the declaration it refuses in the form
`[HeldWith(typeof(MyApp.Case), "CaseId")] on MyApp.CaseLink`: the attribute, the related type,
the member, and the declaring type. A declaration needs no database foreign key, so none is
checked. Readiness also checks each link column against the real table
([Columns](#columns)).

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
