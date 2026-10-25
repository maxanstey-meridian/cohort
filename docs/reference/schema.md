# Schema & Migrations

Cohort stores its ledger, holds and handler queue in five tables in your database. You own
them: they're part of your EF model and your migrations.

## Tables

| Table | Holds |
|---|---|
| `sweep_run` | One row per sweep or erasure run |
| `sweep_run_entity_summary` | Per-entity counts for each run |
| `sweep_run_row_detail` | Per-row audit records and captured handler snapshots |
| `sweep_row_handler_status` | The row-handler queue: state, attempts, claims, errors |
| `retention_holds` | Legal holds |

## Adding them to your model

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    base.OnModelCreating(modelBuilder);
    modelBuilder.ConfigureCohortTables();
}
```

The parameterless overload maps all five tables to `public`. Pass a schema to map them all
somewhere else, together:

```csharp
modelBuilder.ConfigureCohortTables("retention");
```

Each EF model gets its own set of Cohort tables. Two applications or models that share one
database put their Cohort tables in different schemas, with `ConfigureCohortTables(schema)`.
Readiness checks every active hold in the set against the model's retained entities, so a
model sharing another model's tables sees that model's holds as orphaned and refuses to run.

Then generate a migration and apply it before starting the host or calling any Cohort
service. If a Cohort upgrade changes its tables, `dotnet ef migrations add` picks the
change up from your model like any other.

## Readiness

Cohort validates the installed schema but never creates or upgrades it at runtime. Readiness
requires the columns, keys, unique indexes (not deferrable) and named `CHECK` constraints
Cohort relies on. It also refuses an extra `NOT NULL` column on a Cohort table that has no
default and isn't an identity or generated column: Cohort's inserts leave it out, so every
insert would fail. And it refuses a default on `sweep_run_entity_summary`'s `RetainedCount`,
`DeferredCount` and `BlockedCount`, which an older Cohort leaves out of its inserts: with a
default, that version would record it instead of failing.

A missing performance-only index is logged as a warning rather than failing. Any index on
the same key columns counts, unique or not.

The application services and the dispatcher check readiness themselves, so a host that
skipped startup validation still can't run against a missing or outdated schema.

## Schema qualification

Every relation name Cohort writes into SQL is schema-qualified, both for your retained
tables and for its own. A retained table with no explicit schema uses the EF model default
schema, and otherwise `public`. Behaviour never depends on PostgreSQL `search_path`.

## Identities

Two identities run through these tables, and they're deliberately different:

- **`RetentionEntityId`** is the stable UUID you assign to a retained entity type with
  `[RetentionEntityId]`. It survives CLR and table renames, and correlates rules, holds,
  summaries, row details and handler work. `EntityType` on audit rows is readable diagnostic
  metadata only.
- **`RecordId`** is one row's canonical PostgreSQL text identity, `CAST(key AS text)`. Cohort
  stores it as `text` in holds and row details, and compares it with the row's own key cast.
  UUID, integer, string, `numeric`, `citext` and provider-converted keys all match
  consistently.

Record-ID types whose text form depends on session settings are rejected at startup:
`timestamp`/`timestamptz`, `date`, `time`, `interval`, `real`/`double precision`, `money` and
`bytea`. Their canonical text could change between sessions and silently detach holds.
Readiness checks the record-ID column's actual catalog type, so domains, arrays and ranges
built on those types are rejected too.

### Changed entity identities

Holds store the `RetentionEntityId` of the entity they protect. If an entity's
`[RetentionEntityId]` changes, or its `[Retain]` is removed, holds stored under the old
identity never match a row again. Readiness therefore refuses to run while an active hold
names an identity no retained entity in the model declares, and names each such identity and
its count of holds. Expired and removed holds, and audit history, are not checked. The check
reads holds across every tenant, so it runs once per database in its own `System(Readiness)`
scope, whichever operation triggered readiness. That scope must reach the same database as
the caller. The server decides that (database name, server start time and the holds table),
so `localhost` versus `127.0.0.1`, or a pooler versus a direct port, is fine.

The fix is to restore `[RetentionEntityId("old id")]` on the entity. If the change is
deliberate, re-point the holds as part of deploying the new identity:

```sql
UPDATE public.retention_holds  -- or the schema you passed to ConfigureCohortTables
SET "RetentionEntityId" = '<new id>'
WHERE "RetentionEntityId" = '<old id>';
```

If the holds' protection is genuinely no longer wanted, end them instead by setting
`"RemovedAt"` to the current time. During a rolling deploy, a replica still running the old
code can create a hold under the old identity after a new replica has passed readiness;
another reason not to change identities.

## Row-level security

A host that enforces tenant isolation with PostgreSQL row-level security writes policies for
Cohort's tables too. These tenant columns and sentinel values are a schema contract: changing
them is a breaking change, recorded in the changelog like any other change to these tables.

| Table | Tenant column | Special values |
|---|---|---|
| `retention_holds` | `TenantId uuid NULL` | `NULL` is a hold on a tenantless row |
| `sweep_run` | `TenantId uuid` | `00000000-0000-0000-0000-000000000000` (`Guid.Empty`) is a tenantless pass or erasure |
| `sweep_run_entity_summary` | `TenantId uuid` | `Guid.Empty`, as above |
| `sweep_run_row_detail` | `TenantId uuid` | `Guid.Empty`, as above |
| `sweep_row_handler_status` | none | Reached through `SweepRunRowDetailId` → `sweep_run_row_detail.TenantId` |

Cohort only touches `Guid.Empty` rows in system scopes, so tenant policies shouldn't match
them. `NULL` holds are different: tenant scopes read them but never write them. A host that
calls `IRetentionDeletion` or `IRetentionHoldsRepository` in a tenant's scope on a tenantless
row must see that row's holds, or the row looks unheld. `IRetentionDeletion` checks: in a
model with any tenantless entity, it refuses with `RetentionConfigurationException`, deleting
nothing, when its scope can't see an active `NULL` hold that `System(Readiness)` sees. So the holds policies
let a tenant read `NULL` holds and write only its own; create and remove holds on tenantless
rows in a system scope. Example policies, with the tenant in a custom setting:

```sql
CREATE POLICY tenant_isolation ON public.sweep_run
  USING ("TenantId" = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON public.sweep_run_entity_summary
  USING ("TenantId" = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON public.sweep_run_row_detail
  USING ("TenantId" = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY holds_read ON public.retention_holds FOR SELECT
  USING ("TenantId" IS NULL OR "TenantId" = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY holds_write ON public.retention_holds FOR ALL
  USING ("TenantId" = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY tenant_isolation ON public.sweep_row_handler_status
  USING (EXISTS (
    SELECT 1 FROM public.sweep_run_row_detail detail
    WHERE detail."Id" = sweep_row_handler_status."SweepRunRowDetailId"
      AND detail."TenantId" = NULLIF(current_setting('app.tenant_id', true), '')::uuid));
```

Permissive policies combine with `OR`, but only those for the command at hand: a `SELECT`
passes either holds policy, while `INSERT`, `UPDATE` and `DELETE` only have `holds_write`, whose
`USING` also checks new rows. Use the schema you passed to `ConfigureCohortTables` in place of
`public`.

Every tenant can therefore read every hold on a tenantless row, `Reason` included, and
`ListActiveAsync` in a tenant scope returns them. Keep tenant-identifying detail out of those
holds.

`NULLIF` matters: once a session has set a custom setting, clearing or resetting it leaves an
empty string rather than `NULL`, and `''::uuid` is an error. Remember `ENABLE` and, if the
connecting role owns the tables, `FORCE ROW LEVEL SECURITY` on each table.

Policies only bite if Cohort's work runs as the right tenant. See
[Tenant Isolation](/guides/tenant-isolation) for the hook that tells your host whose work each
Cohort scope does, and for the grants the tenant role needs.

## Upgrading to 0.11.0

::: warning Breaking change
0.11.0 refuses to run while an active hold names an entity identity the model no longer
registers, and when two EF models share one set of Cohort tables.

- Before upgrading, look for active holds under identities your model no longer declares, and
  re-point or end them as [Changed entity identities](#changed-entity-identities) shows.
  Readiness names each one and its count of holds.
- Give each EF model that maps Cohort's tables its own schema with
  `ConfigureCohortTables(schema)`.
- Hosts with row-level security: let tenant scopes read `NULL`-tenant holds, as the
  [example policies](#row-level-security) do.
- Add a migration: `sweep_run_entity_summary` gains `RetainedCount`, `DeferredCount` and
  `BlockedCount` (`bigint NOT NULL`, no default once the migration has backfilled them).
  Readiness refuses to run until it's applied, and while any of the three still has a
  default. See
  [Audit Trail](/guides/audit-trail#upgrading-to-0-11-0).
:::
