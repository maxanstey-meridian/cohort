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

Then generate a migration and apply it before starting the host or calling any Cohort
service. If a Cohort upgrade changes its tables, `dotnet ef migrations add` picks the
change up from your model like any other.

## Readiness

Cohort validates the installed schema but never creates or upgrades it at runtime. Readiness
requires the columns, keys, unique indexes (not deferrable) and named `CHECK` constraints
Cohort relies on.

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
