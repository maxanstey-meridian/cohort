# Limitations

These are the deliberate limits of Cohort, so you don't discover them in production.

## Database

- **PostgreSQL only.** The SQL relies on `RETURNING`, `= ANY()`, `FOR UPDATE ... SKIP LOCKED`
  and advisory locks. There's no SQL Server or SQLite support, and none is planned.
- **PostgreSQL 16 or later.** Startup checks anonymised columns with `pg_input_is_valid`,
  which PostgreSQL 16 added; an older server is refused.
- **Session advisory locks need a session of their own.** The worker, each scheduled pass,
  each run and abandoned-run recovery hold PostgreSQL session advisory locks, so they need
  direct connections or session-mode pooling. A transaction-mode pooler (PgBouncer's
  `pool_mode = transaction`) hands the session to someone else between transactions, and the
  locks stop meaning anything. Npgsql `Multiplexing=true` does the same inside one process, so
  readiness refuses it.
- **You own the schema.** Cohort never creates or migrates its tables at runtime. See
  [Schema & Migrations](/reference/schema).

## Model

- One retained entity maps to one independently owned table. Owned types, shared tables,
  entity and table splitting, and inheritance hierarchies are rejected at startup.
- Retained entities can't form a foreign-key cycle.
- A [hold relation](/guides/legal-holds#holding-related-rows) link always names the record ID
  of the row it points at. A link to an alternate key can't be declared. Relations can't form a
  cycle, so an entity can't be held with itself (a message replying to a message), and both
  ends are tenanted or both are `[RetentionTenantless]`.
- No call lists every row a hold covers. `ListCoveringAsync` and `ListCoveringSubjectAsync`
  return holds, not rows.
- A hold covers related rows only while the declarations stay: removing a declaration releases
  what it covered. Cohort doesn't lock holder rows against your own writes to a link column, so
  a link written while a sweep mutates a row further down the chain is judged as of when
  Cohort's mutation statement runs, so the outcome matches the mutation going first and the
  link after ([Writing links](/guides/legal-holds#writing-links)).
- Hold creation, an erasure and an `IRetentionDeletion` callback can deadlock across entities
  when holds cover related rows. PostgreSQL aborts one side with `40P01`, which is retryable.
- Rows with a `NULL` anchor never age out of an ordinary sweep.
- Record IDs whose text form depends on session settings (timestamps, dates, intervals,
  floats, `money`, `bytea`) aren't supported.

## Rules

- No built-in conditional, aliasing or caching rule providers. Your `IRetentionRuleProvider`
  is plain C#, so build whichever of those you need inside it.
- Anonymisation ships `Null`, `EmptyString` and `FixedLiteral`. Hashing, format-preserving
  and other schemes belong in an `IAnonymiseValueFactory` you own.

## Tenant isolation

- System work spans tenants by design. The worker's coordination, abandoned-run recovery,
  history pruning and readiness read and write every tenant's Cohort rows. Your
  `IRetentionExecutionScope` decides how a system scope is authorised and audited. See
  [Tenant Isolation](/guides/tenant-isolation).
- Cohort's tables key tenants on `TenantId`, with fixed sentinels for tenantless work. Write
  your policies against those columns; there is no mapping to a column name of your own. See
  [Row-level security](/reference/schema#row-level-security).

## Delivery

- Cohort keeps no work queue. Work outside the database goes through your own outbox, written
  by the entity's dependents step in the batch's transaction; delivering it is your outbox's
  job. See [Dependents and After-Commit Work](/guides/row-handlers#after-commit-work-through-your-outbox).
- A row PostgreSQL deletes through an `ON DELETE CASCADE` foreign key gets no hook. A child
  that owns anything outside the database needs a `Restrict` foreign key and retiring in its
  parent's dependents step.
- Audit observers are best effort, with no durable outbox. Integrations that must not miss an
  event should read the ledger tables or use CDC.

## Out of scope

- No source generator. Reflection is fine for a daily sweep.
- No OneTrust or Purview adapters.
