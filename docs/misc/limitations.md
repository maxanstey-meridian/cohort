# Limitations

These are the deliberate limits of Cohort, so you don't discover them in production.

## Database

- **PostgreSQL only.** The SQL relies on `RETURNING`, `= ANY()`, `FOR UPDATE ... SKIP LOCKED`
  and advisory locks. There's no SQL Server or SQLite support, and none is planned.
- **You own the schema.** Cohort never creates or migrates its tables at runtime. See
  [Schema & Migrations](/reference/schema).

## Model

- One retained entity maps to one independently owned table. Owned types, shared tables,
  entity and table splitting, and inheritance hierarchies are rejected at startup.
- Retained entities can't form a foreign-key cycle.
- Rows with a `NULL` anchor never age out of an ordinary sweep.
- Record IDs whose text form depends on session settings (timestamps, dates, intervals,
  floats, `money`, `bytea`) aren't supported.

## Rules

- No built-in conditional, aliasing or caching rule providers. Your `IRetentionRuleProvider`
  is plain C#, so build whichever of those you need inside it.
- Anonymisation ships `Null`, `EmptyString` and `FixedLiteral`. Hashing, format-preserving
  and other schemes belong in an `IAnonymiseValueFactory` you own.

## Delivery

- Row-handler `OnAfterAsync` is at-least-once, not exactly-once. Handlers must be idempotent.
- A handler that ignores its cancellation token can overlap its own retry.
- Audit observers are best effort, with no durable outbox. Integrations that must not miss an
  event should read the ledger tables or use CDC.

## Scheduling

- Missed cron occurrences are skipped, not caught up.
- Replicas compare occurrence times using their own clocks, so keep them in sync.

## Out of scope

- No source generator. Reflection is fine for a daily sweep.
- No OneTrust or Purview adapters.
