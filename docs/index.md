---
layout: home
hero:
  name: Cohort
  text: Retention belongs in your model
  tagline: Annotate EF Core entities, map categories to rules, and let Cohort preview, sweep and erase rows in PostgreSQL, respecting tenants, holds and the audit trail.
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started
    - theme: alt
      text: Configuration
      link: /reference/configuration
features:
  - title: Annotations declare membership
    details: "[Retain(category, anchor)] puts an entity in a category. Unannotated entities are left alone, and the policy lives next to the type it governs."
  - title: Rules declare policy
    details: Your IRetentionRuleProvider turns a category into a period, a strategy (purge, soft-delete or anonymise) and an optional legal minimum, per tenant and per moment.
  - title: Cohort executes it safely
    details: Batched, tenant-scoped Postgres mutations that skip held rows, write an authoritative audit ledger, and refuse to start on a model they can't execute.
---

Cohort is a .NET library, not a service. It runs inside your host, against your
`DbContext`, in your database. You own the migration, the rules and the schedule.

Start with [Getting Started](/getting-started), then read
[Retention Rules](/guides/retention-rules) for how categories, rules and strategies fit
together.
