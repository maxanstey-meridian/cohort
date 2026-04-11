# Cohort

Annotation-driven, category-tagged retention for .NET / EF Core. A standalone library that hosts (casebridge, perch, …) consume to declare GDPR retention rules on their entities and have a sweep engine purge / soft-delete / anonymise rows past their retention period.

This is the **pre-Milestone-A skeleton** — only the bare bones (`[Retain]` attribute, registry scan, static resolver, pure cutoff helper) plus the three exemplar test patterns. No sweep engine yet.

- **Plan: full library** — [`.plans/COHORT1.md`](.plans/COHORT1.md) (three milestones A/B/C)
- **Plan: this skeleton** — [`.plans/COHORT0.md`](.plans/COHORT0.md)
- **Agent instructions** — [`CLAUDE.md`](CLAUDE.md) (read before writing tests)
