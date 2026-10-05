# Attributes

All attributes live in `Cohort.Domain`.

## Entity attributes

| Attribute | Meaning |
|---|---|
| `[Retain(category, anchorMember)]` | The entity is retained in `category`, aged by the `anchorMember` column. Optional `AuditRowDetail` overrides the rule's [per-row audit](/guides/audit-trail#per-row-detail) setting. |
| `[RetentionEntityId("uuid")]` | Required on every retained entity. A permanent identity that holds, audit rows and handler work refer to. Never change it. |
| `[ExemptFromRetention(reason)]` | Documents that an entity is deliberately not retained. Optional: unannotated entities are already exempt. |
| `[RetentionTenantless]` | The entity is global and has no tenant column. |

## Property attributes

| Attribute | Meaning |
|---|---|
| `[Anonymise(method, literal?)]` | Scrub this column when anonymising: `Null`, `EmptyString` or `FixedLiteral`. See [Anonymisation](/guides/anonymisation). |
| `[AnonymiseWith(typeof(Factory))]` | Scrub this column with a registered `IAnonymiseValueFactory`. |
| `[ErasureSubject]` | This column identifies the subject for [right-to-erasure](/guides/erasure). Can appear on several properties. |

## Convention overrides

These mark the property Cohort should use instead of the
[naming convention](/reference/configuration#conventions). Each may appear on at most one
property per entity.

| Attribute | Replaces |
|---|---|
| `[RetentionRecordId]` | `Id` |
| `[RetentionTenant]` | `TenantId` |
| `[RetentionSoftDelete]` | `IsDeleted` |
| `[RetentionDeletedAt]` | `DeletedAt` |
| `[RetentionAnonymisedAt]` | `AnonymisedAt` (mandatory for entities that can be anonymised) |

## Handler attributes

| Attribute | Meaning |
|---|---|
| `[RowHandlerPriority(n)]` | Order among an entity's [row handlers](/guides/row-handlers), lowest first. Handlers without it run last. |
