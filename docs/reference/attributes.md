# Attributes

All attributes live in `Cohort.Domain`.

## Entity attributes

| Attribute | Meaning |
|---|---|
| `[Retain(category, anchorMember)]` | The entity is retained in `category`, aged by the `anchorMember` column. Optional `AuditRowDetail` overrides the rule's [per-row audit](/guides/audit-trail#per-row-detail) setting. |
| `[RetentionEntityId("uuid")]` | Required on every retained entity. A permanent identity that holds and audit rows refer to. Never change it: readiness refuses to run while an active hold still names an identity the model no longer declares ([remedy](/reference/schema#changed-entity-identities)). |
| `[ExemptFromRetention(reason)]` | Documents that an entity is deliberately not retained. Optional: unannotated entities are already exempt. |
| `[RetentionTenantless]` | The entity is global and has no tenant column. |
| `[HeldWith(relatedType, linkMember)]` | This entity's rows are held while the row named by its `linkMember` is held (a link's `CaseId`). The member holds the related entity's record ID; a `NULL` link covers nothing. Repeatable, and chains follow through. See [Holding related rows](/guides/legal-holds#holding-related-rows). |
| `[HeldWithReferencing(relatedType, linkMember)]` | This entity's rows are held while a row that names them is held: `linkMember` is a member of `relatedType` holding this entity's record ID (a Case's `SourceSubmissionId`). Repeatable, and chains follow through. See [Holding related rows](/guides/legal-holds#holding-related-rows). |

## Property attributes

| Attribute | Meaning |
|---|---|
| `[Anonymise(method, literal?)]` | Scrub this column when anonymising: `Null`, `EmptyString` or `FixedLiteral`. See [Anonymisation](/guides/anonymisation). |
| `[AnonymiseWith(typeof(Factory))]` | Scrub this column with a registered `IAnonymiseValueFactory`. |
| `[ErasureSubject(kind)]` | This column identifies a subject of `kind` (such as `"user"`) for [right-to-erasure](/guides/erasure). Several properties may share a kind; all columns of one kind hold one CLR type. |

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
