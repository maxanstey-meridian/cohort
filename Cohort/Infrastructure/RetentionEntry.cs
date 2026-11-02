using Cohort.Domain;

namespace Cohort.Infrastructure;

internal sealed record RetentionEntry(
    Type EntityType,
    Guid RetentionEntityId,
    RelationalObjectName Table,
    CohortStoreTables CohortTables,
    string Category,
    string AnchorMember,
    string AnchorColumn,
    RecordIdConvention RecordId,
    IReadOnlyList<AnonymiseField> AnonymiseFields,
    IReadOnlyList<string> MaterializationColumns,
    TenantConvention? Tenant,
    SoftDeleteConvention? SoftDelete,
    bool IsExplicitlyTenantless = false,
    AuditRowDetail AuditRowDetail = AuditRowDetail.Inherit,
    AnonymisedAtConvention? AnonymisedAt = null
)
{
    internal string TableName => Table.Name;

    /// <summary>
    /// The chains of hold-relation hops from this entity up to each entity whose hold would cover
    /// it; empty when no <c>[HeldWith]</c> or <c>[HeldWithReferencing]</c> relation reaches it.
    /// The direct hold on the row itself (the empty chain) isn't listed. See
    /// <see cref="Holds.HoldRelations"/>. Only <see cref="RetentionEntryBuilder.BuildAll"/> fills
    /// it in; an entry from <see cref="RetentionEntryBuilder.TryBuild"/> has none.
    /// </summary>
    internal IReadOnlyList<Holds.HoldPath> HolderPaths { get; init; } = [];

    /// <summary>
    /// For a holder entity, the chains of hops, walked from this entity, down to every row its
    /// hold covers through relations; empty when no relation names this entity as a holder.
    /// See <see cref="Holds.HoldRelations"/> and <see cref="HolderPaths"/>.
    /// </summary>
    internal IReadOnlyList<Holds.HoldPath> CoveredPaths { get; init; } = [];
}

internal sealed record RecordIdConvention(
    string RecordIdMember,
    string RecordIdColumn,
    Type RecordIdType,
    string? RecordIdStoreType = null
);

internal sealed record AnonymisedAtConvention(string AnonymisedAtMember, string AnonymisedAtColumn);

internal sealed record TenantConvention(string TenantMember, string TenantColumn);

internal sealed record SoftDeleteConvention(
    string IsDeletedMember,
    string IsDeletedColumn,
    string? DeletedAtMember,
    string? DeletedAtColumn
);
