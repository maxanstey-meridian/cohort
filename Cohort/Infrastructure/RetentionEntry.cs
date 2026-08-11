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
    internal RetentionEntry(
        Type entityType,
        Guid retentionEntityId,
        string tableName,
        string category,
        string anchorMember,
        string anchorColumn,
        RecordIdConvention recordId,
        IReadOnlyList<AnonymiseField> anonymiseFields,
        IReadOnlyList<string> materializationColumns,
        TenantConvention? tenant,
        SoftDeleteConvention? softDelete,
        bool isExplicitlyTenantless = false,
        AuditRowDetail auditRowDetail = AuditRowDetail.Inherit,
        AnonymisedAtConvention? anonymisedAt = null
    ) : this(
        entityType,
        retentionEntityId,
        new RelationalObjectName("public", tableName),
        CohortStoreTables.Public,
        category,
        anchorMember,
        anchorColumn,
        recordId,
        anonymiseFields,
        materializationColumns,
        tenant,
        softDelete,
        isExplicitlyTenantless,
        auditRowDetail,
        anonymisedAt
    ) { }

    internal string TableName => Table.Name;
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
