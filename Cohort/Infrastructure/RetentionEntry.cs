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
