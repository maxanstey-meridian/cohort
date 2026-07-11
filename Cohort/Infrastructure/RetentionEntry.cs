using Cohort.Domain;

namespace Cohort.Infrastructure;

internal sealed record RetentionEntry(
    Type EntityType,
    Guid EntityId,
    string TableName,
    string Category,
    string AnchorMember,
    string AnchorColumn,
    RecordIdConvention RecordId,
    IReadOnlyList<AnonymiseField> AnonymiseFields,
    TenantConvention? Tenant,
    SoftDeleteConvention? SoftDelete,
    bool IsExplicitlyTenantless = false,
    AuditRowDetail AuditRowDetail = AuditRowDetail.Inherit,
    AnonymisedAtConvention? AnonymisedAt = null
);

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
