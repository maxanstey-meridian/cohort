namespace Cohort.Domain;

public sealed record RetentionEntry(
    Type EntityType,
    string TableName,
    string Category,
    string AnchorMember,
    string AnchorColumn,
    RecordIdConvention RecordId,
    IReadOnlyList<AnonymiseField> AnonymiseFields,
    TenantConvention? Tenant,
    SoftDeleteConvention? SoftDelete
);

public sealed record RecordIdConvention(
    string RecordIdMember,
    string RecordIdColumn
);

public sealed record TenantConvention(
    string TenantMember,
    string TenantColumn
);

public sealed record SoftDeleteConvention(
    string IsDeletedMember,
    string IsDeletedColumn,
    string? DeletedAtMember,
    string? DeletedAtColumn
);
