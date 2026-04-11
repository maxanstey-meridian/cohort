namespace Cohort.Domain;

public sealed record RetentionEntry(
    Type EntityType,
    string TableName,
    string Category,
    string AnchorMember
);
