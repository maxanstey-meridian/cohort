namespace Cohort.Domain;

[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class RetainAttribute(string category, string anchorMember) : Attribute
{
    private AuditRowDetail auditRowDetail = AuditRowDetail.Inherit;

    public string Category { get; } = string.IsNullOrWhiteSpace(category)
        ? throw new ArgumentException("Category cannot be blank.", nameof(category))
        : category;
    public string AnchorMember { get; } = string.IsNullOrWhiteSpace(anchorMember)
        ? throw new ArgumentException("Anchor member cannot be blank.", nameof(anchorMember))
        : anchorMember;
    public AuditRowDetail AuditRowDetail
    {
        get => auditRowDetail;
        init =>
            auditRowDetail = Enum.IsDefined(value)
                ? value
                : throw new ArgumentOutOfRangeException(
                    nameof(value),
                    value,
                    "AuditRowDetail must be defined."
                );
    }
}
