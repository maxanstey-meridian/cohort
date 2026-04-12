namespace Cohort.Domain;

[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class RetainAttribute(string category, string anchorMember) : Attribute
{
    public string Category { get; } = category;
    public string AnchorMember { get; } = anchorMember;
    public AuditRowDetail? AuditRowDetail { get; init; }
}
