namespace Cohort.Domain;

[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class ExemptFromRetentionAttribute(string reason) : Attribute
{
    public string Reason { get; } = reason;
}
