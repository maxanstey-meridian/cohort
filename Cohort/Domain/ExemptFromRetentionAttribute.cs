namespace Cohort.Domain;

[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class ExemptFromRetentionAttribute(string reason) : Attribute
{
    public string Reason { get; } =
        string.IsNullOrWhiteSpace(reason)
            ? throw new ArgumentException(
                "A non-blank reason is required for ExemptFromRetention.",
                nameof(reason)
            )
            : reason;
}
