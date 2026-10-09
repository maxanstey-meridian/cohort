namespace Cohort.Domain;

/// <summary>
/// Marks a column that identifies one kind of erasure subject, such as <c>"user"</c> or
/// <c>"person"</c>. An erasure for a kind matches only the columns marked with that kind,
/// and every column of one kind must share one CLR type across the model.
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class ErasureSubjectAttribute(string kind) : Attribute
{
    public string Kind { get; } =
        string.IsNullOrWhiteSpace(kind)
            ? throw new ArgumentException("Erasure subject kind cannot be blank.", nameof(kind))
            : kind;
}
