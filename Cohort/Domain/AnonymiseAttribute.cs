namespace Cohort.Domain;

[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class AnonymiseAttribute(AnonymiseMethod method, string? literal = null) : Attribute
{
    public AnonymiseMethod Method { get; } =
        method == AnonymiseMethod.FixedLiteral && string.IsNullOrWhiteSpace(literal)
            ? throw new ArgumentException(
                "A literal value is required when the anonymise method is FixedLiteral.",
                nameof(literal)
            )
            : method;

    public string? Literal { get; } = literal;
}
