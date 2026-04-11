namespace Cohort.Domain;

[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class AnonymiseAttribute(AnonymiseMethod method, string? literal = null) : Attribute
{
    public AnonymiseMethod Method { get; } =
        method switch
        {
            AnonymiseMethod.FixedLiteral when string.IsNullOrWhiteSpace(literal) => throw new ArgumentException(
                "A literal value is required when the anonymise method is FixedLiteral.",
                nameof(literal)
            ),
            not AnonymiseMethod.FixedLiteral when literal is not null => throw new ArgumentException(
                "A literal value is only valid when the anonymise method is FixedLiteral.",
                nameof(literal)
            ),
            _ => method,
        };

    public string? Literal { get; } = literal;
}
