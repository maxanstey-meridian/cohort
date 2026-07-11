namespace Cohort.Domain;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class RetentionEntityIdAttribute(string id) : Attribute
{
    public Guid Id { get; } =
        Guid.TryParse(id, out var parsed) && parsed != Guid.Empty
            ? parsed
            : throw new ArgumentException(
                "Retention entity identity must be a non-empty UUID.",
                nameof(id)
            );
}
