namespace Cohort.Domain;

/// <summary>
/// Declares that this retained entity's rows are held while the row they point at is held.
/// <paramref name="linkMember"/> is a member of this entity whose value is the record id of a row
/// of <paramref name="relatedType"/>, for example a <c>CaseId</c> on a link entity that is held
/// with its Case. A hold on the related row, or on a row it is itself held with, covers this row
/// exactly as if the row carried its own hold. A NULL link covers nothing.
/// </summary>
/// <remarks>
/// The link always targets the related entity's retention record id, and the two rows must belong
/// to one tenant. No database foreign key is needed. For the other direction, where the related
/// entity's member points at this entity, use <see cref="HeldWithReferencingAttribute"/>.
/// </remarks>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
public sealed class HeldWithAttribute(Type relatedType, string linkMember) : Attribute
{
    public Type RelatedType { get; } =
        relatedType ?? throw new ArgumentNullException(nameof(relatedType));
    public string LinkMember { get; } =
        string.IsNullOrWhiteSpace(linkMember)
            ? throw new ArgumentException("Link member cannot be blank.", nameof(linkMember))
            : linkMember;
}
