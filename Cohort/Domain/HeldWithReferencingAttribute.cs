namespace Cohort.Domain;

/// <summary>
/// Declares that this retained entity's rows are held while a row that points at them is held.
/// <paramref name="linkMember"/> is a member of <paramref name="relatedType"/> whose value is the
/// record id of a row of this entity, for example a Case's <c>SourceSubmissionId</c> on the
/// Submission it came from. A hold on any such row, or on a row it is itself held with, covers
/// this row exactly as if the row carried its own hold. Several referencing rows can share one
/// row, which stays held while any of them is held.
/// </summary>
/// <remarks>
/// The link always targets this entity's retention record id, and the two rows must belong to one
/// tenant. No database foreign key is needed. For the other direction, where this entity's member
/// points at the related entity, use <see cref="HeldWithAttribute"/>.
/// </remarks>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
public sealed class HeldWithReferencingAttribute(Type relatedType, string linkMember) : Attribute
{
    public Type RelatedType { get; } =
        relatedType ?? throw new ArgumentNullException(nameof(relatedType));
    public string LinkMember { get; } =
        string.IsNullOrWhiteSpace(linkMember)
            ? throw new ArgumentException("Link member cannot be blank.", nameof(linkMember))
            : linkMember;
}
