namespace Cohort.Domain;

/// <summary>
/// The permanent identity a row handler's queued work is persisted and dispatched under.
/// Required on every handler registered with <c>AddRowHandler</c>. Never change it: work
/// queued under the old id stops matching the handler. Renaming or moving the class is
/// safe while the id stays.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class RowHandlerIdAttribute(string id) : Attribute
{
    public Guid Id { get; } =
        Guid.TryParse(id, out var parsed) && parsed != Guid.Empty
            ? parsed
            : throw new ArgumentException("Row handler identity must be a non-empty UUID.", nameof(id));
}
