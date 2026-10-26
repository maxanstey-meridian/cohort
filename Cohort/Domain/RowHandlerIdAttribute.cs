namespace Cohort.Domain;

/// <summary>
/// The permanent identity a row handler's queued work, or a dependents step's failure, is
/// persisted under. Required on every class registered with <c>AddRowHandler</c> or
/// <c>AddRetentionDependents</c>, and unique across their classes whatever their entity; one
/// class may be both the row handler and the dependents step of the same entity under one id.
/// Never change it: work queued under the old id stops matching the handler. Renaming or
/// moving the class is safe while the id stays.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class RowHandlerIdAttribute(string id) : Attribute
{
    public Guid Id { get; } =
        Guid.TryParse(id, out var parsed) && parsed != Guid.Empty
            ? parsed
            : throw new ArgumentException("Row handler identity must be a non-empty UUID.", nameof(id));
}
