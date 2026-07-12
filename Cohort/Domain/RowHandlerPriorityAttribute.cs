namespace Cohort.Domain;

[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class RowHandlerPriorityAttribute(int priority) : Attribute
{
    public int Priority { get; } = priority;
}
