using System.Reflection;

namespace Cohort.Domain;

[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class RowHandlerPriorityAttribute(int priority) : Attribute
{
    public const int DefaultPriority = int.MaxValue;

    public int Priority { get; } = priority;

    public static int GetPriority(Type handlerType)
    {
        ArgumentNullException.ThrowIfNull(handlerType);

        return handlerType.GetCustomAttribute<RowHandlerPriorityAttribute>(inherit: false)?.Priority
            ?? DefaultPriority;
    }
}
