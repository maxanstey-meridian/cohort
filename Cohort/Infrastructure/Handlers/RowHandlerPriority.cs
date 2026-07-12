using System.Reflection;
using Cohort.Domain;

namespace Cohort.Infrastructure;

internal static class RowHandlerPriority
{
    internal const int Default = int.MaxValue;

    internal static int Get(Type handlerType)
    {
        ArgumentNullException.ThrowIfNull(handlerType);

        return handlerType.GetCustomAttribute<RowHandlerPriorityAttribute>(inherit: false)?.Priority
            ?? Default;
    }
}
