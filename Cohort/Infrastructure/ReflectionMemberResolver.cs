using System.Reflection;

namespace Cohort.Infrastructure;

internal static class ReflectionMemberResolver
{
    private const BindingFlags PublicInstance = BindingFlags.Public | BindingFlags.Instance;

    internal static PropertyInfo? FindPropertyByName(Type clrType, string name)
    {
        for (var cursor = clrType; cursor is not null; cursor = cursor.BaseType)
        {
            var found = cursor
                .GetProperties(PublicInstance | BindingFlags.DeclaredOnly)
                .FirstOrDefault(property =>
                    string.Equals(property.Name, name, StringComparison.Ordinal)
                );
            if (found is not null)
            {
                return found;
            }
        }

        return null;
    }
}
