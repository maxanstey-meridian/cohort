using System.Reflection;

namespace Cohort.Infrastructure;

internal static class ReflectionMemberResolver
{
    private const BindingFlags PublicInstance = BindingFlags.Public | BindingFlags.Instance;

    internal static PropertyInfo? FindPropertyByName(Type clrType, string name)
    {
        ArgumentNullException.ThrowIfNull(clrType);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var declared = clrType
            .GetProperties(PublicInstance | BindingFlags.DeclaredOnly)
            .FirstOrDefault(property =>
                string.Equals(property.Name, name, StringComparison.Ordinal)
            );
        if (declared is not null)
        {
            return declared;
        }

        for (var cursor = clrType.BaseType; cursor is not null; cursor = cursor.BaseType)
        {
            var inherited = cursor
                .GetProperties(PublicInstance | BindingFlags.DeclaredOnly)
                .FirstOrDefault(property =>
                    string.Equals(property.Name, name, StringComparison.Ordinal)
                );
            if (inherited is not null)
            {
                return inherited;
            }
        }

        return null;
    }
}
