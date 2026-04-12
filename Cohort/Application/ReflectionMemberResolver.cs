using System.Reflection;

namespace Cohort.Application;

/// <summary>
/// Looks up properties by name in a way that is safe when a derived type shadows a base member with `new`.
/// </summary>
/// <remarks>
/// <see cref="Type.GetProperty(string, BindingFlags)"/> throws <see cref="AmbiguousMatchException"/>
/// when a derived type uses <c>new</c> to shadow an inherited property whose declaring type is generic
/// (e.g. <c>IdentityUser&lt;TKey&gt;.Id</c> shadowed by <c>ApplicationUser.Id</c>). This resolver walks the
/// full set of declared + inherited public instance properties and prefers the most-derived match, which
/// is the selection rule the language uses when you access the member from source.
/// </remarks>
public static class ReflectionMemberResolver
{
    private const BindingFlags PublicInstance = BindingFlags.Public | BindingFlags.Instance;

    public static PropertyInfo? FindPropertyByName(Type clrType, string name)
    {
        ArgumentNullException.ThrowIfNull(clrType);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        // Declared-only first — if the derived type owns the name, that's the winner regardless of
        // whether a base also declares it.
        var declared = clrType
            .GetProperties(PublicInstance | BindingFlags.DeclaredOnly)
            .FirstOrDefault(property => string.Equals(property.Name, name, StringComparison.Ordinal));
        if (declared is not null)
        {
            return declared;
        }

        // Walk up the base hierarchy. Stop at the first type that declares the name.
        for (var cursor = clrType.BaseType; cursor is not null; cursor = cursor.BaseType)
        {
            var inherited = cursor
                .GetProperties(PublicInstance | BindingFlags.DeclaredOnly)
                .FirstOrDefault(property => string.Equals(property.Name, name, StringComparison.Ordinal));
            if (inherited is not null)
            {
                return inherited;
            }
        }

        return null;
    }
}
